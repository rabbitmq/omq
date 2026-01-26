package amqp10

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/rabbitmq/omq/pkg/metrics"

	amqp "github.com/Azure/go-amqp"
)

type Amqp10Consumer struct {
	Id         int
	Connection *amqp.Conn
	Session    *amqp.Session
	Receiver   *amqp.Receiver
	Terminus   string
	Config     config.Config
	whichUri   int
	ctx        context.Context
}

func NewConsumer(ctx context.Context, cfg config.Config, id int) *Amqp10Consumer {
	consumer := &Amqp10Consumer{
		Id:         id,
		Connection: nil,
		Session:    nil,
		Receiver:   nil,
		Terminus:   utils.ResolveTerminus(cfg.ConsumeFrom, cfg.ConsumeFromTemplate, id, cfg),
		Config:     cfg,
		whichUri:   0,
		ctx:        ctx,
	}

	if cfg.SpreadConnections {
		consumer.whichUri = (id - 1) % len(cfg.ConsumerUri)
	}

	consumer.Connect()

	return consumer
}

func (c *Amqp10Consumer) Connect() {
	if c.Receiver != nil {
		_ = c.Receiver.Close(c.ctx)
	}
	if c.Session != nil {
		_ = c.Session.Close(context.Background())
	}
	if c.Connection != nil {
		_ = c.Connection.Close()
	}
	c.Receiver = nil
	c.Session = nil
	c.Connection = nil

	for c.Connection == nil {
		if c.whichUri >= len(c.Config.ConsumerUri) {
			c.whichUri = 0
		}
		uri := c.Config.ConsumerUri[c.whichUri]
		c.whichUri++
		hostname, vhost := hostAndVHost(uri)
		conn, err := amqp.Dial(c.ctx, uri, &amqp.ConnOptions{
			ContainerID: utils.InjectId(c.Config.ConsumerId, c.Id),
			SASLType:    amqp.SASLTypeAnonymous(),
			HostName:    vhost,
			TLSConfig: &tls.Config{
				ServerName: hostname,
			},
		})
		if err != nil {
			select {
			case <-c.ctx.Done():
				return
			default:
				log.Error("consumer failed to connect", "id", c.Id, "error", err.Error())
				time.Sleep(1 * time.Second)
			}
		} else {
			log.Debug("consumer connected", "id", c.Id, "uri", uri)
			c.Connection = conn
		}
	}

	for c.Session == nil {
		session, err := c.Connection.NewSession(c.ctx, nil)
		if err != nil {
			if err == context.Canceled {
				return
			} else {
				log.Error("consumer failed to create a session", "id", c.Id, "error", err.Error())
				time.Sleep(1 * time.Second)
			}
		} else {
			c.Session = session
		}
	}
}

func (c *Amqp10Consumer) CreateReceiver(ctx context.Context) {
	var durability amqp.Durability
	switch c.Config.QueueDurability {
	case config.None:
		durability = amqp.DurabilityNone
	case config.Configuration:
		durability = amqp.DurabilityConfiguration
	case config.UnsettledState:
		durability = amqp.DurabilityUnsettledState
	}

	for c.Receiver == nil && c.Session != nil {
		select {
		case <-ctx.Done():
			return
		default:
			receiver, err := c.Session.NewReceiver(context.TODO(),
				c.Terminus,
				&amqp.ReceiverOptions{
					SourceDurability: durability,
					Credit:           int32(c.Config.ConsumerCredits),
					Properties:       buildLinkProperties(c.Config),
					Filters:          buildLinkFilters(c.Config),
				})
			if err != nil {
				if err == context.Canceled {
					return
				}
				log.Error("consumer failed to create a receiver", "id", c.Id, "error", err.Error())
				time.Sleep(1 * time.Second)
			} else {
				c.Receiver = receiver
			}
		}
	}
}

func (c *Amqp10Consumer) Start(consumerReady chan bool) {
	c.CreateReceiver(c.ctx)
	close(consumerReady)
	log.Info("consumer started", "id", c.Id, "terminus", c.Terminus)
	previousMessageTimeSent := time.Unix(0, 0)

	for i := 1; i <= c.Config.ConsumeCount; {
		if c.Receiver == nil {
			c.CreateReceiver(c.ctx)
			log.Debug("consumer subscribed", "id", c.Id, "terminus", c.Terminus)
		}

		select {
		case <-c.ctx.Done():
			c.Stop("time limit reached")
			return
		default:
			msg, err := c.Receiver.Receive(c.ctx, nil)
			if err != nil {
				if err == context.Canceled {
					c.Stop("context canceled")
					return
				}
				log.Error("failed to receive a message", "id", c.Id, "terminus", c.Terminus, "error", err.Error())
				c.Connect()
				continue
			}

			payload := msg.GetData()
			priority := int(msg.Header.Priority)
			timeSent, latency := utils.CalculateEndToEndLatency(&payload)
			metrics.EndToEndLatency.UpdateDuration(timeSent)

			// Check for delayed message accuracy
			if msg.Annotations != nil {
				if xDelayValue, exists := msg.Annotations["x-delay-processed"]; exists {
					var delayMs int64
					var parsed bool

					if val, ok := xDelayValue.(int64); ok {
						delayMs = val
						parsed = true
					} else if val, ok := xDelayValue.(int32); ok {
						delayMs = int64(val)
						parsed = true
					} else if val, ok := xDelayValue.(int); ok {
						delayMs = int64(val)
						parsed = true
					} else if val, ok := xDelayValue.(string); ok {
						if parsedVal, err := strconv.ParseInt(val, 10, 64); err == nil {
							delayMs = parsedVal
							parsed = true
						}
					}

					if parsed {
						delayAccuracy, isDelayed := utils.CalculateDelayAccuracy(&payload, delayMs)
						if isDelayed {
							metrics.DelayAccuracy.Update(delayAccuracy.Seconds())
							if delayAccuracy < 0 {
								metrics.MessagesDeliveredTooEarly.Inc()
							}
						}
					} else {
						log.Debug("could not parse x-delay annotation", "value", xDelayValue, "type", fmt.Sprintf("%T", xDelayValue))
					}
				}
			}

			if c.Config.LogOutOfOrder && timeSent.Before(previousMessageTimeSent) {
				metrics.MessagesConsumedOutOfOrderMetric(priority).Inc()
				log.Info("out of order message received. This message was sent before the previous message",
					"this messsage", timeSent,
					"previous message", previousMessageTimeSent)
			}
			previousMessageTimeSent = timeSent

			log.Debug("message received",
				"id", c.Id,
				"terminus", c.Terminus,
				"size", len(payload),
				"priority", priority,
				"latency", latency,
				"appProps", msg.ApplicationProperties,
				"annotations", msg.Annotations)

			// Handle consumer latency (always use template)
			var consumerLatency time.Duration
			if c.Config.ConsumerLatencyTemplate != nil {
				latencyStr := utils.ExecuteTemplate(c.Config.ConsumerLatencyTemplate, c.Id)
				if parsedLatency, err := time.ParseDuration(latencyStr); err == nil {
					consumerLatency = parsedLatency
				} else {
					log.Error("failed to parse template-generated latency", "value", latencyStr, "error", err)
					os.Exit(1)
				}
			}

			if consumerLatency > 0 {
				log.Debug("consumer latency", "id", c.Id, "latency", consumerLatency)
				time.Sleep(consumerLatency)
			}

			outcome, err := c.outcome(c.ctx, msg, priority)
			if err != nil {
				if err == context.Canceled {
					c.Stop("context canceled")
					return
				}
				log.Error("failed to "+outcome+" message", "id", c.Id, "terminus", c.Terminus, "error", err)
			} else {
				metrics.MessagesConsumedMetric(priority).Inc()
				i++
				log.Debug("message "+pastTense(outcome), "id", c.Id, "terminus", c.Terminus)
			}
		}
	}

	c.Stop("--cmessages value reached")
	log.Debug("consumer finished", "id", c.Id)
}

func (c *Amqp10Consumer) outcome(ctx context.Context, msg *amqp.Message, priority int) (string, error) {
	requeuePriorityMatch := len(c.Config.RequeueWhenPriority) == 0 || slices.Contains(c.Config.RequeueWhenPriority, priority)
	discardPriorityMatch := len(c.Config.DiscardWhenPriority) == 0 || slices.Contains(c.Config.DiscardWhenPriority, priority)

	// don't generate random numbers if not necessary
	if c.Config.RequeueRate == 0 && c.Config.DiscardRate == 0 {
		// No rate-based logic, just check priority filters for 100% requeue/discard
		if len(c.Config.RequeueWhenPriority) > 0 && slices.Contains(c.Config.RequeueWhenPriority, priority) {
			return "release", c.Receiver.ReleaseMessage(ctx, msg)
		}
		if len(c.Config.DiscardWhenPriority) > 0 && slices.Contains(c.Config.DiscardWhenPriority, priority) {
			return "reject", c.Receiver.RejectMessage(ctx, msg, nil)
		}
		return "accept", c.Receiver.AcceptMessage(ctx, msg)
	}

	n := rand.IntN(100)
	if requeuePriorityMatch && n < c.Config.RequeueRate {
		return "release", c.Receiver.ReleaseMessage(ctx, msg)
	} else if discardPriorityMatch && n < c.Config.RequeueRate+c.Config.DiscardRate {
		return "reject", c.Receiver.RejectMessage(ctx, msg, nil)
	}
	return "accept", c.Receiver.AcceptMessage(ctx, msg)
}

func pastTense(outcome string) string {
	switch outcome {
	case "accept":
		return "accepted"
	case "release":
		return "released"
	case "reject":
		return "rejected"
	}
	return outcome
}

func (c *Amqp10Consumer) Stop(reason string) {
	if c.Receiver != nil {
		_ = c.Receiver.Close(context.Background())
	}
	if c.Session != nil {
		_ = c.Session.Close(context.Background())
	}
	if c.Connection != nil {
		_ = c.Connection.Close()
	}
	log.Debug("consumer stopped", "id", c.Id, "reason", reason)
}

func buildLinkProperties(cfg config.Config) map[string]any {
	props := map[string]any{}

	if cfg.ConsumerPriority != 0 {
		props["rabbitmq:priority"] = cfg.ConsumerPriority
	}

	return props
}

func buildLinkFilters(cfg config.Config) []amqp.LinkFilter {
	var filters []amqp.LinkFilter

	if cfg.StreamOffset != "" {
		filters = append(filters, amqp.NewLinkFilter("rabbitmq:stream-offset-spec", 0, cfg.StreamOffset))
	}

	if cfg.StreamFilterValues != "" {
		filters = append(filters, amqp.NewLinkFilter("rabbitmq:stream-filter", 0, cfg.StreamFilterValues))
	}

	for appProperty, filterExpression := range cfg.Amqp.AppPropertyFilters {
		filters = append(filters, amqp.NewLinkFilter("amqp:application-properties-filter",
			0,
			map[string]any{
				appProperty: filterExpression,
			}))
	}

	for property, filterExpression := range cfg.Amqp.PropertyFilters {
		filters = append(filters,
			amqp.NewLinkFilter("amqp:properties-filter",
				0,
				map[amqp.Symbol]any{
					amqp.Symbol(property): filterExpression,
				}))
	}
	if cfg.Amqp.SQLFilter != "" {
		filters = append(filters, amqp.NewLinkFilter("sql-filter", 0x120,
			cfg.Amqp.SQLFilter))
	}
	return filters
}
