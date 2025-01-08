package amqp10

import (
	"context"
	"crypto/tls"
	"math/rand"
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
		Terminus:   utils.InjectId(cfg.ConsumeFrom, id),
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
			TCPNoDelay: &c.Config.TCPNoDelay,
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
				"appProps", msg.ApplicationProperties)

			if c.Config.ConsumerLatency > 0 {
				log.Debug("consumer latency", "id", c.Id, "latency", c.Config.ConsumerLatency)
				time.Sleep(c.Config.ConsumerLatency)
			}

			outcome, err := c.outcome(c.ctx, msg)
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

func (c *Amqp10Consumer) outcome(ctx context.Context, msg *amqp.Message) (string, error) {
	// don't generate random numbers if not necessary
	if c.Config.Amqp.ReleaseRate == 0 && c.Config.Amqp.RejectRate == 0 {
		return "accept", c.Receiver.AcceptMessage(ctx, msg)
	}

	n := rand.Intn(100)
	if n < c.Config.Amqp.ReleaseRate {
		return "release", c.Receiver.ReleaseMessage(ctx, msg)
	} else if n < c.Config.Amqp.ReleaseRate+c.Config.Amqp.RejectRate {
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
	return "huh?"
}

func (c *Amqp10Consumer) Stop(reason string) {
	if c.Session != nil {
		_ = c.Session.Close(context.Background())
	}
	if c.Connection != nil {
		_ = c.Connection.Close()
	}
	log.Debug("consumer stopped", "id", c.Id, "reason", reason)
}

func buildLinkProperties(cfg config.Config) map[string]any {
	props := map[string]any{
		"rabbitmq:priority": cfg.ConsumerPriority,
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
	return filters
}
