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
		Terminus:   utils.ResolveTerminus(cfg.ConsumeFromTemplate, id),
		Config:     cfg,
		whichUri:   0,
		ctx:        ctx,
	}

	if cfg.SpreadConnections {
		consumer.whichUri = id % len(cfg.ConsumerUri)
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
		uri := utils.NextURI(c.Config.ConsumerUri, &c.whichUri)
		hostname, vhost := hostAndVHost(uri)
		conn, err := amqp.Dial(c.ctx, uri, &amqp.ConnOptions{
			ContainerID:     utils.InjectId(c.Config.ConsumerId, c.Id),
			SASLType:        amqp.SASLTypeAnonymous(),
			HostName:        vhost,
			WriteQueueDepth: 10,
			TLSConfig: &tls.Config{
				ServerName:         hostname,
				InsecureSkipVerify: c.Config.InsecureSkipTLSVerify,
			},
		})
		if err != nil {
			log.Error("consumer failed to connect", "id", c.Id, "error", err.Error())
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(config.ReconnectDelay):
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
			}
			log.Error("consumer failed to create a session", "id", c.Id, "error", err.Error())
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(config.ReconnectDelay):
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
			linkProperties := buildLinkProperties(c.Config, c.Id)
			receiverOpts := &amqp.ReceiverOptions{
				SourceDurability:          durability,
				Credit:                    int32(c.Config.ConsumerCredits),
				Properties:                linkProperties,
				Filters:                   buildLinkFilters(c.Config),
				RequestedSenderSettleMode: requestedSenderSettleMode(c.Config),
				OnLinkStateProperties: func(props map[string]any) {
					if active, ok := props["rabbitmq:active"]; ok {
						if activeBool, ok := active.(bool); ok {
							logArgs := []any{"id", c.Id}
							if priority, ok := linkProperties["rabbitmq:priority"]; ok {
								logArgs = append(logArgs, "priority", priority)
							}
							if activeBool {
								log.Info("consumer is active", logArgs...)
							} else {
								log.Info("consumer is not active", logArgs...)
							}
						}
					}
				},
			}
			if c.Config.Amqp.Browse {
				receiverOpts.SourceDistributionMode = "copy"
			}
			receiver, err := c.Session.NewReceiver(ctx, c.Terminus, receiverOpts)
			if err != nil {
				if err == context.Canceled {
					return
				}
				log.Error("consumer failed to create a receiver", "id", c.Id, "error", err.Error())
				select {
				case <-ctx.Done():
					return
				case <-time.After(config.ReconnectDelay):
				}
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
	var oooTracker *utils.OutOfOrderTracker
	if c.Config.DetectOutOfOrder || c.Config.DetectGaps {
		oooTracker = utils.NewOutOfOrderTracker()
	}

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
			metrics.RecordEndToEndLatency(latency)

			// Check for delayed message accuracy
			if msg.Annotations != nil {
				var delayAccuracy time.Duration
				var hasDelay bool

				if val, ok := annotationToInt64(msg.Annotations, "x-delay-processed"); ok {
					delayAccuracy, hasDelay = utils.CalculateDelayAccuracy(&payload, val)
				} else if val, ok := annotationToInt64(msg.Annotations, "x-opt-delivery-time"); ok {
					delayAccuracy, hasDelay = utils.CalculateDelayAccuracyFromDeliveryTime(val)
				}

				if hasDelay {
					metrics.RecordDelayAccuracy(delayAccuracy)
				}
			}

			if oooTracker != nil && msg.Annotations != nil {
				if pubID, seq, ok := extractOrderingInfo(msg.Annotations); ok {
					result := oooTracker.Check(pubID, seq)
					switch result.Status {
					case utils.SequenceOutOfOrder:
						if c.Config.DetectOutOfOrder {
							metrics.MessagesConsumedOutOfOrderMetric(priority).Inc()
							log.Info("out-of-order message",
								"publisher", pubID, "seq", seq, "lastSeq", result.LastSeq, "timeSent", timeSent)
						}
					case utils.SequenceGap:
						if c.Config.DetectGaps {
							metrics.MessagesConsumedGapsMetric(priority).Inc()
							log.Info("gap in sequence (missing messages)",
								"publisher", pubID, "seq", seq, "lastSeq", result.LastSeq,
								"missed", seq-result.LastSeq-1, "timeSent", timeSent)
						}
					}
				}
			}

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
				log.Debug("message "+utils.PastTense(outcome), "id", c.Id, "terminus", c.Terminus)
			}
		}
	}

	c.Stop("--cmessages value reached")
	log.Debug("consumer finished", "id", c.Id)
}

func (c *Amqp10Consumer) modifyMessage(ctx context.Context, msg *amqp.Message) (string, error) {
	opts := &amqp.ModifyMessageOptions{}
	modifyCfg := c.Config.Amqp.ModifyOptions

	if modifyCfg.DeliveryFailed != nil {
		opts.DeliveryFailed = utils.ExecuteTemplate(modifyCfg.DeliveryFailed, c.Id) == "true"
	}
	if modifyCfg.UndeliverableHere != nil {
		opts.UndeliverableHere = utils.ExecuteTemplate(modifyCfg.UndeliverableHere, c.Id) == "true"
	}
	if len(modifyCfg.AnnotationTemplates) > 0 {
		opts.Annotations = make(amqp.Annotations, len(modifyCfg.AnnotationTemplates))
		for _, at := range modifyCfg.AnnotationTemplates {
			key := utils.ExecuteTemplate(at.Key, c.Id)
			opts.Annotations[key] = maybeConvertNumeric(utils.ExecuteTemplate(at.Value, c.Id))
		}
	}

	return "modify", c.Receiver.ModifyMessage(ctx, msg, opts)
}

func maybeConvertNumeric(value string) any {
	if value == "true" {
		return true
	}
	if value == "false" {
		return false
	}
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f
	}
	return value
}

func (c *Amqp10Consumer) outcome(ctx context.Context, msg *amqp.Message, priority int) (string, error) {
	requeuePriorityMatch := len(c.Config.RequeueWhenPriority) == 0 || slices.Contains(c.Config.RequeueWhenPriority, priority)
	discardPriorityMatch := len(c.Config.DiscardWhenPriority) == 0 || slices.Contains(c.Config.DiscardWhenPriority, priority)

	if c.Config.RequeueRate == 0 && c.Config.DiscardRate == 0 && c.Config.Amqp.ModifyRate == 0 {
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
	} else if n < c.Config.RequeueRate+c.Config.DiscardRate+c.Config.Amqp.ModifyRate {
		return c.modifyMessage(ctx, msg)
	}
	return "accept", c.Receiver.AcceptMessage(ctx, msg)
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

func requestedSenderSettleMode(cfg config.Config) *amqp.SenderSettleMode {
	if cfg.Amqp.ConsumeSettled {
		return amqp.SenderSettleModeSettled.Ptr()
	}
	return nil
}

func buildLinkProperties(cfg config.Config, id int) map[string]any {
	props := map[string]any{}

	if cfg.ConsumerPriorityTemplate != nil {
		priorityStr := utils.ExecuteTemplate(cfg.ConsumerPriorityTemplate, id)
		if priority, err := strconv.ParseInt(priorityStr, 10, 32); err == nil {
			props["rabbitmq:priority"] = int32(priority)
		} else {
			log.Error("failed to parse template-generated consumer priority", "value", priorityStr, "error", err)
			os.Exit(1)
		}
	}

	return props
}

func extractOrderingInfo(annotations amqp.Annotations) (int, uint64, bool) {
	pubIDVal, hasPubID := annotations[utils.HeaderPublisherID]
	seqVal, hasSeq := annotations[utils.HeaderSequence]
	if !hasPubID || !hasSeq {
		return 0, 0, false
	}
	pubID, okP := toInt(pubIDVal)
	seq, okS := toUint64(seqVal)
	return pubID, seq, okP && okS
}

func annotationToInt64(annotations amqp.Annotations, key string) (int64, bool) {
	v, exists := annotations[key]
	if !exists {
		return 0, false
	}
	switch val := v.(type) {
	case int64:
		return val, true
	case int32:
		return int64(val), true
	case int:
		return int64(val), true
	case string:
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			return parsed, true
		}
	}
	log.Debug("could not parse annotation", "key", key, "value", v, "type", fmt.Sprintf("%T", v))
	return 0, false
}

func toInt(v any) (int, bool) {
	switch val := v.(type) {
	case int64:
		return int(val), true
	case int32:
		return int(val), true
	case int:
		return val, true
	}
	return 0, false
}

func toUint64(v any) (uint64, bool) {
	switch val := v.(type) {
	case int64:
		return uint64(val), true
	case int32:
		return uint64(val), true
	case int:
		return uint64(val), true
	case uint64:
		return val, true
	}
	return 0, false
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
	if cfg.Amqp.JMSSelectorFilter != "" {
		filters = append(filters, amqp.NewLinkFilter("jms-selector", 0x0000468C00000004,
			cfg.Amqp.JMSSelectorFilter))
	}
	return filters
}
