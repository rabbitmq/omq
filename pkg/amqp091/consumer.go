package amqp091

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/rabbitmq/omq/pkg/metrics"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

type Amqp091Consumer struct {
	Id         int
	Connection *amqp091.Connection
	Channel    *amqp091.Channel
	Terminus   string
	Messages   <-chan amqp091.Delivery
	Config     config.Config
	whichUri   int
	ctx        context.Context
}

func NewConsumer(ctx context.Context, cfg config.Config, id int) *Amqp091Consumer {
	consumer := &Amqp091Consumer{
		Id:         id,
		Connection: nil,
		Channel:    nil,
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

func (c *Amqp091Consumer) Connect() {
	if c.Channel != nil {
		_ = c.Channel.Close()
	}
	if c.Connection != nil {
		_ = c.Connection.Close()
	}
	c.Channel = nil
	c.Connection = nil

	for c.Connection == nil {
		uri := utils.NextURI(c.Config.ConsumerUri, &c.whichUri)
		dialCfg := amqp091.Config{
			Properties: amqp091.Table{
				"connection_name": fmt.Sprintf("omq-consumer-%d", c.Id),
			},
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: c.Config.InsecureSkipTLSVerify,
			},
			Dial: amqp091.DefaultDial(10 * time.Second),
		}
		conn, err := amqp091.DialConfig(uri, dialCfg)
		if err != nil {
			log.Error("consumer connection failed", "id", c.Id, "error", err.Error())
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(config.ReconnectDelay):
				continue
			}
		} else {
			log.Debug("consumer connected", "id", c.Id, "uri", uri)
			c.Connection = conn
		}
	}

	for c.Channel == nil {
		channel, err := c.Connection.Channel()
		if err != nil {
			if err == context.Canceled {
				return
			} else {
				log.Error("consumer failed to create a channel", "id", c.Id, "error", err.Error())
				time.Sleep(config.ReconnectDelay)
			}
		} else {
			c.Channel = channel
		}
	}
}

func (c *Amqp091Consumer) Subscribe() {
	if c.Connection != nil {
		_ = c.Channel.Qos(c.Config.ConsumerCredits, 0, false)

		if c.Config.Queues == config.Exclusive {
			queueName := strings.TrimPrefix(c.Terminus, "/queues/")
			_, err := c.Channel.QueueDeclare(
				queueName, // name
				false,     // durable
				false,     // autoDelete
				true,      // exclusive
				false,     // noWait
				nil,       // args
			)
			if err != nil {
				log.Error("failed to declare exclusive queue", "id", c.Id, "queue", queueName, "error", err.Error())
				return
			}

			// Bind the exclusive queue to the configured exchange
			if c.Config.Exchange != "" {
				err = c.Channel.QueueBind(
					queueName,           // queue name
					c.Config.BindingKey, // routing key
					c.Config.Exchange,   // exchange
					false,               // noWait
					nil,                 // args
				)
				if err != nil {
					log.Error("failed to bind exclusive queue to exchange", "id", c.Id, "queue", queueName, "exchange", c.Config.Exchange, "binding_key", c.Config.BindingKey, "error", err.Error())
					return
				}
				log.Debug("exclusive queue bound to exchange", "id", c.Id, "queue", queueName, "exchange", c.Config.Exchange, "binding_key", c.Config.BindingKey)
			}
		}

		// TODO add auto-ack
		consumeArgs := amqp091.Table{}
		if c.Config.StreamOffset != "" {
			consumeArgs["x-stream-offset"] = c.Config.StreamOffset
		}
		if c.Config.ConsumerPriorityTemplate != nil {
			priorityStr := utils.ExecuteTemplate(c.Config.ConsumerPriorityTemplate, c.Id)
			if priority, err := strconv.Atoi(priorityStr); err == nil {
				consumeArgs["x-priority"] = priority
			} else {
				log.Error("failed to parse template-generated consumer priority", "value", priorityStr, "error", err)
				os.Exit(1)
			}
		}
		sub, err := c.Channel.Consume(strings.TrimPrefix(c.Terminus, "/queues/"), "", false, false, false, false, consumeArgs)
		if err != nil {
			log.Error("subscription failed", "id", c.Id, "queue", c.Terminus, "error", err.Error())
			return
		}
		c.Messages = sub
	}
}

func (c *Amqp091Consumer) Start(consumerReady chan bool) {
	c.Subscribe()
	close(consumerReady)
	log.Info("consumer started", "id", c.Id, "terminus", c.Terminus)
	var oooTracker *utils.OutOfOrderTracker
	if c.Config.DetectOutOfOrder || c.Config.DetectGaps {
		oooTracker = utils.NewOutOfOrderTracker()
	}

	for i := 1; i <= c.Config.ConsumeCount; {
		for c.Messages == nil {
			select {
			case <-c.ctx.Done():
				c.Stop("context cancelled")
				return
			default:
				c.Subscribe()
			}
		}

		select {
		case <-c.ctx.Done():
			c.Stop("context cancelled")
			return
		case msg, ok := <-c.Messages:
			if !ok {
				log.Info("channel closed, reconnecting", "id", c.Id)
				c.Connect()
				c.Messages = nil
				continue
			}

			payload := msg.Body
			priority := int(msg.Priority)
			timeSent, latency := utils.CalculateEndToEndLatency(&payload)
			metrics.RecordEndToEndLatency(latency)

			if len(msg.Headers) > 0 {
				var delayAccuracy time.Duration
				var hasDelay bool

				if val, ok := headerToInt64(msg.Headers, "x-delay-processed"); ok {
					delayAccuracy, hasDelay = utils.CalculateDelayAccuracy(&payload, val)
				} else if val, ok := headerToInt64(msg.Headers, "x-opt-delivery-time"); ok {
					delayAccuracy, hasDelay = utils.CalculateDelayAccuracyFromDeliveryTime(val)
				}

				if hasDelay {
					metrics.RecordDelayAccuracy(delayAccuracy)
				}
			}

			if oooTracker != nil && len(msg.Headers) > 0 {
				if pubID, seq, ok := extractOrderingInfoFromHeaders(msg.Headers); ok {
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

			headerInfo := ""
			if len(msg.Headers) > 0 {
				headerPairs := make([]string, 0, len(msg.Headers))
				for key, value := range msg.Headers {
					headerPairs = append(headerPairs, fmt.Sprintf("%s:%v", key, value))
				}
				headerInfo = fmt.Sprintf(" headers=[%s]", strings.Join(headerPairs, ","))
			}

			log.Debug("message received",
				"id", c.Id,
				"terminus", c.Terminus,
				"size", len(payload),
				"priority", priority,
				"latency", latency,
				"headers", headerInfo)

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

			outcome, err := c.outcome(msg.DeliveryTag, priority)
			if err != nil {
				if err == context.Canceled {
					c.Stop("context canceled")
					return
				}
				if err == amqp091.ErrClosed {
					log.Info("channel closed during acknowledgment, reconnecting", "id", c.Id)
					c.Connect()
					c.Messages = nil
					continue
				}
				log.Error("failed to "+outcome+" message", "id", c.Id, "terminus", c.Terminus, "error", err)
				// Don't increment counter on error, but continue to avoid infinite loop
				continue
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

func (c *Amqp091Consumer) outcome(tag uint64, priority int) (string, error) {
	requeuePriorityMatch := len(c.Config.RequeueWhenPriority) == 0 || slices.Contains(c.Config.RequeueWhenPriority, priority)
	discardPriorityMatch := len(c.Config.DiscardWhenPriority) == 0 || slices.Contains(c.Config.DiscardWhenPriority, priority)

	// don't generate random numbers if not necessary
	if c.Config.RequeueRate == 0 && c.Config.DiscardRate == 0 {
		// No rate-based logic, just check priority filters for 100% requeue/discard
		if len(c.Config.RequeueWhenPriority) > 0 && slices.Contains(c.Config.RequeueWhenPriority, priority) {
			return "nack-requeue", c.Channel.Nack(tag, false, true)
		}
		if len(c.Config.DiscardWhenPriority) > 0 && slices.Contains(c.Config.DiscardWhenPriority, priority) {
			return "nack-discard", c.Channel.Nack(tag, false, false)
		}
		return "acknowledge", c.Channel.Ack(tag, false)
	}

	n := rand.IntN(100)
	if requeuePriorityMatch && n < c.Config.RequeueRate {
		return "nack-requeue", c.Channel.Nack(tag, false, true)
	} else if discardPriorityMatch && n < c.Config.RequeueRate+c.Config.DiscardRate {
		return "nack-discard", c.Channel.Nack(tag, false, false)
	}
	return "acknowledge", c.Channel.Ack(tag, false)
}

func extractOrderingInfoFromHeaders(headers amqp091.Table) (int, uint64, bool) {
	pubIDVal, hasPubID := headers[utils.HeaderPublisherID]
	seqVal, hasSeq := headers[utils.HeaderSequence]
	if !hasPubID || !hasSeq {
		return 0, 0, false
	}
	pubID, okP := amqpTableValToInt(pubIDVal)
	seq, okS := amqpTableValToUint64(seqVal)
	return pubID, seq, okP && okS
}

func headerToInt64(headers amqp091.Table, key string) (int64, bool) {
	v, exists := headers[key]
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
	log.Debug("could not parse header", "key", key, "value", v, "type", fmt.Sprintf("%T", v))
	return 0, false
}

func amqpTableValToInt(v any) (int, bool) {
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

func amqpTableValToUint64(v any) (uint64, bool) {
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

func (c *Amqp091Consumer) Stop(reason string) {
	if c.Channel != nil {
		_ = c.Channel.Close()
	}
	if c.Connection != nil {
		_ = c.Connection.Close()
	}
	log.Debug("consumer stopped", "id", c.Id, "reason", reason)
}
