package amqp091

import (
	"context"
	"fmt"
	"os"
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
		if c.whichUri >= len(c.Config.ConsumerUri) {
			c.whichUri = 0
		}
		uri := c.Config.ConsumerUri[c.whichUri]
		c.whichUri++
		config := amqp091.Config{
			Properties: amqp091.Table{
				"connection_name": fmt.Sprintf("omq-consumer-%d", c.Id),
			},
		}
		conn, err := amqp091.DialConfig(uri, config)
		if err != nil {
			log.Error("consumer connection failed", "id", c.Id, "error", err.Error())
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(1 * time.Second):
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
				time.Sleep(1 * time.Second)
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
		}

		// TODO add auto-ack and exclusive options
		consumeArgs := amqp091.Table{}
		if c.Config.StreamOffset != "" {
			consumeArgs["x-stream-offset"] = c.Config.StreamOffset
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
	previousMessageTimeSent := time.Unix(0, 0)

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
		case msg := <-c.Messages:
			payload := msg.Body
			priority := int(msg.Priority)
			timeSent, latency := utils.CalculateEndToEndLatency(&payload)
			metrics.EndToEndLatency.UpdateDuration(timeSent)

			if c.Config.LogOutOfOrder && timeSent.Before(previousMessageTimeSent) {
				metrics.MessagesConsumedOutOfOrderMetric(priority).Inc()
				log.Info("out of order message received. This message was sent before the previous message",
					"this messsage", timeSent,
					"previous message", previousMessageTimeSent)
			}
			previousMessageTimeSent = timeSent

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

			outcome, err := c.outcome(msg.DeliveryTag)
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

func (c *Amqp091Consumer) outcome(tag uint64) (string, error) {
	// don't generate random numbers if not necessary
	if c.Config.Amqp.ReleaseRate == 0 && c.Config.Amqp.RejectRate == 0 {
		return "acknowledge", c.Channel.Ack(tag, false)
	}
	// TODO implement NACKing
	log.Error("AMQP 0.9.1 doesn't support release/reject rates yet")
	os.Exit(1)
	return "", nil
}

func pastTense(outcome string) string {
	switch outcome {
	case "accept":
		return "accepted"
	case "release":
		return "released"
	case "reject":
		return "rejected"
	case "acknowledge":
		return "acknowledged"
	}
	return outcome
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
