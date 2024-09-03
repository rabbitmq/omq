package stomp_client

import (
	"context"
	"strconv"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/topic"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
)

type StompConsumer struct {
	Id           int
	Connection   *stomp.Conn
	Subscription *stomp.Subscription
	Topic        string
	Config       config.Config
	whichUri     int
}

func NewConsumer(cfg config.Config, id int) *StompConsumer {

	consumer := &StompConsumer{
		Id:           id,
		Connection:   nil,
		Subscription: nil,
		Topic:        topic.CalculateTopic(cfg.ConsumeFrom, id),
		Config:       cfg,
		whichUri:     0,
	}

	if cfg.SpreadConnections {
		consumer.whichUri = (id - 1) % len(cfg.ConsumerUri)
	}

	consumer.Connect()

	return consumer
}

func (c *StompConsumer) Connect() {
	if c.Subscription != nil {
		_ = c.Subscription.Unsubscribe()
	}
	if c.Connection != nil {
		_ = c.Connection.Disconnect()
	}
	c.Subscription = nil
	c.Connection = nil

	for c.Connection == nil {
		if c.whichUri >= len(c.Config.ConsumerUri) {
			c.whichUri = 0
		}
		uri := c.Config.ConsumerUri[c.whichUri]
		c.whichUri++
		parsedUri := utils.ParseURI(uri, "stomp", "61613")

		var o []func(*stomp.Conn) error = []func(*stomp.Conn) error{
			stomp.ConnOpt.Login(parsedUri.Username, parsedUri.Password),
			stomp.ConnOpt.Host("/"), // TODO
		}

		log.Debug("connecting to broker", "id", c.Id, "broker", parsedUri.Broker)
		conn, err := stomp.Dial("tcp", parsedUri.Broker, o...)

		if err != nil {
			log.Error("consumer connection failed", "id", c.Id, "error", err.Error())
			time.Sleep(1 * time.Second)
		} else {
			c.Connection = conn
		}
	}

}

func (c *StompConsumer) Subscribe() {
	var sub *stomp.Subscription
	var err error

	sub, err = c.Connection.Subscribe(c.Topic, stomp.AckClient, buildSubscribeOpts(c.Config)...)
	if err != nil {
		log.Error("subscription failed", "id", c.Id, "queue", c.Topic, "error", err.Error())
		return
	}
	c.Subscription = sub
}

func (c *StompConsumer) Start(ctx context.Context, subscribed chan bool) {
	c.Subscribe()
	close(subscribed)
	log.Info("consumer started", "id", c.Id, "destination", c.Topic)

	previousMessageTimeSent := time.Unix(0, 0)

	for i := 1; i <= c.Config.ConsumeCount; {
		if c.Subscription == nil {
			c.Subscribe()
		}

		select {
		case msg := <-c.Subscription.C:
			if msg.Err != nil {
				log.Error("failed to receive a message", "id", c.Id, "c.Topic", c.Topic, "error", msg.Err)
				c.Connect()
				continue
			}

			timeSent, latency := utils.CalculateEndToEndLatency(&msg.Body)
			metrics.EndToEndLatency.UpdateDuration(timeSent)

			priority, _ := strconv.Atoi(msg.Header.Get("priority"))

			if c.Config.LogOutOfOrder && timeSent.Before(previousMessageTimeSent) {
				metrics.MessagesConsumedOutOfOrderMetric(priority).Inc()
				log.Info("Out of order message received. This message was sent before the previous message", "this messsage", timeSent, "previous message", previousMessageTimeSent)
			}
			previousMessageTimeSent = timeSent

			log.Debug("message received", "id", c.Id, "destination", c.Topic, "size", len(msg.Body), "ack required", msg.ShouldAck(), "priority", priority, "latency", latency)

			if c.Config.ConsumerLatency > 0 {
				log.Debug("consumer latency", "id", c.Id, "latency", c.Config.ConsumerLatency)
				time.Sleep(c.Config.ConsumerLatency)
			}

			err := c.Connection.Ack(msg)
			if err != nil {
				log.Error("message NOT acknowledged", "id", c.Id, "destination", c.Topic)

			} else {
				metrics.MessagesConsumedMetric(priority).Inc()
				i++
				log.Debug("message acknowledged", "id", c.Id, "terminus", c.Topic)
			}
		case <-ctx.Done():
			c.Stop("time limit reached")
			return
		}

	}

	c.Stop("message count reached")
	log.Debug("consumer finished", "id", c.Id)

}

func (c *StompConsumer) Stop(reason string) {
	if c.Subscription != nil {
		err := c.Subscription.Unsubscribe()
		if err != nil {
			log.Info("failed to unsubscribe", "id", c.Id, "error", err.Error())
		}
	}
	if c.Connection != nil {
		err := c.Connection.Disconnect()
		if err != nil {
			log.Info("failed to disconnect", "id", c.Id, "error", err.Error())
		}
	}
	log.Debug("consumer stopped", "id", c.Id)
}

func buildSubscribeOpts(cfg config.Config) []func(*frame.Frame) error {
	var subscribeOpts []func(*frame.Frame) error

	subscribeOpts = append(subscribeOpts,
		stomp.SubscribeOpt.Header("x-stream-offset", cfg.StreamOffset),
		stomp.SubscribeOpt.Header("prefetch-count", strconv.Itoa(cfg.ConsumerCredits)))

	if cfg.ConsumerPriority != 0 {
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-priority", strconv.Itoa(int(cfg.ConsumerPriority))))
	}

	if cfg.QueueDurability != config.None {
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("durable", "true"),
			stomp.SubscribeOpt.Header("auto-delete", "false"),
		)
	}

	if cfg.StreamFilterValues != "" {
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-stream-filter", cfg.StreamFilterValues))
	}
	return subscribeOpts
}
