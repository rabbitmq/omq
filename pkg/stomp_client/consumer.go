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
	"github.com/prometheus/client_golang/prometheus"
)

// these are the default options that work with RabbitMQ
var o []func(*stomp.Conn) error = []func(*stomp.Conn) error{
	stomp.ConnOpt.Login("guest", "guest"),
	stomp.ConnOpt.Host("/"),
}

type StompConsumer struct {
	Id           int
	Connection   *stomp.Conn
	Subscription *stomp.Subscription
	Topic        string
	Config       config.Config
}

func NewConsumer(cfg config.Config, id int) *StompConsumer {
	conn, err := stomp.Dial("tcp", cfg.ConsumerUri, o...)

	if err != nil {
		log.Error("consumer connection failed", "protocol", "STOMP", "consumerId", id, "error", err.Error())
		return nil
	}

	topic := topic.CalculateTopic(cfg.ConsumeFrom, id)

	return &StompConsumer{
		Id:         id,
		Connection: conn,
		Topic:      topic,
		Config:     cfg,
	}
}

func (c StompConsumer) Start(ctx context.Context, subscribed chan bool) {
	var sub *stomp.Subscription
	var err error

	log.Info("subscribing to durable queue", "protocol", "STOMP", "consumerId", c.Id, "queue", c.Topic, "offset", c.Config.StreamOffset, "credits", c.Config.ConsumerCredits)
	sub, err = c.Connection.Subscribe(c.Topic, stomp.AckClient, buildSubscribeOpts(c.Config)...)
	if err != nil {
		log.Error("subscription failed", "protocol", "STOMP", "consumerId", c.Id, "queue", c.Topic, "error", err.Error())
		return
	}
	c.Subscription = sub
	close(subscribed)

	m := metrics.EndToEndLatency.With(prometheus.Labels{"protocol": "stomp"})

	log.Info("consumer started", "protocol", "STOMP", "consumerId", c.Id, "destination", c.Topic)
	previousMessageTimeSent := time.Unix(0, 0)

	for i := 1; i <= c.Config.ConsumeCount; i++ {
		select {
		case msg := <-sub.C:
			if msg.Err != nil {
				log.Error("failed to receive a message", "protocol", "STOMP", "consumerId", c.Id, "c.Topic", c.Topic, "error", msg.Err)
				return
			}

			timeSent, latency := utils.CalculateEndToEndLatency(&msg.Body)
			m.Observe(latency.Seconds())

			priority := msg.Header.Get("priority")

			if timeSent.Before(previousMessageTimeSent) {
				metrics.MessagesConsumedOutOfOrder.With(prometheus.Labels{"protocol": "amqp-1.0", "priority": priority}).Inc()
				log.Info("Out of order message received. This message was sent before the previous message", "this messsage", timeSent, "previous message", previousMessageTimeSent)
			}
			previousMessageTimeSent = timeSent

			log.Debug("message received", "protocol", "stomp", "consumerId", c.Id, "destination", c.Topic, "size", len(msg.Body), "ack required", msg.ShouldAck(), "priority", priority, "latency", latency)

			if c.Config.ConsumerLatency > 0 {
				log.Debug("consumer latency", "protocol", "stomp", "consumerId", c.Id, "latency", c.Config.ConsumerLatency)
				time.Sleep(c.Config.ConsumerLatency)
			}

			err = c.Connection.Ack(msg)
			if err != nil {
				log.Error("message NOT acknowledged", "protocol", "stomp", "consumerId", c.Id, "destination", c.Topic)

			}
			metrics.MessagesConsumed.With(prometheus.Labels{"protocol": "stomp", "priority": priority}).Inc()
		case <-ctx.Done():
			c.Stop("time limit reached")
			return
		}

	}

	c.Stop("message count reached")
	log.Debug("consumer finished", "protocol", "STOMP", "consumerId", c.Id)

}

func (c StompConsumer) Stop(reason string) {
	log.Debug("closing connection", "protocol", "stomp", "consumerId", c.Id, "reason", reason)
	_ = c.Subscription.Unsubscribe()
	_ = c.Connection.Disconnect()
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
	log.Info("subscribe options", "filter", cfg.StreamFilterValues)
	return subscribeOpts
}
