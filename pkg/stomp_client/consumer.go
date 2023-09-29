package stomp_client

import (
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/topic"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/go-stomp/stomp/v3"
	"github.com/prometheus/client_golang/prometheus"
)

// these are the default options that work with RabbitMQ
var o []func(*stomp.Conn) error = []func(*stomp.Conn) error{
	stomp.ConnOpt.Login("guest", "guest"),
	stomp.ConnOpt.Host("/"),
}

type StompConsumer struct {
	Id         int
	Connection *stomp.Conn
	Topic      string
	Config     config.Config
}

func NewConsumer(cfg config.Config, id int) *StompConsumer {
	conn, err := stomp.Dial("tcp", cfg.ConsumerUri, o...)

	if err != nil {
		log.Error("consumer connection failed", "protocol", "STOMP", "consumerId", id, "error", err.Error())
		return nil
	}

	topic := topic.CalculateTopic(cfg, id)

	return &StompConsumer{
		Id:         id,
		Connection: conn,
		Topic:      topic,
		Config:     cfg,
	}
}

func (c StompConsumer) Start(subscribed chan bool) {
	sub, err := c.Connection.Subscribe(c.Topic, stomp.AckClient)
	if err != nil {
		log.Error("subscription failed", "protocol", "STOMP", "consumerId", c.Id, "queue", c.Topic, "error", err.Error())
		return
	}
	close(subscribed)

	m := metrics.EndToEndLatency.With(prometheus.Labels{"protocol": "stomp"})
	log.Info("consumer started", "protocol", "STOMP", "consumerId", c.Id, "destination", c.Topic)
	for i := 1; i <= c.Config.ConsumeCount; i++ {
		msg := <-sub.C
		if msg.Err != nil {
			log.Error("failed to receive a message", "protocol", "STOMP", "subscriberId", c.Id, "c.Topic", c.Topic, "error", msg.Err)
			return
		}
		m.Observe(utils.CalculateEndToEndLatency(&msg.Body))
		log.Debug("message received", "protocol", "stomp", "subscriberId", c.Id, "destination", c.Topic, "size", len(msg.Body), "ack required", msg.ShouldAck())

		err = c.Connection.Ack(msg)
		if err != nil {
			log.Error("message NOT acknowledged", "protocol", "stomp", "subscriberId", c.Id, "destination", c.Topic)

		}

		metrics.MessagesConsumed.With(prometheus.Labels{"protocol": "stomp"}).Inc()
	}

	log.Debug("consumer finished", "protocol", "STOMP", "consumerId", c.Id)

}
