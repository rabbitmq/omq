package amqp10_client

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/rabbitmq/omq/pkg/metrics"

	amqp "github.com/Azure/go-amqp"
	"github.com/prometheus/client_golang/prometheus"
)

func Publisher(cfg config.Config, n int) {
	// sleep random interval to avoid all publishers publishing at the same time
	s := rand.Intn(cfg.Publishers)
	time.Sleep(time.Duration(s) * time.Millisecond)

	// open connection
	conn, err := amqp.Dial(context.TODO(), cfg.PublisherUri, nil)
	if err != nil {
		log.Error("publisher connection failed", "protocol", "amqp-1.0", "publisherId", n, "error", err.Error())
		return
	}

	// open session
	session, err := conn.NewSession(context.TODO(), nil)
	if err != nil {
		log.Error("publisher failed to create a session", "protocol", "amqp-1.0", "publisherId", n, "error", err.Error())
		return
	}

	queue := fmt.Sprintf("/queue/%s-%d", cfg.QueueNamePrefix, ((n-1)%cfg.QueueCount)+1)
	sender, err := session.NewSender(context.TODO(), queue, &amqp.SenderOptions{
		Durability: amqp.DurabilityUnsettledState})
	if err != nil {
		log.Error("publisher failed to create a sender", "protocol", "amqp-1.0", "publisherId", n, "error", err.Error())
		return
	}

	// message payload will be result with the first bytes overwritten
	msg := utils.MessageBody(cfg)

	log.Info("publisher started", "protocol", "amqp-1.0", "publisherId", n, "terminus", queue)

	// main loop
	for i := 1; i <= cfg.PublishCount; i++ {
		utils.UpdatePayload(cfg.UseMillis, &msg)
		timer := prometheus.NewTimer(metrics.PublishingLatency.With(prometheus.Labels{"protocol": "amqp-1.0"}))
		err = sender.Send(context.TODO(), amqp.NewMessage(msg), nil)
		timer.ObserveDuration()
		if err != nil {
			log.Error("message sending failure", "protocol", "amqp-1.0", "publisherId", n, "error", err.Error())
			return
		}
		metrics.MessagesPublished.With(prometheus.Labels{"protocol": "amqp-1.0"}).Inc()
		log.Error("message sent", "protocol", "amqp-1.0", "publisherId", n)
		utils.WaitBetweenMessages(cfg.Rate)
	}

	log.Debug("publisher stopped", "protocol", "amqp-1.0", "publisherId", n)
}

func Consumer(cfg config.Config, subscribed chan bool, n int) {
	// open connection
	conn, err := amqp.Dial(context.TODO(), cfg.ConsumerUri, nil)
	if err != nil {
		log.Error("consumer failed to connect", "protocol", "amqp-1.0", "consumerId", n, "error", err.Error())
		return
	}

	// open seesion
	session, err := conn.NewSession(context.TODO(), nil)
	if err != nil {
		log.Error("consumer failed to create a session", "protocol", "amqp-1.0", "consumerId", n, "error", err.Error())
		return
	}

	// calculate what queue to subscribe to
	queue := fmt.Sprintf("/queue/%s-%d", cfg.QueueNamePrefix, ((n-1)%cfg.QueueCount)+1)

	receiver, err := session.NewReceiver(context.TODO(), queue, &amqp.ReceiverOptions{Durability: amqp.DurabilityUnsettledState})
	if err != nil {
		log.Error("consumer failed to create a receiver", "protocol", "amqp-1.0", "consumerId", n, "error", err.Error())
		return
	}
	close(subscribed)
	log.Debug("consumer subscribed", "protocol", "amqp-1.0", "subscriberId", n, "terminus", queue)

	m := metrics.EndToEndLatency.With(prometheus.Labels{"protocol": "amqp-1.0"})

	log.Info("consumer started", "protocol", "amqp-1.0", "consumerId", n, "terminus", queue)

	// main loop
	for i := 1; i <= cfg.ConsumeCount; i++ {
		msg, err := receiver.Receive(context.TODO(), nil)
		if err != nil {
			log.Error("failed to receive a message", "protocol", "amqp-1.0", "subscriberId", n, "terminus", queue)
			return
		}

		payload := msg.GetData()
		m.Observe(utils.CalculateEndToEndLatency(&payload))

		log.Debug("message received", "protocol", "amqp-1.0", "subscriberId", n, "terminus", queue, "size", len(payload))

		err = receiver.AcceptMessage(context.TODO(), msg)
		if err != nil {
			log.Error("message NOT accepted", "protocol", "amqp-1.0", "subscriberId", n, "terminus", queue)
		}
		metrics.MessagesConsumed.With(prometheus.Labels{"protocol": "amqp-1.0"}).Inc()
		log.Debug("message accepted", "protocol", "amqp-1.0", "subscriberId", n, "terminus", queue)
	}

	log.Debug("consumer finished", "protocol", "amqp-1.0", "subscriberId", n)

}
