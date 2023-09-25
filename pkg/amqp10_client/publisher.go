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

type Amqp10Publisher struct {
	Id     int
	Sender *amqp.Sender
	Topic  string
	Config config.Config
}

func NewPublisher(cfg config.Config, n int) *Amqp10Publisher {
	// open connection
	conn, err := amqp.Dial(context.TODO(), cfg.PublisherUri, nil)
	if err != nil {
		log.Error("publisher connection failed", "protocol", "amqp-1.0", "publisherId", n, "error", err.Error())
		return nil
	}

	// open session
	session, err := conn.NewSession(context.TODO(), nil)
	if err != nil {
		log.Error("publisher failed to create a session", "protocol", "amqp-1.0", "publisherId", n, "error", err.Error())
		return nil
	}

	topic := fmt.Sprintf("/queue/%s-%d", cfg.QueueNamePrefix, ((n-1)%cfg.QueueCount)+1)
	sender, err := session.NewSender(context.TODO(), topic, &amqp.SenderOptions{
		Durability: amqp.DurabilityUnsettledState})
	if err != nil {
		log.Error("publisher failed to create a sender", "protocol", "amqp-1.0", "publisherId", n, "error", err.Error())
		return nil
	}

	return &Amqp10Publisher{
		Id:     n,
		Sender: sender,
		Topic:  topic,
		Config: cfg,
	}
}

func (p Amqp10Publisher) Start() {
	// sleep random interval to avoid all publishers publishing at the same time
	s := rand.Intn(p.Config.Publishers)
	time.Sleep(time.Duration(s) * time.Millisecond)

	if p.Config.Rate == -1 {
		p.StartFullSpeed()
	} else {
		p.StartRateLimited()
	}
}

func (p Amqp10Publisher) StartFullSpeed() {
	log.Info("publisher started", "protocol", "AMQP-1.0", "publisherId", p.Id, "rate", "unlimited", "destination", p.Topic)

	msg := utils.MessageBody(p.Config)

	for i := 1; i <= p.Config.PublishCount; i++ {
		utils.UpdatePayload(p.Config.UseMillis, &msg)
		timer := prometheus.NewTimer(metrics.PublishingLatency.With(prometheus.Labels{"protocol": "amqp-1.0"}))
		err := p.Sender.Send(context.TODO(), amqp.NewMessage(msg), nil)
		timer.ObserveDuration()
		if err != nil {
			log.Error("message sending failure", "protocol", "amqp-1.0", "publisherId", p.Id, "error", err.Error())
			return
		}
		metrics.MessagesPublished.With(prometheus.Labels{"protocol": "amqp-1.0"}).Inc()
		log.Debug("message sent", "protocol", "amqp-1.0", "publisherId", p.Id)
		utils.WaitBetweenMessages(p.Config.Rate)
	}

	log.Debug("publisher stopped", "protocol", "amqp-1.0", "publisherId", p.Id)
}

func (p Amqp10Publisher) StartRateLimited() {
	log.Info("publisher started", "protocol", "AMQP-1.0", "publisherId", p.Id, "rate", p.Config.Rate, "destination", p.Topic)
}
