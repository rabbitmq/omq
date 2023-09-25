package stomp_client

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/go-stomp/stomp/v3"
	"github.com/prometheus/client_golang/prometheus"
)

// these are the default options that work with RabbitMQ
var opts []func(*stomp.Conn) error = []func(*stomp.Conn) error{
	stomp.ConnOpt.Login("guest", "guest"),
	stomp.ConnOpt.Host("/"),
}

type StompPublisher struct {
	Id         int
	Connection *stomp.Conn
	Topic      string
	Config     config.Config
}

func NewPublisher(cfg config.Config, id int) *StompPublisher {
	// sleep random interval to avoid all publishers publishing at exactly the same time
	s := rand.Intn(cfg.Publishers)
	time.Sleep(time.Duration(s) * time.Millisecond)

	conn, err := stomp.Dial("tcp", cfg.PublisherUri, opts...)
	if err != nil {
		log.Error("publisher connection failed", "protocol", "STOMP", "publisherId", id, "error", err.Error())
		return nil
	}
	log.Info("publisher connected", "protocol", "STOMP", "publisherId", id)

	topic := fmt.Sprintf("/exchange/amq.topic/%s-%d", cfg.QueueNamePrefix, ((id-1)%cfg.QueueCount)+1)

	return &StompPublisher{
		Id:         id,
		Connection: conn,
		Topic:      topic,
		Config:     cfg,
	}
}

func (p StompPublisher) Start() {
	// sleep random interval to avoid all publishers publishing at the same time
	s := rand.Intn(p.Config.Publishers)
	time.Sleep(time.Duration(s) * time.Millisecond)

	if p.Config.Rate == -1 {
		p.StartFullSpeed()
	} else {
		p.StartRateLimited()
	}
}

func (p StompPublisher) StartFullSpeed() {
	log.Info("publisher started", "protocol", "STOMP", "publisherId", p.Id, "rate", "unlimited", "destination", p.Topic)

	msg := utils.MessageBody(p.Config)

	for i := 1; i <= p.Config.PublishCount; i++ {
		utils.UpdatePayload(p.Config.UseMillis, &msg)
		timer := prometheus.NewTimer(metrics.PublishingLatency.With(prometheus.Labels{"protocol": "stomp"}))
		err := p.Connection.Send(p.Topic, "", msg, stomp.SendOpt.Receipt, stomp.SendOpt.Header("persistent", "true"))
		timer.ObserveDuration()
		if err != nil {
			log.Error("message sending failure", "protocol", "STOMP", "publisherId", p.Id, "error", err)
			return
		}
		log.Debug("message sent", "protocol", "STOMP", "publisherId", p.Id, "destination", p.Topic)

		metrics.MessagesPublished.With(prometheus.Labels{"protocol": "stomp"}).Inc()
		utils.WaitBetweenMessages(p.Config.Rate)
	}

	log.Debug("publisher finished", "publisherId", p.Id)
}

func (p StompPublisher) StartRateLimited() {
	log.Info("publisher started", "protocol", "STOMP", "publisherId", p.Id, "rate", p.Config.Rate, "destination", p.Topic)
}
