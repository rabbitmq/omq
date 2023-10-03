package stomp_client

import (
	"math/rand"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/topic"
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
	msg        []byte
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

	topic := topic.CalculateTopic(cfg.PublishTo, id)

	return &StompPublisher{
		Id:         id,
		Connection: conn,
		Topic:      topic,
		Config:     cfg,
	}
}

func (p StompPublisher) Start() {
	// sleep random interval to avoid all publishers publishing at the same time
	s := rand.Intn(1000)
	time.Sleep(time.Duration(s) * time.Millisecond)

	p.msg = utils.MessageBody(p.Config.Size)

	if p.Config.Rate == -1 {
		p.StartFullSpeed()
	} else {
		p.StartRateLimited()
	}
}

func (p StompPublisher) StartFullSpeed() {
	log.Info("publisher started", "protocol", "STOMP", "publisherId", p.Id, "rate", "unlimited", "destination", p.Topic)

	for i := 1; i <= p.Config.PublishCount; i++ {
		p.Send()
	}

	log.Debug("publisher finished", "publisherId", p.Id)
}

func (p StompPublisher) StartRateLimited() {
	log.Info("publisher started", "protocol", "STOMP", "publisherId", p.Id, "rate", p.Config.Rate, "destination", p.Topic)
	ticker := time.NewTicker(time.Duration(1000/float64(p.Config.Rate)) * time.Millisecond)
	done := make(chan bool)

	msgsSent := 0
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				p.Send()
				msgsSent++
			}
		}
	}()
	for {
		time.Sleep(1 * time.Second)
		if msgsSent >= p.Config.PublishCount {
			break
		}
	}
}

func (p StompPublisher) Send() {
	utils.UpdatePayload(p.Config.UseMillis, &p.msg)
	var msgDurability string
	if p.Config.MessageDurability {
		msgDurability = "true"
	} else {
		msgDurability = "false"
	}

	timer := prometheus.NewTimer(metrics.PublishingLatency.With(prometheus.Labels{"protocol": "stomp"}))
	err := p.Connection.Send(p.Topic, "", p.msg, stomp.SendOpt.Receipt, stomp.SendOpt.Header("persistent", msgDurability))
	timer.ObserveDuration()
	if err != nil {
		log.Error("message sending failure", "protocol", "STOMP", "publisherId", p.Id, "error", err)
		return
	}
	log.Debug("message sent", "protocol", "STOMP", "publisherId", p.Id, "destination", p.Topic)

	metrics.MessagesPublished.With(prometheus.Labels{"protocol": "stomp"}).Inc()
}
