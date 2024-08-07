package stomp_client

import (
	"context"
	"math/rand"
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

type StompPublisher struct {
	Id         int
	Connection *stomp.Conn
	Topic      string
	Config     config.Config
	msg        []byte
}

func NewPublisher(cfg config.Config, id int) *StompPublisher {
	parsedUri := utils.ParseURI(cfg.PublisherUri, "61613")

	var o []func(*stomp.Conn) error = []func(*stomp.Conn) error{
		stomp.ConnOpt.Login(parsedUri.Username, parsedUri.Password),
		stomp.ConnOpt.Host("/"), // TODO
	}

	conn, err := stomp.Dial("tcp", parsedUri.Broker, o...)
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

func (p StompPublisher) Start(ctx context.Context) {
	// sleep random interval to avoid all publishers publishing at the same time
	s := rand.Intn(1000)
	time.Sleep(time.Duration(s) * time.Millisecond)

	p.msg = utils.MessageBody(p.Config.Size)

	switch p.Config.Rate {
	case -1:
		p.StartFullSpeed(ctx)
	case 0:
		p.StartIdle(ctx)
	default:
		p.StartRateLimited(ctx)
	}
}

func (p StompPublisher) StartFullSpeed(ctx context.Context) {
	log.Info("publisher started", "protocol", "STOMP", "publisherId", p.Id, "rate", "unlimited", "destination", p.Topic)

	for i := 1; i <= p.Config.PublishCount; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			p.Send()
		}
	}
	log.Debug("publisher completed", "protocol", "stomp", "publisherId", p.Id)
}

func (p StompPublisher) StartIdle(ctx context.Context) {
	log.Info("publisher started", "protocol", "STOMP", "publisherId", p.Id, "rate", "-", "destination", p.Topic)

	_ = ctx.Done()
}

func (p StompPublisher) StartRateLimited(ctx context.Context) {
	log.Info("publisher started", "protocol", "STOMP", "publisherId", p.Id, "rate", p.Config.Rate, "destination", p.Topic)
	ticker := time.NewTicker(time.Duration(1000/float64(p.Config.Rate)) * time.Millisecond)

	msgSent := 0
	for {
		select {
		case <-ctx.Done():
			p.Stop("time limit reached")
			return
		case <-ticker.C:
			p.Send()
			msgSent++
			if msgSent >= p.Config.PublishCount {
				p.Stop("publish count reached")
				return
			}
		}
	}
}

func (p StompPublisher) Send() {
	utils.UpdatePayload(p.Config.UseMillis, &p.msg)

	timer := prometheus.NewTimer(metrics.PublishingLatency.With(prometheus.Labels{"protocol": "stomp"}))
	err := p.Connection.Send(p.Topic, "", p.msg, buildHeaders(p.Config)...)
	timer.ObserveDuration()
	if err != nil {
		log.Error("message sending failure", "protocol", "STOMP", "publisherId", p.Id, "error", err)
		return
	}
	log.Debug("message sent", "protocol", "STOMP", "publisherId", p.Id, "destination", p.Topic)

	metrics.MessagesPublished.With(prometheus.Labels{"protocol": "stomp"}).Inc()
}

func (p StompPublisher) Stop(reason string) {
	log.Debug("closing connection", "protocol", "stomp", "publisherId", p.Id, "reason", reason)
	_ = p.Connection.Disconnect()
}

func buildHeaders(cfg config.Config) []func(*frame.Frame) error {
	var headers []func(*frame.Frame) error

	headers = append(headers, stomp.SendOpt.Receipt)

	var msgDurability string
	if cfg.MessageDurability {
		msgDurability = "true"
	} else {
		msgDurability = "false"
	}
	headers = append(headers, stomp.SendOpt.Header("persistent", msgDurability))
	if cfg.MessagePriority != "" {
		headers = append(headers, stomp.SendOpt.Header("priority", cfg.MessagePriority))
	}

	if cfg.StreamFilterValueSet != "" {
		headers = append(headers, stomp.SendOpt.Header("x-stream-filter-value", cfg.StreamFilterValueSet))
	}

	return headers
}
