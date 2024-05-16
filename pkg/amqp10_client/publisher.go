package amqp10_client

import (
	"context"
	"crypto/tls"
	"math/rand"
	"strconv"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/topic"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/rabbitmq/omq/pkg/metrics"

	"github.com/Azure/go-amqp"
	"github.com/prometheus/client_golang/prometheus"
)

type Amqp10Publisher struct {
	Id               int
	Sender           *amqp.Sender
	Connectionection *amqp.Conn
	Topic            string
	Config           config.Config
	msg              []byte
}

func NewPublisher(cfg config.Config, n int) *Amqp10Publisher {
	// open connection
	hostname, vhost := hostAndVHost(cfg.PublisherUri)
	conn, err := amqp.Dial(context.TODO(), cfg.PublisherUri, &amqp.ConnOptions{
		HostName: vhost,
		TLSConfig: &tls.Config{
			ServerName: hostname}})
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

	var durability amqp.Durability
	switch cfg.QueueDurability {
	case config.None:
		durability = amqp.DurabilityNone
	case config.Configuration:
		durability = amqp.DurabilityConfiguration
	case config.UnsettledState:
		durability = amqp.DurabilityUnsettledState
	}

	terminus := topic.CalculateTopic(cfg.PublishTo, n)
	sender, err := session.NewSender(context.TODO(), terminus, &amqp.SenderOptions{
		TargetDurability: durability})
	if err != nil {
		log.Error("publisher failed to create a sender", "protocol", "amqp-1.0", "publisherId", n, "error", err.Error())
		return nil
	}

	return &Amqp10Publisher{
		Id:               n,
		Connectionection: conn,
		Sender:           sender,
		Topic:            terminus,
		Config:           cfg,
	}
}

func (p Amqp10Publisher) Start(ctx context.Context) {
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

	log.Debug("publisher completed", "protocol", "amqp-1.0", "publisherId", p.Id)
}

func (p Amqp10Publisher) StartFullSpeed(ctx context.Context) {
	log.Info("publisher started", "protocol", "AMQP-1.0", "publisherId", p.Id, "rate", "unlimited", "destination", p.Topic)

	for i := 1; i <= p.Config.PublishCount; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			p.Send()
		}
	}
}

func (p Amqp10Publisher) StartIdle(ctx context.Context) {
	log.Info("publisher started", "protocol", "AMQP-1.0", "publisherId", p.Id, "rate", "-", "destination", p.Topic)

	_ = ctx.Done()
}

func (p Amqp10Publisher) StartRateLimited(ctx context.Context) {
	log.Info("publisher started", "protocol", "AMQP-1.0", "publisherId", p.Id, "rate", p.Config.Rate, "destination", p.Topic)
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

func (p Amqp10Publisher) Send() {
	utils.UpdatePayload(p.Config.UseMillis, &p.msg)
	msg := amqp.NewMessage(p.msg)
	if p.Config.Amqp.Subject != "" {
		msg.Properties = &amqp.MessageProperties{Subject: &p.Config.Amqp.Subject}
	}

	if p.Config.StreamFilterValueSet != "" {
		msg.Annotations = amqp.Annotations{"x-stream-filter-value": p.Config.StreamFilterValueSet}
	}

	msg.Header = &amqp.MessageHeader{}
	msg.Header.Durable = p.Config.MessageDurability
	if p.Config.MessagePriority != "" {
		// already validated in root.go
		priority, _ := strconv.ParseUint(p.Config.MessagePriority, 10, 8)
		msg.Header.Priority = uint8(priority)
	}

	timer := prometheus.NewTimer(metrics.PublishingLatency.With(prometheus.Labels{"protocol": "amqp-1.0"}))
	err := p.Sender.Send(context.TODO(), msg, nil)
	timer.ObserveDuration()
	if err != nil {
		log.Error("message sending failure", "protocol", "amqp-1.0", "publisherId", p.Id, "error", err.Error())
		return
	}
	metrics.MessagesPublished.With(prometheus.Labels{"protocol": "amqp-1.0"}).Inc()
	log.Debug("message sent", "protocol", "amqp-1.0", "publisherId", p.Id)
}

func (p Amqp10Publisher) Stop(reason string) {
	log.Debug("closing connection", "protocol", "amqp-1.0", "publisherId", p.Id, "reason", reason)
	_ = p.Connectionection.Close()
}
