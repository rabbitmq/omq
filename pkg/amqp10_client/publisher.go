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
	Id         int
	Connection *amqp.Conn
	Session    *amqp.Session
	Sender     *amqp.Sender
	Terminus   string
	Config     config.Config
	msg        []byte
	whichUri   int
}

func NewPublisher(cfg config.Config, id int) *Amqp10Publisher {
	publisher := &Amqp10Publisher{
		Id:         id,
		Connection: nil,
		Sender:     nil,
		Config:     cfg,
		Terminus:   topic.CalculateTopic(cfg.PublishTo, id),
		whichUri:   0,
	}

	if cfg.SpreadConnections {
		publisher.whichUri = (id - 1) % len(cfg.PublisherUri)
	}

	publisher.Connect()

	return publisher
}

func (p *Amqp10Publisher) Connect() {
	var conn *amqp.Conn
	var err error

	// clean up when reconnecting
	if p.Session != nil {
		_ = p.Session.Close(context.Background())
	}
	if p.Connection != nil {
		_ = p.Connection.Close()
	}
	p.Sender = nil
	p.Session = nil
	p.Connection = nil

	for p.Connection == nil {
		if p.whichUri >= len(p.Config.PublisherUri) {
			p.whichUri = 0
		}
		uri := p.Config.PublisherUri[p.whichUri]
		p.whichUri++
		hostname, vhost := hostAndVHost(uri)
		conn, err = amqp.Dial(context.TODO(), uri, &amqp.ConnOptions{
			HostName: vhost,
			TLSConfig: &tls.Config{
				ServerName: hostname,
			},
		})

		if err != nil {
			log.Error("connection failed", "publisherId", p.Id, "error", err.Error())
			time.Sleep(1 * time.Second)
		} else {
			log.Debug("connection established", "publisherId", p.Id, "uri", uri)
			p.Connection = conn
		}
	}

	for p.Session == nil {
		session, err := p.Connection.NewSession(context.TODO(), nil)
		if err != nil {
			log.Error("publisher failed to create a session", "publisherId", p.Id, "error", err.Error())
			time.Sleep(1 * time.Second)
			p.Connect()
		} else {
			p.Session = session
		}
	}

	p.CreateSender()
}

func (p *Amqp10Publisher) CreateSender() {
	var durability amqp.Durability
	switch p.Config.QueueDurability {
	case config.None:
		durability = amqp.DurabilityNone
	case config.Configuration:
		durability = amqp.DurabilityConfiguration
	case config.UnsettledState:
		durability = amqp.DurabilityUnsettledState
	}

	settleMode := amqp.SenderSettleModeUnsettled.Ptr()
	if p.Config.Amqp.SendSettled {
		settleMode = amqp.SenderSettleModeSettled.Ptr()
	}

	for p.Sender == nil {
		sender, err := p.Session.NewSender(context.TODO(), p.Terminus, &amqp.SenderOptions{
			SettlementMode:   settleMode,
			TargetDurability: durability})
		if err != nil {
			log.Error("publisher failed to create a sender", "publisherId", p.Id, "error", err.Error())
			time.Sleep(1 * time.Second)
			p.Connect()
		} else {
			p.Sender = sender
		}
	}
}

func (p *Amqp10Publisher) Start(ctx context.Context) {
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

	log.Debug("publisher completed", "publisherId", p.Id)
}

func (p *Amqp10Publisher) StartFullSpeed(ctx context.Context) {
	log.Info("publisher started", "publisherId", p.Id, "rate", "unlimited", "destination", p.Terminus)

	for i := 1; i <= p.Config.PublishCount; {
		select {
		case <-ctx.Done():
			return
		default:
			err := p.Send()
			if err != nil {
				p.Connect()
			} else {
				i++
			}
		}
	}
}

func (p *Amqp10Publisher) StartIdle(ctx context.Context) {
	log.Info("publisher started", "publisherId", p.Id, "rate", "-", "destination", p.Terminus)

	_ = ctx.Done()
}

func (p *Amqp10Publisher) StartRateLimited(ctx context.Context) {
	log.Info("publisher started", "publisherId", p.Id, "rate", p.Config.Rate, "destination", p.Terminus)
	ticker := time.NewTicker(time.Duration(1000/float64(p.Config.Rate)) * time.Millisecond)

	msgSent := 0
	for {
		select {
		case <-ctx.Done():
			p.Stop("time limit reached")
			return
		case <-ticker.C:
			err := p.Send()
			if err != nil {
				p.Connect()
			} else {
				msgSent++
				if msgSent >= p.Config.PublishCount {
					p.Stop("publish count reached")
					return
				}
			}
		}
	}
}

func (p *Amqp10Publisher) Send() error {
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
		log.Error("message sending failure", "publisherId", p.Id, "error", err.Error())
		return err
	}
	metrics.MessagesPublished.With(prometheus.Labels{"protocol": "amqp-1.0"}).Inc()
	log.Debug("message sent", "publisherId", p.Id)
	return nil
}

func (p *Amqp10Publisher) Stop(reason string) {
	log.Debug("closing connection", "publisherId", p.Id, "reason", reason)
	_ = p.Connection.Close()
}
