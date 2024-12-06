package stomp

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
)

type StompPublisher struct {
	Id         int
	Connection *stomp.Conn
	Topic      string
	Config     config.Config
	msg        []byte
	whichUri   int
}

func NewPublisher(ctx context.Context, cfg config.Config, id int) *StompPublisher {
	publisher := &StompPublisher{
		Id:         id,
		Connection: nil,
		Topic:      utils.InjectId(cfg.PublishTo, id),
		Config:     cfg,
	}

	if cfg.SpreadConnections {
		publisher.whichUri = (id - 1) % len(cfg.PublisherUri)
	}

	publisher.Connect()

	return publisher
}

func (p *StompPublisher) Connect() {
	if p.Connection != nil {
		_ = p.Connection.Disconnect()
	}

	for p.Connection == nil {
		if p.whichUri >= len(p.Config.PublisherUri) {
			p.whichUri = 0
		}
		uri := p.Config.PublisherUri[p.whichUri]
		p.whichUri++
		parsedUri := utils.ParseURI(uri, "stomp", "61613")

		var o []func(*stomp.Conn) error = []func(*stomp.Conn) error{
			stomp.ConnOpt.Login(parsedUri.Username, parsedUri.Password),
			stomp.ConnOpt.Host("/"), // TODO
		}

		conn, err := stomp.Dial("tcp", parsedUri.Broker, o...)
		if err != nil {
			log.Error("publisher connection failed", "id", p.Id, "error", err.Error())
			time.Sleep(1 * time.Second)
		} else {
			p.Connection = conn
		}
		log.Info("connection established", "id", p.Id)
	}
}

func (p *StompPublisher) Start(ctx context.Context) {
	// sleep random interval to avoid all publishers publishing at the same time
	s := rand.Intn(1000)
	time.Sleep(time.Duration(s) * time.Millisecond)

	p.msg = utils.MessageBody(p.Config.Size)

	log.Info("publisher started", "id", p.Id, "rate", "unlimited", "destination", p.Topic)

	var farewell string
	if p.Config.Rate == 0 {
		// idle connection
		<-ctx.Done()
		farewell = "context cancelled"
	} else {
		farewell = p.StartPublishing(ctx)
	}
	p.Stop(farewell)
}

func (p *StompPublisher) StartPublishing(ctx context.Context) string {
	limiter := utils.RateLimiter(p.Config.Rate)

	var msgSent atomic.Int64
	for {
		select {
		case <-ctx.Done():
			return "context cancelled"
		default:
			if msgSent.Add(1) > int64(p.Config.PublishCount) {
				return "--pmessages value reached"
			}
			if p.Config.Rate > 0 {
				_ = limiter.Wait(context.Background())
			}
			err := p.Send()
			if err != nil {
				p.Connect()
			}
		}
	}
}

func (p *StompPublisher) Send() error {
	utils.UpdatePayload(p.Config.UseMillis, &p.msg)

	startTime := time.Now()
	err := p.Connection.Send(p.Topic, "", p.msg, buildHeaders(p.Config)...)
	latency := time.Since(startTime)
	if err != nil {
		log.Error("message sending failure", "id", p.Id, "error", err)
		return err
	}
	metrics.MessagesPublished.Inc()
	metrics.PublishingLatency.Update(latency.Seconds())
	log.Debug("message sent", "id", p.Id, "destination", p.Topic, "latency", latency)
	return nil
}

func (p *StompPublisher) Stop(reason string) {
	log.Debug("closing connection", "id", p.Id, "reason", reason)
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
	if cfg.MessageTTL.Milliseconds() > 0 {
		headers = append(headers, stomp.SendOpt.Header("expiration", fmt.Sprint(cfg.MessageTTL.Milliseconds())))
	}

	if cfg.StreamFilterValueSet != "" {
		headers = append(headers, stomp.SendOpt.Header("x-stream-filter-value", cfg.StreamFilterValueSet))
	}

	return headers
}
