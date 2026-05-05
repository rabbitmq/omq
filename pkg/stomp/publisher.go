package stomp

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
)

const dialTimeout = 10 * time.Second

type StompPublisher struct {
	Id         int
	Connection *stomp.Conn
	Topic      string
	Config     config.Config
	ctx        context.Context
	msg        []byte
	whichUri   int
	msgSent    atomic.Uint64
}

func NewPublisher(ctx context.Context, cfg config.Config, id int) *StompPublisher {
	publisher := &StompPublisher{
		Id:         id,
		Connection: nil,
		Topic:      utils.ResolveTerminus(cfg.PublishToTemplate, id),
		Config:     cfg,
		ctx:        ctx,
	}

	if cfg.SpreadConnections {
		publisher.whichUri = id % len(cfg.PublisherUri)
	}

	publisher.Connect()

	return publisher
}

func (p *StompPublisher) Connect() {
	if p.Connection != nil {
		_ = p.Connection.Disconnect()
	}

	for p.Connection == nil {
		uri := utils.NextURI(p.Config.PublisherUri, &p.whichUri)
		useTLS := strings.HasPrefix(uri, "stomp+ssl://") || strings.HasPrefix(uri, "stomps://")
		parsedUri := utils.ParseURI(uri, "stomp", "61613")

		var o = []func(*stomp.Conn) error{
			stomp.ConnOpt.Login(parsedUri.Username, parsedUri.Password),
			stomp.ConnOpt.Host("/"), // TODO
		}

		var conn *stomp.Conn
		var err error
		dialer := &net.Dialer{Timeout: dialTimeout}
		if useTLS {
			netConn, tlsErr := tls.DialWithDialer(dialer, "tcp", parsedUri.Broker, &tls.Config{
				InsecureSkipVerify: p.Config.InsecureSkipTLSVerify,
			})
			if tlsErr != nil {
				err = tlsErr
			} else {
				conn, err = stomp.Connect(netConn, o...)
				if err != nil {
					_ = netConn.Close()
				}
			}
		} else {
			netConn, dialErr := dialer.Dial("tcp", parsedUri.Broker)
			if dialErr != nil {
				err = dialErr
			} else {
				conn, err = stomp.Connect(netConn, o...)
				if err != nil {
					_ = netConn.Close()
				}
			}
		}
		if err != nil {
			log.Error("publisher connection failed", "id", p.Id, "error", err.Error())
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(config.ReconnectDelay):
			}
		} else {
			p.Connection = conn
		}
		log.Info("connection established", "id", p.Id)
	}
}

func (p *StompPublisher) Start(publisherReady chan bool, startPublishing chan bool) {
	p.msg = utils.MessageBody(p.Config.Size, p.Config.SizeTemplate, p.Id)

	close(publisherReady)

	select {
	case <-p.ctx.Done():
		return
	case <-startPublishing:
		// short random delay to avoid all publishers publishing at the same time
		time.Sleep(time.Duration(rand.IntN(1000)) * time.Millisecond)
	}

	log.Info("publisher started", "id", p.Id, "rate", "unlimited", "destination", p.Topic)

	var farewell string
	if p.Config.Rate == 0 {
		// idle connection
		<-p.ctx.Done()
		farewell = "context cancelled"
	} else {
		farewell = p.StartPublishing()
	}
	p.Stop(farewell)
}

func (p *StompPublisher) StartPublishing() string {
	limiter := utils.RateLimiter(p.Config.Rate)

	var msgSent atomic.Int64
	for {
		select {
		case <-p.ctx.Done():
			return "context cancelled"
		default:
			if msgSent.Add(1) > int64(p.Config.PublishCount) {
				return "--pmessages value reached"
			}
			if p.Config.Rate > 0 {
				_ = limiter.Wait(p.ctx)
			}
			err := p.Send()
			if err != nil {
				p.Connect()
			}
		}
	}
}

func (p *StompPublisher) Send() error {
	seq := p.msgSent.Add(1) - 1

	if p.Config.SizeTemplate != nil {
		p.msg = utils.MessageBody(p.Config.Size, p.Config.SizeTemplate, p.Id)
	}
	utils.UpdatePayload(p.Config.UseMillis, &p.msg)

	headers := buildHeaders(p.Config, p.Id)
	if p.Config.DetectOutOfOrder || p.Config.DetectGaps {
		headers = append(headers,
			stomp.SendOpt.Header(utils.HeaderPublisherID, strconv.Itoa(p.Id)),
			stomp.SendOpt.Header(utils.HeaderSequence, strconv.FormatUint(seq, 10)))
	}

	startTime := time.Now()
	err := p.Connection.Send(p.Topic, "", p.msg, headers...)
	latency := time.Since(startTime)
	if err != nil {
		log.Error("message sending failure", "id", p.Id, "error", err)
		return err
	}
	metrics.MessagesPublished.Inc()
	metrics.RecordPublishingLatency(latency)
	log.Debug("message sent", "id", p.Id, "destination", p.Topic, "latency", latency)
	return nil
}

func (p *StompPublisher) Stop(reason string) {
	log.Debug("closing publisher connection", "id", p.Id, "reason", reason)
	_ = p.Connection.Disconnect()
}

func buildHeaders(cfg config.Config, publisherId int) []func(*frame.Frame) error {
	var headers []func(*frame.Frame) error

	headers = append(headers, stomp.SendOpt.Receipt)

	var msgDurability string
	if cfg.MessageDurability {
		msgDurability = "true"
	} else {
		msgDurability = "false"
	}
	headers = append(headers, stomp.SendOpt.Header("persistent", msgDurability))

	// Handle message priority (always use template)
	if cfg.MessagePriorityTemplate != nil {
		priorityStr := utils.ExecuteTemplate(cfg.MessagePriorityTemplate, publisherId)
		headers = append(headers, stomp.SendOpt.Header("priority", priorityStr))
	}
	if cfg.MessageTTLTemplate != nil {
		ttlStr := utils.ExecuteTemplate(cfg.MessageTTLTemplate, publisherId)
		if ttl, err := time.ParseDuration(ttlStr); err == nil && ttl.Milliseconds() > 0 {
			headers = append(headers, stomp.SendOpt.Header("expiration", fmt.Sprint(ttl.Milliseconds())))
		} else if err != nil {
			log.Error("failed to parse template-generated TTL", "value", ttlStr, "error", err)
		}
	}

	if cfg.StreamFilterValueSet != "" {
		headers = append(headers, stomp.SendOpt.Header("x-stream-filter-value", cfg.StreamFilterValueSet))
	}

	return headers
}
