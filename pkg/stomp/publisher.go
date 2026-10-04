package stomp

import (
	"context"
	"crypto/tls"
	"math/rand/v2"
	"net"
	"net/url"
	"strconv"
	"strings"
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
	msgSent    uint64
	latency    *metrics.LatencyRecorder
	idStr      string
	static     []func(*frame.Frame) error // headers that are the same for every message
	headers    []func(*frame.Frame) error // reused per message: static followed by dynamic
	filterOpts []func(*frame.Frame) error // one pre-built header per stream filter value
	priorityOK bool
	ttlOK      bool
}

func NewPublisher(ctx context.Context, cfg config.Config, id int) *StompPublisher {
	publisher := &StompPublisher{
		Id:         id,
		Connection: nil,
		Topic:      utils.ResolveTerminus(cfg.PublishToTemplate, id),
		Config:     cfg,
		ctx:        ctx,
		latency:    metrics.NewLatencyRecorder(),
		idStr:      strconv.Itoa(id),
	}
	publisher.buildStaticHeaders()

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
	p.Connection = nil

	for p.Connection == nil {
		uri := utils.NextURI(p.Config.PublisherUri, &p.whichUri)
		useTLS := strings.HasPrefix(uri, "stomp+ssl://") || strings.HasPrefix(uri, "stomps://")
		parsedUri := utils.ParseURI(uri, "stomp", "61613")

		vhost := "/"
		if u, err := url.Parse(uri); err == nil {
			if u.Path != "" && u.Path != "/" {
				if unescaped, err := url.PathUnescape(strings.TrimPrefix(u.Path, "/")); err == nil {
					vhost = unescaped
				} else {
					vhost = strings.TrimPrefix(u.Path, "/")
				}
			}
		}

		var o = []func(*stomp.Conn) error{
			stomp.ConnOpt.Login(parsedUri.Username, parsedUri.Password),
			stomp.ConnOpt.Host(vhost),
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
			log.Info("connection established", "id", p.Id)
		}
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

	var msgSent int64
	for {
		select {
		case <-p.ctx.Done():
			return "context cancelled"
		default:
			msgSent++
			if msgSent > int64(p.Config.PublishCount) {
				return "--pmessages value reached"
			}
			if p.Config.Rate > 0 {
				_ = limiter.Wait(p.ctx)
			}
			err := p.Send()
			if err != nil {
				log.Info("publisher disconnected; reconnecting...", "id", p.Id, "error", err.Error())
				p.Connect()
			}
		}
	}
}

func (p *StompPublisher) Send() error {
	seq := p.msgSent
	p.msgSent++

	if p.Config.SizeTemplate != nil {
		p.msg = utils.MessageBody(p.Config.Size, p.Config.SizeTemplate, p.Id)
	}

	headers := p.headers[:0]
	headers = append(headers, p.static...)
	headers = p.appendDynamicHeaders(headers, seq)
	p.headers = headers

	startTime := time.Now()
	utils.UpdatePayloadAt(startTime, p.Config.UseMillis, &p.msg)
	err := p.Connection.Send(p.Topic, "", p.msg, headers...)
	latency := time.Since(startTime)
	if err != nil {
		log.Error("message sending failure", "id", p.Id, "error", err)
		return err
	}
	metrics.MessagesPublished.Inc()
	p.latency.Record(latency)
	if log.IsDebug() {
		log.Debug("message sent", "id", p.Id, "destination", p.Topic, "latency", latency)
	}
	return nil
}

func (p *StompPublisher) Stop(reason string) {
	log.Debug("closing publisher connection", "id", p.Id, "reason", reason)
	_ = p.Connection.Disconnect()
}

// buildStaticHeaders pre-builds everything that does not depend on the message:
// the fixed headers, and priority/TTL when their templates have no actions.
func (p *StompPublisher) buildStaticHeaders() {
	cfg := p.Config
	p.static = append(p.static, stomp.SendOpt.Receipt)

	msgDurability := "false"
	if cfg.MessageDurability {
		msgDurability = "true"
	}
	p.static = append(p.static, stomp.SendOpt.Header("persistent", msgDurability))

	if v, ok := utils.StaticTemplateValue(cfg.MessagePriorityTemplate, p.Id); ok {
		p.static = append(p.static, stomp.SendOpt.Header("priority", v))
		p.priorityOK = true
	}
	if v, ok := utils.StaticTemplateValue(cfg.MessageTTLTemplate, p.Id); ok {
		if ttl, err := time.ParseDuration(v); err == nil {
			p.ttlOK = true
			if ttl.Milliseconds() > 0 {
				p.static = append(p.static, stomp.SendOpt.Header("expiration", strconv.FormatInt(ttl.Milliseconds(), 10)))
			}
		}
	}
	for _, v := range cfg.StreamFilterValueSet {
		p.filterOpts = append(p.filterOpts, stomp.SendOpt.Header("x-stream-filter-value", v))
	}
}

// appendDynamicHeaders adds the headers that depend on the message sequence or
// on templates that have to be evaluated for every message.
func (p *StompPublisher) appendDynamicHeaders(headers []func(*frame.Frame) error, seq uint64) []func(*frame.Frame) error {
	cfg := p.Config

	if cfg.MessagePriorityTemplate != nil && !p.priorityOK {
		headers = append(headers, stomp.SendOpt.Header("priority", utils.ExecuteTemplate(cfg.MessagePriorityTemplate, p.Id, seq)))
	}
	if cfg.MessageTTLTemplate != nil && !p.ttlOK {
		ttlStr := utils.ExecuteTemplate(cfg.MessageTTLTemplate, p.Id, seq)
		if ttl, err := time.ParseDuration(ttlStr); err == nil && ttl.Milliseconds() > 0 {
			headers = append(headers, stomp.SendOpt.Header("expiration", strconv.FormatInt(ttl.Milliseconds(), 10)))
		} else if err != nil {
			log.Error("failed to parse template-generated TTL", "value", ttlStr, "error", err)
		}
	}

	if len(p.filterOpts) > 0 {
		headers = append(headers, p.filterOpts[seq%uint64(len(p.filterOpts))])
	}

	if cfg.DetectOutOfOrder || cfg.DetectGaps {
		headers = append(headers,
			stomp.SendOpt.Header(utils.HeaderPublisherID, p.idStr),
			stomp.SendOpt.Header(utils.HeaderSequence, strconv.FormatUint(seq, 10)))
	}
	return headers
}
