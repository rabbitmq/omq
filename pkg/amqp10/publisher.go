package amqp10

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/rabbitmq/omq/pkg/metrics"

	"github.com/Azure/go-amqp"
)

type Amqp10Publisher struct {
	Id          int
	Connection  *amqp.Conn
	Session     *amqp.Session
	Sender      *amqp.Sender
	Terminus    string
	Config      config.Config
	msg         []byte
	whichUri    int
	msgSent     atomic.Uint64
	settlements chan amqp.Settlement
	sem         chan struct{}
	done        chan struct{}
	wg          sync.WaitGroup
	ctx         context.Context
}

func NewPublisher(ctx context.Context, cfg config.Config, id int) *Amqp10Publisher {
	publisher := &Amqp10Publisher{
		Id:         id,
		Connection: nil,
		Sender:     nil,
		Config:     cfg,
		Terminus:   utils.ResolveTerminus(cfg.PublishTo, cfg.PublishToTemplate, id, cfg),
		whichUri:   0,
		ctx:        ctx,
	}

	if !cfg.Amqp.SendSettled {
		publisher.settlements = make(chan amqp.Settlement, cfg.MaxInFlight)
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
			PipelineDepth: 10,
			ContainerID:   utils.InjectId(p.Config.PublisherId, p.Id),
			SASLType:      amqp.SASLTypeAnonymous(),
			HostName:      vhost,
			TLSConfig: &tls.Config{
				ServerName: hostname,
			},
		})

		if err != nil {
			log.Error("connection failed", "id", p.Id, "error", err.Error())
			select {
			case <-time.After(1 * time.Second):
				continue
			case <-p.ctx.Done():
				return
			}
		} else {
			log.Debug("publisher connected", "id", p.Id, "uri", uri)
			p.Connection = conn
		}
	}

	for p.Session == nil {
		session, err := p.Connection.NewSession(context.TODO(), nil)
		if err != nil {
			log.Error("publisher failed to create a session", "id", p.Id, "error", err.Error())
			time.Sleep(1 * time.Second)
			p.Connect()
		} else {
			p.Session = session
		}
	}

	p.CreateSender()
}

func (p *Amqp10Publisher) CreateSender() {
	// TODO do we need this?
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
			TargetDurability: durability,
			Settlements:      p.settlements,
		})
		if err != nil {
			log.Error("publisher failed to create a sender", "id", p.Id, "error", err.Error())
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(1 * time.Second):
				p.Connect()
			}
		} else {
			p.Sender = sender
		}
	}
}

func (p *Amqp10Publisher) Start(publisherReady chan bool, startPublishing chan bool) {
	p.msg = utils.MessageBody(p.Config.Size, p.Config.SizeTemplate, p.Id)

	close(publisherReady)

	select {
	case <-p.ctx.Done():
		return
	case <-startPublishing:
		// short random delay to avoid all publishers publishing at the same time
		time.Sleep(time.Duration(rand.IntN(1000)) * time.Millisecond)
	}

	log.Info("publisher started", "id", p.Id, "rate", utils.Rate(p.Config.Rate), "destination", p.Terminus)
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

func (p *Amqp10Publisher) StartPublishing() string {
	if p.Config.Amqp.SendSettled {
		return p.publishSettled()
	}
	return p.publishUnsettled()
}

func (p *Amqp10Publisher) publishSettled() string {
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
			msg := p.prepareMessage()
			startTime := time.Now()
			if p.Sender == nil {
				p.Connect()
				continue
			}
			err := p.Sender.Send(context.TODO(), msg, nil)
			latency := time.Since(startTime)
			log.Debug("message sent", "id", p.Id, "destination", p.Terminus, "latency", latency, "appProps", msg.ApplicationProperties)
			if err = p.handleSendErrors(p.ctx, err); err != nil {
				p.Connect()
				continue
			}
			metrics.MessagesPublished.Inc()
			metrics.MessagesConfirmed.Inc()
			metrics.PublishingLatency.Update(latency.Seconds())
		}
	}
}

func (p *Amqp10Publisher) publishUnsettled() string {
	limiter := utils.RateLimiter(p.Config.Rate)
	p.sem = make(chan struct{}, p.Config.MaxInFlight)
	p.done = make(chan struct{})
	var ptMu sync.Mutex
	publishTimes := make(map[uint64]time.Time, p.Config.MaxInFlight)

	p.wg.Go(func() {
		for {
			select {
			case s := <-p.settlements:
				p.handleSettlement(s, &ptMu, publishTimes)
				<-p.sem
			case <-p.done:
				p.drainSettlements(&ptMu, publishTimes)
				return
			case <-p.ctx.Done():
				return
			}
		}
	})

	var msgSent atomic.Int64
	for {
		select {
		case <-p.ctx.Done():
			close(p.done)
			return "context cancelled"
		default:
			if msgSent.Add(1) > int64(p.Config.PublishCount) {
				close(p.done)
				return "--pmessages value reached"
			}
			if p.Config.Rate > 0 {
				_ = limiter.Wait(p.ctx)
			}
			select {
			case p.sem <- struct{}{}:
			case <-p.ctx.Done():
				close(p.done)
				return "context cancelled"
			}
			msg := p.prepareMessage()
			if p.Sender == nil {
				<-p.sem
				p.Connect()
				continue
			}
			startTime := time.Now()
			receipt, err := p.Sender.SendWithReceipt(p.ctx, msg, nil)
			if err != nil {
				<-p.sem
				select {
				case <-p.ctx.Done():
					close(p.done)
					return "context cancelled"
				default:
					if err = p.handleSendErrors(p.ctx, err); err != nil {
						p.Connect()
					}
					continue
				}
			}
			tag := binary.BigEndian.Uint64(receipt.DeliveryTag())
			ptMu.Lock()
			publishTimes[tag] = startTime
			ptMu.Unlock()
		}
	}
}

// drainSettlements waits for remaining in-flight settlements after the send
// loop has finished. It returns immediately once all in-flight messages are
// settled, or after 5 seconds of no new settlements arriving.
func (p *Amqp10Publisher) drainSettlements(ptMu *sync.Mutex, publishTimes map[uint64]time.Time) {
	if len(p.sem) == 0 {
		return
	}
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case s := <-p.settlements:
			p.handleSettlement(s, ptMu, publishTimes)
			<-p.sem
			if len(p.sem) == 0 {
				return
			}
			if !timeout.Stop() {
				<-timeout.C
			}
			timeout.Reset(5 * time.Second)
		case <-timeout.C:
			log.Info("timed out waiting for remaining settlements", "id", p.Id, "inFlight", len(p.sem))
			return
		}
	}
}

// handleSendErrors returns an error if the error suggests we should reconnect
// (this is native, but amqp-go-client should handle this better in the future)
// otherwise we log an error but return nil to keep publishing
func (p *Amqp10Publisher) handleSendErrors(ctx context.Context, err error) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		var connErr *amqp.ConnError
		var linkErr *amqp.LinkError
		if errors.As(err, &connErr) {
			log.Error("publisher connection failure; reconnecting...", "id", p.Id, "error", connErr.Error())
			return err
		}

		if errors.As(err, &linkErr) {
			log.Error("publisher link failure; reconnecting...", "id", p.Id, "error", linkErr.Error())
			return err
		}

		if err != nil {
			log.Error("message sending failure", "id", p.Id, "error", err)
		}

		return nil
	}
}

func (p *Amqp10Publisher) handleSettlement(s amqp.Settlement, ptMu *sync.Mutex, publishTimes map[uint64]time.Time) {
	var latency time.Duration
	tag := binary.BigEndian.Uint64(s.DeliveryTag)
	ptMu.Lock()
	if published, ok := publishTimes[tag]; ok {
		delete(publishTimes, tag)
		latency = time.Since(published)
	}
	ptMu.Unlock()
	log.Debug("message settled", "id", p.Id, "destination", p.Terminus, "latency", latency)
	switch stateType := s.DeliveryState.(type) {
	case *amqp.StateAccepted:
		metrics.MessagesPublished.Inc()
		metrics.MessagesConfirmed.Inc()
		metrics.PublishingLatency.Update(latency.Seconds())
	case *amqp.StateModified:
		log.Debug("server requires modifications to accept this message", "state", stateType)
	case *amqp.StateReceived:
		log.Debug("message received but not processed by the broker", "state", stateType)
	case *amqp.StateRejected:
		if stateType.Error != nil {
			log.Info("message rejected by the broker", "state", stateType.Error)
		}
	case *amqp.StateReleased:
		log.Debug("message released by the broker", "state", stateType)
	}
}

func (p *Amqp10Publisher) Stop(reason string) {
	p.wg.Wait()
	log.Debug("closing publisher connection", "id", p.Id, "reason", reason)
	if p.Sender != nil {
		_ = p.Sender.Close(context.Background())
	}
	if p.Session != nil {
		_ = p.Session.Close(context.Background())
	}
	if p.Connection != nil {
		_ = p.Connection.Close()
	}
}

// maybeConvertToInt converts string values to integers if they look like integers
func maybeConvertToInt(value string) any {
	// Try to parse as int64 first
	if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
		return intVal
	}
	// If it's not an integer, return as string
	return value
}

func (p *Amqp10Publisher) prepareMessage() *amqp.Message {
	seq := p.msgSent.Add(1) - 1

	var body []byte
	if p.Config.SizeTemplate != nil {
		body = utils.MessageBody(p.Config.Size, p.Config.SizeTemplate, p.Id)
	} else {
		body = make([]byte, len(p.msg))
		copy(body, p.msg)
	}
	utils.UpdatePayload(p.Config.UseMillis, &body)
	msg := amqp.NewMessage(body)
	msg.Properties = &amqp.MessageProperties{}

	// Handle template-based application properties
	if len(p.Config.Amqp.AppPropertyTemplates) > 0 {
		if msg.ApplicationProperties == nil {
			msg.ApplicationProperties = make(map[string]any)
		}
		for key, tmpl := range p.Config.Amqp.AppPropertyTemplates {
			stringValue := utils.ExecuteTemplate(tmpl, p.Id, seq)
			msg.ApplicationProperties[key] = maybeConvertToInt(stringValue)
		}
	}

	// Handle template-based message annotations
	if len(p.Config.Amqp.MsgAnnotationTemplates) > 0 {
		if msg.Annotations == nil {
			msg.Annotations = make(map[any]any)
		}
		for key, tmpl := range p.Config.Amqp.MsgAnnotationTemplates {
			stringValue := utils.ExecuteTemplate(tmpl, p.Id, seq)
			msg.Annotations[key] = maybeConvertToInt(stringValue)
		}
	}

	if len(p.Config.Amqp.Subjects) > 0 {
		msg.Properties.Subject = &p.Config.Amqp.Subjects[seq%uint64(len(p.Config.Amqp.Subjects))]
	}

	if len(p.Config.Amqp.To) > 0 {
		msg.Properties.To = &p.Config.Amqp.To[seq%uint64(len(p.Config.Amqp.To))]
	}

	if p.Config.StreamFilterValueSet != "" {
		msg.Annotations = amqp.Annotations{"x-stream-filter-value": p.Config.StreamFilterValueSet}
	}

	msg.Header = &amqp.MessageHeader{}
	msg.Header.Durable = p.Config.MessageDurability

	// Handle message priority (always use template)
	if p.Config.MessagePriorityTemplate != nil {
		priorityStr := utils.ExecuteTemplate(p.Config.MessagePriorityTemplate, p.Id, seq)
		if priority, err := strconv.ParseUint(priorityStr, 10, 8); err == nil {
			msg.Header.Priority = uint8(priority)
		} else {
			log.Error("failed to parse template-generated priority", "value", priorityStr, "error", err)
			os.Exit(1)
		}
	}
	if p.Config.MessageTTL.Microseconds() > 0 {
		msg.Header.TTL = p.Config.MessageTTL
	}
	return msg
}
