package stream

import (
	"context"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/utils"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type StreamPublisher struct {
	Id           int
	Environment  *stream.Environment
	Producer     *stream.Producer
	Topic        string
	Config       config.Config
	ctx          context.Context
	msg          []byte
	msgSent      atomic.Uint64
	sem          chan struct{}
	wg           sync.WaitGroup
	confirmsChan chan bool
}

func NewPublisher(ctx context.Context, cfg config.Config, id int) *StreamPublisher {
	topic := strings.TrimPrefix(cfg.PublishTo, "/queues/")
	topic = utils.InjectId(topic, id)

	return &StreamPublisher{
		Id:     id,
		Topic:  topic,
		Config: cfg,
		ctx:    ctx,
		sem:    make(chan struct{}, cfg.MaxInFlight),
	}
}

func (p *StreamPublisher) Connect() {
	var uriStr string
	if len(p.Config.PublisherUri) > 0 {
		uriStr = p.Config.PublisherUri[p.Id%len(p.Config.PublisherUri)]
	} else if len(p.Config.Uri) > 0 {
		uriStr = p.Config.Uri[p.Id%len(p.Config.Uri)]
	} else {
		uriStr = "rabbitmq-stream://guest:guest@localhost:5552"
	}

	parsedUri := utils.ParseURI(uriStr, "rabbitmq-stream", "5552")

	opts := stream.NewEnvironmentOptions().
		SetHost(strings.Split(parsedUri.Broker, ":")[0]).
		SetPort(5552).
		SetUser(parsedUri.Username).
		SetPassword(parsedUri.Password)

	if parts := strings.Split(parsedUri.Broker, ":"); len(parts) > 1 {
		if port, err := strconv.Atoi(parts[1]); err == nil {
			opts.SetPort(port)
		}
	}

	env, err := stream.NewEnvironment(opts)
	if err != nil {
		log.Error("failed to create stream environment", "id", p.Id, "error", err)
		return
	}
	p.Environment = env

	_ = env.DeclareStream(p.Topic, &stream.StreamOptions{})

	producerOpts := stream.NewProducerOptions()
	producerOpts.SetProducerName("omq-publisher-" + strconv.Itoa(p.Id))

	producer, err := env.NewProducer(p.Topic, producerOpts)
	if err != nil {
		log.Error("failed to create stream producer", "id", p.Id, "error", err)
		return
	}
	p.Producer = producer

	chPublishConfirm := producer.NotifyPublishConfirmation()
	p.confirmsChan = make(chan bool, 1)
	p.handlePublishConfirm(chPublishConfirm, p.Config.PublishCount, p.confirmsChan)
}

func (p *StreamPublisher) handlePublishConfirm(confirms stream.ChannelPublishConfirm, messageCount int, ch chan bool) {
	go func() {
		confirmedCount := 0
		for confirmed := range confirms {
			for _, msg := range confirmed {
				if msg.IsConfirmed() {
					confirmedCount++
					metrics.MessagesConfirmed.Inc()
					select {
					case <-p.sem:
					default:
					}
					if confirmedCount == messageCount {
						ch <- true
						return
					}
				}
			}
		}
	}()
}

func (p *StreamPublisher) Start(publisherReady chan bool, startPublishing chan bool) {
	defer func() {
		if p.Producer != nil {
			_ = p.Producer.Close()
		}
		if p.Environment != nil {
			_ = p.Environment.Close()
		}
	}()

	p.Connect()

	p.msg = utils.MessageBody(p.Config.Size, p.Config.SizeTemplate, p.Id)

	close(publisherReady)

	select {
	case <-p.ctx.Done():
		return
	case <-startPublishing:
		time.Sleep(time.Duration(rand.IntN(1000)) * time.Millisecond)
	}

	log.Info("publisher started", "id", p.Id, "rate", p.Config.Rate, "destination", p.Topic)

	var farewell string
	if p.Config.Rate == 0 {
		<-p.ctx.Done()
		farewell = "context cancelled"
	} else {
		farewell = p.StartPublishing()
	}
	p.Stop(farewell)
}

func (p *StreamPublisher) StartPublishing() string {
	limiter := utils.RateLimiter(p.Config.Rate)

	var msgSent atomic.Int64
	for {
		select {
		case <-p.ctx.Done():
			return "time limit reached"
		default:
			seq := uint64(msgSent.Add(1) - 1)
			if seq >= uint64(p.Config.PublishCount) {
				return "--pmessages value reached"
			}
			if p.Config.Rate > 0 {
				_ = limiter.Wait(p.ctx)
			}
			select {
			case p.sem <- struct{}{}:
			case <-p.ctx.Done():
				return "context cancelled"
			}
			p.Send(seq)
		}
	}
}

func (p *StreamPublisher) Send(seq uint64) {
	if p.Producer == nil {
		return
	}

	var body []byte
	if p.Config.SizeTemplate != nil {
		body = utils.MessageBody(p.Config.Size, p.Config.SizeTemplate, p.Id)
	} else {
		body = make([]byte, len(p.msg))
		copy(body, p.msg)
	}
	utils.UpdatePayload(p.Config.UseMillis, &body)

	msg := amqp.NewMessage(body)

	if p.Config.DetectOutOfOrder || p.Config.DetectGaps {
		props := &amqp.MessageProperties{
			CorrelationID: strconv.FormatUint(seq, 10),
		}
		msg.Properties = props
		msg.ApplicationProperties = map[string]any{
			utils.HeaderPublisherID: int64(p.Id),
			utils.HeaderSequence:    int64(seq),
		}
	}

	startTime := time.Now()
	err := p.Producer.Send(msg)
	if err != nil {
		if !strings.Contains(err.Error(), "use of closed network connection") &&
			!strings.Contains(err.Error(), "context canceled") {
			log.Error("message sending failure", "id", p.Id, "error", err)
		}
		return
	}
	latency := time.Since(startTime)
	metrics.MessagesPublished.Inc()
	metrics.RecordPublishingLatency(latency)
	log.Debug("message sent", "id", p.Id, "destination", p.Topic, "latency", latency)
}

func (p *StreamPublisher) Stop(reason string) {
	if p.confirmsChan != nil {
		select {
		case <-p.confirmsChan:
			log.Debug("all messages confirmed by broker")
		case <-time.After(5 * time.Second):
			log.Error("timeout waiting for publish confirmations")
		}
	}
	log.Debug("closing publisher connection", "id", p.Id, "reason", reason)
}
