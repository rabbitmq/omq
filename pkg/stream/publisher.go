package stream

import (
	"context"
	"crypto/tls"
	"math/rand/v2"
	"os"
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
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type StreamPublisher struct {
	Id               int
	Environment      *stream.Environment
	Producer         *ha.ReliableProducer
	SuperProducer    *ha.ReliableSuperStreamProducer
	Topic            string
	Config           config.Config
	ctx              context.Context
	msg              []byte
	sem              chan struct{}
	publishTimes     map[int64]time.Time
	publishTimesLock sync.Mutex
	basePublishingId int64
}

func NewPublisher(ctx context.Context, cfg config.Config, id int) *StreamPublisher {
	topic := utils.ResolveTerminus(cfg.PublishToTemplate, id)
	topic = strings.TrimPrefix(topic, "/queues/")

	return &StreamPublisher{
		Id:           id,
		Topic:        topic,
		Config:       cfg,
		ctx:          ctx,
		sem:          make(chan struct{}, cfg.MaxInFlight),
		publishTimes: make(map[int64]time.Time),
	}
}

func (p *StreamPublisher) Connect() {
	var uriStr string
	if len(p.Config.PublisherUri) > 0 {
		idx := 0
		if p.Config.SpreadConnections {
			idx = p.Id % len(p.Config.PublisherUri)
		}
		uriStr = p.Config.PublisherUri[idx]
	} else if len(p.Config.Uri) > 0 {
		idx := 0
		if p.Config.SpreadConnections {
			idx = p.Id % len(p.Config.Uri)
		}
		uriStr = p.Config.Uri[idx]
	} else {
		uriStr = "rabbitmq-stream://guest:guest@localhost:5552"
	}

	defaultPort := "5552"
	if strings.HasPrefix(uriStr, "rabbitmq-stream+tls") {
		defaultPort = "5551"
	}
	parsedUri := utils.ParseURI(uriStr, "rabbitmq-stream", defaultPort)

	isTLS := parsedUri.Scheme == "rabbitmq-stream+tls"
	opts := stream.NewEnvironmentOptions().
		SetHost(strings.Split(parsedUri.Broker, ":")[0]).
		SetUser(parsedUri.Username).
		SetPassword(parsedUri.Password).
		IsTLS(isTLS)

	if isTLS {
		opts.SetTLSConfig(&tls.Config{
			InsecureSkipVerify: p.Config.InsecureSkipTLSVerify,
		})
	}

	if parts := strings.Split(parsedUri.Broker, ":"); len(parts) > 1 {
		if port, err := strconv.Atoi(parts[1]); err == nil {
			opts.SetPort(port)
		}
	}

	env, err := stream.NewEnvironment(opts)
	if err != nil {
		log.Error("failed to create stream environment", "id", p.Id, "error", err.Error())
		os.Exit(1)
	}
	p.Environment = env

	producerName := "omq-publisher-" + strconv.Itoa(p.Id)
	producerOpts := stream.NewProducerOptions()
	producerOpts.SetProducerName(producerName)

	if len(p.Config.StreamFilterValueSet) > 0 {
		producerOpts.SetFilter(stream.NewProducerFilter(func(message message.StreamMessage) string {
			props := message.GetApplicationProperties()
			if props != nil {
				val := props["x-stream-filter-value"]
				if val != nil {
					if str, ok := val.(string); ok {
						return str
					}
				}
			}
			return ""
		}))
	}

	confirmHandler := func(confirms []*stream.ConfirmationStatus) {
		for _, msg := range confirms {
			publishingId := msg.GetPublishingId()
			if msg.IsConfirmed() {
				metrics.MessagesConfirmed.Inc()

				p.publishTimesLock.Lock()
				startTime, exists := p.publishTimes[publishingId]
				if exists {
					delete(p.publishTimes, publishingId)
				}
				p.publishTimesLock.Unlock()

				if exists {
					latency := time.Since(startTime)
					metrics.RecordPublishingLatency(latency)
					log.Debug("message confirmed", "id", p.Id, "publishing_id", publishingId, "latency", latency)
				}

				select {
				case <-p.sem:
				default:
				}
			} else {
				p.publishTimesLock.Lock()
				delete(p.publishTimes, publishingId)
				p.publishTimesLock.Unlock()
				select {
				case <-p.sem:
				default:
				}
				log.Debug("message not confirmed by the broker", "id", p.Id, "publishing_id", publishingId)
			}
		}
	}

	if p.Config.StreamSuperStream {
		if p.Config.Queues != config.Predeclared {
			partitions := p.Config.StreamSuperStreamPartitions
			if partitions <= 0 {
				partitions = 3
			}
			if err := env.DeclareSuperStream(p.Topic, stream.NewPartitionsOptions(partitions)); err != nil {
				log.Error("failed to declare super stream", "id", p.Id, "error", err.Error())
				os.Exit(1)
			}
		}

		superOpts := stream.NewSuperStreamProducerOptions(
			stream.NewHashRoutingStrategy(func(msg message.StreamMessage) string {
				return strconv.FormatInt(msg.GetPublishingId(), 10)
			}),
		)

		superProducer, err := ha.NewReliableSuperStreamProducer(env, p.Topic, superOpts,
			func(confirms []*stream.PartitionPublishConfirm) {
				for _, partConfirm := range confirms {
					confirmHandler(partConfirm.ConfirmationStatus)
				}
			})
		if err != nil {
			log.Error("failed to create super stream producer", "id", p.Id, "error", err.Error())
			os.Exit(1)
		}
		p.SuperProducer = superProducer
		return
	}

	lastId, err := env.QuerySequence(producerName, p.Topic)
	if err != nil {
		log.Debug("failed to query last publishing ID, starting from 0", "id", p.Id, "error", err.Error())
		p.basePublishingId = 0
	} else {
		p.basePublishingId = lastId
		log.Debug("queried last publishing ID", "id", p.Id, "lastId", lastId)
	}

	producer, err := ha.NewReliableProducer(env, p.Topic, producerOpts, confirmHandler)
	if err != nil {
		log.Error("failed to create stream producer", "id", p.Id, "error", err.Error())
		os.Exit(1)
	}
	p.Producer = producer
}

func (p *StreamPublisher) Start(publisherReady chan bool, startPublishing chan bool) {
	defer func() {
		if p.SuperProducer != nil {
			_ = p.SuperProducer.Close()
		}
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
	if p.Producer == nil && p.SuperProducer == nil {
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
	publishingId := p.basePublishingId + 1 + int64(seq)
	msg.SetPublishingId(publishingId)

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

	if len(p.Config.StreamFilterValueSet) > 0 {
		filterValue := p.Config.StreamFilterValueSet[seq%uint64(len(p.Config.StreamFilterValueSet))]
		if msg.ApplicationProperties == nil {
			msg.ApplicationProperties = map[string]any{
				"x-stream-filter-value": filterValue,
			}
		} else {
			msg.ApplicationProperties["x-stream-filter-value"] = filterValue
		}
	}

	startTime := time.Now()
	p.publishTimesLock.Lock()
	p.publishTimes[publishingId] = startTime
	p.publishTimesLock.Unlock()

	var sendErr error
	if p.Config.StreamSuperStream {
		sendErr = p.SuperProducer.Send(msg)
	} else {
		sendErr = p.Producer.Send(msg)
	}
	if sendErr != nil {
		p.publishTimesLock.Lock()
		delete(p.publishTimes, publishingId)
		p.publishTimesLock.Unlock()
		select {
		case <-p.sem:
		default:
		}
		if !strings.Contains(sendErr.Error(), "use of closed network connection") &&
			!strings.Contains(sendErr.Error(), "context canceled") {
			log.Error("message sending failure", "id", p.Id, "error", sendErr)
		}
		return
	}
	metrics.MessagesPublished.Inc()
	log.Debug("message sent", "id", p.Id, "destination", p.Topic)
}

func (p *StreamPublisher) Stop(reason string) {
	log.Debug("closing publisher connection", "id", p.Id, "reason", reason)
	limit := time.Now().Add(5 * time.Second)
	for time.Now().Before(limit) {
		p.publishTimesLock.Lock()
		empty := len(p.publishTimes) == 0
		p.publishTimesLock.Unlock()
		if empty {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}
