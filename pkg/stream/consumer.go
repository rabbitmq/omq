package stream

import (
	"context"
	"crypto/tls"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/utils"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type StreamConsumer struct {
	Id          int
	Environment *stream.Environment
	Consumer    *stream.Consumer
	Topic       string
	Config      config.Config
	ctx         context.Context
}

func NewConsumer(ctx context.Context, cfg config.Config, id int) *StreamConsumer {
	topic := utils.ResolveTerminus(cfg.ConsumeFromTemplate, id)
	topic = strings.TrimPrefix(topic, "/queues/")

	return &StreamConsumer{
		Id:     id,
		Topic:  topic,
		Config: cfg,
		ctx:    ctx,
	}
}

func (c *StreamConsumer) Connect() {
	var uriStr string
	if len(c.Config.ConsumerUri) > 0 {
		idx := 0
		if c.Config.SpreadConnections {
			idx = c.Id % len(c.Config.ConsumerUri)
		}
		uriStr = c.Config.ConsumerUri[idx]
	} else if len(c.Config.Uri) > 0 {
		idx := 0
		if c.Config.SpreadConnections {
			idx = c.Id % len(c.Config.Uri)
		}
		uriStr = c.Config.Uri[idx]
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
			InsecureSkipVerify: c.Config.InsecureSkipTLSVerify,
		})
	}

	if parts := strings.Split(parsedUri.Broker, ":"); len(parts) > 1 {
		if port, err := strconv.Atoi(parts[1]); err == nil {
			opts.SetPort(port)
		}
	}

	env, err := stream.NewEnvironment(opts)
	if err != nil {
		log.Error("failed to create stream environment", "id", c.Id, "error", err.Error())
		os.Exit(1)
	}
	c.Environment = env
}

func (c *StreamConsumer) Start(consumerReady chan bool) {
	c.Connect()
	if c.Environment == nil {
		close(consumerReady)
		return
	}

	var msgsReceived atomic.Int64
	var oooTracker *utils.OutOfOrderTracker
	if c.Config.DetectOutOfOrder || c.Config.DetectGaps {
		oooTracker = utils.NewOutOfOrderTracker()
	}

	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		metrics.MessagesConsumedMetric(0).Inc()
		payload := message.GetData()
		timeSent, latency := utils.CalculateEndToEndLatency(&payload)
		metrics.RecordEndToEndLatency(latency)

		if oooTracker != nil && message.ApplicationProperties != nil {
			pubIDVal := message.ApplicationProperties[utils.HeaderPublisherID]
			seqVal := message.ApplicationProperties[utils.HeaderSequence]
			if pubIDVal != nil && seqVal != nil {
				var pubID int
				var seq uint64
				switch v := pubIDVal.(type) {
				case int64:
					pubID = int(v)
				case int32:
					pubID = int(v)
				case int:
					pubID = v
				}
				switch v := seqVal.(type) {
				case int64:
					seq = uint64(v)
				case int32:
					seq = uint64(v)
				case int:
					seq = uint64(v)
				case uint64:
					seq = v
				}

				result := oooTracker.Check(pubID, seq)
				switch result.Status {
				case utils.SequenceOutOfOrder:
					if c.Config.DetectOutOfOrder {
						metrics.MessagesConsumedOutOfOrderMetric(0).Inc()
						log.Info("out-of-order message",
							"publisher", pubID, "seq", seq, "lastSeq", result.LastSeq, "timeSent", timeSent)
					}
				case utils.SequenceGap:
					if c.Config.DetectGaps {
						metrics.MessagesConsumedGapsMetric(0).Inc()
						log.Info("gap in sequence (missing messages)",
							"publisher", pubID, "seq", seq, "lastSeq", result.LastSeq,
							"missed", seq-result.LastSeq-1, "timeSent", timeSent)
					}
				}
			}
		}

		var consumerLatency time.Duration
		if c.Config.ConsumerLatencyTemplate != nil {
			latencyStr := utils.ExecuteTemplate(c.Config.ConsumerLatencyTemplate, c.Id)
			if parsedLatency, err := time.ParseDuration(latencyStr); err == nil {
				consumerLatency = parsedLatency
			} else {
				log.Error("failed to parse template-generated latency", "value", latencyStr, "error", err)
				os.Exit(1)
			}
		}

		if consumerLatency > 0 {
			log.Debug("consumer latency", "id", c.Id, "latency", consumerLatency)
			time.Sleep(consumerLatency)
		}

		msgsReceived.Add(1)
		log.Debug("message received", "id", c.Id, "topic", c.Topic, "size", len(payload), "latency", latency)
	}

	consumerOpts := stream.NewConsumerOptions().
		SetConsumerName("omq-consumer-" + strconv.Itoa(c.Id)).
		SetCRCCheck(false)

	if c.Config.ConsumerCredits > 0 {
		consumerOpts.SetInitialCredits(int16(c.Config.ConsumerCredits))
	}

	if c.Config.StreamOffset != nil && c.Config.StreamOffset != "" {
		switch v := c.Config.StreamOffset.(type) {
		case string:
			if v == "first" {
				consumerOpts.SetOffset(stream.OffsetSpecification{}.First())
			} else if v == "last" {
				consumerOpts.SetOffset(stream.OffsetSpecification{}.Last())
			} else if v == "next" {
				consumerOpts.SetOffset(stream.OffsetSpecification{}.Next())
			}
		case int64:
			consumerOpts.SetOffset(stream.OffsetSpecification{}.Offset(v))
		case time.Time:
			consumerOpts.SetOffset(stream.OffsetSpecification{}.Timestamp(v.UnixNano() / int64(time.Millisecond)))
		}
	} else {
		consumerOpts.SetOffset(stream.OffsetSpecification{}.Next())
	}

	if c.Config.StreamFilterValues != "" {
		filterValues := strings.Split(c.Config.StreamFilterValues, ",")
		for i, v := range filterValues {
			filterValues[i] = strings.TrimSpace(v)
		}
		postFilter := func(message *amqp.Message) bool {
			var val any
			if message.ApplicationProperties != nil {
				val = message.ApplicationProperties["x-stream-filter-value"]
			}
			if val == nil && message.Annotations != nil {
				val = message.Annotations["x-stream-filter-value"]
			}
			if val == nil {
				return false
			}
			valStr, ok := val.(string)
			if !ok {
				return false
			}
			for _, fv := range filterValues {
				if fv == valStr {
					return true
				}
			}
			return false
		}
		filter := stream.NewConsumerFilter(filterValues, false, postFilter)
		consumerOpts.SetFilter(filter)
	}

	consumer, err := c.Environment.NewConsumer(c.Topic, handleMessages, consumerOpts)
	if err != nil {
		log.Error("failed to create stream consumer", "id", c.Id, "error", err.Error())
		os.Exit(1)
	}
	c.Consumer = consumer

	close(consumerReady)
	log.Info("consumer started", "id", c.Id, "destination", c.Topic)

	for msgsReceived.Load() < int64(c.Config.ConsumeCount) {
		select {
		case <-c.ctx.Done():
			c.Stop("time limit reached")
			return
		case <-time.After(100 * time.Millisecond):
		}
	}

	c.Stop("--cmessages value reached")
}

func (c *StreamConsumer) Stop(reason string) {
	log.Debug("closing consumer connection", "id", c.Id, "reason", reason)
	if c.Consumer != nil {
		_ = c.Consumer.Close()
	}
	if c.Environment != nil {
		_ = c.Environment.Close()
	}
}
