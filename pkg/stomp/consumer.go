package stomp

import (
	"context"
	"crypto/tls"
	"net"
	"os"
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

type StompConsumer struct {
	Id           int
	Connection   *stomp.Conn
	Subscription *stomp.Subscription
	Topic        string
	Config       config.Config
	ctx          context.Context
	whichUri     int
}

func NewConsumer(ctx context.Context, cfg config.Config, id int) *StompConsumer {
	consumer := &StompConsumer{
		Id:           id,
		Connection:   nil,
		Subscription: nil,
		Topic:        utils.ResolveTerminus(cfg.ConsumeFromTemplate, id),
		Config:       cfg,
		ctx:          ctx,
		whichUri:     0,
	}

	if cfg.SpreadConnections {
		consumer.whichUri = id % len(cfg.ConsumerUri)
	}

	consumer.Connect()

	return consumer
}

func (c *StompConsumer) Connect() {
	if c.Subscription != nil {
		_ = c.Subscription.Unsubscribe()
	}
	if c.Connection != nil {
		_ = c.Connection.Disconnect()
	}
	c.Subscription = nil
	c.Connection = nil

	for c.Connection == nil {
		uri := utils.NextURI(c.Config.ConsumerUri, &c.whichUri)
		useTLS := strings.HasPrefix(uri, "stomp+ssl://") || strings.HasPrefix(uri, "stomps://")
		parsedUri := utils.ParseURI(uri, "stomp", "61613")

		var o = []func(*stomp.Conn) error{
			stomp.ConnOpt.Login(parsedUri.Username, parsedUri.Password),
			stomp.ConnOpt.Host("/"), // TODO
		}

		log.Debug("connecting to broker", "id", c.Id, "broker", parsedUri.Broker)
		var conn *stomp.Conn
		var err error
		dialer := &net.Dialer{Timeout: dialTimeout}
		if useTLS {
			netConn, tlsErr := tls.DialWithDialer(dialer, "tcp", parsedUri.Broker, &tls.Config{
				InsecureSkipVerify: c.Config.InsecureSkipTLSVerify,
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
			log.Error("consumer connection failed", "id", c.Id, "error", err.Error())
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(config.ReconnectDelay):
				continue
			}
		} else {
			c.Connection = conn
		}
	}
}

func (c *StompConsumer) Subscribe() {
	var sub *stomp.Subscription
	var err error

	if c.Connection != nil {
		sub, err = c.Connection.Subscribe(c.Topic, stomp.AckClient, buildSubscribeOpts(c.Config, c.Topic, c.Id)...)
		if err != nil {
			log.Error("subscription failed", "id", c.Id, "queue", c.Topic, "error", err.Error())
			return
		}
		c.Subscription = sub
	}
}

func (c *StompConsumer) Start(consumerReady chan bool) {
	c.Subscribe()
	close(consumerReady)
	log.Info("consumer started", "id", c.Id, "destination", c.Topic)

	var oooTracker *utils.OutOfOrderTracker
	if c.Config.DetectOutOfOrder || c.Config.DetectGaps {
		oooTracker = utils.NewOutOfOrderTracker()
	}

	for i := 1; i <= c.Config.ConsumeCount; {
		for c.Subscription == nil {
			select {
			case <-c.ctx.Done():
				c.Stop("context cancelled")
				return
			default:
				c.Subscribe()
			}
		}

		select {
		case msg := <-c.Subscription.C:
			if msg.Err != nil {
				log.Error("failed to receive a message", "id", c.Id, "c.Topic", c.Topic, "error", msg.Err)
				c.Connect()
				continue
			}

			timeSent, latency := utils.CalculateEndToEndLatency(&msg.Body)
			metrics.RecordEndToEndLatency(latency)

			if val, err := strconv.ParseInt(msg.Header.Get("x-delay-processed"), 10, 64); err == nil {
				if delayAccuracy, ok := utils.CalculateDelayAccuracy(&msg.Body, val); ok {
					metrics.RecordDelayAccuracy(delayAccuracy)
				}
			} else if val, err := strconv.ParseInt(msg.Header.Get("x-opt-delivery-time"), 10, 64); err == nil {
				if delayAccuracy, ok := utils.CalculateDelayAccuracyFromDeliveryTime(val); ok {
					metrics.RecordDelayAccuracy(delayAccuracy)
				}
			}

			priority, _ := strconv.Atoi(msg.Header.Get("priority"))

			if oooTracker != nil {
				if pubID, seq, ok := extractStompOrderingInfo(msg); ok {
					result := oooTracker.Check(pubID, seq)
					switch result.Status {
					case utils.SequenceOutOfOrder:
						if c.Config.DetectOutOfOrder {
							metrics.MessagesConsumedOutOfOrderMetric(priority).Inc()
							log.Info("out-of-order message",
								"publisher", pubID, "seq", seq, "lastSeq", result.LastSeq, "timeSent", timeSent)
						}
					case utils.SequenceGap:
						if c.Config.DetectGaps {
							metrics.MessagesConsumedGapsMetric(priority).Inc()
							log.Info("gap in sequence (missing messages)",
								"publisher", pubID, "seq", seq, "lastSeq", result.LastSeq,
								"missed", seq-result.LastSeq-1, "timeSent", timeSent)
						}
					}
				}
			}

			log.Debug("message received", "id", c.Id, "destination", c.Topic, "size", len(msg.Body), "ack required", msg.ShouldAck(), "priority", priority, "latency", latency)

			// Handle consumer latency (always use template)
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

			err := c.Connection.Ack(msg)
			if err != nil {
				log.Error("message NOT acknowledged", "id", c.Id, "destination", c.Topic)
			} else {
				metrics.MessagesConsumedMetric(priority).Inc()
				i++
				log.Debug("message acknowledged", "id", c.Id, "terminus", c.Topic)
			}
		case <-c.ctx.Done():
			c.Stop("time limit reached")
			return
		}

	}

	c.Stop("--cmessages value reached")
	log.Debug("consumer finished", "id", c.Id)
}

func (c *StompConsumer) Stop(reason string) {
	if c.Subscription != nil {
		err := c.Subscription.Unsubscribe()
		if err != nil {
			log.Info("failed to unsubscribe", "id", c.Id, "error", err.Error())
		}
	}
	if c.Connection != nil {
		err := c.Connection.Disconnect()
		if err != nil {
			log.Info("failed to disconnect", "id", c.Id, "error", err.Error())
		}
	}
	log.Debug("consumer stopped", "id", c.Id, "reason", reason)
}

func extractStompOrderingInfo(msg *stomp.Message) (int, uint64, bool) {
	pubIDStr := msg.Header.Get(utils.HeaderPublisherID)
	seqStr := msg.Header.Get(utils.HeaderSequence)
	if pubIDStr == "" || seqStr == "" {
		return 0, 0, false
	}
	pubID, err1 := strconv.Atoi(pubIDStr)
	seq, err2 := strconv.ParseUint(seqStr, 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return pubID, seq, true
}

func buildSubscribeOpts(cfg config.Config, destination string, id int) []func(*frame.Frame) error {
	var subscribeOpts []func(*frame.Frame) error

	subscribeOpts = append(subscribeOpts,
		stomp.SubscribeOpt.Header("x-stream-offset", offsetToString(cfg.StreamOffset)),
		stomp.SubscribeOpt.Header("prefetch-count", strconv.Itoa(cfg.ConsumerCredits)))

	if cfg.ConsumerPriorityTemplate != nil {
		priorityStr := utils.ExecuteTemplate(cfg.ConsumerPriorityTemplate, id)
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-priority", priorityStr))
	}

	if cfg.QueueDurability != config.None {
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("durable", "true"),
			stomp.SubscribeOpt.Header("auto-delete", "false"),
		)
	}

	if cfg.StreamFilterValues != "" {
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-stream-filter", cfg.StreamFilterValues))
	}

	switch cfg.Queues {
	case config.Classic:
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-queue-type", "classic"))
	case config.Quorum:
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-queue-type", "quorum"))
	case config.Stream:
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-queue-type", "stream"))
	case config.Exclusive:
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("exclusive", "true"))
	case config.Predeclared:
		// For /topic/ destinations with transient queues (QueueDurability == None),
		// use exclusive to avoid deprecated transient non-exclusive queues
		if strings.HasPrefix(destination, "/topic/") && cfg.QueueDurability == config.None {
			subscribeOpts = append(subscribeOpts,
				stomp.SubscribeOpt.Header("exclusive", "true"))
		}
	}

	return subscribeOpts
}

func offsetToString(offset any) string {
	if s, ok := offset.(string); ok {
		return s
	}
	if t, ok := offset.(time.Time); ok {
		return "timestamp=" + strconv.FormatInt(t.Unix(), 10)
	}
	if i, ok := offset.(int); ok {
		return "offset=" + strconv.Itoa(i)
	}
	return "next"
}
