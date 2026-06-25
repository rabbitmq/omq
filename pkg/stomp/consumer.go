package stomp

import (
	"context"
	"crypto/tls"
	"math/rand/v2"
	"net"
	"net/url"
	"os"
	"slices"
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
	lastOffset   *int64
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
		lastOffset:   nil,
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

	utils.Retry(c.ctx, config.ReconnectDelay, func() bool {
		uri := utils.NextURI(c.Config.ConsumerUri, &c.whichUri)
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

		log.Debug("connecting to broker", "id", c.Id, "broker", parsedUri.Broker)
		dialer := &net.Dialer{Timeout: dialTimeout}
		var conn *stomp.Conn
		var err error
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
			return false
		}
		c.Connection = conn
		return true
	})
}

func (c *StompConsumer) Subscribe() {
	var sub *stomp.Subscription
	var err error

	if c.Connection != nil {
		ackMode := stomp.AckClient
		if c.Config.Amqp.ConsumeSettled {
			ackMode = stomp.AckAuto
		}
		sub, err = c.Connection.Subscribe(c.Topic, ackMode, c.buildSubscribeOpts(c.Topic)...)
		if err != nil {
			log.Error("subscription failed", "id", c.Id, "queue", c.Topic, "error", err.Error())
			return
		}
		c.Subscription = sub
	}
}

func (c *StompConsumer) Start(consumerReady chan bool) {
	if !utils.Retry(c.ctx, config.ReconnectDelay, func() bool {
		c.Subscribe()
		if c.Subscription != nil {
			return true
		}
		log.Error("subscription failed, reconnecting", "id", c.Id, "destination", c.Topic)
		c.Connect()
		return false
	}) {
		close(consumerReady)
		c.Stop("context cancelled")
		return
	}
	// TODO?
	// Since go-stomp's Subscribe() is asynchronous and doesn't wait for server confirmation,
	// perhaps we should sleep here for a moment?
	// otherwise we may start publishing before the consuemr is ready
	// time.Sleep(50 * time.Millisecond)
	close(consumerReady)
	log.Info("consumer started", "id", c.Id, "destination", c.Topic)

	var oooTracker *utils.OutOfOrderTracker
	if c.Config.DetectOutOfOrder || c.Config.DetectGaps {
		oooTracker = utils.NewOutOfOrderTracker()
	}

	for i := 1; i <= c.Config.ConsumeCount; {
		if c.Subscription == nil {
			if !utils.Retry(c.ctx, config.ReconnectDelay, func() bool {
				c.Subscribe()
				if c.Subscription != nil {
					return true
				}
				log.Error("subscription failed, reconnecting", "id", c.Id, "destination", c.Topic)
				c.Connect()
				return false
			}) {
				c.Stop("context cancelled")
				return
			}
		}

		select {
		case msg := <-c.Subscription.C:
			if msg.Err != nil {
				log.Error("failed to receive a message", "id", c.Id, "c.Topic", c.Topic, "error", msg.Err)
				c.Connect()
				c.Subscription = nil // Reset subscription so the outer loop retries with RetryDelay
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

			if c.Config.StreamOffset != "" {
				if offsetStr := msg.Header.Get("x-stream-offset"); offsetStr != "" {
					if val, err := strconv.ParseInt(offsetStr, 10, 64); err == nil {
						c.lastOffset = &val
					}
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

			if c.Config.Amqp.ConsumeSettled {
				metrics.MessagesConsumedMetric(priority).Inc()
				i++
				log.Debug("message auto-acknowledged", "id", c.Id, "terminus", c.Topic)
			} else {
				outcome, err := c.outcome(msg, priority)
				if err != nil {
					log.Error("failed to "+outcome+" message", "id", c.Id, "destination", c.Topic, "error", err)
				} else {
					metrics.MessagesConsumedMetric(priority).Inc()
					i++
					log.Debug("message "+utils.PastTense(outcome), "id", c.Id, "terminus", c.Topic)
				}
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
		go func(sub *stomp.Subscription) {
			for range sub.C {
			}
		}(c.Subscription)
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

func (c *StompConsumer) outcome(msg *stomp.Message, priority int) (string, error) {
	requeuePriorityMatch := len(c.Config.RequeueWhenPriority) == 0 || slices.Contains(c.Config.RequeueWhenPriority, priority)
	discardPriorityMatch := len(c.Config.DiscardWhenPriority) == 0 || slices.Contains(c.Config.DiscardWhenPriority, priority)

	// don't generate random numbers if not necessary
	if c.Config.RequeueRate == 0 && c.Config.DiscardRate == 0 {
		// No rate-based logic, just check priority filters for 100% requeue/discard
		if len(c.Config.RequeueWhenPriority) > 0 && slices.Contains(c.Config.RequeueWhenPriority, priority) {
			return "nack-requeue", c.Connection.Nack(msg)
		}
		if len(c.Config.DiscardWhenPriority) > 0 && slices.Contains(c.Config.DiscardWhenPriority, priority) {
			// In STOMP, we discard by acknowledging the message (removing it from queue)
			return "ack-discard", c.Connection.Ack(msg)
		}
		return "acknowledge", c.Connection.Ack(msg)
	}

	n := rand.IntN(100)
	if requeuePriorityMatch && n < c.Config.RequeueRate {
		return "nack-requeue", c.Connection.Nack(msg)
	} else if discardPriorityMatch && n < c.Config.RequeueRate+c.Config.DiscardRate {
		return "ack-discard", c.Connection.Ack(msg)
	}
	return "acknowledge", c.Connection.Ack(msg)
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

func (c *StompConsumer) buildSubscribeOpts(destination string) []func(*frame.Frame) error {
	var subscribeOpts []func(*frame.Frame) error

	var offsetHeader string
	if c.lastOffset != nil {
		log.Info("reconnecting stream consumer", "id", c.Id, "offset", *c.lastOffset+1)
		offsetHeader = "offset=" + strconv.FormatInt(*c.lastOffset+1, 10)
	} else {
		offsetHeader = offsetToString(c.Config.StreamOffset)
	}

	subscribeOpts = append(subscribeOpts,
		stomp.SubscribeOpt.Header("x-stream-offset", offsetHeader),
		stomp.SubscribeOpt.Header("prefetch-count", strconv.Itoa(c.Config.ConsumerCredits)))

	if c.Config.ConsumerPriorityTemplate != nil {
		priorityStr := utils.ExecuteTemplate(c.Config.ConsumerPriorityTemplate, c.Id)
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-priority", priorityStr))
	}

	if c.Config.QueueDurability != config.None {
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("durable", "true"),
			stomp.SubscribeOpt.Header("auto-delete", "false"),
		)
	} else {
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("durable", "false"),
			stomp.SubscribeOpt.Header("auto-delete", "true"),
		)
	}

	if c.Config.StreamFilterValues != "" {
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-stream-filter", c.Config.StreamFilterValues))
	}

	switch c.Config.Queues {
	case config.Classic:
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-queue-type", "classic"))
	case config.Quorum:
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-queue-type", "quorum"))
	case config.Stream:
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-queue-type", "stream"))
	case config.JMS:
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-queue-type", "jms"))
	case config.Delayed:
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("x-queue-type", "delayed"))
	case config.Exclusive:
		subscribeOpts = append(subscribeOpts,
			stomp.SubscribeOpt.Header("exclusive", "true"))
	case config.Predeclared:
		// For /topic/ destinations with transient queues (QueueDurability == None),
		// use exclusive to avoid deprecated transient non-exclusive queues
		if strings.HasPrefix(destination, "/topic/") && c.Config.QueueDurability == config.None {
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
	if i, ok := offset.(int64); ok {
		return "offset=" + strconv.FormatInt(i, 10)
	}
	return "next"
}
