package mqtt

import (
	"context"
	"math"
	"net/url"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"
)

type Consumer interface {
	Start(chan bool)
}

type Publisher interface {
	Start(chan bool, chan bool)
}

func NewConsumer(ctx context.Context, cfg config.Config, id int) Consumer {
	if cfg.MqttConsumer.Version == 5 {
		return NewMqtt5Consumer(ctx, cfg, id)
	} else {
		return NewMqttConsumer(ctx, cfg, id)
	}
}

func NewPublisher(ctx context.Context, cfg config.Config, id int) Publisher {
	if cfg.MqttPublisher.Version == 5 {
		return NewMqtt5Publisher(ctx, cfg, id)
	} else {
		return NewMqttPublisher(ctx, cfg, id)
	}
}

// connectReceiveMaximum caps the CONNECT Receive Maximum. paho otherwise defaults
// it to 65535 and sizes an internal publish buffer to match (~512KB per connection).
func connectReceiveMaximum(n uint16) func(*paho.Connect, *url.URL) (*paho.Connect, error) {
	if n == 0 {
		n = 1
	}
	return func(cp *paho.Connect, _ *url.URL) (*paho.Connect, error) {
		if cp.Properties == nil {
			cp.Properties = &paho.ConnectProperties{}
		}
		receiveMaximum := n
		cp.Properties.ReceiveMaximum = &receiveMaximum
		return cp, nil
	}
}

// inFlightReceiveMaximum clamps a publisher in-flight limit to the MQTT uint16
// Receive Maximum range.
func inFlightReceiveMaximum(maxInFlight int) uint16 {
	if maxInFlight < 1 {
		return 1
	}
	if maxInFlight > int(math.MaxUint16) {
		return math.MaxUint16
	}
	return uint16(maxInFlight)
}

// subscribeWithRetry subscribes to the given topics and, on failure, keeps retrying
// in the background until it succeeds or ctx is cancelled. `subscribed` is a buffered
// (size 1) channel that gets signalled (non-blocking) once the subscription succeeds,
// so it's safe to call this from within OnConnectionUp.
func subscribeWithRetry(ctx context.Context, cm *autopaho.ConnectionManager, subscriptions []paho.SubscribeOptions, subscribed chan struct{}, role string, id int) {
	logSubscribed := func() {
		for _, sub := range subscriptions {
			log.Info(role+" subscribed", "id", id, "topic", sub.Topic)
		}
		select {
		case subscribed <- struct{}{}:
		default:
		}
	}

	if _, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: subscriptions}); err != nil {
		log.Error("failed to subscribe, retrying", "id", id, "error", err)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(config.ReconnectDelay):
				}
				if _, retryErr := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: subscriptions}); retryErr == nil {
					logSubscribed()
					return
				} else {
					log.Error("failed to subscribe, retrying", "id", id, "error", retryErr)
				}
			}
		}()
	} else {
		logSubscribed()
	}
}

func stringsToUrls(connectionStrings []string) []*url.URL {
	var serverUrls []*url.URL
	for _, uri := range connectionStrings {
		parsedUrl, err := url.Parse(uri)
		if err != nil {
			panic(err)
		}
		serverUrls = append(serverUrls, parsedUrl)
	}
	return serverUrls
}

// flushTopic is unsubscribed from by QoS 0 publishers before disconnecting, as
// a way to wait for the broker to process everything published so far.
// It is opt-in (--mqtt-publisher-flush), as the UNSUBSCRIBE shows up in broker
// metrics, which is noise with many publishers.
const (
	flushTopic       = "omq/flush"
	flushTimeout     = 2 * time.Minute
	flushNoticeDelay = time.Second
)

var flushNotice sync.Once

func needsFlush(cfg config.Config) bool {
	return cfg.MqttPublisher.QoS == 0 && cfg.MqttPublisher.FlushOnStop
}

// noticeSlowFlush logs, once per process, that shutdown is waiting for the
// broker, unless the returned timer is stopped first.
func noticeSlowFlush(ctx context.Context) *time.Timer {
	return time.AfterFunc(flushNoticeDelay, func() {
		flushNotice.Do(func() {
			msg := "waiting for the broker to process messages published with QoS 0"
			// once omq is stopping, the signal handler exits on the next ^C
			if ctx.Err() != nil {
				msg += " (press ^C to exit immediately)"
			}
			log.Info(msg)
		})
	})
}

// publisherDestination returns the topic of a publisher or, when the topic
// is resolved per message, its unresolved form for logging.
func publisherDestination(cfg config.Config, id int) (topic string, perMessage bool) {
	if utils.UsesMessageNumber(cfg.PublishToTemplate) {
		return trimTopic(utils.InjectId(cfg.PublishTo, id)), true
	}
	return publisherTopic(cfg.PublishToTemplate, id), false
}

func publisherTopic(topicTemplate *template.Template, id int) string {
	// Resolve the destination using the generic helper
	return trimTopic(utils.ResolveTerminus(topicTemplate, id))
}

// publisherTopicForSeq resolves a topic template that uses the message number
// (%n or {{ .n }}) for the message with the given 0-based sequence number.
func publisherTopicForSeq(topicTemplate *template.Template, id int, seq uint64) string {
	return trimTopic(utils.ExecuteTemplateN(topicTemplate, id, seq+1))
}

func trimTopic(topic string) string {
	// AMQP-1.0 and STOMP allow /exchange/amq.topic/ prefix
	// since MQTT has no concept of exchanges, we need to remove it
	// this should get more flexible in the future
	topic = strings.TrimPrefix(topic, "/exchange/amq.topic/")
	topic = strings.TrimPrefix(topic, "/topic/")
	return topic
}
