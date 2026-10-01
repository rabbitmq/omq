package mqtt

import (
	"context"
	"math"
	"net/url"
	"strings"
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

// subscribeWithRetry subscribes to the given topics and, on failure, keeps retrying
// in the background until it succeeds or ctx is cancelled. `subscribed` is a buffered
// (size 1) channel that gets signalled (non-blocking) once the subscription succeeds,
// so it's safe to call this from within OnConnectionUp.
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

func publisherTopic(topicTemplate *template.Template, id int) string {
	// Resolve the destination using the generic helper
	topic := utils.ResolveTerminus(topicTemplate, id)
	// AMQP-1.0 and STOMP allow /exchange/amq.topic/ prefix
	// since MQTT has no concept of exchanges, we need to remove it
	// this should get more flexible in the future
	topic = strings.TrimPrefix(topic, "/exchange/amq.topic/")
	topic = strings.TrimPrefix(topic, "/topic/")
	return topic
}
