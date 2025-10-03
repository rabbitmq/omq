package mqtt

import (
	"context"
	"net/url"
	"strings"
	"text/template"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/utils"
)

type Consumer interface {
	Start(chan bool)
}

type Publisher interface {
	Start(chan bool, chan bool)
}

func NewConsumer(ctx context.Context, cfg config.Config, id int) Consumer {
	if cfg.MqttPublisher.Version == 5 {
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

func publisherTopic(topic string, topicTemplate *template.Template, id int, cfg config.Config) string {
	// Resolve the destination using the generic helper
	topic = utils.ResolveTerminus(topic, topicTemplate, id, cfg)
	// AMQP-1.0 and STOMP allow /exchange/amq.topic/ prefix
	// since MQTT has no concept of exchanges, we need to remove it
	// this should get more flexible in the future
	topic = strings.TrimPrefix(topic, "/exchange/amq.topic/")
	topic = strings.TrimPrefix(topic, "/topic/")
	return topic
}
