package common

import (
	"fmt"

	"github.com/rabbitmq/omq/pkg/amqp10_client"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/mqtt_client"
	"github.com/rabbitmq/omq/pkg/stomp_client"
)

type Publisher interface {
	Start()
}

type Consumer interface {
	Start(chan bool)
}

type Protocol int

const (
	AMQP Protocol = iota
	STOMP
	MQTT
)

func NewPublisher(protocol Protocol, cfg config.Config, id int) (Publisher, error) {
	switch protocol {
	case AMQP:
		p := amqp10_client.NewPublisher(cfg, id)
		if p == nil {
			return nil, fmt.Errorf("Failed to create an AMQP-1.0 publisher")
		}
		return p, nil
	case STOMP:
		p := stomp_client.NewPublisher(cfg, id)
		if p == nil {
			return nil, fmt.Errorf("Failed to create a STOMP publisher")
		}
		return p, nil
	case MQTT:
		p := mqtt_client.NewPublisher(cfg, id)
		if p == nil {
			return nil, fmt.Errorf("Failed to create an MQTT publisher")
		}
		return p, nil
	}

	return nil, fmt.Errorf("Unknown protocol")
}

func NewConsumer(protocol Protocol, cfg config.Config, id int) (Consumer, error) {
	switch protocol {
	case AMQP:
		c := amqp10_client.NewConsumer(cfg, id)
		if c == nil {
			return nil, fmt.Errorf("Failed to create an AMQP-1.0 consumer")
		}
		return c, nil
	case STOMP:
		c := stomp_client.NewConsumer(cfg, id)
		if c == nil {
			return nil, fmt.Errorf("Failed to create an AMQP-1.0 consumer")
		}
		return c, nil
	case MQTT:
		c := mqtt_client.NewConsumer(cfg, id)
		if c == nil {
			return nil, fmt.Errorf("Failed to create an AMQP-1.0 consumer")
		}
		return c, nil
	}

	return nil, fmt.Errorf("Unknown protocol")
}

func CalculateTopic(cfg config.Config, id int) string {
	topic := cfg.QueueNamePrefix
	if cfg.QueueCount > 1 {
		topic = topic + "-" + fmt.Sprint(((id-1)%cfg.QueueCount)+1)
	}
	return topic
}
