package common

import (
	"context"
	"fmt"

	"github.com/rabbitmq/omq/pkg/amqp10_client"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/mqtt_client"
	"github.com/rabbitmq/omq/pkg/stomp_client"
)

type Publisher interface {
	Start(context.Context)
}

type Consumer interface {
	Start(context.Context, chan bool)
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
			return nil, fmt.Errorf("failed to create an AMQP-1.0 publisher")
		}
		return p, nil
	case STOMP:
		p := stomp_client.NewPublisher(cfg, id)
		if p == nil {
			return nil, fmt.Errorf("failed to create a STOMP publisher")
		}
		return p, nil
	case MQTT:
		p := mqtt_client.NewPublisher(cfg, id)
		if p == nil {
			return nil, fmt.Errorf("failed to create an MQTT publisher")
		}
		return p, nil
	}

	return nil, fmt.Errorf("unknown protocol")
}

func NewConsumer(protocol Protocol, cfg config.Config, id int) (Consumer, error) {
	switch protocol {
	case AMQP:
		c := amqp10_client.NewConsumer(cfg, id)
		if c == nil {
			return nil, fmt.Errorf("failed to create an AMQP-1.0 consumer")
		}
		return c, nil
	case STOMP:
		c := stomp_client.NewConsumer(cfg, id)
		if c == nil {
			return nil, fmt.Errorf("failed to create an AMQP-1.0 consumer")
		}
		return c, nil
	case MQTT:
		c := mqtt_client.NewConsumer(cfg, id)
		if c == nil {
			return nil, fmt.Errorf("failed to create an AMQP-1.0 consumer")
		}
		return c, nil
	}

	return nil, fmt.Errorf("unknown protocol")
}
