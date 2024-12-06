package common

import (
	"context"
	"fmt"

	"github.com/rabbitmq/omq/pkg/amqp10"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/mqtt"
	"github.com/rabbitmq/omq/pkg/stomp"
)

type Publisher interface {
	Start(context.Context, chan bool, chan bool)
}

type Consumer interface {
	Start(context.Context, chan bool)
}

func NewPublisher(ctx context.Context, cfg config.Config, id int) (Publisher, error) {
	switch cfg.PublisherProto {
	case config.AMQP:
		p := amqp10.NewPublisher(ctx, cfg, id)
		if p == nil {
			return nil, fmt.Errorf("failed to create an AMQP-1.0 publisher")
		}
		return p, nil
	case config.STOMP:
		p := stomp.NewPublisher(ctx, cfg, id)
		if p == nil {
			return nil, fmt.Errorf("failed to create a STOMP publisher")
		}
		return p, nil
	case config.MQTT:
		p := mqtt.NewPublisher(ctx, cfg, id)
		if p == nil {
			return nil, fmt.Errorf("failed to create an MQTT publisher")
		}
		return p, nil
	}

	return nil, fmt.Errorf("unknown protocol")
}

func NewConsumer(ctx context.Context, protocol config.Protocol, cfg config.Config, id int) (Consumer, error) {
	switch protocol {
	case config.AMQP:
		c := amqp10.NewConsumer(ctx, cfg, id)
		if c == nil {
			return nil, fmt.Errorf("failed to create an AMQP-1.0 consumer")
		}
		return c, nil
	case config.STOMP:
		c := stomp.NewConsumer(ctx, cfg, id)
		if c == nil {
			return nil, fmt.Errorf("failed to create a STOMP consumer")
		}
		return c, nil
	case config.MQTT:
		c := mqtt.NewConsumer(ctx, cfg, id)
		if c == nil {
			return nil, fmt.Errorf("failed to create an MQTT consumer")
		}
		return c, nil
	}

	return nil, fmt.Errorf("unknown protocol")
}
