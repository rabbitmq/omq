package config

import (
	"time"

	"github.com/thediveo/enumflag/v2"
)

type AmqpDurabilityMode enumflag.Flag

const (
	None AmqpDurabilityMode = iota
	Configuration
	UnsettledState
)

var AmqpDurabilityModes = map[AmqpDurabilityMode][]string{
	None:           {"none"},
	Configuration:  {"configuration"},
	UnsettledState: {"unsettled-state"},
}

type AmqpOptions struct {
	Subject string
}

type MqttOptions struct {
	QoS          int
	CleanSession bool
}

type Config struct {
	PublisherUri         string
	ConsumerUri          string
	Publishers           int
	Consumers            int
	PublishCount         int
	ConsumeCount         int
	PublishTo            string
	ConsumeFrom          string
	ConsumerCredits      int
	Size                 int
	Rate                 int
	Duration             time.Duration
	UseMillis            bool
	QueueDurability      AmqpDurabilityMode
	MessageDurability    bool
	StreamOffset         string
	StreamFilterValues   string
	StreamFilterValueSet string
	Amqp                 AmqpOptions
	MqttPublisher        MqttOptions
	MqttConsumer         MqttOptions
}

func NewConfig() Config {
	return Config{
		QueueDurability: Configuration,
	}
}
