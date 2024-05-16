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
	ConsumerLatency      time.Duration
	Size                 int
	Rate                 float32
	Duration             time.Duration
	UseMillis            bool
	QueueDurability      AmqpDurabilityMode
	MessageDurability    bool
	MessagePriority      string // to allow for "unset" value and STOMP takes strings anyway
	StreamOffset         string
	StreamFilterValues   string
	StreamFilterValueSet string
	Amqp                 AmqpOptions
	MqttPublisher        MqttOptions
	MqttConsumer         MqttOptions
	MetricTags           map[string]string
}

func NewConfig() Config {
	return Config{
		QueueDurability: Configuration,
	}
}
