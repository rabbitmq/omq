package config

import (
	"time"

	"github.com/thediveo/enumflag/v2"
)

type Protocol int

const (
	AMQP Protocol = iota
	STOMP
	MQTT
)

type QueueType enumflag.Flag

type AmqpDurabilityMode enumflag.Flag

const (
	None AmqpDurabilityMode = iota
	Configuration
	UnsettledState
)

const (
	Predeclared = iota
	Classic
	Quorum
	Stream
)

var AmqpDurabilityModes = map[AmqpDurabilityMode][]string{
	None:           {"none"},
	Configuration:  {"configuration"},
	UnsettledState: {"unsettled-state"},
}

var QueueTypes = map[QueueType][]string{
	Predeclared: {"predeclared"},
	Classic:     {"classic"},
	Quorum:      {"quorum"},
	Stream:      {"stream"},
}

type AmqpOptions struct {
	Subjects           []string
	SendSettled        bool
	ReleaseRate        int
	RejectRate         int
	BindingKey         string
	PropertyFilters    map[string]string
	AppProperties      map[string][]string
	AppPropertyFilters map[string]string
}

type MqttOptions struct {
	QoS          int
	CleanSession bool
}

type Config struct {
	ConsumerProto        Protocol
	PublisherProto       Protocol
	Uri                  []string
	PublisherUri         []string
	ConsumerUri          []string
	Publishers           int
	Consumers            int
	SpreadConnections    bool
	PublishCount         int
	ConsumeCount         int
	PublishTo            string
	ConsumeFrom          string
	Queues               QueueType
	CleanupQueues        bool
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
	ConsumerPriority     int32
	Amqp                 AmqpOptions
	MqttPublisher        MqttOptions
	MqttConsumer         MqttOptions
	MetricTags           map[string]string
	LogOutOfOrder        bool
	ConsumerStartupDelay time.Duration
}

func NewConfig() Config {
	return Config{
		QueueDurability: Configuration,
	}
}
