package config

import (
	"time"

	"github.com/thediveo/enumflag/v2"
)

type Protocol int

const (
	AMQP Protocol = iota
	AMQP091
	STOMP
	MQTT
	MQTT5
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
	To                 []string
	SendSettled        bool
	ReleaseRate        int
	RejectRate         int
	PropertyFilters    map[string]string
	AppProperties      map[string][]string
	AppPropertyFilters map[string]string
}

type MqttOptions struct {
	Version               int
	QoS                   int
	CleanSession          bool
	SessionExpiryInterval time.Duration
}

type Config struct {
	ExpectedInstances    int
	SyncName             string
	ConsumerProto        Protocol
	PublisherProto       Protocol
	PublisherId          string
	ConsumerId           string
	Uri                  []string
	PublisherUri         []string
	ConsumerUri          []string
	ManagementUri        []string
	Publishers           int
	Consumers            int
	SpreadConnections    bool
	PublishCount         int
	ConsumeCount         int
	PublishTo            string
	ConsumeFrom          string
	Queues               QueueType
	Exchange             string
	BindingKey           string
	CleanupQueues        bool
	ConsumerCredits      int
	ConsumerLatency      time.Duration
	Size                 int
	Rate                 float32
	MaxInFlight          int
	Duration             time.Duration
	UseMillis            bool
	QueueDurability      AmqpDurabilityMode
	MessageDurability    bool
	MessagePriority      string // to allow for "unset" value and STOMP takes strings anyway
	MessageTTL           time.Duration
	StreamOffset         any
	StreamFilterValues   string
	StreamFilterValueSet string
	ConsumerPriority     int32
	Amqp                 AmqpOptions
	MqttPublisher        MqttOptions
	MqttConsumer         MqttOptions
	MetricTags           map[string]string
	LogOutOfOrder        bool
	PrintAllMetrics      bool
	ConsumerStartupDelay time.Duration
	TCPNoDelay           bool
}

func NewConfig() Config {
	return Config{
		QueueDurability: Configuration,
	}
}
