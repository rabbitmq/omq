package config

import (
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
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
	Subjects               []string
	To                     []string
	SendSettled            bool
	ReleaseRate            int
	RejectRate             int
	PropertyFilters        map[string]string
	AppPropertyTemplates   map[string]*template.Template
	MsgAnnotationTemplates map[string]*template.Template
	AppPropertyFilters     map[string]string
	SQLFilter              string
}

type MqttOptions struct {
	Version               int
	QoS                   int
	CleanSession          bool
	SessionExpiryInterval time.Duration
}

type Amqp091Options struct {
	Mandatory       bool
	HeaderTemplates map[string]*template.Template
}

type Config struct {
	ExpectedInstances       int
	SyncName                string
	ConsumerProto           Protocol
	PublisherProto          Protocol
	PublisherId             string
	ConsumerId              string
	Uri                     []string
	PublisherUri            []string
	ConsumerUri             []string
	ManagementUri           []string
	Publishers              int
	Consumers               int
	SpreadConnections       bool
	PublishCount            int
	ConsumeCount            int
	PublishTo               string
	PublishToTemplate       *template.Template
	ConsumeFrom             string
	ConsumeFromTemplate     *template.Template
	Queues                  QueueType
	Exchange                string
	BindingKey              string
	CleanupQueues           bool
	ConsumerCredits         int
	ConsumerLatencyTemplate *template.Template
	Size                    int
	SizeTemplate            *template.Template
	Rate                    float32
	MaxInFlight             int
	Duration                time.Duration
	UseMillis               bool
	QueueDurability         AmqpDurabilityMode
	MessageDurability       bool
	MessagePriorityTemplate *template.Template
	MessageTTL              time.Duration
	StreamOffset            any
	StreamFilterValues      string
	StreamFilterValueSet    string
	ConsumerPriority        int32
	Amqp                    AmqpOptions
	Amqp091                 Amqp091Options
	MqttPublisher           MqttOptions
	MqttConsumer            MqttOptions
	MetricTags              map[string]string
	LogOutOfOrder           bool
	PrintAllMetrics         bool
	ConsumerStartupDelay    time.Duration
}

func NewConfig() Config {
	return Config{
		QueueDurability: Configuration,
	}
}

// ParseTemplateValue parses a value as a template. If the value doesn't contain
// template syntax, it will still be parsed as a template but will return the
// original string when executed.
func ParseTemplateValue(value string) (*template.Template, error) {
	tmpl, err := template.New("template").Funcs(sprig.FuncMap()).Parse(value)
	if err != nil {
		return nil, err
	}
	return tmpl, nil
}
