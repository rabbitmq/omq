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
	ConsumerCredits int
	Durability      AmqpDurabilityMode
}

type Config struct {
	PublisherUri    string
	ConsumerUri     string
	Publishers      int
	Consumers       int
	PublishCount    int
	ConsumeCount    int
	QueueNamePrefix string
	QueueCount      int
	Size            int
	Rate            int
	Duration        time.Duration
	UseMillis       bool
	Amqp            AmqpOptions
}
