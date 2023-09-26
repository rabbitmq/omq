package config

import "time"

type AmqpOptions struct {
	ConsumerCredits int
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
