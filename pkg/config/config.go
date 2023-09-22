package config

import "time"

type Config struct {
	AmqpUrl         string
	StompUrl        string
	MqttUrl         string
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
}
