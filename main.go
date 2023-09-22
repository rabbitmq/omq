package main

import (
	"github.com/rabbitmq/omq/pkg/metrics"

	"github.com/rabbitmq/omq/cmd"
)

func main() {
	go metrics.Start()
	cmd.Execute()
}
