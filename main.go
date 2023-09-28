package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/rabbitmq/omq/cmd"
	"github.com/rabbitmq/omq/pkg/metrics"
)

func main() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		// Run Cleanup
		shutdown()
	}()
	cmd.Execute()
}

func shutdown() {
	metrics.PrintMetrics()
	os.Exit(1)
}
