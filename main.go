package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/rabbitmq/omq/cmd"
	"github.com/rabbitmq/omq/pkg/metrics"
)

var metricsServer *metrics.MetricsServer

func main() {
	metricsServer := metrics.GetMetricsServer()
	metricsServer.Start()

	// handle ^C
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		shutdown()
	}()

	cmd.Execute()
	metricsServer.PrintMetrics()
}

func shutdown() {
	metricsServer := metrics.GetMetricsServer()
	metricsServer.PrintMetrics()
	os.Exit(1)
}
