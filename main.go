package main

import (
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"github.com/rabbitmq/omq/cmd"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
)

func main() {
	if os.Getenv("OMQ_PPROF") == "true" {
		cpuFile, err := os.Create("omq-cpu.pprof")
		if err != nil {
			log.Error("can't create omq-cpu.pprof", "error", err)
		}
		defer pprof.StopCPUProfile()
		_ = pprof.StartCPUProfile(cpuFile)
	}

	metricsServer := metrics.GetMetricsServer()
	metricsServer.Start()

	// handle ^C
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		shutdown()
	}()

	cmd.Execute()
	metricsServer.PrintMetrics()

	if os.Getenv("OMQ_PPROF") == "true" {
		memFile, err := os.Create("omq-memory.pprof")
		if err != nil {
			log.Error("can't create omq-memory.pprof", "error", err)
		}
		_ = pprof.WriteHeapProfile(memFile)
		defer memFile.Close()
	}
}

func shutdown() {
	metricsServer := metrics.GetMetricsServer()
	metricsServer.PrintMetrics()
	os.Exit(1)
}
