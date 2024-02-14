package main

import (
	"os"
	"runtime/pprof"

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

	defer metrics.GetMetricsServer().PrintMetrics()

	cmd.Execute()

	if os.Getenv("OMQ_PPROF") == "true" {
		memFile, err := os.Create("omq-memory.pprof")
		if err != nil {
			log.Error("can't create omq-memory.pprof", "error", err)
		}
		_ = pprof.WriteHeapProfile(memFile)
		defer memFile.Close()
	}
}
