package main

import (
	"net/http"
	"os"
	"runtime/pprof"

	"github.com/felixge/fgprof"

	"github.com/rabbitmq/omq/cmd"
	"github.com/rabbitmq/omq/pkg/log"
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
	if os.Getenv("OMQ_FGPROF") == "true" {
		http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())
		go func() {
			_ = http.ListenAndServe(":6060", nil)
		}()
	}

	cmd.Execute()

	if os.Getenv("OMQ_PPROF") == "true" {
		memFile, err := os.Create("omq-memory.pprof")
		if err != nil {
			log.Error("can't create omq-memory.pprof", "error", err)
		}
		_ = pprof.WriteHeapProfile(memFile)
		defer func() { _ = memFile.Close() }()
	}
}
