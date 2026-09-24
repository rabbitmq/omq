package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"

	"github.com/felixge/fgprof"

	"github.com/rabbitmq/omq/cmd"
	"github.com/rabbitmq/omq/pkg/log"
)

func main() {
	pprofEnabled := os.Getenv("OMQ_PPROF") == "true"
	fgprofEnabled := os.Getenv("OMQ_FGPROF") == "true"

	if pprofEnabled {
		cpuFile, err := os.Create("omq-cpu.pprof")
		if err != nil {
			log.Error("can't create omq-cpu.pprof", "error", err)
		}
		defer pprof.StopCPUProfile()
		_ = pprof.StartCPUProfile(cpuFile)
	}
	if fgprofEnabled {
		http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())
	}
	// importing net/http/pprof registers the /debug/pprof/ handlers on DefaultServeMux
	if pprofEnabled || fgprofEnabled {
		go func() {
			_ = http.ListenAndServe(":6060", nil)
		}()
	}

	cmd.Execute()

	if pprofEnabled {
		memFile, err := os.Create("omq-memory.pprof")
		if err != nil {
			log.Error("can't create omq-memory.pprof", "error", err)
		}
		_ = pprof.WriteHeapProfile(memFile)
		defer func() { _ = memFile.Close() }()
	}
}
