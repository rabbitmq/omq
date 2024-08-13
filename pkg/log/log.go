package log

import (
	"os"

	"log/slog"

	"github.com/charmbracelet/log"
)

var logger *slog.Logger

var Levels = map[log.Level][]string{
	log.DebugLevel: {"debug"},
	log.InfoLevel:  {"info"},
	log.ErrorLevel: {"error"},
}

var Level log.Level

func Setup() {
	handler := log.New(os.Stderr)
	handler.SetLevel(Level)
	logger = slog.New(handler)
}

func Debug(format string, v ...interface{}) {
	if logger == nil {
		Setup()
	}
	logger.Debug(format, v...)
}

func Info(format string, v ...interface{}) {
	logger.Info(format, v...)
}

func Error(format string, v ...interface{}) {
	logger.Error(format, v...)
}
