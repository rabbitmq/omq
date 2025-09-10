package log

import (
	"os"

	"github.com/charmbracelet/log"
)

var logger *log.Logger

var Levels = map[log.Level][]string{
	log.DebugLevel: {"debug"},
	log.InfoLevel:  {"info"},
	log.ErrorLevel: {"error"},
}

var Level log.Level

func Setup() {
	logger = log.NewWithOptions(os.Stderr, log.Options{
		ReportTimestamp: true,
	})
	logger.SetLevel(Level)
}

func Debug(msg any, keyvals ...any) {
	logger.Debug(msg, keyvals...)
}

func Info(msg any, keyvals ...any) {
	logger.Info(msg, keyvals...)
}

func Error(msg any, keyvals ...any) {
	logger.Error(msg, keyvals...)
}

func Print(msg any, keyvals ...any) {
	logger.Print(msg, keyvals...)
}
