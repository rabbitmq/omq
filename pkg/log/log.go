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

func Debug(msg interface{}, keyvals ...interface{}) {
	logger.Debug(msg, keyvals...)
}

func Info(msg interface{}, keyvals ...interface{}) {
	logger.Info(msg, keyvals...)
}

func Error(msg interface{}, keyvals ...interface{}) {
	logger.Error(msg, keyvals...)
}

func Print(msg interface{}, keyvals ...interface{}) {
	logger.Print(msg, keyvals...)
}
