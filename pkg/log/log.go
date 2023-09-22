package log

import (
	"os"
	"strings"

	"log/slog"
)

var logger *slog.Logger

func init() {
	var logLevelEnv = os.Getenv("OMQ_LOG_LEVEL")
	var logLevel slog.Level
	switch strings.ToUpper(logLevelEnv) {
	case "DEBUG":
		logLevel = slog.LevelDebug
	case "INFO":
		logLevel = slog.LevelInfo
	case "ERROR":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
}

func Debug(format string, v ...interface{}) {
	logger.Debug(format, v...)
}

func Info(format string, v ...interface{}) {
	logger.Info(format, v...)
}

func Error(format string, v ...interface{}) {
	logger.Error(format, v...)
}
