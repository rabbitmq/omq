package log

import (
	"os"

	"log/slog"
)

var logger *slog.Logger

var Levels = map[slog.Level][]string{
	slog.LevelDebug: {"debug"},
	slog.LevelInfo:  {"info"},
	slog.LevelError: {"error"},
}

var Level slog.Level

func Setup() {
	logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: Level}))
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
