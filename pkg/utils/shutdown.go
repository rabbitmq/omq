package utils

import "sync/atomic"

var shutdownWaiters atomic.Int32

// WaitingOnShutdown marks a shutdown step that is deliberately waiting for the
// broker, so that the forced exit after a signal doesn't cut it short. Call the
// returned function when done.
func WaitingOnShutdown() (done func()) {
	shutdownWaiters.Add(1)
	return func() { shutdownWaiters.Add(-1) }
}

// ShutdownIsWaiting reports whether any WaitingOnShutdown step is in progress.
func ShutdownIsWaiting() bool {
	return shutdownWaiters.Load() > 0
}
