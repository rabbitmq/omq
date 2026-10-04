package metrics

import (
	"math"
	"sync"
	"time"
)

// flushInterval bounds how stale the latency summary can be for Prometheus
// scrapes. The per-second console output flushes explicitly before reading.
const flushInterval = 100 * time.Millisecond

// LatencyRecorder collects publishing latencies for a single publisher without
// touching any state shared with other publishers. The shared Summary takes a
// mutex on every update, which becomes the bottleneck with many publishers, so
// samples are buffered here and handed over in bulk by a single flusher.
type LatencyRecorder struct {
	mu      sync.Mutex
	samples []time.Duration
	spare   []time.Duration // only touched by the flusher
}

var (
	recordersMu sync.Mutex
	recorders   []*LatencyRecorder
	flushMu     sync.Mutex
	flusherOnce sync.Once
)

// NewLatencyRecorder returns a recorder that is flushed periodically into the
// publishing latency metrics.
func NewLatencyRecorder() *LatencyRecorder {
	r := &LatencyRecorder{}
	recordersMu.Lock()
	recorders = append(recorders, r)
	recordersMu.Unlock()
	flusherOnce.Do(func() {
		go func() {
			for range time.Tick(flushInterval) {
				FlushLatencies()
			}
		}()
	})
	return r
}

// Record is safe for concurrent use, but is meant to be called by one goroutine
// per recorder so that the mutex stays uncontended.
func (r *LatencyRecorder) Record(latency time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, latency)
	r.mu.Unlock()
}

// FlushLatencies moves all buffered publishing latencies into the metrics.
func FlushLatencies() {
	flushMu.Lock()
	defer flushMu.Unlock()

	recordersMu.Lock()
	rs := make([]*LatencyRecorder, len(recorders))
	copy(rs, recorders)
	recordersMu.Unlock()

	minLat, maxLat := time.Duration(math.MaxInt64), time.Duration(0)
	seen := false
	for _, r := range rs {
		r.mu.Lock()
		full := r.samples
		r.samples = r.spare[:0]
		r.mu.Unlock()

		for _, l := range full {
			if PublishingLatency == nil {
				break
			}
			PublishingLatency.Update(l.Seconds())
			minLat = min(minLat, l)
			maxLat = max(maxLat, l)
			seen = true
		}
		r.spare = full[:0]
	}
	if seen {
		pubLatencyTracker.record(minLat)
		pubLatencyTracker.record(maxLat)
	}
}
