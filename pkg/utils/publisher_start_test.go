package utils_test

import (
	"math"
	"testing"
	"time"

	"github.com/rabbitmq/omq/pkg/utils"
)

func TestPublisherStartDelay(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rate   float32
		window time.Duration
	}{
		{name: "slow", rate: 0.1, window: 10 * time.Second},
		{name: "fractional", rate: 0.125, window: 8 * time.Second},
		{name: "one per second", rate: 1, window: time.Second},
		{name: "fast", rate: 10000, window: time.Second},
		{name: "unlimited", rate: -1, window: time.Second},
		{name: "idle", rate: 0, window: time.Second},
		{name: "extremely slow", rate: math.SmallestNonzeroFloat32, window: time.Duration(math.MaxInt64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buckets [10]int
			for range 10000 {
				delay := utils.PublisherStartDelay(tc.rate, -1)
				if delay < 0 || delay >= tc.window {
					t.Fatalf("delay %v outside [0, %v)", delay, tc.window)
				}
				buckets[int(float64(delay)/float64(tc.window)*float64(len(buckets)))]++
			}
			for i, count := range buckets {
				if count < 750 || count > 1250 {
					t.Errorf("bucket %d has %d publishers; expected roughly 1000", i, count)
				}
			}
		})
	}
}

func TestPublisherStartDelayOverride(t *testing.T) {
	if d := utils.PublisherStartDelay(0.1, 0); d != 0 {
		t.Errorf("zero override: got %v, want 0", d)
	}
	for range 1000 {
		if d := utils.PublisherStartDelay(0.1, 2*time.Second); d < 0 || d >= 2*time.Second {
			t.Fatalf("delay %v outside [0, 2s)", d)
		}
	}
}
