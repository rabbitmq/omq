package utils

import (
	"sync/atomic"
	"time"
)

// TagTimes remembers when each in-flight message was sent, keyed by a
// monotonically increasing delivery tag, without maps or locks. One goroutine
// calls Set and another calls Take.
//
// The slot count is at least twice the in-flight limit, so a slot is normally
// recycled only after its tag has been settled. If a very late confirmation
// finds its slot reused, Take reports a miss instead of a wrong latency.
type TagTimes struct {
	mask  uint64
	slots []tagSlot
}

type tagSlot struct {
	tag   atomic.Uint64 // 0 means empty; tags start at 1
	nanos atomic.Int64
}

func NewTagTimes(maxInFlight int) *TagTimes {
	size := uint64(16)
	for size < uint64(maxInFlight)*2 {
		size <<= 1
	}
	return &TagTimes{mask: size - 1, slots: make([]tagSlot, size)}
}

func (t *TagTimes) Set(tag uint64, sent time.Time) {
	s := &t.slots[tag&t.mask]
	// invalidate first so a concurrent Take can never pair the old tag with the new timestamp
	s.tag.Store(0)
	s.nanos.Store(sent.UnixNano())
	s.tag.Store(tag)
}

// Take returns the time elapsed since the tag was recorded and forgets it.
func (t *TagTimes) Take(tag uint64) (time.Duration, bool) {
	s := &t.slots[tag&t.mask]
	if s.tag.Load() != tag {
		return 0, false
	}
	sent := s.nanos.Load()
	if !s.tag.CompareAndSwap(tag, 0) {
		return 0, false
	}
	return time.Duration(time.Now().UnixNano() - sent), true
}
