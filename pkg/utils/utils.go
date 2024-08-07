package utils

import (
	"encoding/binary"
	"time"
)

func MessageBody(size int) []byte {
	b := make([]byte, size)
	binary.BigEndian.PutUint32(b[0:], uint32(1234)) // currently unused, for compatibility with perf-test
	return b
}

func UpdatePayload(useMillis bool, payload *[]byte) *[]byte {
	if useMillis {
		binary.BigEndian.PutUint64((*payload)[4:], uint64(time.Now().UnixMilli()))
	} else {
		binary.BigEndian.PutUint64((*payload)[4:], uint64(time.Now().UnixNano()))
	}
	return payload
}

func CalculateEndToEndLatency(payload *[]byte) (time.Time, time.Duration) {
	if len(*payload) < 12 {
		// message sent without latency tracking
		return time.Unix(0, 0), 0
	}
	now := time.Now()
	timeSent := FormatTimestamp(binary.BigEndian.Uint64((*payload)[4:]))
	latency := now.Sub(timeSent)
	return timeSent, latency
}

func FormatTimestamp(timestamp uint64) time.Time {
	var t time.Time
	// should be updated before the year 2100 ;)
	if timestamp < 4102441200000 {
		t = time.UnixMilli(int64(timestamp))
	} else {
		t = time.Unix(0, int64(timestamp))
	}
	return t
}
