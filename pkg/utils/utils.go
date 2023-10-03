package utils

import (
	"encoding/binary"
	"time"
)

func MessageBody(size int) []byte {
	b := make([]byte,size)
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

func CalculateEndToEndLatency(useMillis bool, payload *[]byte) float64 {
	if len(*payload) < 12 {
		// message sent without latency tracking
		return 0
	}
	timeSent := binary.BigEndian.Uint64((*payload)[4:])

	if useMillis {
		// less precise but necessary when a different process publishes and consumes
		now := uint64(time.Now().UnixMilli())
		latency := now - timeSent
		return (float64(latency) / 1000)
	} else {
		// nanoseconds - more precise when the same process publishes and consumes
		now := uint64(time.Now().UnixNano())
		latency := now - timeSent
		return (float64(latency) / 1000000000)
	}
}
