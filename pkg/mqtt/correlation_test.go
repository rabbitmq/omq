package mqtt

import (
	"math"
	"testing"
	"time"
)

func TestCorrelationRoundTrip(t *testing.T) {
	encoded := encodeCorrelation(7, 42)
	if len(encoded) != correlationDataLen {
		t.Fatalf("len = %d, want %d", len(encoded), correlationDataLen)
	}
	id, seq, ok := decodeCorrelation(encoded)
	if !ok || id != 7 || seq != 42 {
		t.Fatalf("decode = (%d, %d, %v), want (7, 42, true)", id, seq, ok)
	}
}

func TestRpcMessageExpiryOutlivesClientTimeout(t *testing.T) {
	if got := rpcMessageExpiry(5 * time.Second); got != 6 {
		t.Fatalf("rpcMessageExpiry(5s) = %d, want 6", got)
	}
	if got := rpcMessageExpiry(500 * time.Millisecond); got != 1 {
		t.Fatalf("rpcMessageExpiry(500ms) = %d, want 1", got)
	}
}

func TestInFlightReceiveMaximumClampsToUint16(t *testing.T) {
	if got := inFlightReceiveMaximum(0); got != 1 {
		t.Fatalf("inFlightReceiveMaximum(0) = %d, want 1", got)
	}
	if got := inFlightReceiveMaximum(8); got != 8 {
		t.Fatalf("inFlightReceiveMaximum(8) = %d, want 8", got)
	}
	if got := inFlightReceiveMaximum(math.MaxUint16 + 5); got != math.MaxUint16 {
		t.Fatalf("inFlightReceiveMaximum(overflow) = %d, want %d", got, uint16(math.MaxUint16))
	}
}

func TestDecodeCorrelationRejectsUnexpectedLength(t *testing.T) {
	if _, _, ok := decodeCorrelation(make([]byte, 8)); ok {
		t.Fatal("8-byte correlation should be rejected")
	}
	if _, _, ok := decodeCorrelation(nil); ok {
		t.Fatal("empty correlation should be rejected")
	}
}
