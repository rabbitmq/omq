package mqtt

import "testing"

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

func TestDecodeCorrelationRejectsUnexpectedLength(t *testing.T) {
	if _, _, ok := decodeCorrelation(make([]byte, 8)); ok {
		t.Fatal("8-byte correlation should be rejected")
	}
	if _, _, ok := decodeCorrelation(nil); ok {
		t.Fatal("empty correlation should be rejected")
	}
}
