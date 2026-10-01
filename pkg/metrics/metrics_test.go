package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/rabbitmq/omq/pkg/log"
)

func TestBuildRateFieldsPrefersDirectionalRpcLatency(t *testing.T) {
	log.Setup()
	registerMetrics(nil, 1, 10)

	RecordRpcRequestLatency(1500 * time.Microsecond)
	RecordRpcReplyLatency(2500 * time.Microsecond)
	RecordRoundTripLatency(50 * time.Millisecond)
	RecordEndToEndLatency(0)

	fields := buildRateFields(1, 1)
	got := strings.Join(stringFields(fields), " ")
	for _, want := range []string{"request_min", "request_max", "reply_min", "reply_max", "rtt_min"} {
		if !strings.Contains(got, want) {
			t.Fatalf("fields %v missing %s", fields, want)
		}
	}
	if strings.Contains(got, "e2e_min") {
		t.Fatalf("end-to-end tracker should stay unused for rpc samples, got %v", fields)
	}
	reqAt := strings.Index(got, "request_min")
	rttAt := strings.Index(got, "rtt_min")
	if reqAt > rttAt {
		t.Fatalf("directional latency should be printed before round trip, got %v", fields)
	}
}

func stringFields(fields []any) []string {
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		if s, ok := field.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
