package mqtt

import (
	"context"
	"sync"
	"testing"
	"time"

	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/eclipse/paho.golang/paho"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/utils"
)

func TestTimeoutWorkerSchedulesPublishedRequests(t *testing.T) {
	r := replyTestRequester(t)
	r.Config.MqttRpc.Timeout = 20 * time.Millisecond
	r.pendingChanged = make(chan struct{}, 1)
	r.sem <- struct{}{}
	r.pending[1] = pendingReq{started: time.Now().Add(-time.Hour)}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); r.sweepTimeouts(ctx) }()
	defer func() { cancel(); <-done }()
	// An unpublished request must not get a deadline, even with an old RTT clock.
	time.Sleep(2 * r.Config.MqttRpc.Timeout)
	if len(r.sem) != 1 {
		t.Fatal("worker expired a request before Publish completed")
	}
	r.markPublished(1, time.Now())
	// A send succeeds only once the worker has released the outstanding slot.
	select {
	case r.sem <- struct{}{}:
	case <-time.After(time.Second):
		t.Fatal("idle timeout worker missed the new published request")
	}
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	if len(r.pending) != 0 {
		t.Fatal("worker released the slot without expiring the request")
	}
}

func TestTimeoutWorkerRearmsForEarlierDeadline(t *testing.T) {
	r := replyTestRequester(t)
	r.Config.MqttRpc.Timeout = time.Hour
	r.pendingChanged = make(chan struct{}, 1)
	r.sem <- struct{}{}
	r.pending[1] = pendingReq{started: time.Now(), publishedAt: time.Now()}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); r.sweepTimeouts(ctx) }()
	defer func() { cancel(); <-done }()
	readyDeadline := time.Now().Add(time.Second)
	for {
		r.pendingMu.Lock()
		armed := !r.sweepDeadline.IsZero()
		r.pendingMu.Unlock()
		if armed {
			break
		}
		if time.Now().After(readyDeadline) {
			t.Fatal("worker did not arm its initial deadline")
		}
		time.Sleep(time.Millisecond)
	}
	r.markPublished(1, time.Now().Add(-time.Hour))
	select {
	case r.sem <- struct{}{}:
	case <-time.After(time.Second):
		t.Fatal("worker did not rearm for an earlier deadline")
	}
}

func TestPublishedRequestOnlyNotifiesForEarlierDeadline(t *testing.T) {
	now := time.Now()
	r := &Mqtt5Requester{
		pending:        map[uint64]pendingReq{1: {}, 2: {}},
		pendingChanged: make(chan struct{}, 1),
		Config:         config.Config{MqttRpc: config.MqttRpcOptions{Timeout: time.Second}},
		sweepDeadline:  now.Add(time.Second),
	}
	r.markPublished(1, now.Add(time.Second))
	if len(r.pendingChanged) != 0 {
		t.Fatal("later publish woke the deadline worker unnecessarily")
	}
	r.markPublished(2, now.Add(-time.Millisecond))
	if len(r.pendingChanged) != 1 {
		t.Fatal("earlier publish did not wake the deadline worker")
	}
}

func replyTestRequester(t *testing.T) *Mqtt5Requester {
	t.Helper()
	log.Setup()
	metrics.RoundTripLatency = vmetrics.GetOrCreateSummary("test_rpc_roundtrip_seconds")
	metrics.RpcReplyLatency = vmetrics.GetOrCreateSummary("test_rpc_reply_seconds")
	metrics.RpcTimeouts = vmetrics.GetOrCreateCounter("test_rpc_timeouts_total")
	return &Mqtt5Requester{Id: 7, sem: make(chan struct{}, 1), pending: make(map[uint64]pendingReq), Config: config.Config{MqttRpc: config.MqttRpcOptions{Timeout: time.Millisecond}}}
}

func testReply(seq uint64) paho.PublishReceived {
	body := make([]byte, 12)
	utils.UpdatePayload(false, &body)
	return paho.PublishReceived{Packet: &paho.Publish{Payload: body, Properties: &paho.PublishProperties{CorrelationData: encodeCorrelation(7, seq)}}}
}

func TestReplyBeforePublishCompletionDoesNotResurrectRequest(t *testing.T) {
	r := replyTestRequester(t)
	r.sem <- struct{}{}
	r.pending[1] = pendingReq{started: time.Now().Add(-time.Millisecond)}
	consumed := metrics.MessagesConsumedMetric(0).Get()
	r.handleReply(testReply(1))
	r.markPublished(1, time.Now())
	r.handleReply(testReply(1))
	if len(r.pending) != 0 || len(r.sem) != 0 {
		t.Fatal("early reply left an outstanding request or slot")
	}
	if metrics.MessagesConsumedMetric(0).Get() != consumed+1 {
		t.Fatal("duplicate reply counted as another completion")
	}
}

func TestLateReplyDoesNotReleaseAnotherRequestsSlot(t *testing.T) {
	r := replyTestRequester(t)
	old := time.Now().Add(-time.Second)
	r.sem <- struct{}{}
	r.pending[1] = pendingReq{started: old, publishedAt: old}
	r.expireDue()
	r.sem <- struct{}{}
	r.pending[2] = pendingReq{started: time.Now()}
	consumed := metrics.MessagesConsumedMetric(0).Get()
	r.handleReply(testReply(1))
	if len(r.sem) != 1 || len(r.pending) != 1 {
		t.Fatal("late reply released a different request's slot")
	}
	if metrics.MessagesConsumedMetric(0).Get() != consumed {
		t.Fatal("late reply counted as a completion")
	}
}

func TestConcurrentReplyAndTimeoutReleaseExactlyOnce(t *testing.T) {
	r := replyTestRequester(t)
	for seq := uint64(0); seq < 100; seq++ {
		old := time.Now().Add(-time.Second)
		r.sem <- struct{}{}
		r.pending[seq] = pendingReq{started: old, publishedAt: old}
		consumed := metrics.MessagesConsumedMetric(0).Get()
		timedOut := metrics.RpcTimeouts.Get()
		reply := testReply(seq)
		ready := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-ready; r.handleReply(reply) }()
		go func() { defer wg.Done(); <-ready; r.expireDue() }()
		close(ready)
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("reply/timeout race released the slot twice and blocked")
		}
		if len(r.pending) != 0 || len(r.sem) != 0 {
			t.Fatal("race left an outstanding request")
		}
		completed := metrics.MessagesConsumedMetric(0).Get() - consumed + metrics.RpcTimeouts.Get() - timedOut
		if completed != 1 {
			t.Fatalf("request completed %d times", completed)
		}
	}
}
