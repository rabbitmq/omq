package mqtt

import (
	"context"
	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"testing"
	"time"
)

func TestReplyQueueCloseDrainsAcceptedJobs(t *testing.T) {
	q := newReplyQueue(2)
	q.push(replyJob{responseTopic: "accepted"})
	q.close()
	q.push(replyJob{responseTopic: "late"})
	job, ok := q.pop()
	if !ok || job.responseTopic != "accepted" {
		t.Fatal("accepted job was lost")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if q.waitIdle(ctx) {
		t.Fatal("drain completed with a publish in flight")
	}
	q.finished()
	if !q.waitIdle(t.Context()) {
		t.Fatal("queue did not drain")
	}
	if _, ok := q.pop(); ok {
		t.Fatal("closed queue accepted a new job")
	}
}

func TestReplyQueueAbortDiscardsBacklog(t *testing.T) {
	q := newReplyQueue(2)
	q.push(replyJob{})
	q.push(replyJob{})
	q.pop()
	q.close()
	q.abort()
	q.finished()
	if !q.waitIdle(t.Context()) {
		t.Fatal("aborted queue did not drain")
	}
	if _, ok := q.pop(); ok {
		t.Fatal("aborted queue retained jobs")
	}
}

func TestReplyQueueLimitDropsNewest(t *testing.T) {
	q := newReplyQueue(2)
	if !q.push(replyJob{responseTopic: "first"}) || !q.push(replyJob{responseTopic: "second"}) {
		t.Fatal("queue rejected jobs below its limit")
	}
	if q.push(replyJob{responseTopic: "overflow"}) {
		t.Fatal("queue exceeded its limit")
	}
	if q.depth() != 2 {
		t.Fatal("incorrect backlog depth")
	}
	job, _ := q.pop()
	if job.responseTopic != "first" {
		t.Fatal("overflow displaced the oldest job")
	}
	if !q.push(replyJob{responseTopic: "third"}) {
		t.Fatal("in-flight reply counted against waiting limit")
	}
	q.finished()
	q.close()
	for _, want := range []string{"second", "third"} {
		job, ok := q.pop()
		if !ok || job.responseTopic != want {
			t.Fatalf("job = %v, want %s", job, want)
		}
		q.finished()
	}
	if q.depth() != 0 {
		t.Fatal("depth did not return to zero")
	}
}

func TestReplyQueueWrapGrowAndReuse(t *testing.T) {
	q := newReplyQueue(5)
	for _, topic := range []string{"0", "1", "2", "3"} {
		q.push(replyJob{responseTopic: topic})
	}
	for _, want := range []string{"0", "1"} {
		job, _ := q.pop()
		if job.responseTopic != want {
			t.Fatalf("got %q, want %q", job.responseTopic, want)
		}
		q.finished()
	}
	for _, topic := range []string{"4", "5", "6"} {
		if !q.push(replyJob{responseTopic: topic}) {
			t.Fatal("queue rejected a job below its limit")
		}
	}
	for _, want := range []string{"2", "3", "4", "5", "6"} {
		job, _ := q.pop()
		if job.responseTopic != want {
			t.Fatalf("got %q, want %q", job.responseTopic, want)
		}
		q.finished()
	}
	for _, job := range q.jobs {
		if job.responseTopic != "" || job.correlationData != nil {
			t.Fatal("drained queue retained packet data")
		}
	}
	allocs := testing.AllocsPerRun(100, func() {
		q.push(replyJob{responseTopic: "reuse"})
		q.pop()
		q.finished()
	})
	if allocs != 0 {
		t.Fatalf("empty queue allocated %g times per reply", allocs)
	}
}

func BenchmarkReplyQueue(b *testing.B) {
	q := newReplyQueue(1024)
	job := replyJob{responseTopic: "reply"}
	b.ReportAllocs()
	for b.Loop() {
		q.push(job)
		q.pop()
		q.finished()
	}
}

func TestResponderDrainDeadlineCancelsPublishAndDiscardsWaitingJobs(t *testing.T) {
	log.Setup()
	metrics.RpcRepliesDropped = vmetrics.GetOrCreateCounter("test_rpc_replies_dropped_total")
	q := newReplyQueue(2)
	q.push(replyJob{})
	q.push(replyJob{})
	q.pop() // Simulate a publish that is waiting for an acknowledgement.
	pubCtx, cancelPublish := context.WithCancel(t.Context())
	defer cancelPublish()
	c := Mqtt5Responder{Config: config.Config{MqttRpc: config.MqttRpcOptions{DrainTimeout: time.Millisecond}}}
	dropped := metrics.RpcRepliesDropped.Get()
	done := make(chan struct{})
	go func() { c.stop(nil, q, cancelPublish, "test"); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("drain deadline failed to bound shutdown")
	}
	if pubCtx.Err() == nil {
		t.Fatal("in-flight publish was not cancelled")
	}
	if q.depth() != 0 || metrics.RpcRepliesDropped.Get() != dropped+1 {
		t.Fatal("waiting reply was not discarded and counted")
	}
	q.finished()
	if _, ok := q.pop(); ok {
		t.Fatal("worker can still pick up a discarded reply")
	}
}

func TestReplyQueueDrainsWhileProducerRemainsActive(t *testing.T) {
	q := newReplyQueue(8)
	q.push(replyJob{})
	q.pop() // Hold one publish in flight while admission is closed.
	producing := make(chan struct{})
	stop := make(chan struct{})
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		q.push(replyJob{})
		close(producing)
		for {
			select {
			case <-stop:
				return
			default:
				q.push(replyJob{})
			}
		}
	}()
	<-producing
	q.close()
	q.finished()
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for {
			if _, ok := q.pop(); !ok {
				return
			}
			q.finished()
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	drained := q.waitIdle(ctx)
	close(stop)
	<-producerDone
	<-workerDone
	if !drained {
		t.Fatal("continued arrivals prevented shutdown draining")
	}
	if q.depth() != 0 {
		t.Fatal("producer added jobs after draining")
	}
}
