package cmd

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestPublishConsume(t *testing.T) {
	type test struct {
		publish  string
		consume  string
		priority string // expected/default message priority
	}

	tests := []test{
		{publish: "stomp", consume: "stomp", priority: ""},
		{publish: "stomp", consume: "amqp", priority: "4"},
		{publish: "stomp", consume: "mqtt", priority: ""},
		{publish: "amqp", consume: "amqp", priority: "0"},  // https://github.com/Azure/go-amqp/issues/313
		{publish: "amqp", consume: "stomp", priority: "0"}, // https://github.com/Azure/go-amqp/issues/313
		{publish: "amqp", consume: "mqtt", priority: ""},
		{publish: "mqtt", consume: "mqtt", priority: ""},
		{publish: "mqtt", consume: "stomp", priority: ""},
		{publish: "mqtt", consume: "amqp", priority: "4"},
	}

	for _, tc := range tests {
		t.Run(tc.publish+"-"+tc.consume, func(t *testing.T) {
			var publishProtoLabel, consumeProtoLabel string
			if tc.publish == "amqp" {
				publishProtoLabel = "amqp-1.0"
			} else {
				publishProtoLabel = tc.publish
			}
			if tc.consume == "amqp" {
				consumeProtoLabel = "amqp-1.0"
			} else {
				consumeProtoLabel = tc.consume
			}
			rootCmd := RootCmd()

			topic := "/topic/" + tc.publish + tc.consume
			args := []string{tc.publish + "-" + tc.consume, "-C", "1", "-D", "1", "-t", topic, "-T", topic}
			rootCmd.SetArgs(args)
			fmt.Println("Running test: omq", strings.Join(args, " "))

			err := rootCmd.Execute()
			assert.Nil(t, err)

			metrics.GetMetricsServer().PrintMetrics()

			assert.Eventually(t, func() bool {
				return assert.Equal(t, 1.0, testutil.ToFloat64(metrics.MessagesPublished.WithLabelValues(publishProtoLabel)))

			}, 2*time.Second, 100*time.Millisecond)
			assert.Eventually(t, func() bool {
				return assert.Equal(t, 1.0, testutil.ToFloat64(metrics.MessagesConsumed.WithLabelValues(consumeProtoLabel, tc.priority)))
			}, 2*time.Second, 100*time.Millisecond)
			metrics.Reset()
		})
	}
}

func TestPublishWithPriorities(t *testing.T) {
	type test struct {
		publish string
		consume string
	}

	tests := []test{
		// mqtt has no concept of message priority
		{publish: "stomp", consume: "stomp"},
		{publish: "stomp", consume: "amqp"},
		{publish: "amqp", consume: "amqp"},
		{publish: "amqp", consume: "stomp"},
	}

	for _, tc := range tests {
		t.Run(tc.publish+"-"+tc.consume, func(t *testing.T) {
			var publishProtoLabel, consumeProtoLabel string
			if tc.publish == "amqp" {
				publishProtoLabel = "amqp-1.0"
			} else {
				publishProtoLabel = tc.publish
			}
			if tc.consume == "amqp" {
				consumeProtoLabel = "amqp-1.0"
			} else {
				consumeProtoLabel = tc.consume
			}
			rootCmd := RootCmd()

			topic := "/topic/" + tc.publish + tc.consume
			args := []string{tc.publish + "-" + tc.consume, "-C", "1", "-D", "1", "-t", topic, "-T", topic, "--message-priority", "13"}
			rootCmd.SetArgs(args)
			fmt.Println("Running test: omq", strings.Join(args, " "))

			err := rootCmd.Execute()
			assert.Nil(t, err)

			assert.Eventually(t, func() bool {
				return assert.Equal(t, 1.0, testutil.ToFloat64(metrics.MessagesPublished.WithLabelValues(publishProtoLabel)))

			}, 2*time.Second, 100*time.Millisecond)
			assert.Eventually(t, func() bool {
				consumeCounter, err := metrics.MessagesConsumed.GetMetricWith(prometheus.Labels{"protocol": consumeProtoLabel, "priority": "13"})
				return err == nil && assert.Equal(t, 1.0, testutil.ToFloat64(consumeCounter))
			}, 2*time.Second, 100*time.Millisecond)
			metrics.Reset()
		})
	}
}

func TestLatencyCalculationNano(t *testing.T) {
	testMsg := utils.MessageBody(100)
	utils.UpdatePayload(false, &testMsg)
	time.Sleep(1 * time.Microsecond)
	_, latency := utils.CalculateEndToEndLatency(&testMsg)
	// not very precise but we just care about the order of magnitude
	assert.Greater(t, latency.Nanoseconds(), 0.000001)
	assert.Less(t, latency.Nanoseconds(), 0.001)
}

func TestLatencyCalculationMillis(t *testing.T) {
	testMsg := utils.MessageBody(100)
	utils.UpdatePayload(true, &testMsg)
	time.Sleep(2 * time.Millisecond)
	_, latency := utils.CalculateEndToEndLatency(&testMsg)
	// not very precise but we just care about the order of magnitude
	assert.Greater(t, latency, 0.001)
	assert.Less(t, latency, 0.010)
}

func TestAutoUseMillis(t *testing.T) {
	// by default, use-millis is false
	args := []string{"amqp", "-C", "1", "-D", "1"}
	rootCmd := RootCmd()
	rootCmd.SetArgs(args)
	_ = rootCmd.Execute()
	assert.Equal(t, false, cfg.UseMillis)

	// if -x 0, use-millis is true
	args = []string{"amqp", "-x", "0", "-D", "0"}
	rootCmd = RootCmd()
	rootCmd.SetArgs(args)
	_ = rootCmd.Execute()
	assert.Equal(t, true, cfg.UseMillis)

	// if -y 0, use-millis is true
	args = []string{"amqp", "-y", "0", "-C", "0"}
	rootCmd = RootCmd()
	rootCmd.SetArgs(args)
	_ = rootCmd.Execute()
	assert.Equal(t, true, cfg.UseMillis)

}

// benchmarking the latency calculation
func BenchmarkLatencyCalculation(b *testing.B) {
	testMsg := utils.MessageBody(1000)
	utils.UpdatePayload(false, &testMsg)

	for i := 0; i < b.N; i++ {
		_, _ = utils.CalculateEndToEndLatency(&testMsg)
	}
}

// benchmarking the latency calculation
var metric *prometheus.SummaryVec

func BenchmarkObservingLatency(b *testing.B) {
	if metric == nil {
		metric = promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name:       "benchmaking_latency_seconds",
			Help:       "Time from sending a message to receiving a confirmation",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
		}, []string{"protocol"})
	} else {
		metric.Reset()
	}

	testMsg := utils.MessageBody(1000)

	for i := 0; i < b.N; i++ {
		utils.UpdatePayload(false, &testMsg)
		_, latency := utils.CalculateEndToEndLatency(&testMsg)
		metric.With(prometheus.Labels{"protocol": "foo"}).Observe(latency.Seconds())
	}
}

func BenchmarkObservingLatencyMillis(b *testing.B) {
	if metric == nil {
		metric = promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name:       "benchmaking_latency_seconds",
			Help:       "Time from sending a message to receiving a confirmation",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
		}, []string{"protocol"})
	} else {
		metric.Reset()
	}

	testMsg := utils.MessageBody(1000)

	for i := 0; i < b.N; i++ {
		utils.UpdatePayload(true, &testMsg)
		_, latency := utils.CalculateEndToEndLatency(&testMsg)
		metric.With(prometheus.Labels{"protocol": "foo"}).Observe(latency)
	}
}

var result time.Time

func BenchmarkFormatTimestamp(b *testing.B) {
	benchmarks := []struct {
		name      string
		useMillis bool
	}{
		{"nanoseconds", false},
		{"milliseconds", true},
	}

	var r time.Time
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			var timestamp uint64
			if bm.useMillis {
				timestamp = uint64(time.Now().UTC().UnixMilli())
			} else {
				timestamp = uint64(time.Now().UTC().UnixNano())
			}

			for i := 0; i < b.N; i++ {
				r = utils.FormatTimestamp(timestamp)
			}
		})
	}
	result = r
}
