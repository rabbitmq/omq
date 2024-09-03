package cmd

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

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
			rootCmd := RootCmd()

			topic := "/topic/" + tc.publish + tc.consume
			args := []string{tc.publish + "-" + tc.consume,
				"-C", "1",
				"-D", "1",
				"-t", topic,
				"-T", topic,
				"--queue-durability", "none"}
			rootCmd.SetArgs(args)
			fmt.Println("Running test: omq", strings.Join(args, " "))

			err := rootCmd.Execute()
			assert.Nil(t, err)

			assert.Eventually(t, func() bool {
				return 1 == metrics.MessagesPublished.Get()

			}, 2*time.Second, 100*time.Millisecond)
			assert.Eventually(t, func() bool {
				return 1 == metrics.MessagesConsumedNormalPriority.Get()
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
			rootCmd := RootCmd()

			topic := "/topic/" + tc.publish + tc.consume
			args := []string{tc.publish + "-" + tc.consume, "-C", "1", "-D", "1", "-t", topic, "-T", topic, "--message-priority", "13", "--queue-durability", "none"}
			rootCmd.SetArgs(args)
			fmt.Println("Running test: omq", strings.Join(args, " "))

			err := rootCmd.Execute()
			assert.Nil(t, err)

			assert.Eventually(t, func() bool {
				return 1 == metrics.MessagesPublished.Get()

			}, 2*time.Second, 100*time.Millisecond)
			assert.Eventually(t, func() bool {
				return 1 == metrics.MessagesConsumedHighPriority.Get()
			}, 2*time.Second, 100*time.Millisecond)
			metrics.Reset()
		})
	}
}

func TestConsumerStartupDelay(t *testing.T) {
	rootCmd := RootCmd()

	args := []string{"amqp",
		"-z", "5s",
		"-r", "1",
		"-D", "1",
		"-t", "/topic/consumer-startup-delay",
		"-T", "/topic/consumer-startup-delay",
		"--consumer-startup-delay", "3s"}
	rootCmd.SetArgs(args)
	fmt.Println("Running test: omq", strings.Join(args, " "))

	var wg sync.WaitGroup
	go func() {
		defer wg.Done()
		wg.Add(1)
		err := rootCmd.Execute()
		assert.Nil(t, err)
	}()

	time.Sleep(2 * time.Second)
	assert.Equal(t, uint64(0), metrics.MessagesConsumedNormalPriority.Get())

	assert.Eventually(t, func() bool {
		return 1 == metrics.MessagesConsumedNormalPriority.Get()
	}, 10*time.Second, 100*time.Millisecond)

	wg.Wait()
	metrics.Reset()
}

func TestLatencyCalculationA(t *testing.T) {
	tests := []struct {
		name      string
		useMillis bool
	}{
		{"nanoseconds", false},
		{"milliseconds", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testMsg := utils.MessageBody(100)
			utils.UpdatePayload(tc.useMillis, &testMsg)
			time.Sleep(10 * time.Millisecond)
			_, latency := utils.CalculateEndToEndLatency(&testMsg)
			// not very precise but we just care about the order of magnitude
			assert.Greater(t, latency.Milliseconds(), int64(9))
			assert.Less(t, latency.Milliseconds(), int64(40))
		})
	}
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
