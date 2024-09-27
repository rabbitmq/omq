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
		publishProto      string
		publishToPrefix   string
		consumeProto      string
		consumeFromPrefix string
		msgPriority       string // expected/default message priority
	}

	tests := []test{
		{
			publishProto:      "amqp",
			publishToPrefix:   "/queues/",
			consumeProto:      "amqp",
			consumeFromPrefix: "/queues/",
			msgPriority:       "0", // https://github.com/Azure/go-amqp/issues/313
		},
		{
			publishProto:      "stomp",
			publishToPrefix:   "/topic/",
			consumeProto:      "amqp",
			consumeFromPrefix: "/queues/",
			msgPriority:       "4",
		},
		{
			publishProto:      "mqtt",
			publishToPrefix:   "/topic/",
			consumeProto:      "amqp",
			consumeFromPrefix: "/queues/",
			msgPriority:       "4",
		},
		{
			publishProto:      "amqp",
			publishToPrefix:   "/exchanges/amq.topic/",
			consumeProto:      "stomp",
			consumeFromPrefix: "/topic/",
			msgPriority:       "0", // https://github.com/Azure/go-amqp/issues/313
		},
		{
			publishProto:      "amqp",
			publishToPrefix:   "/exchanges/amq.topic/",
			consumeProto:      "mqtt",
			consumeFromPrefix: "/topic/",
			msgPriority:       "",
		},
		{
			publishProto:      "stomp",
			publishToPrefix:   "/topic/",
			consumeProto:      "stomp",
			consumeFromPrefix: "/topic/",
			msgPriority:       "",
		},
		{
			publishProto:      "stomp",
			publishToPrefix:   "/topic/",
			consumeProto:      "mqtt",
			consumeFromPrefix: "/topic/",
			msgPriority:       "",
		},
		{
			publishProto:      "mqtt",
			publishToPrefix:   "/topic/",
			consumeProto:      "mqtt",
			consumeFromPrefix: "/topic/",
			msgPriority:       "",
		},
		{
			publishProto:      "mqtt",
			publishToPrefix:   "/topic/",
			consumeProto:      "stomp",
			consumeFromPrefix: "/topic/",
			msgPriority:       "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.publishProto+"-"+tc.consumeProto, func(t *testing.T) {
			rootCmd := RootCmd()

			publishTo := tc.publishToPrefix + tc.publishProto + tc.consumeProto
			consumeFrom := tc.consumeFromPrefix + tc.publishProto + tc.consumeProto
			args := []string{tc.publishProto + "-" + tc.consumeProto,
				"-C", "1",
				"-D", "1",
				"-t", publishTo,
				"-T", consumeFrom,
				"--queue-durability", "none",
				"--time", "3s", // don't want to long in case of issues
			}
			if tc.consumeProto == "amqp" {
				args = append(args, "--queues", "classic", "--cleanup-queues=true")
			}
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
		publishProto      string
		publishToPrefix   string
		consumeProto      string
		consumeFromPrefix string
	}

	tests := []test{
		// mqtt has no concept of message priority
		{publishProto: "stomp", publishToPrefix: "/topic/", consumeProto: "stomp", consumeFromPrefix: "/topic/"},
		{publishProto: "stomp", publishToPrefix: "/topic/", consumeProto: "amqp", consumeFromPrefix: "/queues/"},
		{publishProto: "amqp", publishToPrefix: "/queues/", consumeProto: "amqp", consumeFromPrefix: "/queues/"},
		{publishProto: "amqp", publishToPrefix: "/exchanges/amq.topic/", consumeProto: "stomp", consumeFromPrefix: "/topic/"},
	}

	for _, tc := range tests {
		t.Run(tc.publishProto+"-"+tc.consumeProto, func(t *testing.T) {
			rootCmd := RootCmd()

			publishTo := tc.publishToPrefix + tc.publishProto + tc.consumeProto
			consumeFrom := tc.consumeFromPrefix + tc.publishProto + tc.consumeProto
			args := []string{
				tc.publishProto + "-" + tc.consumeProto,
				"-C", "1",
				"-D", "1",
				"-t", publishTo,
				"-T", consumeFrom,
				"--message-priority", "13",
				"--queue-durability", "none",
				"--time", "3s"}
			if tc.consumeProto == "amqp" {
				args = append(args, "--queues", "classic", "--cleanup-queues=true")
			}
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

func TestFanInFromMQTTtoAMQP(t *testing.T) {
	rootCmd := RootCmd()

	args := []string{"mqtt-amqp",
		"--publishers", "3",
		"--consumers", "1",
		"-C", "5",
		"--publish-to", "sensor/%d",
		"--consume-from", "/queues/sensors",
		"--amqp-binding-key", "sensor.#",
		"--queues", "classic",
		"--cleanup-queues=true",
		"--time", "5s",
	}

	rootCmd.SetArgs(args)
	fmt.Println("Running test: omq", strings.Join(args, " "))
	err := rootCmd.Execute()
	assert.Nil(t, err)

	assert.Eventually(t, func() bool {
		return 15 == metrics.MessagesPublished.Get()

	}, 2*time.Second, 100*time.Millisecond)
	assert.Eventually(t, func() bool {
		return 15 == metrics.MessagesConsumedNormalPriority.Get()
	}, 2*time.Second, 100*time.Millisecond)
}

func TestConsumerStartupDelay(t *testing.T) {
	rootCmd := RootCmd()

	args := []string{"amqp",
		"-z", "5s",
		"-r", "1",
		"-D", "1",
		"-t", "/queues/consumer-startup-delay",
		"-T", "/queues/consumer-startup-delay",
		"--queues", "classic",
		"--cleanup-queues=true",
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
	args := []string{"amqp", "-C", "1", "-D", "1", "--queues", "classic", "--time", "2s"}
	rootCmd := RootCmd()
	rootCmd.SetArgs(args)
	_ = rootCmd.Execute()
	assert.Equal(t, false, cfg.UseMillis)

	// if -x 0, use-millis is true
	args = []string{"amqp", "-x", "0", "-D", "0", "--queues", "classic", "--time", "2s"}
	rootCmd = RootCmd()
	rootCmd.SetArgs(args)
	_ = rootCmd.Execute()
	assert.Equal(t, true, cfg.UseMillis)

	// if -y 0, use-millis is true
	args = []string{"amqp", "-t", "/exchanges/amq.topic/foobar", "-y", "0", "-C", "0", "--queues", "classic", "--time", "2s"}
	rootCmd = RootCmd()
	rootCmd.SetArgs(args)
	_ = rootCmd.Execute()
	assert.Equal(t, true, cfg.UseMillis)

}
