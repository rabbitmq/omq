package cmd

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/stretchr/testify/assert"
)

func TestAmqpCmd(t *testing.T) {
	rootCmd := RootCmd()
	rootCmd.SetArgs([]string{"amqp", "-C", "1", "-D", "1", "-q", "amqp"})

	publishedBefore := testutil.ToFloat64(metrics.MessagesPublished.WithLabelValues("amqp"))
	consumedBefore := testutil.ToFloat64(metrics.MessagesConsumed.WithLabelValues("amqp"))

	err := rootCmd.Execute()

	assert.Nil(t, err)
	assert.Equal(t, publishedBefore+1, testutil.ToFloat64(metrics.MessagesPublished.WithLabelValues("amqp-1.0")))
	assert.Equal(t, consumedBefore+1, testutil.ToFloat64(metrics.MessagesConsumed.WithLabelValues("amqp-1.0")))
}

func TestMqttCmd(t *testing.T) {
	rootCmd := RootCmd()
	rootCmd.SetArgs([]string{"mqtt", "-C", "1", "-D", "1", "-q", "mqtt"})

	publishedBefore := testutil.ToFloat64(metrics.MessagesPublished.WithLabelValues("mqtt"))
	consumedBefore := testutil.ToFloat64(metrics.MessagesConsumed.WithLabelValues("mqtt"))

	err := rootCmd.Execute()

	assert.Nil(t, err)
	assert.Equal(t, publishedBefore+1, testutil.ToFloat64(metrics.MessagesPublished.WithLabelValues("mqtt")))
	assert.Equal(t, consumedBefore+1, testutil.ToFloat64(metrics.MessagesConsumed.WithLabelValues("mqtt")))
}

func TestStompCmd(t *testing.T) {
	rootCmd := RootCmd()
	rootCmd.SetArgs([]string{"stomp", "-C", "1", "-D", "1", "-q", "stomp"})

	publishedBefore := testutil.ToFloat64(metrics.MessagesPublished.WithLabelValues("stomp"))
	consumedBefore := testutil.ToFloat64(metrics.MessagesConsumed.WithLabelValues("stomp"))

	err := rootCmd.Execute()

	assert.Nil(t, err)
	assert.Equal(t, publishedBefore+1, testutil.ToFloat64(metrics.MessagesPublished.WithLabelValues("stomp")))
	assert.Equal(t, consumedBefore+1, testutil.ToFloat64(metrics.MessagesConsumed.WithLabelValues("stomp")))
}

func TestMixCmd(t *testing.T) {
	type test struct {
		publish string
		consume string
	}

	tests := []test{
		{publish: "stomp", consume: "stomp"},
		{publish: "stomp", consume: "amqp"},
		{publish: "amqp", consume: "amqp"},
		{publish: "amqp", consume: "stomp"},
		{publish: "mqtt", consume: "mqtt"},
		// TODO MQTT->amqp/stomp
	}

	for _, tc := range tests {
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
		args := []string{tc.publish + "-" + tc.consume, "-z", "3s", "-C", "1", "-D", "1", "-q", tc.publish + tc.consume}
		rootCmd.SetArgs(args)
		fmt.Println("Running test: omq", strings.Join(args, " "))
		publishedBefore := testutil.ToFloat64(metrics.MessagesPublished.WithLabelValues(publishProtoLabel))
		consumedBefore := testutil.ToFloat64(metrics.MessagesConsumed.WithLabelValues(consumeProtoLabel))

		err := rootCmd.Execute()

		assert.Nil(t, err)
		assert.Equal(t, publishedBefore+1, testutil.ToFloat64(metrics.MessagesPublished.WithLabelValues(publishProtoLabel)))
		assert.Equal(t, consumedBefore+1, testutil.ToFloat64(metrics.MessagesConsumed.WithLabelValues(consumeProtoLabel)))
	}
}
