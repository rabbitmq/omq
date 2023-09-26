package cmd

import (
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
