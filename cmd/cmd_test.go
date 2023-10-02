package cmd

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/stretchr/testify/assert"
)

func TestPublishConsume(t *testing.T) {
	type test struct {
		publish string
		consume string
	}

	tests := []test{
		{publish: "stomp", consume: "stomp"},
		{publish: "stomp", consume: "amqp"},
		{publish: "stomp", consume: "mqtt"},
		{publish: "amqp", consume: "amqp"},
		{publish: "amqp", consume: "stomp"},
		{publish: "amqp", consume: "mqtt"},
		{publish: "mqtt", consume: "mqtt"},
		{publish: "mqtt", consume: "stomp"},
		{publish: "mqtt", consume: "amqp"},
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
		args := []string{tc.publish + "-" + tc.consume, "-C", "1", "-D", "1", "-q", "/exchange/amq.topic/" + tc.publish + tc.consume}
		rootCmd.SetArgs(args)
		fmt.Println("Running test: omq", strings.Join(args, " "))
		publishedBefore := testutil.ToFloat64(metrics.MessagesPublished.WithLabelValues(publishProtoLabel))
		consumedBefore := testutil.ToFloat64(metrics.MessagesConsumed.WithLabelValues(consumeProtoLabel))

		err := rootCmd.Execute()

		assert.Nil(t, err)
		assert.Eventually(t, func() bool {
			return testutil.ToFloat64(metrics.MessagesPublished.WithLabelValues(publishProtoLabel)) == publishedBefore+1

		}, 10*time.Second, 100*time.Millisecond)
		assert.Eventually(t, func() bool {
			return testutil.ToFloat64(metrics.MessagesConsumed.WithLabelValues(consumeProtoLabel)) == consumedBefore+1
		}, 10*time.Second, 100*time.Millisecond)
	}
}
