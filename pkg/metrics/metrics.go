package metrics

import (
	"net/http"
	"os"

	"github.com/rabbitmq/omq/pkg/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	MessagesPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "omq_messages_published_total",
		Help: "The total number of published messages"},
		[]string{
			"protocol",
		},
	)
	MessagesConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "omq_messages_consumed_total",
		Help: "The total number of consumed messages"},
		[]string{
			"protocol",
		},
	)
	PublishingLatency = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "omq_publishing_latency_seconds",
		Help:       "Time from sending a message to receiving a confirmation",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
	},
		[]string{
			"protocol",
		},
	)
	EndToEndLatency = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "omq_end_to_end_latency_seconds",
		Help:       "Time from sending a message to receiving the message",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
	},
		[]string{
			"protocol",
		},
	)
)

func Start() {
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe("127.0.0.1:8080", nil)
	if err != nil {
		log.Error("Prometheus metrics failed to start", "error", err.Error())
		os.Exit(1)
	}
}
