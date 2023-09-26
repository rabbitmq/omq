package metrics

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rabbitmq/omq/pkg/log"
)

type MetricsServer struct {
	httpServer *http.Server
}

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

func NewMetricsServer() *MetricsServer {
	return &MetricsServer{
		httpServer: &http.Server{
			Addr:    "127.0.0.1:8080",
			Handler: promhttp.Handler(),
		},
	}
}

func (m MetricsServer) Start() {
	go func() {
		log.Debug("Starting Prometheus metrics server")
		_ = m.httpServer.ListenAndServe()
	}()
}

func (m MetricsServer) Stop() {
	m.httpServer.Shutdown(context.TODO())
	log.Debug("Prometheus metrics stopped")
}
