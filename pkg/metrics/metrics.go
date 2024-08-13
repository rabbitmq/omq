package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rabbitmq/omq/pkg/log"
)

type MetricsServer struct {
	httpServer *http.Server
	running    bool
}

var lock = &sync.Mutex{}

var metricsServer *MetricsServer

var (
	MessagesPublished          *prometheus.CounterVec
	MessagesConsumed           *prometheus.CounterVec
	MessagesConsumedOutOfOrder *prometheus.CounterVec
	PublishingLatency          *prometheus.SummaryVec
	EndToEndLatency            *prometheus.SummaryVec
)

func RegisterMetrics(globalLabels prometheus.Labels) {
	if MessagesPublished == nil {
		MessagesPublished = promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "omq_messages_published_total",
			Help:        "The total number of published messages",
			ConstLabels: globalLabels,
		}, []string{"protocol"})
	}
	if MessagesConsumed == nil {
		MessagesConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "omq_messages_consumed_total",
			Help:        "The total number of consumed messages",
			ConstLabels: globalLabels,
		}, []string{"protocol", "priority"})
	}
	if MessagesConsumedOutOfOrder == nil {
		MessagesConsumedOutOfOrder = promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "omq_messages_consumed_out_of_order",
			Help:        "The number of messages consumed out of order",
			ConstLabels: globalLabels,
		}, []string{"protocol", "priority"})
	}
	if PublishingLatency == nil {
		PublishingLatency = promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name:        "omq_publishing_latency_seconds",
			Help:        "Time from sending a message to receiving a confirmation",
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
			ConstLabels: globalLabels,
		}, []string{"protocol"})
	}
	if EndToEndLatency == nil {
		EndToEndLatency = promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name:        "omq_end_to_end_latency_seconds",
			Help:        "Time from sending a message to receiving the message",
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
			ConstLabels: globalLabels,
		}, []string{"protocol"})
	}
}

func Reset() {
	MessagesPublished.Reset()
	MessagesConsumed.Reset()
	PublishingLatency.Reset()
	EndToEndLatency.Reset()
}

func GetMetricsServer() *MetricsServer {
	lock.Lock()
	defer lock.Unlock()
	if metricsServer == nil {
		metricsServer =
			&MetricsServer{
				httpServer: &http.Server{
					Addr:    get_metrics_ip() + ":8080",
					Handler: promhttp.Handler(),
				},
			}
	}

	return metricsServer
}

func (m MetricsServer) Start() {
	if m.running {
		return
	}

	go func() {
		for {
			m.httpServer.RegisterOnShutdown(func() {
				m.running = false
			})
			log.Debug("Starting Prometheus metrics server", "address", m.httpServer.Addr)
			m.running = true
			err := m.httpServer.ListenAndServe()
			if errors.Is(err, syscall.EADDRINUSE) {
				port, _ := strconv.Atoi(strings.Split(m.httpServer.Addr, ":")[1])
				m.httpServer.Addr = get_metrics_ip() + ":" + fmt.Sprint(port+1)
				log.Info("Prometheus metrics: port already in use, trying the next one", "port", m.httpServer.Addr)
			}
		}
	}()
}

func (m MetricsServer) Stop() {
	_ = m.httpServer.Shutdown(context.TODO())
	log.Debug("Prometheus metrics stopped")
}

func (m MetricsServer) PrintMetrics() {
	endpoint := fmt.Sprintf("http://%s/metrics", m.httpServer.Addr)
	resp, err := http.Get(endpoint)
	if err != nil {
		return
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("Error reading metrics", "error", err)
		return
	}

	log.Print(" *********** RESULTS *********** ")
	metrics := strings.Split(string(body), "\n")
	for _, metric := range metrics {
		if strings.HasPrefix(metric, "omq_") {
			fmt.Println(metric)
		}
	}

}

func get_metrics_ip() string {
	// on macoOS, return 127.0.0.1, otherwise 0.0.0.0
	if runtime.GOOS == "darwin" {
		return "127.0.0.1"
	} else {
		return "0.0.0.0"
	}
}
