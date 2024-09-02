package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promutils"
	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/rabbitmq/omq/pkg/log"
)

type MetricsServer struct {
	httpServer *http.Server
	running    bool
	started    time.Time
}

var lock = &sync.Mutex{}

var metricsServer *MetricsServer

var (
	MessagesPublished                        *vmetrics.Counter
	MessagesConsumedNormalPriority           *vmetrics.Counter
	MessagesConsumedHighPriority             *vmetrics.Counter
	MessagesConsumedOutOfOrderNormalPriority *vmetrics.Counter
	MessagesConsumedOutOfOrderHighPriority   *vmetrics.Counter
	PublishingLatency                        *vmetrics.Summary
	EndToEndLatency                          *vmetrics.Summary
)

func RegisterMetrics(labels map[string]string) {
	globalLabels := promutils.NewLabelsFromMap(labels).String()

	normal := promutils.NewLabelsFromMap(labels)
	normal.Add("priority", "normal")
	normalPrioLabels := normal.String()

	high := promutils.NewLabelsFromMap(labels)
	high.Add("priority", "high")
	highPrioLabels := high.String()

	MessagesPublished = vmetrics.GetOrCreateCounter("omq_messages_published_total" + globalLabels)
	MessagesConsumedNormalPriority = vmetrics.GetOrCreateCounter(`omq_messages_consumed_total` + normalPrioLabels)
	MessagesConsumedHighPriority = vmetrics.GetOrCreateCounter(`omq_messages_consumed_total` + highPrioLabels)
	MessagesConsumedOutOfOrderNormalPriority = vmetrics.GetOrCreateCounter(`omq_messages_consumed_out_of_order` + normalPrioLabels)
	MessagesConsumedOutOfOrderHighPriority = vmetrics.GetOrCreateCounter(`omq_messages_consumed_out_of_order` + highPrioLabels)
	PublishingLatency = vmetrics.GetOrCreateSummaryExt(`omq_publishing_latency_seconds`+globalLabels, 1*time.Second, []float64{0.5, 0.9, 0.95, 0.99})
	EndToEndLatency = vmetrics.GetOrCreateSummaryExt(`omq_end_to_end_latency_seconds`+globalLabels, 1*time.Second, []float64{0.5, 0.9, 0.95, 0.99})
}

func MessagesConsumedMetric(priority int) *vmetrics.Counter {
	// we assume AMQP-1.0 priority definition
	if priority > 4 {
		return MessagesConsumedHighPriority
	}
	return MessagesConsumedNormalPriority
}

func MessagesConsumedOutOfOrderMetric(priority int) *vmetrics.Counter {
	// we assume AMQP-1.0 priority definition
	if priority > 4 {
		return MessagesConsumedOutOfOrderHighPriority
	}
	return MessagesConsumedOutOfOrderNormalPriority
}

func Reset() {
	MessagesPublished.Set(0)
	MessagesConsumedNormalPriority.Set(0)
	MessagesConsumedHighPriority.Set(0)
	MessagesConsumedOutOfOrderNormalPriority.Set(0)
	MessagesConsumedOutOfOrderHighPriority.Set(0)
	// PublishingLatency.Reset()
	// EndToEndLatency.Reset()
}

func GetMetricsServer() *MetricsServer {
	lock.Lock()
	defer lock.Unlock()
	if metricsServer == nil {
		http.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
			vmetrics.WritePrometheus(w, true)
		})

		metricsServer =
			&MetricsServer{
				httpServer: &http.Server{
					Addr: get_metrics_ip() + ":8080",
				},
			}
	}

	return metricsServer
}

func (m *MetricsServer) Start() {
	if m.running {
		return
	}

	go func() {
		for {
			m.httpServer.RegisterOnShutdown(func() {
				m.running = false
			})
			m.started = time.Now()
			m.running = true
			log.Debug("Starting Prometheus metrics server", "address", m.httpServer.Addr)
			err := m.httpServer.ListenAndServe()
			if errors.Is(err, syscall.EADDRINUSE) {
				port, _ := strconv.Atoi(strings.Split(m.httpServer.Addr, ":")[1])
				m.httpServer.Addr = get_metrics_ip() + ":" + fmt.Sprint(port+1)
				log.Info("Prometheus metrics: port already in use, trying the next one", "port", m.httpServer.Addr)
			}
		}
	}()
}

var previouslyPublished uint64
var previouslyConsumed uint64

func (m *MetricsServer) PrintMessageRates(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				published := MessagesPublished.Get()
				consumed := MessagesConsumedNormalPriority.Get() + MessagesConsumedHighPriority.Get()

				log.Print("",
					"published", fmt.Sprintf("%v/s", published-previouslyPublished),
					"consumed", fmt.Sprintf("%v/s", consumed-previouslyConsumed))

				previouslyPublished = published
				previouslyConsumed = consumed

			}

		}
	}()
}

func (m *MetricsServer) PrintFinalMetrics() {
	log.Print("TOTAL PUBLISHED",
		"messages", MessagesPublished.Get(),
		"rate", fmt.Sprintf("%.2f/s", float64(MessagesPublished.Get())/time.Since(m.started).Seconds()))
	log.Print("TOTAL CONSUMED",
		"consumed", MessagesConsumedNormalPriority.Get(),
		"rate", fmt.Sprintf("%.2f/s", float64(MessagesConsumedNormalPriority.Get()+MessagesConsumedHighPriority.Get())/time.Since(m.started).Seconds()))

}

func get_metrics_ip() string {
	// on macoOS, return 127.0.0.1, otherwise 0.0.0.0
	if runtime.GOOS == "darwin" {
		return "127.0.0.1"
	} else {
		return "0.0.0.0"
	}
}
