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

	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/rabbitmq/omq/pkg/log"
)

type MetricsServer struct {
	httpServer *http.Server
	running    bool
}

var lock = &sync.Mutex{}

var metricsServer *MetricsServer

var (
	MessagesPublished          *vmetrics.Counter
	MessagesConsumed           *vmetrics.Counter
	MessagesConsumedOutOfOrder *vmetrics.Counter
	PublishingLatency          *vmetrics.Summary
	EndToEndLatency            *vmetrics.Summary
)

func RegisterMetrics(globalLabels map[string]string) {
	labels := ""
	if len(globalLabels) > 0 {
		labels = "{"
		for label, value := range globalLabels {
			labels += label + `="` + value + `",`
		}
		labels = strings.TrimSuffix(labels, ",") + "}"
	}

	MessagesPublished = vmetrics.GetOrCreateCounter("omq_messages_published_total" + labels)
	MessagesConsumed = vmetrics.GetOrCreateCounter("omq_messages_consumed_total" + labels)
	MessagesConsumedOutOfOrder = vmetrics.GetOrCreateCounter("omq_messages_consumed_out_of_order" + labels)
	PublishingLatency = vmetrics.GetOrCreateSummaryExt(`omq_publishing_latency_seconds`+labels, 1*time.Second, []float64{0.5, 0.9, 0.95, 0.99})
	EndToEndLatency = vmetrics.GetOrCreateSummaryExt(`omq_end_to_end_latency_seconds`+labels, 1*time.Second, []float64{0.5, 0.9, 0.95, 0.99})
}

func Reset() {
	MessagesPublished.Set(0)
	MessagesConsumed.Set(0)
	MessagesConsumedOutOfOrder.Set(0)
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
			err := m.httpServer.ListenAndServe()
			if errors.Is(err, syscall.EADDRINUSE) {
				port, _ := strconv.Atoi(strings.Split(m.httpServer.Addr, ":")[1])
				m.httpServer.Addr = get_metrics_ip() + ":" + fmt.Sprint(port+1)
				log.Info("Prometheus metrics: port already in use, trying the next one", "port", m.httpServer.Addr)
			}
			m.running = true
		}
	}()
}

func (m MetricsServer) Stop() {
	_ = m.httpServer.Shutdown(context.TODO())
	log.Debug("Prometheus metrics stopped")
}

var previouslyPublished uint64
var previouslyConsumed uint64

func (m MetricsServer) PrintMessageRates(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				published := MessagesPublished.Get()
				consumed := MessagesConsumed.Get()

				log.Print("",
					"published", fmt.Sprintf("%v/s", published-previouslyPublished),
					"consumed", fmt.Sprintf("%v/s", consumed-previouslyConsumed))

				previouslyPublished = published
				previouslyConsumed = consumed

			}

		}
	}()
}

func (m MetricsServer) PrintFinalMetrics() {
	log.Print("SUMMARY",
		"published", MessagesPublished.Get(),
		"consumed", MessagesConsumed.Get())

}

func get_metrics_ip() string {
	// on macoOS, return 127.0.0.1, otherwise 0.0.0.0
	if runtime.GOOS == "darwin" {
		return "127.0.0.1"
	} else {
		return "0.0.0.0"
	}
}
