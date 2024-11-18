package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
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

func RegisterMetrics(globalLabels map[string]string) {
	normal := map[string]string{"priority": "normal"}
	maps.Copy(normal, globalLabels)
	normalPriorityLabels := labelsToString(normal)
	high := map[string]string{"priority": "high"}
	maps.Copy(high, globalLabels)
	highPriorityLabels := labelsToString(high)

	MessagesPublished = vmetrics.GetOrCreateCounter("omq_messages_published_total" + labelsToString(globalLabels))
	MessagesConsumedNormalPriority = vmetrics.GetOrCreateCounter(`omq_messages_consumed_total` + normalPriorityLabels)
	MessagesConsumedHighPriority = vmetrics.GetOrCreateCounter(`omq_messages_consumed_total` + highPriorityLabels)
	MessagesConsumedOutOfOrderNormalPriority = vmetrics.GetOrCreateCounter(`omq_messages_consumed_out_of_order` + normalPriorityLabels)
	MessagesConsumedOutOfOrderHighPriority = vmetrics.GetOrCreateCounter(`omq_messages_consumed_out_of_order` + highPriorityLabels)
	PublishingLatency = vmetrics.GetOrCreateSummaryExt(`omq_publishing_latency_seconds`+labelsToString(globalLabels), 1*time.Second, []float64{0.5, 0.9, 0.95, 0.99})
	EndToEndLatency = vmetrics.GetOrCreateSummaryExt(`omq_end_to_end_latency_seconds`+labelsToString(globalLabels), 1*time.Second, []float64{0.5, 0.9, 0.95, 0.99})
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

func (m *MetricsServer) PrintSummary() {
	// this might ve called before the metrics were registered
	// eg. by `omq --help`
	if MessagesPublished == nil {
		return
	}

	log.Print("TOTAL PUBLISHED",
		"messages", MessagesPublished.Get(),
		"rate", fmt.Sprintf("%.2f/s", float64(MessagesPublished.Get())/time.Since(m.started).Seconds()))
	log.Print("TOTAL CONSUMED",
		"messages", MessagesConsumedNormalPriority.Get()+MessagesConsumedHighPriority.Get(),
		"rate", fmt.Sprintf("%.2f/s", float64(MessagesConsumedNormalPriority.Get()+MessagesConsumedHighPriority.Get())/time.Since(m.started).Seconds()))
}

func (m MetricsServer) PrintAll() {
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

func labelsToString(labels map[string]string) string {
	result := ""
	if len(labels) > 0 {
		result = "{"
		for label, value := range labels {
			result += label + `="` + value + `",`
		}
		result = strings.TrimSuffix(result, ",") + "}"
	}
	return result
}
