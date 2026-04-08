package metrics

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"golang.org/x/exp/slices"
)

//go:embed console.html
var consoleHTML string

type MetricsServer struct {
	httpServer     *http.Server
	started        time.Time
	printAllOnStop bool
}

var once sync.Once

var metricsServer *MetricsServer

var (
	CommandLineArgs           *vmetrics.Gauge
	MessagesPublished         *vmetrics.Counter
	MessagesConfirmed         *vmetrics.Counter
	MessagesReturned          *vmetrics.Counter
	messagesConsumedCounters  sync.Map
	MessagesDeliveredTooEarly *vmetrics.Counter
	PublishingLatency         *vmetrics.Summary
	EndToEndLatency           *vmetrics.Summary
	DelayAccuracy             *vmetrics.Summary
	globalLabels              map[string]string
)

func startServer() {
	once.Do(func() {
		http.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
			vmetrics.WritePrometheus(w, true)
		})

		http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path == "/" {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				_, _ = w.Write([]byte(consoleHTML))
			} else {
				http.NotFound(w, req)
			}
		})

		metricsServer = &MetricsServer{
			httpServer: &http.Server{
				// we'll try to listen on 8080, if it's already in use, we'll try the next one
				Addr: get_metrics_ip() + ":8080",
			},
		}
	})
}

func Start(ctx context.Context, cfg config.Config) *MetricsServer {
	startServer()
	metricsServer.printAllOnStop = cfg.PrintAllMetrics
	registerMetrics(cfg.MetricTags, cfg.Publishers, cfg.Rate)
	registerCommandLineMetric(cfg, cfg.MetricTags)
	metricsServer.printMessageRates(ctx)

	go func() {
		for {
			metricsServer.started = time.Now() // updated later for higher accuracy
			log.Debug("starting Prometheus metrics server", "address", metricsServer.httpServer.Addr)
			err := metricsServer.httpServer.ListenAndServe()
			if errors.Is(err, syscall.EADDRINUSE) {
				port, _ := strconv.Atoi(strings.Split(metricsServer.httpServer.Addr, ":")[1])
				metricsServer.httpServer.Addr = get_metrics_ip() + ":" + fmt.Sprint(port+1)
				log.Info("prometheus metrics: port already in use, trying the next one", "port", metricsServer.httpServer.Addr)
			}
		}
	}()
	return metricsServer
}

func registerMetrics(labels map[string]string, publishers int, rate float32) {
	SummaryWindow := time.Second
	if rate > 0 {
		effectiveRate := float64(publishers) * float64(rate)
		// Span enough time that we expect at least one message in the summary window
		if effectiveRate > 0 && effectiveRate <= 1 {
			sec := int(1.0/effectiveRate) + 1
			SummaryWindow = time.Duration(sec) * time.Second
			log.Info("Low rate metrics mode", "summary_window", SummaryWindow)
		}
	}

	globalLabels = labels

	MessagesPublished = vmetrics.GetOrCreateCounter("omq_messages_published_total" + labelsToString(globalLabels))
	MessagesConfirmed = vmetrics.GetOrCreateCounter("omq_messages_confirmed_total" + labelsToString(globalLabels))
	MessagesReturned = vmetrics.GetOrCreateCounter("omq_messages_returned_total" + labelsToString(globalLabels))
	MessagesDeliveredTooEarly = vmetrics.GetOrCreateCounter("omq_early_messages_total" + labelsToString(globalLabels))
	PublishingLatency = vmetrics.GetOrCreateSummaryExt(`omq_publishing_latency_seconds`+labelsToString(globalLabels), SummaryWindow, []float64{0.5, 0.9, 0.95, 0.99})
	EndToEndLatency = vmetrics.GetOrCreateSummaryExt(`omq_end_to_end_latency_seconds`+labelsToString(globalLabels), SummaryWindow, []float64{0.5, 0.9, 0.95, 0.99})
	DelayAccuracy = vmetrics.GetOrCreateSummaryExt(`omq_delay_accuracy_seconds`+labelsToString(globalLabels), SummaryWindow, []float64{0.5, 0.9, 0.95, 0.99})
}

func registerCommandLineMetric(cfg config.Config, globalLabels map[string]string) {
	var args []string
	// some of the command line args are not useful in the metric
	ignoredArgs := []string{
		"--print-all-metrics",
		"--cleanup-queues",
		"--expected-instances",
		"--uri",
		"--consumer-uri",
		"--publisher-uri",
		"--metric-tags",
	}
	ignoredArgs = append(ignoredArgs, cfg.ConsumerUri...)
	ignoredArgs = append(ignoredArgs, cfg.PublisherUri...)
	for _, arg := range os.Args[1:] {
		if slices.ContainsFunc(ignoredArgs, func(a string) bool { return strings.HasPrefix(arg, a) }) {
			continue
		}
		args = append(args, arg)
	}

	labels := map[string]string{
		"command_line": strings.Join(args, " "),
	}
	maps.Copy(labels, globalLabels)
	CommandLineArgs = vmetrics.GetOrCreateGauge("omq_args"+labelsToString(labels), func() float64 { return 1.0 })
}

func MessagesConsumedMetric(priority int) *vmetrics.Counter {
	if counter, ok := messagesConsumedCounters.Load(priority); ok {
		return counter.(*vmetrics.Counter)
	}

	labels := map[string]string{"priority": strconv.Itoa(priority)}
	maps.Copy(labels, globalLabels)
	counter := vmetrics.GetOrCreateCounter(`omq_messages_consumed_total` + labelsToString(labels))

	actual, _ := messagesConsumedCounters.LoadOrStore(priority, counter)
	return actual.(*vmetrics.Counter)
}

func MessagesConsumedOutOfOrderMetric(priority int) *vmetrics.Counter {
	labels := map[string]string{"priority": strconv.Itoa(priority)}
	maps.Copy(labels, globalLabels)
	return vmetrics.GetOrCreateCounter(`omq_messages_consumed_out_of_order` + labelsToString(labels))
}

func MessagesConsumedGapsMetric(priority int) *vmetrics.Counter {
	labels := map[string]string{"priority": strconv.Itoa(priority)}
	maps.Copy(labels, globalLabels)
	return vmetrics.GetOrCreateCounter(`omq_messages_consumed_gaps` + labelsToString(labels))
}

type latencyTracker struct {
	min atomic.Int64
	max atomic.Int64
}

func newLatencyTracker() *latencyTracker {
	t := &latencyTracker{}
	t.min.Store(math.MaxInt64)
	return t
}

func (t *latencyTracker) record(latency time.Duration) {
	ns := latency.Nanoseconds()
	for {
		old := t.min.Load()
		if ns >= old || t.min.CompareAndSwap(old, ns) {
			break
		}
	}
	for {
		old := t.max.Load()
		if ns <= old || t.max.CompareAndSwap(old, ns) {
			break
		}
	}
}

func (t *latencyTracker) reset() (min, max time.Duration, ok bool) {
	minNs := t.min.Swap(math.MaxInt64)
	maxNs := t.max.Swap(0)
	if minNs == math.MaxInt64 {
		return 0, 0, false
	}
	return time.Duration(minNs), time.Duration(maxNs), true
}

var (
	previouslyPublished uint64
	previouslyConsumed  uint64
	pubLatencyTracker   = newLatencyTracker()
	e2eLatencyTracker   = newLatencyTracker()
)

func RecordPublishingLatency(latency time.Duration) {
	PublishingLatency.Update(latency.Seconds())
	pubLatencyTracker.record(latency)
}

func RecordEndToEndLatency(latency time.Duration) {
	if latency <= 0 {
		return
	}
	EndToEndLatency.Update(latency.Seconds())
	e2eLatencyTracker.record(latency)
}

func RecordDelayAccuracy(accuracy time.Duration) {
	DelayAccuracy.Update(accuracy.Seconds())
	if accuracy < 0 {
		MessagesDeliveredTooEarly.Inc()
	}
}

func (m *MetricsServer) printMessageRates(ctx context.Context) {
	go func() {
		zeroRateSeconds := 0
		paused := false
		everPrinted := false

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				published := MessagesPublished.Get()
				consumed := getTotalConsumed()

				// Only start printing once we have some activity
				if !everPrinted && published == 0 && consumed == 0 {
					continue
				}

				publishedRate := published - previouslyPublished
				consumedRate := consumed - previouslyConsumed

				previouslyPublished = published
				previouslyConsumed = consumed

				fields := buildRateFields(publishedRate, consumedRate)

				if publishedRate == 0 && consumedRate == 0 {
					if !paused {
						zeroRateSeconds++
						if zeroRateSeconds >= 5 {
							log.Info("metric printing paused (no activity), will resume when there's something new to print")
							paused = true
						} else {
							log.Print("", fields...)
						}
					}
				} else {
					everPrinted = true
					if paused {
						log.Info("metric printing resumed")
						paused = false
					}
					zeroRateSeconds = 0
					log.Print("", fields...)
				}
			}
		}
	}()
}

func buildRateFields(publishedRate, consumedRate uint64) []any {
	var fields []any
	fields = append(fields, "published", fmt.Sprintf("%v/s", publishedRate))
	fields = append(fields, "consumed", fmt.Sprintf("%v/s", consumedRate))
	if pubMin, pubMax, ok := pubLatencyTracker.reset(); ok {
		fields = append(fields, "pub_min", formatLatency(pubMin), "pub_max", formatLatency(pubMax))
	}
	if e2eMin, e2eMax, ok := e2eLatencyTracker.reset(); ok {
		fields = append(fields, "e2e_min", formatLatency(e2eMin), "e2e_max", formatLatency(e2eMax))
	}
	return fields
}

func formatLatency(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		us := float64(d.Microseconds())
		if us == float64(int64(us)) {
			return fmt.Sprintf("%dµs", int64(us))
		}
		return fmt.Sprintf("%.1f µs", us)
	case d < time.Second:
		ms := float64(d.Nanoseconds()) / float64(time.Millisecond)
		if ms == float64(int64(ms)) {
			return fmt.Sprintf("%dms", int64(ms))
		}
		return fmt.Sprintf("%.1fms", ms)
	default:
		s := d.Seconds()
		if s == float64(int64(s)) {
			return fmt.Sprintf("%ds", int64(s))
		}
		return fmt.Sprintf("%.2fs", s)
	}
}

func (m *MetricsServer) StartTime(t time.Time) {
	m.started = t
}

func (m *MetricsServer) Stop() {
	publishedFinal := MessagesPublished.Get()
	consumedFinal := getTotalConsumed()
	publishedRate := publishedFinal - previouslyPublished
	consumedRate := consumedFinal - previouslyConsumed
	if publishedRate > 0 || consumedRate > 0 {
		fields := buildRateFields(publishedRate, consumedRate)
		log.Print("", fields...)
	}
	m.PrintSummary()
	if m.printAllOnStop {
		m.PrintAll()
	}
}

func (m *MetricsServer) PrintSummary() {
	log.Print("TOTAL PUBLISHED",
		"messages", MessagesPublished.Get(),
		"confirmed", MessagesConfirmed.Get(),
		"returned", MessagesReturned.Get(),
		"rate", fmt.Sprintf("%.2f/s", float64(MessagesPublished.Get())/time.Since(m.started).Seconds()))
	consumed := getTotalConsumed()
	log.Print("TOTAL CONSUMED",
		"messages", consumed,
		"rate", fmt.Sprintf("%.2f/s", float64(consumed)/time.Since(m.started).Seconds()))
}

func getTotalConsumed() uint64 {
	var total uint64

	messagesConsumedCounters.Range(func(key, value any) bool {
		counter := value.(*vmetrics.Counter)
		total += counter.Get()
		return true
	})

	return total
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

	metrics := strings.SplitSeq(string(body), "\n")
	for metric := range metrics {
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
			result += label + "=" + strconv.Quote(value) + ","
		}
		result = strings.TrimSuffix(result, ",") + "}"
	}
	return result
}
