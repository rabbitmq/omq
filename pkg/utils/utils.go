package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	"golang.org/x/time/rate"

	"github.com/Masterminds/sprig/v3"
	"github.com/panjf2000/ants/v2"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
)

// ParseSize parses a size string that may include units (e.g., "10mb", "1kb", "100")
// Supported units: b, kb, mb (case-insensitive)
// If no unit is specified, assumes bytes
func ParseSize(sizeStr string) (int, error) {
	sizeStr = strings.TrimSpace(sizeStr)

	// Try to parse as plain integer first (for backward compatibility)
	if size, err := strconv.Atoi(sizeStr); err == nil {
		return size, nil
	}

	// Regular expression to match number and optional unit (no whitespace allowed)
	re := regexp.MustCompile(`^(\d+(?:\.\d+)?)([a-zA-Z]*)$`)
	matches := re.FindStringSubmatch(sizeStr)

	if len(matches) != 3 {
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}

	// Parse the numeric part
	value, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid numeric value in size: %s", sizeStr)
	}

	// Parse the unit (if present)
	unit := strings.ToLower(matches[2])
	var multiplier float64

	switch unit {
	case "", "b":
		multiplier = 1
	case "kb":
		multiplier = 1024
	case "mb":
		multiplier = 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown size unit: %s", unit)
	}

	result := int(value * multiplier)
	return result, nil
}

func MessageBody(staticSize int, sizeTemplate *template.Template, id int) []byte {
	size := 12

	if staticSize > 0 {
		size = staticSize
	} else if sizeTemplate != nil {
		sizeStr := ExecuteTemplate(sizeTemplate, id)
		if parsedSize, err := ParseSize(sizeStr); err == nil {
			size = parsedSize
		}
	}

	b := make([]byte, size)
	binary.BigEndian.PutUint32(b[0:], uint32(1234)) // currently unused, for compatibility with perf-test
	return b
}

func UpdatePayload(useMillis bool, payload *[]byte) *[]byte {
	if useMillis {
		binary.BigEndian.PutUint64((*payload)[4:], uint64(time.Now().UnixMilli()))
	} else {
		binary.BigEndian.PutUint64((*payload)[4:], uint64(time.Now().UnixNano()))
	}
	return payload
}

func CalculateEndToEndLatency(payload *[]byte) (time.Time, time.Duration) {
	if len(*payload) < 12 {
		// message sent without latency tracking
		return time.Unix(0, 0), 0
	}
	now := time.Now()
	timeSent := FormatTimestamp(binary.BigEndian.Uint64((*payload)[4:]))
	latency := now.Sub(timeSent)
	return timeSent, latency
}

// CalculateDelayAccuracy calculates the accuracy of delayed message delivery.
// It compares the expected delivery time (timeSent + delay) with the actual delivery time.
// Returns the delay accuracy (negative means early, positive means late) and whether the message was delayed.
func CalculateDelayAccuracy(payload *[]byte, delayMs int64) (time.Duration, bool) {
	if len(*payload) < 12 || delayMs <= 0 {
		// message sent without latency tracking or no delay specified
		return 0, false
	}

	now := time.Now()
	timeSent := FormatTimestamp(binary.BigEndian.Uint64((*payload)[4:]))
	expectedDeliveryTime := timeSent.Add(time.Duration(delayMs) * time.Millisecond)
	delayAccuracy := now.Sub(expectedDeliveryTime)

	return delayAccuracy, true
}

func FormatTimestamp(timestamp uint64) time.Time {
	var t time.Time
	// should be updated before the year 2100 ;)
	if timestamp < 4102441200000 {
		t = time.UnixMilli(int64(timestamp))
	} else {
		t = time.Unix(0, int64(timestamp))
	}
	return t
}

type uri struct {
	Broker   string
	Username string
	Password string
}

func ParseURI(rawURI string, defaultScheme string, defaultPort string) uri {
	if !strings.HasPrefix(rawURI, defaultScheme) {
		rawURI = defaultScheme + "://" + rawURI
	}

	u, err := url.Parse(rawURI)
	if err != nil {
		log.Error("Cannot parse consumer URI", err)
		os.Exit(1)
	}

	port := defaultPort
	if u.Port() != "" {
		port = u.Port()
	}
	user := "guest"
	if u.User.Username() != "" {
		user = u.User.Username()
	}

	pass := "guest"
	if p, isSet := u.User.Password(); isSet {
		pass = p
	}

	result := &uri{
		Broker:   u.Hostname() + ":" + port,
		Username: user,
		Password: pass,
	}

	return *result
}

// generate a sequence of `len` integres starting with `start`
// and wrapped, so that it contains all values from 0 to len-1
func WrappedSequence(len int, start int) []int {
	if start > len {
		start = start % len
	}
	seq := make([]int, len)
	for i := range len {
		seq[i] = (start + i) % len
	}
	return seq
}

func InjectId(s string, id int) string {
	return strings.ReplaceAll(s, "%d", strconv.Itoa(id))
}

func Rate(rate float32) string {
	switch rate {
	case -1:
		return "unlimited"
	case 0:
		return "idle"
	default:
		return strconv.FormatFloat(float64(rate), 'f', -1, 64)
	}
}

func RateLimiter(publishRate float32) *rate.Limiter {
	var limit rate.Limit
	if publishRate == -1 {
		limit = rate.Inf
	} else {
		limit = rate.Limit(publishRate)
	}
	// burst may need to be adjusted/dynamic, but for now it works pretty well
	return rate.NewLimiter(limit, 1)
}

func AntsPool(maxInFlight int) (*ants.Pool, error) {
	return ants.NewPool(maxInFlight, ants.WithExpiryDuration(time.Duration(10*time.Second)), ants.WithNonblocking(false))
}

// ParseHeaders parses header strings in the format "key1=value1,key2=value2"
// and converts numeric values to appropriate types (int64 or float64)
func ParseHeaders(headerStrings []string) map[string]any {
	headers := make(map[string]any)

	for _, headerString := range headerStrings {
		pairs := strings.SplitSeq(headerString, ",")
		for pair := range pairs {
			parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])

				if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
					headers[key] = intVal
				} else if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
					headers[key] = floatVal
				} else {
					headers[key] = value
				}
			}
		}
	}

	return headers
}

// ParseHeadersWithTemplates parses header strings as templates.
// Non-template strings will return as-is when executed.
func ParseHeadersWithTemplates(headerStrings []string) (map[string]any, map[string]*template.Template, error) {
	headers := make(map[string]any)
	templates := make(map[string]*template.Template)

	for _, headerString := range headerStrings {
		pairs := strings.SplitSeq(headerString, ",")
		for pair := range pairs {
			parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])

				// Always parse as template (non-template strings will return as-is when executed)
				tmpl, err := template.New("header").Funcs(sprig.FuncMap()).Parse(value)
				if err != nil {
					return nil, nil, err
				}
				templates[key] = tmpl
			}
		}
	}

	return headers, templates, nil
}

func ExecuteTemplate(tmpl *template.Template, id int) string {
	if tmpl == nil {
		return ""
	}

	data := map[string]any{
		"id": id,
	}

	var buf bytes.Buffer
	err := tmpl.Execute(&buf, data)
	if err != nil {
		log.Error("template execution failed", "error", err)
		os.Exit(1)
	}
	result := buf.String()

	// If the result doesn't contain template syntax...
	if !strings.Contains(result, "{{") {
		// Apply InjectId to handle %d
		result = InjectId(result, id)

		// If the result has commas, cycle through values
		if strings.Contains(result, ",") {
			values := strings.Split(result, ",")
			// Use message count to cycle through values
			var index uint64
			if metrics.MessagesPublished != nil {
				index = metrics.MessagesPublished.Get() % uint64(len(values))
			}
			return strings.TrimSpace(values[index])
		}
	}

	return result
}

func ResolveTerminus(destination string, template *template.Template, id int, cfg config.Config) string {
	return ExecuteTemplate(template, id)
}
