package utils

import (
	"encoding/binary"
	"math/rand/v2"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/template"
	"time"

	"golang.org/x/time/rate"

	"github.com/Masterminds/sprig/v3"
	"github.com/panjf2000/ants/v2"
	"github.com/rabbitmq/omq/pkg/log"
)

func MessageBody(size int) []byte {
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
	s = strings.ReplaceAll(s, "%d", strconv.Itoa(id))
	return strings.ReplaceAll(s, "%r", strconv.Itoa(rand.Int()))
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

// ParseHeadersWithTemplates parses header strings and separates templates from regular values
// Returns both regular headers and template headers
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

				// Check if it's a template
				if strings.Contains(value, "{{") && strings.Contains(value, "}}") {
					// Parse as template
					tmpl, err := template.New("header").Funcs(sprig.FuncMap()).Parse(value)
					if err != nil {
						return nil, nil, err
					}
					templates[key] = tmpl
				} else {
					// Parse as regular value
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
	}

	return headers, templates, nil
}
