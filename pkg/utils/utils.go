package utils

import (
	"encoding/binary"
	"math/rand/v2"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"

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
	for i := 0; i < len; i++ {
		seq[i] = (start + i) % len
	}
	return seq
}

func InjectId(s string, id int) string {
	s = strings.Replace(s, "%d", strconv.Itoa(id), -1)
	return strings.Replace(s, "%r", strconv.Itoa(rand.Int()), -1)
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
