package utils_test

import (
	"strconv"
	"testing"

	"github.com/rabbitmq/omq/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestURIParsing(t *testing.T) {
	type test struct {
		rawURI        string
		defaultScheme string
		defaultPort   string
		broker        string
		username      string
		password      string
	}

	tests := []test{
		{rawURI: "mqtt://user:pass@name.com", defaultScheme: "mqtt", defaultPort: "1234", broker: "name.com:1234", username: "user", password: "pass"},
		{rawURI: "mqtt://name.com", defaultScheme: "mqtt", defaultPort: "1234", broker: "name.com:1234", username: "guest", password: "guest"},
		{rawURI: "mqtts://local:4321", defaultScheme: "mqtt", defaultPort: "1234", broker: "local:4321", username: "guest", password: "guest"},
		{rawURI: "local:4321", defaultScheme: "mqtt", defaultPort: "1234", broker: "local:4321", username: "guest", password: "guest"},
	}

	for _, tc := range tests {
		t.Run(tc.rawURI+"-"+tc.defaultPort, func(t *testing.T) {
			parsed := utils.ParseURI(tc.rawURI, tc.defaultScheme, tc.defaultPort)
			assert.Equal(t, tc.broker, parsed.Broker)
			assert.Equal(t, tc.username, parsed.Username)
			assert.Equal(t, tc.password, parsed.Password)
		})
	}
}

func TestWrappedSequence(t *testing.T) {
	type test struct {
		length           int
		start            int
		expectedSequence []int
	}

	tests := []test{
		{length: 5, start: 0, expectedSequence: []int{0, 1, 2, 3, 4}},
		{length: 5, start: 1, expectedSequence: []int{1, 2, 3, 4, 0}},
		{length: 3, start: 1, expectedSequence: []int{1, 2, 0}},
		{length: 1, start: 2, expectedSequence: []int{0}},
	}

	for n, tc := range tests {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			assert.Equal(t, tc.expectedSequence, utils.WrappedSequence(tc.length, tc.start))
		})
	}

}
