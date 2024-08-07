package utils_test

import (
	"testing"

	"github.com/rabbitmq/omq/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestURIParsing(t *testing.T) {
	type test struct {
		rawURI      string
		defaultPort string
		broker      string
		username    string
		password    string
	}

	tests := []test{
		{rawURI: "mqtt://user:pass@name.com", defaultPort: "1234", broker: "name.com:1234", username: "user", password: "pass"},
		{rawURI: "mqtt://name.com", defaultPort: "1234", broker: "name.com:1234", username: "guest", password: "guest"},
		{rawURI: "mqtt://local:4321", defaultPort: "1234", broker: "local:4321", username: "guest", password: "guest"},
	}

	for _, tc := range tests {
		t.Run(tc.rawURI+"-"+tc.defaultPort, func(t *testing.T) {
			parsed := utils.ParseURI(tc.rawURI, tc.defaultPort)
			assert.Equal(t, tc.broker, parsed.Broker)
			assert.Equal(t, tc.username, parsed.Username)
			assert.Equal(t, tc.password, parsed.Password)
		})
	}
}
