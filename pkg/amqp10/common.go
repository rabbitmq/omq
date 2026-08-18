package amqp10

import (
	"net/url"
	"os"
	"strings"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"

	"github.com/Azure/go-amqp"
)

// https://docs.oasis-open.org/amqp/soleconn/v1.0/soleconn-v1.0.pdf
const soleConnectionCapability = "sole-connection-for-container"

func hostAndVHost(connectionString string) (string, string) {
	uri, err := url.Parse(connectionString)
	if err != nil {
		log.Error("failed to parse connection string", "error", err.Error())
		os.Exit(1)
	}

	vhost := "/"
	if uri.Path != "" && uri.Path != "/" {
		vhost = strings.TrimPrefix(uri.Path, "/")
	}

	return uri.Hostname(), "vhost:" + vhost
}

// applySoleConnectionOptions requests the AMQP 1.0 sole connection
// capability on the Open frame, if enabled via --amqp-sole-connection.
func applySoleConnectionOptions(connOptions *amqp.ConnOptions, amqpCfg config.AmqpOptions) {
	if !amqpCfg.SoleConnection {
		return
	}

	var policy uint32 // 0 == refuse-connection
	if amqpCfg.SoleConnectionPolicy == "close-existing" {
		policy = 1
	}

	connOptions.DesiredCapabilities = []string{soleConnectionCapability}
	connOptions.Properties = map[string]any{
		"sole-connection-enforcement-policy": policy,
	}
}
