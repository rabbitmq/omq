package amqp10_client

import (
	"net/url"
	"os"
	"strings"

	"github.com/rabbitmq/omq/pkg/log"
)

func amqpVHost(connectionString string) string {
	uri, err := url.Parse(connectionString)
	if err != nil {
		log.Error("failed to parse connection string", "error", err.Error())
		os.Exit(1)
	}

	vhost := "/"
	if uri.Path != "/" {
		vhost = strings.TrimPrefix(uri.Path, "/")
	}

	return "vhost:" + vhost
}
