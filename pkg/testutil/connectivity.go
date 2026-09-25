package testutil

import (
	"fmt"
	"net"
	"time"
)

func CheckRabbitMQConnectivity() error {
	// Check if we can connect to the standard RabbitMQ ports
	ports := []string{
		"1883",  // MQTT
		"5672",  // AMQP
		"61613", // STOMP
	}

	for _, port := range ports {
		conn, err := net.DialTimeout("tcp", "localhost:"+port, 2*time.Second)
		if err != nil {
			return fmt.Errorf("cannot connect to localhost:%s: %w", port, err)
		}
		_ = conn.Close()
	}

	if _, err := ManagementAPIURL(); err != nil {
		return err
	}

	return nil
}

func ManagementAPIURL() (string, error) {
	endpoints := []struct {
		url  string
		addr string
	}{
		{"https://127.0.0.1:15671", "127.0.0.1:15671"},
		{"http://127.0.0.1:15672", "127.0.0.1:15672"},
	}

	var lastErr error
	for _, endpoint := range endpoints {
		conn, err := net.DialTimeout("tcp", endpoint.addr, 2*time.Second)
		if err == nil {
			_ = conn.Close()
			return endpoint.url, nil
		}
		lastErr = err
	}

	return "", fmt.Errorf("cannot connect to RabbitMQ Management API: %w", lastErr)
}

// RabbitMQFailureMessage returns a standardized failure message for when RabbitMQ is not available
func RabbitMQFailureMessage(err error) string {
	return `To run tests, start RabbitMQ locally`
}
