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

	return nil
}

// RabbitMQFailureMessage returns a standardized failure message for when RabbitMQ is not available
func RabbitMQFailureMessage(err error) string {
	return `To run tests, start RabbitMQ locally`
}
