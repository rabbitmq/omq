package mqtt_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMqttClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MqttClient Suite")
}
