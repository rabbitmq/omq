package mqtt

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/omq/pkg/config"
)

var _ = Context("MQTT 3.1/3.1.1 client", func() {
	Describe("uses the correct URI for the publisher", func() {
		cfg := config.Config{
			PublisherUri: []string{"mqtt://publisher:1883"},
			PublisherId:  "my-client-id-%d",
			MqttPublisher: config.MqttOptions{
				Version:      3,
				CleanSession: true,
			},
		}
		publisher := NewMqttPublisher(cfg, 1)
		opts := publisher.connectionOptions()
		Expect(opts.ClientID).To(Equal("my-client-id-1"))
		Expect(opts.Servers[0].Host).To(Equal("publisher:1883"))
	})
	Describe("MQTT 5.0 client", func() {
		Describe("uses the correct URI for the publisher", func() {
			It("--publisher-uri", func() {
				cfg := config.Config{
					PublisherUri: []string{"mqtt://publisher:1883"},
					PublisherId:  "my-client-id-%d",
					MqttPublisher: config.MqttOptions{
						Version:      5,
						CleanSession: true,
					},
				}
				publisher := NewMqtt5Publisher(cfg, 1)
				opts := publisher.connectionOptions()
				Expect(opts.ClientID).To(Equal("my-client-id-1"))
				Expect(opts.ServerUrls[0].Host).To(Equal("publisher:1883"))
			})
		})
	})
})
