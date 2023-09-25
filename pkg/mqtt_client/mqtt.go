package mqtt_client

import (
	"fmt"
	"math/rand"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/rabbitmq/omq/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus"
)

func Publisher(cfg config.Config, n int) {
	var token mqtt.Token

	// sleep random interval to avoid all publishers publishing at the same time
	s := rand.Intn(cfg.Publishers)
	time.Sleep(time.Duration(s) * time.Millisecond)

	// open connection
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.PublisherUri).
		SetUsername("guest").
		SetPassword("guest").
		SetClientID(fmt.Sprintf("omq-pub-%d", n)).
		SetAutoReconnect(true).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Info("connection lost", "protocol", "MQTT", "publisherId", n)
		}).
		SetProtocolVersion(4)

	c := mqtt.NewClient(opts)
	token = c.Connect()
	token.Wait()
	log.Info("publisher started", "protocol", "MQTT", "publisherId", n)

	topic := fmt.Sprintf("%s-%d", cfg.QueueNamePrefix, ((n-1)%cfg.QueueCount)+1)

	// message payload will be reused with the first bytes overwritten
	msg := utils.MessageBody(cfg)

	// main loop
	for i := 1; i <= cfg.PublishCount; i++ {
		utils.UpdatePayload(cfg.UseMillis, &msg)
		timer := prometheus.NewTimer(metrics.PublishingLatency.With(prometheus.Labels{"protocol": "mqtt"}))
		token = c.Publish(topic, 1, false, msg)
		token.Wait()
		timer.ObserveDuration()
		if token.Error() != nil {
			log.Error("message sending failure", "protocol", "MQTT", "publisherId", n, "error", token.Error())
		}
		log.Debug("message sent", "protocol", "MQTT", "publisherId", n)
		metrics.MessagesPublished.With(prometheus.Labels{"protocol": "mqtt"}).Inc()
		utils.WaitBetweenMessages(cfg.Rate)
	}

	log.Debug("publisher stopped", "protocol", "MQTT", "publisherId", n)
}

func Consumer(cfg config.Config, subscribed chan bool, n int) {
	// sleep random interval to avoid all consumers publishing at the same time
	s := rand.Intn(cfg.Consumers)
	time.Sleep(time.Duration(s) * time.Millisecond)

	// open connection
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.ConsumerUri).
		SetUsername("guest").
		SetPassword("guest").
		SetClientID(fmt.Sprintf("omq-sub-%d", n)).
		SetAutoReconnect(true).
		SetCleanSession(false).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Info("connection lost", "protocol", "MQTT", "consumerId", n)
		}).
		SetProtocolVersion(4)

	var token mqtt.Token
	c := mqtt.NewClient(opts)
	token = c.Connect()
	token.Wait()

	topic := fmt.Sprintf("%s-%d", cfg.QueueNamePrefix, ((n-1)%cfg.QueueCount)+1)

	m := metrics.EndToEndLatency.With(prometheus.Labels{"protocol": "mqtt"})

	msgsReceived := 0

	handler := func(client mqtt.Client, msg mqtt.Message) {
		metrics.MessagesConsumed.With(prometheus.Labels{"protocol": "mqtt"}).Inc()
		payload := msg.Payload()
		m.Observe(utils.CalculateEndToEndLatency(&payload))
		msgsReceived++
		log.Debug("message received", "protocol", "MQTT", "subscriberId", n, "terminus", topic, "size", len(payload))
	}

	close(subscribed)
	token = c.Subscribe(topic, 1, handler)
	token.Wait()
	if token.Error() != nil {
		log.Error("failed to subscribe", "protocol", "MQTT", "publisherId", n, "error", token.Error())
	}
	log.Info("consumer started", "protocol", "MQTT", "publisherId", n, "topic", topic)

	for {
		time.Sleep(1 * time.Second)
		if msgsReceived >= cfg.ConsumeCount {
			break
		}
	}
	log.Debug("consumer finished", "protocol", "MQTT", "publisherId", n)
}
