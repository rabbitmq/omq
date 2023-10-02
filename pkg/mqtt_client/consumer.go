package mqtt_client

import (
	"fmt"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/topic"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/rabbitmq/omq/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus"
)

type MqttConsumer struct {
	Id         int
	Connection mqtt.Client
	Topic      string
	Config     config.Config
}

func NewConsumer(cfg config.Config, id int) *MqttConsumer {
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.ConsumerUri).
		SetUsername("guest").
		SetPassword("guest").
		SetClientID(fmt.Sprintf("omq-sub-%d", id)).
		SetAutoReconnect(true).
		SetCleanSession(cfg.MqttConsumer.CleanSession).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Info("connection lost", "protocol", "MQTT", "consumerId", id)
		}).
		SetProtocolVersion(4)

	var token mqtt.Token
	c := mqtt.NewClient(opts)
	token = c.Connect()
	token.Wait()

	topic := topic.CalculateTopic(cfg, id)
	topic = strings.TrimPrefix(topic, "/exchange/amq.topic/")
	topic = strings.TrimPrefix(topic, "/topic/")

	return &MqttConsumer{
		Id:         id,
		Connection: c,
		Topic:      topic,
		Config:     cfg,
	}
}

func (c MqttConsumer) Start(subscribed chan bool) {
	m := metrics.EndToEndLatency.With(prometheus.Labels{"protocol": "mqtt"})

	msgsReceived := 0

	handler := func(client mqtt.Client, msg mqtt.Message) {
		metrics.MessagesConsumed.With(prometheus.Labels{"protocol": "mqtt"}).Inc()
		payload := msg.Payload()
		m.Observe(utils.CalculateEndToEndLatency(&payload))
		msgsReceived++
		log.Debug("message received", "protocol", "MQTT", "subscriberc.Id", c.Id, "topic", c.Topic, "size", len(payload))
	}

	close(subscribed)
	token := c.Connection.Subscribe(c.Topic, byte(c.Config.MqttConsumer.QoS), handler)
	token.Wait()
	if token.Error() != nil {
		log.Error("failed to subscribe", "protocol", "MQTT", "publisherc.Id", c.Id, "error", token.Error())
	}
	log.Info("consumer started", "protocol", "MQTT", "publisherc.Id", c.Id, "c.Topic", c.Topic)

	defer c.Connection.Disconnect(250)
	for {
		time.Sleep(1 * time.Second)
		if msgsReceived >= c.Config.ConsumeCount {
			break
		}
	}
	log.Debug("consumer finished", "protocol", "MQTT", "publisherc.Id", c.Id)
}
