package mqtt_client

import (
	"context"
	"fmt"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/topic"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/rabbitmq/omq/pkg/metrics"
)

type MqttConsumer struct {
	Id         int
	Connection mqtt.Client
	Topic      string
	Config     config.Config
}

func NewConsumer(cfg config.Config, id int) *MqttConsumer {
	opts := mqtt.NewClientOptions().
		SetClientID(fmt.Sprintf("omq-sub-%d", id)).
		SetAutoReconnect(true).
		SetCleanSession(cfg.MqttConsumer.CleanSession).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Info("connection lost", "consumerId", id)
		}).
		SetProtocolVersion(4)

	for _, n := range utils.WrappedSequence(len(cfg.ConsumerUri), id-1) {
		parsedUri := utils.ParseURI(cfg.ConsumerUri[n], "mqtt", "1883")
		opts.AddBroker(parsedUri.Broker).
			SetUsername(parsedUri.Username).
			SetPassword(parsedUri.Password)
	}

	var token mqtt.Token
	c := mqtt.NewClient(opts)
	token = c.Connect()
	token.Wait()

	topic := topic.CalculateTopic(cfg.ConsumeFrom, id)
	topic = strings.TrimPrefix(topic, "/exchange/amq.topic/")
	topic = strings.TrimPrefix(topic, "/topic/")

	return &MqttConsumer{
		Id:         id,
		Connection: c,
		Topic:      topic,
		Config:     cfg,
	}
}

func (c MqttConsumer) Start(ctx context.Context, subscribed chan bool) {
	msgsReceived := 0

	previousMessageTimeSent := time.Unix(0, 0)

	handler := func(client mqtt.Client, msg mqtt.Message) {
		metrics.MessagesConsumedNormalPriority.Inc()
		payload := msg.Payload()
		timeSent, latency := utils.CalculateEndToEndLatency(&payload)
		metrics.EndToEndLatency.UpdateDuration(timeSent)

		if c.Config.LogOutOfOrder && timeSent.Before(previousMessageTimeSent) {
			metrics.MessagesConsumedOutOfOrderNormalPriority.Inc()
			log.Info("Out of order message received. This message was sent before the previous message", "this messsage", timeSent, "previous message", previousMessageTimeSent)
		}
		previousMessageTimeSent = timeSent

		msgsReceived++
		log.Debug("message received", "consumerId", c.Id, "topic", c.Topic, "size", len(payload), "latency", latency)
	}

	close(subscribed)
	token := c.Connection.Subscribe(c.Topic, byte(c.Config.MqttConsumer.QoS), handler)
	token.Wait()
	if token.Error() != nil {
		log.Error("failed to subscribe", "consumerId", c.Id, "error", token.Error())
	}
	log.Info("consumer started", "id", c.Id, "topic", c.Topic)

	// TODO: currently we can consume more than ConsumerCount messages
	for msgsReceived < c.Config.ConsumeCount {
		select {
		case <-ctx.Done():
			c.Stop("time limit reached")
			return
		default:
			time.Sleep(1 * time.Second)

		}
	}
	c.Stop("message count reached")
}

func (c MqttConsumer) Stop(reason string) {
	log.Debug("closing connection", "consumerId", c.Id, "reason", reason)
	c.Connection.Disconnect(250)
}
