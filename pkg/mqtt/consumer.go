package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/rabbitmq/omq/pkg/metrics"
)

type MqttConsumer struct {
	Id         int
	Connection mqtt.Client
	Topic      string
	Config     config.Config
	ctx        context.Context
}

func NewMqttConsumer(ctx context.Context, cfg config.Config, id int) MqttConsumer {
	topic := publisherTopic(cfg.ConsumeFromTemplate, id)
	return MqttConsumer{
		Id:         id,
		Connection: nil,
		Topic:      topic,
		Config:     cfg,
		ctx:        ctx,
	}
}

func (c MqttConsumer) Start(cosumerReady chan bool) {
	msgsReceived := 0

	handler := func(client mqtt.Client, msg mqtt.Message) {
		payload := msg.Payload()
		metrics.MessagesConsumedMetric(0).Inc()
		_, latency := utils.CalculateEndToEndLatency(&payload)
		metrics.RecordEndToEndLatency(latency)

		msgsReceived++
		log.Debug("message received", "id", c.Id, "topic", c.Topic, "size", len(payload), "latency", latency)
	}

	opts := mqtt.NewClientOptions().
		SetClientID(utils.InjectId(c.Config.ConsumerId, c.Id)).
		SetAutoReconnect(true).
		SetCleanSession(c.Config.MqttConsumer.CleanSession).
		SetConnectTimeout(30 * time.Second).
		SetWriteTimeout(30 * time.Second).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Info("consumer connection lost", "id", c.Id)
		}).
		SetProtocolVersion(uint(c.Config.MqttConsumer.Version)).
		SetTLSConfig(&tls.Config{
			InsecureSkipVerify: c.Config.InsecureSkipTLSVerify,
		})

	opts.OnConnect = func(client mqtt.Client) {
		subsPerConsumer := c.Config.MqttConsumer.SubscriptionsPerConsumer
		if subsPerConsumer == 0 {
			log.Info("consumer connected (no subscriptions)", "id", c.Id)
			return
		}
		for i := 1; i <= subsPerConsumer; i++ {
			topic := c.Topic
			if subsPerConsumer > 1 {
				topic = fmt.Sprintf("%s/%d", c.Topic, i)
			}
			token := client.Subscribe(topic, byte(c.Config.MqttConsumer.QoS), handler)
			token.Wait()
			if token.Error() != nil {
				log.Error("failed to subscribe", "id", c.Id, "error", token.Error())
			}
			log.Info("consumer subscribed", "id", c.Id, "topic", topic)
		}
	}

	var j int
	for i, n := range utils.WrappedSequence(len(c.Config.ConsumerUri), c.Id) {
		if c.Config.SpreadConnections {
			j = n
		} else {
			j = i
		}
		parsedUri := utils.ParseURI(c.Config.ConsumerUri[j], "mqtt", "1883")
		opts.AddBroker(parsedUri.Scheme + "://" + parsedUri.Broker).SetUsername(parsedUri.Username).SetPassword(parsedUri.Password)
	}

	c.Connection = mqtt.NewClient(opts)
	token := c.Connection.Connect()
	// Use WaitTimeout to allow checking context cancellation
	for !token.WaitTimeout(100 * time.Millisecond) {
		select {
		case <-c.ctx.Done():
			close(cosumerReady)
			c.Stop("context cancelled")
			return
		default:
		}
	}
	if token.Error() != nil {
		log.Error("failed to connect", "id", c.Id, "error", token.Error())
	}

	close(cosumerReady)

	// TODO: currently we can consume more than ConsumerCount messages
	for msgsReceived < c.Config.ConsumeCount {
		select {
		case <-c.ctx.Done():
			c.Stop("time limit reached")
			return
		case <-time.After(100 * time.Millisecond):
			// Check more frequently to respond to context cancellation faster
		}
	}
	c.Stop("--cmessages value reached")
}

func (c MqttConsumer) Stop(reason string) {
	log.Debug("closing consumer connection", "id", c.Id, "reason", reason)
	if c.Connection != nil {
		c.Connection.Disconnect(250)
	}
}
