package mqtt

import (
	"context"
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
	topic := publisherTopic(cfg.ConsumeFrom, id)
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
	previousMessageTimeSent := time.Unix(0, 0)

	handler := func(client mqtt.Client, msg mqtt.Message) {
		payload := msg.Payload()
		handleMessage(payload)
		metrics.MessagesConsumedNormalPriority.Inc()
		timeSent, latency := utils.CalculateEndToEndLatency(&payload)
		metrics.EndToEndLatency.UpdateDuration(timeSent)

		if c.Config.LogOutOfOrder && timeSent.Before(previousMessageTimeSent) {
			metrics.MessagesConsumedOutOfOrderNormalPriority.Inc()
			log.Info("out of order message received. This message was sent before the previous message", "this messsage", timeSent, "previous message", previousMessageTimeSent)
		}
		previousMessageTimeSent = timeSent

		msgsReceived++
		log.Debug("message received", "id", c.Id, "topic", c.Topic, "size", len(payload), "latency", latency)
	}

	opts := mqtt.NewClientOptions().
		SetClientID(utils.InjectId(c.Config.ConsumerId, c.Id)).
		SetAutoReconnect(true).
		SetCleanSession(c.Config.MqttConsumer.CleanSession).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Info("consumer connection lost", "id", c.Id)
		}).
		SetProtocolVersion(uint(c.Config.MqttConsumer.Version))

	opts.OnConnect = func(client mqtt.Client) {
		token := client.Subscribe(c.Topic, byte(c.Config.MqttConsumer.QoS), handler)
		token.Wait()
		if token.Error() != nil {
			log.Error("failed to subscribe", "id", c.Id, "error", token.Error())
		}
		log.Info("consumer subscribed", "id", c.Id, "topic", c.Topic)
	}

	var j int
	for i, n := range utils.WrappedSequence(len(c.Config.ConsumerUri), c.Id-1) {
		if c.Config.SpreadConnections {
			j = n
		} else {
			j = i
		}
		parsedUri := utils.ParseURI(c.Config.ConsumerUri[j], "mqtt", "1883")
		opts.AddBroker(parsedUri.Broker).SetUsername(parsedUri.Username).SetPassword(parsedUri.Password)
	}

	var token mqtt.Token
	c.Connection = mqtt.NewClient(opts)
	token = c.Connection.Connect()
	token.Wait()
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
		default:
			time.Sleep(1 * time.Second)

		}
	}
	c.Stop("--cmessages value reached")
}

func (c MqttConsumer) Stop(reason string) {
	log.Debug("closing consumer connection", "id", c.Id, "reason", reason)
	c.Connection.Disconnect(250)
}

func handleMessage(msg []byte) {
}
