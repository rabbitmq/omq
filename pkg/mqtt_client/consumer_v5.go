package mqtt_client

import (
	"context"
	"fmt"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/utils"
)

type Mqtt5Consumer struct {
	Id         int
	Connection *autopaho.ConnectionManager
	Topic      string
	Config     config.Config
}

func (c Mqtt5Consumer) Start(ctx context.Context, subscribed chan bool) {
	msgsReceived := 0
	previousMessageTimeSent := time.Unix(0, 0)

	handler := func(rcv paho.PublishReceived) (bool, error) {
		metrics.MessagesConsumedNormalPriority.Inc()
		payload := rcv.Packet.Payload
		timeSent, latency := utils.CalculateEndToEndLatency(&payload)
		metrics.EndToEndLatency.UpdateDuration(timeSent)

		if c.Config.LogOutOfOrder && timeSent.Before(previousMessageTimeSent) {
			metrics.MessagesConsumedOutOfOrderNormalPriority.Inc()
			log.Info("Out of order message received. This message was sent before the previous message", "this messsage", timeSent, "previous message", previousMessageTimeSent)
		}
		previousMessageTimeSent = timeSent

		msgsReceived++
		log.Debug("message received", "id", c.Id, "topic", c.Topic, "size", len(payload), "latency", latency)
		return true, nil
	}

	opts := autopaho.ClientConfig{
		ServerUrls:                    stringsToUrls(c.Config.ConsumerUri),
		CleanStartOnInitialConnection: c.Config.MqttConsumer.CleanSession,
		KeepAlive:                     20,
		ConnectRetryDelay:             1 * time.Second,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			log.Info("consumer connected", "id", c.Id, "topic", c.Topic)
			if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: c.Topic, QoS: 1},
				},
			}); err != nil {
				fmt.Printf("failed to subscribe (%s). This is likely to mean no messages will be received.", err)
			}
		},
		OnConnectError: func(err error) {
			log.Info("consumer failed to connect ", "id", c.Id, "error", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: utils.InjectId(c.Config.ConsumerId, c.Id),
			OnClientError: func(err error) {
				log.Error("consumer error", "id", c.Id, "error", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				log.Error("consumer disconnected", "id", c.Id, "reasonCode", d.ReasonCode, "reasonString", d.Properties.ReasonString)
			},
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				handler,
			},
		},
	}

	var err error
	c.Connection, err = autopaho.NewConnection(context.TODO(), opts)
	if err != nil {
		log.Error("consumer connection failed", "id", c.Id, "error", err)
	}
	close(subscribed)

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

func (c Mqtt5Consumer) Stop(reason string) {
	log.Debug("closing connection", "id", c.Id, "reason", reason)
	_ = c.Connection.Disconnect(context.TODO())
}
