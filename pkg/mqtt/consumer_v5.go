package mqtt

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
	ctx        context.Context
}

func NewMqtt5Consumer(ctx context.Context, cfg config.Config, id int) Mqtt5Consumer {
	topic := publisherTopic(cfg.ConsumeFrom, cfg.ConsumeFromTemplate, id, cfg)
	return Mqtt5Consumer{
		Id:         id,
		Connection: nil,
		Topic:      topic,
		Config:     cfg,
		ctx:        ctx,
	}
}

func (c Mqtt5Consumer) Start(consumerReady chan bool) {
	msgsReceived := 0
	previousMessageTimeSent := time.Unix(0, 0)

	handler := func(rcv paho.PublishReceived) (bool, error) {
		metrics.MessagesConsumedMetric(0).Inc()
		payload := rcv.Packet.Payload
		timeSent, latency := utils.CalculateEndToEndLatency(&payload)
		metrics.EndToEndLatency.UpdateDuration(timeSent)

		if c.Config.LogOutOfOrder && timeSent.Before(previousMessageTimeSent) {
			metrics.MessagesConsumedOutOfOrderMetric(0).Inc()
			log.Info("out of order message received. This message was sent before the previous message", "this messsage", timeSent, "previous message", previousMessageTimeSent)
		}
		previousMessageTimeSent = timeSent

		msgsReceived++
		log.Debug("message received", "id", c.Id, "topic", c.Topic, "size", len(payload), "latency", latency)
		return true, nil
	}

	urls := stringsToUrls(c.Config.ConsumerUri)
	pass, _ := urls[0].User.Password()
	opts := autopaho.ClientConfig{
		ServerUrls:                    urls,
		ConnectUsername:               urls[0].User.Username(),
		ConnectPassword:               []byte(pass),
		CleanStartOnInitialConnection: c.Config.MqttConsumer.CleanSession,
		SessionExpiryInterval:         uint32(c.Config.MqttConsumer.SessionExpiryInterval.Seconds()),
		KeepAlive:                     20,
		ConnectRetryDelay:             1 * time.Second,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			log.Info("consumer connected", "id", c.Id, "topic", c.Topic)
			if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{
						Topic: c.Topic,
						QoS:   byte(c.Config.MqttConsumer.QoS),
					},
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
	c.Connection, err = autopaho.NewConnection(c.ctx, opts)
	if err != nil {
		log.Error("consumer connection failed", "id", c.Id, "error", err)
	}
	err = c.Connection.AwaitConnection(c.ctx)
	if err != nil {
		// AwaitConnection only returns an error if the context is cancelled
		return
	}
	close(consumerReady)

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

func (c Mqtt5Consumer) Stop(reason string) {
	log.Debug("closing consumer connection", "id", c.Id, "reason", reason)
	if c.Connection != nil {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = c.Connection.Disconnect(disconnectCtx)
	}
}
