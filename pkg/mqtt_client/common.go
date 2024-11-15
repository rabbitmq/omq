package mqtt_client

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"
)

type Consumer interface {
	Start(context.Context, chan bool)
}

type Publisher interface {
	Start(context.Context)
}

func NewConsumer(cfg config.Config, id int) Consumer {

	topic := utils.InjectId(cfg.ConsumeFrom, id)
	topic = strings.TrimPrefix(topic, "/exchange/amq.topic/")
	topic = strings.TrimPrefix(topic, "/topic/")

	if cfg.MqttConsumer.Version == 5 {
		return &Mqtt5Consumer{
			Id:         id,
			Connection: nil,
			Topic:      topic,
			Config:     cfg,
		}
	} else {
		return &MqttConsumer{
			Id:         id,
			Connection: nil,
			Topic:      topic,
			Config:     cfg,
		}
	}

}

func NewPublisher(cfg config.Config, id int) Publisher {
	topic := utils.InjectId(cfg.PublishTo, id)
	// AMQP-1.0 and STOMP allow /exchange/amq.topic/ prefix
	// since MQTT has no concept of exchanges, we need to remove it
	// this should get more flexible in the future
	topic = strings.TrimPrefix(topic, "/exchange/amq.topic/")
	topic = strings.TrimPrefix(topic, "/topic/")

	if cfg.MqttPublisher.Version == 5 {
		connection := newMqtt5Connection(cfg, id)
		return &Mqtt5Publisher{
			Id:         id,
			Connection: connection,
			Topic:      topic,
			Config:     cfg,
		}
	} else {
		connection := newMqtt34Connection(cfg, id)
		return &MqttPublisher{
			Id:         id,
			Connection: connection,
			Topic:      topic,
			Config:     cfg,
		}
	}

}

func newMqtt34Connection(cfg config.Config, id int) mqtt.Client {
	var token mqtt.Token

	opts := mqtt.NewClientOptions().
		SetClientID(fmt.Sprintf("omq-pub-%d", id)).
		SetAutoReconnect(true).
		SetCleanSession(cfg.MqttPublisher.CleanSession).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Info("publisher connection lost", "id", id)
		}).
		SetProtocolVersion(uint(cfg.MqttPublisher.Version))

	var j int
	for i, n := range utils.WrappedSequence(len(cfg.PublisherUri), id-1) {
		if cfg.SpreadConnections {
			j = n
		} else {
			j = i
		}
		parsedUri := utils.ParseURI(cfg.PublisherUri[j], "mqtt", "1883")
		opts.AddBroker(parsedUri.Broker).SetUsername(parsedUri.Username).SetPassword(parsedUri.Password)
	}

	connection := mqtt.NewClient(opts)
	token = connection.Connect()
	token.Wait()
	if token.Error() != nil {
		log.Error("publisher connection failed", "id", id, "error", token.Error())
	}
	return connection
}

func newMqtt5Connection(cfg config.Config, id int) *autopaho.ConnectionManager {
	u, err := url.Parse("mqtt://localhost:1883")
	if err != nil {
		panic(err)
	}
	opts := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		CleanStartOnInitialConnection: cfg.MqttPublisher.CleanSession,
		KeepAlive:                     20,
		ConnectRetryDelay:             1 * time.Second,
		OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) {
			log.Info("publisher connected", "id", id)
		},
		OnConnectError: func(err error) {
			log.Info("publisher failed to connect ", "id", id, "error", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: fmt.Sprintf("omq-publisher-%d", id),
			OnClientError: func(err error) {
				log.Error("publisher error", "id", id, "error", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				log.Error("publisher disconnected", "id", id, "reasonCode", d.ReasonCode, "reasonString", d.Properties.ReasonString)
			},
		},
	}

	connection, err := autopaho.NewConnection(context.TODO(), opts)
	if err != nil {
		log.Error("publisher connection failed", "id", id, "error", err)
	}
	return connection
}
