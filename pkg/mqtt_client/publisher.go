package mqtt_client

import (
	"context"
	"fmt"
	"math/rand"
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

type MqttPublisher struct {
	Id         int
	Connection mqtt.Client
	Topic      string
	Config     config.Config
	msg        []byte
}

func NewPublisher(cfg config.Config, n int) *MqttPublisher {
	var token mqtt.Token

	// open connection
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.PublisherUri).
		SetUsername("guest").
		SetPassword("guest").
		SetClientID(fmt.Sprintf("omq-pub-%d", n)).
		SetAutoReconnect(true).
		SetCleanSession(cfg.MqttPublisher.CleanSession).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Info("connection lost", "protocol", "MQTT", "publisherId", n)
		}).
		SetProtocolVersion(4)

	connection := mqtt.NewClient(opts)
	token = connection.Connect()
	token.Wait()

	topic := topic.CalculateTopic(cfg.PublishTo, n)
	// AMQP-1.0 and STOMP allow /exchange/amq.topic/ prefix
	// since MQTT has no concept of exchanges, we need to remove it
	// this should get more flexible in the future
	topic = strings.TrimPrefix(topic, "/exchange/amq.topic/")
	topic = strings.TrimPrefix(topic, "/topic/")

	return &MqttPublisher{
		Id:         n,
		Connection: connection,
		Topic:      topic,
		Config:     cfg,
	}

}

func (p MqttPublisher) Start(ctx context.Context) {
	// sleep random interval to avoid all publishers publishing at the same time
	s := rand.Intn(1000)
	time.Sleep(time.Duration(s) * time.Millisecond)

	defer p.Connection.Disconnect(250)

	p.msg = utils.MessageBody(p.Config.Size)

	switch p.Config.Rate {
	case -1:
		p.StartFullSpeed(ctx)
	case 0:
		p.StartIdle(ctx)
	default:
		p.StartRateLimited(ctx)
	}
	log.Debug("publisher stopped", "protocol", "MQTT", "publisherId", p.Id)
}

func (p MqttPublisher) StartFullSpeed(ctx context.Context) {
	log.Info("publisher started", "protocol", "MQTT", "publisherId", p.Id, "rate", "unlimited", "destination", p.Topic)

	for i := 1; i <= p.Config.PublishCount; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			p.Send()
		}
	}
}

func (p MqttPublisher) StartIdle(ctx context.Context) {
	log.Info("publisher started", "protocol", "MQTT", "publisherId", p.Id, "rate", "-", "destination", p.Topic)

	_ = ctx.Done()
}

func (p MqttPublisher) StartRateLimited(ctx context.Context) {
	log.Info("publisher started", "protocol", "MQTT", "publisherId", p.Id, "rate", p.Config.Rate, "destination", p.Topic)
	ticker := time.NewTicker(time.Duration(1000/float64(p.Config.Rate)) * time.Millisecond)

	msgSent := 0
	for {
		select {
		case <-ctx.Done():
			p.Stop("time limit reached")
			return
		case <-ticker.C:
			p.Send()
			msgSent++
			if msgSent >= p.Config.PublishCount {
				p.Stop("publish count reached")
				return
			}
		}
	}
}

func (p MqttPublisher) Send() {
	utils.UpdatePayload(p.Config.UseMillis, &p.msg)
	timer := prometheus.NewTimer(metrics.PublishingLatency.With(prometheus.Labels{"protocol": "mqtt"}))
	token := p.Connection.Publish(p.Topic, byte(p.Config.MqttPublisher.QoS), false, p.msg)
	token.Wait()
	timer.ObserveDuration()
	if token.Error() != nil {
		log.Error("message sending failure", "protocol", "MQTT", "publisherId", p.Id, "error", token.Error())
	}
	log.Debug("message sent", "protocol", "MQTT", "publisherId", p.Id)
	metrics.MessagesPublished.With(prometheus.Labels{"protocol": "mqtt"}).Inc()
}

func (p MqttPublisher) Stop(reason string) {
	log.Debug("closing connection", "protocol", "mqtt", "publisherId", p.Id, "reason", reason)
	p.Connection.Disconnect(250)
}
