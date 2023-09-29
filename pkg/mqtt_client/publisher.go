package mqtt_client

import (
	"fmt"
	"math/rand"
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
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Info("connection lost", "protocol", "MQTT", "publisherId", n)
		}).
		SetProtocolVersion(4)

	connection := mqtt.NewClient(opts)
	token = connection.Connect()
	token.Wait()

	// topic := fmt.Sprintf("%s-%d", cfg.QueueNamePrefix, ((n-1)%cfg.QueueCount)+1)
	topic := topic.CalculateTopic(cfg, n)

	return &MqttPublisher{
		Id:         n,
		Connection: connection,
		Topic:      topic,
		Config:     cfg,
	}

}

func (p MqttPublisher) Start() {
	// sleep random interval to avoid all publishers publishing at the same time
	s := rand.Intn(1000)
	time.Sleep(time.Duration(s) * time.Millisecond)

	p.msg = utils.MessageBody(p.Config)

	if p.Config.Rate == -1 {
		p.StartFullSpeed()
	} else {
		p.StartRateLimited()
	}
}

func (p MqttPublisher) StartFullSpeed() {
	log.Info("publisher started", "protocol", "MQTT", "publisherId", p.Id, "rate", "unlimited", "destination", p.Topic)
	for i := 1; i <= p.Config.PublishCount; i++ {
		p.Send()
	}

	log.Debug("publisher stopped", "protocol", "MQTT", "publisherId", p.Id)
}

func (p MqttPublisher) StartRateLimited() {
	log.Info("publisher started", "protocol", "MQTT", "publisherId", p.Id, "rate", p.Config.Rate, "destination", p.Topic)
	ticker := time.NewTicker(time.Duration(1000/float64(p.Config.Rate)) * time.Millisecond)
	done := make(chan bool)

	msgsSent := 0
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				p.Send()
				msgsSent++
			}
		}
	}()
	for {
		time.Sleep(1 * time.Second)
		if msgsSent >= p.Config.PublishCount {
			break
		}
	}
}

func (p MqttPublisher) Send() {
	utils.UpdatePayload(p.Config.UseMillis, &p.msg)
	timer := prometheus.NewTimer(metrics.PublishingLatency.With(prometheus.Labels{"protocol": "mqtt"}))
	token := p.Connection.Publish(p.Topic, 1, false, p.msg)
	token.Wait()
	timer.ObserveDuration()
	if token.Error() != nil {
		log.Error("message sending failure", "protocol", "MQTT", "publisherId", p.Id, "error", token.Error())
	}
	log.Debug("message sent", "protocol", "MQTT", "publisherId", p.Id)
	metrics.MessagesPublished.With(prometheus.Labels{"protocol": "mqtt"}).Inc()
}
