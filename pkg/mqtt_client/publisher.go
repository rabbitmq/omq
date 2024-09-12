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
)

type MqttPublisher struct {
	Id         int
	Connection mqtt.Client
	Topic      string
	Config     config.Config
	msg        []byte
}

func NewPublisher(cfg config.Config, id int) *MqttPublisher {
	var token mqtt.Token

	opts := mqtt.NewClientOptions().
		SetClientID(fmt.Sprintf("omq-pub-%d", id)).
		SetAutoReconnect(true).
		SetCleanSession(cfg.MqttPublisher.CleanSession).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Info("publisher connection lost", "id", id)
		}).
		SetProtocolVersion(4)

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

	topic := topic.CalculateTopic(cfg.PublishTo, id)
	// AMQP-1.0 and STOMP allow /exchange/amq.topic/ prefix
	// since MQTT has no concept of exchanges, we need to remove it
	// this should get more flexible in the future
	topic = strings.TrimPrefix(topic, "/exchange/amq.topic/")
	topic = strings.TrimPrefix(topic, "/topic/")

	return &MqttPublisher{
		Id:         id,
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
	log.Debug("publisher stopped", "id", p.Id)
}

func (p MqttPublisher) StartFullSpeed(ctx context.Context) {
	log.Info("publisher started", "id", p.Id, "rate", "unlimited", "destination", p.Topic)

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
	log.Info("publisher started", "id", p.Id, "rate", "-", "destination", p.Topic)

	_ = ctx.Done()
}

func (p MqttPublisher) StartRateLimited(ctx context.Context) {
	log.Info("publisher started", "id", p.Id, "rate", p.Config.Rate, "destination", p.Topic)
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
	if !p.Connection.IsConnected() {
		time.Sleep(1 * time.Second)
		return
	}
	utils.UpdatePayload(p.Config.UseMillis, &p.msg)
	startTime := time.Now()
	token := p.Connection.Publish(p.Topic, byte(p.Config.MqttPublisher.QoS), false, p.msg)
	token.Wait()
	latency := time.Since(startTime)
	if token.Error() != nil {
		log.Error("message sending failure", "id", p.Id, "error", token.Error())
	} else {
		metrics.MessagesPublished.Inc()
		metrics.PublishingLatency.Update(latency.Seconds())
		log.Debug("message sent", "id", p.Id, "destination", p.Topic, "latency", latency)
	}
}

func (p MqttPublisher) Stop(reason string) {
	log.Debug("closing connection", "id", p.Id, "reason", reason)
	p.Connection.Disconnect(250)
}
