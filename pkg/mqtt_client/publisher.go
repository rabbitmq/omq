package mqtt_client

import (
	"context"
	"math/rand"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
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

	for msgSent := 0; msgSent < p.Config.PublishCount; msgSent++ {
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

	<-ctx.Done()
}

func (p MqttPublisher) StartRateLimited(ctx context.Context) {
	log.Info("publisher started", "id", p.Id, "rate", p.Config.Rate, "destination", p.Topic)
	ticker := utils.RateTicker(p.Config.Rate)

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
				p.Stop("--pmessages value reached")
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
