package mqtt_client

import (
	"context"
	"math/rand"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/rabbitmq/omq/pkg/metrics"
)

type Mqtt5Publisher struct {
	Id         int
	Connection *autopaho.ConnectionManager
	Topic      string
	Config     config.Config
	msg        []byte
}

func (p Mqtt5Publisher) Start(ctx context.Context) {
	// sleep random interval to avoid all publishers publishing at the same time
	s := rand.Intn(1000)
	time.Sleep(time.Duration(s) * time.Millisecond)

	defer func() {
		_ = p.Connection.Disconnect(context.TODO())
	}()

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

func (p Mqtt5Publisher) StartFullSpeed(ctx context.Context) {
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

func (p Mqtt5Publisher) StartIdle(ctx context.Context) {
	log.Info("publisher started", "id", p.Id, "rate", "-", "destination", p.Topic)

	_ = ctx.Done()
}

func (p Mqtt5Publisher) StartRateLimited(ctx context.Context) {
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
				p.Stop("--pmessages value reached")
				return
			}
		}
	}
}

func (p Mqtt5Publisher) Send() {
	// if !p.Connection.IsConnected() {
	// 	time.Sleep(1 * time.Second)
	// 	return
	// }
	utils.UpdatePayload(p.Config.UseMillis, &p.msg)
	startTime := time.Now()
	_, err := p.Connection.Publish(context.TODO(), &paho.Publish{
		QoS:     byte(p.Config.MqttPublisher.QoS),
		Topic:   p.Topic,
		Payload: p.msg,
	})
	if err != nil {
		log.Error("message sending failure", "id", p.Id, "error", err)
		return
	}
	latency := time.Since(startTime)
	metrics.MessagesPublished.Inc()
	metrics.PublishingLatency.Update(latency.Seconds())
	log.Debug("message sent", "id", p.Id, "destination", p.Topic, "latency", latency)
}

func (p Mqtt5Publisher) Stop(reason string) {
	log.Debug("closing connection", "id", p.Id, "reason", reason)
	_ = p.Connection.Disconnect(context.TODO())
}
