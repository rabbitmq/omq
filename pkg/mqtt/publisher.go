package mqtt

import (
	"context"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"
	"golang.org/x/exp/rand"

	"github.com/rabbitmq/omq/pkg/metrics"
)

type MqttPublisher struct {
	Id         int
	Connection mqtt.Client
	Topic      string
	Config     config.Config
	msg        []byte
}

func NewMqttPublisher(cfg config.Config, id int) MqttPublisher {
	topic := publisherTopic(cfg.PublishTo, id)
	return MqttPublisher{
		Id:         id,
		Connection: nil,
		Topic:      topic,
		Config:     cfg,
	}
}

func (p *MqttPublisher) Connect(ctx context.Context) {
	var token mqtt.Token

	opts := p.connectionOptions()
	connection := mqtt.NewClient(opts)
	for {
		token = connection.Connect()
		token.Wait()
		if token.Error() == nil {
			break
		}
		log.Error("publisher connection failed", "id", p.Id, "error", token.Error())
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
			continue
		}
	}
	p.Connection = connection
}

func (p MqttPublisher) connectionOptions() *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions().
		SetClientID(utils.InjectId(p.Config.PublisherId, p.Id)).
		SetAutoReconnect(true).
		SetCleanSession(p.Config.MqttPublisher.CleanSession).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Info("publisher connection lost", "id", p.Id)
		}).
		SetProtocolVersion(uint(p.Config.MqttPublisher.Version))

	var j int
	for i, n := range utils.WrappedSequence(len(p.Config.PublisherUri), p.Id-1) {
		if p.Config.SpreadConnections {
			j = n
		} else {
			j = i
		}
		parsedUri := utils.ParseURI(p.Config.PublisherUri[j], "mqtt", "1883")
		opts.AddBroker(parsedUri.Broker).SetUsername(parsedUri.Username).SetPassword(parsedUri.Password)
	}
	return opts
}

func (p MqttPublisher) Start(ctx context.Context, publisherReady chan bool, startPublishing chan bool) {
	defer func() {
		if p.Connection != nil {
			p.Connection.Disconnect(250)
		}
	}()

	p.Connect(ctx)

	p.msg = utils.MessageBody(p.Config.Size)

	close(publisherReady)

	select {
	case <-ctx.Done():
		return
	case <-startPublishing:
		// short random delay to avoid all publishers publishing at the same time
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
	}

	log.Info("publisher started", "id", p.Id, "rate", p.Config.Rate, "destination", p.Topic)

	var farewell string
	if p.Config.Rate == 0 {
		// idle connection
		<-ctx.Done()
		farewell = "context cancelled"
	} else {
		farewell = p.StartPublishing(ctx)
	}
	p.Stop(farewell)
}

func (p MqttPublisher) StartPublishing(ctx context.Context) string {
	limiter := utils.RateLimiter(p.Config.Rate)

	var msgSent atomic.Int64
	for {
		select {
		case <-ctx.Done():
			return "time limit reached"
		default:
			if msgSent.Add(1) > int64(p.Config.PublishCount) {
				return "--pmessages value reached"
			}
			if p.Config.Rate > 0 {
				_ = limiter.Wait(ctx)
			}
			p.Send()
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
	log.Debug("closing publisher connection", "id", p.Id, "reason", reason)
	if p.Connection != nil {
		p.Connection.Disconnect(250)
	}
}
