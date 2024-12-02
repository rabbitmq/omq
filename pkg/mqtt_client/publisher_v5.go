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

func NewMqtt5Publisher(cfg config.Config, id int) Mqtt5Publisher {
	topic := publisherTopic(cfg.PublishTo, id)
	return Mqtt5Publisher{
		Id:         id,
		Connection: nil,
		Topic:      topic,
		Config:     cfg,
	}
}

func (p Mqtt5Publisher) Start(ctx context.Context) {
	// sleep random interval to avoid all publishers publishing at the same time
	s := rand.Intn(1000)
	time.Sleep(time.Duration(s) * time.Millisecond)

	defer p.Stop("shutting down")

	p.Connect(ctx)

	p.msg = utils.MessageBody(p.Config.Size)

	switch p.Config.Rate {
	case -1:
		p.StartFullSpeed(ctx)
	case 0:
		p.StartIdle(ctx)
	default:
		p.StartRateLimited(ctx)
	}
	// TODO it seems that sometimes if we stop quickly after sending
	// a message, this message is not delivered, even though Publish
	// is supposed to by synchronous; to be investigated
	time.Sleep(500 * time.Millisecond)
	log.Debug("publisher stopped", "id", p.Id)
}

func (p *Mqtt5Publisher) Connect(ctx context.Context) {
	opts := p.connectionOptions()
	connection, err := autopaho.NewConnection(ctx, opts)
	if err != nil {
		log.Error("publisher connection failed", "id", p.Id, "error", err)
	}
	err = connection.AwaitConnection(ctx)
	if err != nil {
		// AwaitConnection only returns an error if the context is cancelled
		return
	}
	p.Connection = connection
}

func (p Mqtt5Publisher) connectionOptions() autopaho.ClientConfig {
	urls := stringsToUrls(p.Config.PublisherUri)
	pass, _ := urls[0].User.Password()
	opts := autopaho.ClientConfig{
		ServerUrls:                    urls,
		ConnectUsername:               urls[0].User.Username(),
		ConnectPassword:               []byte(pass),
		CleanStartOnInitialConnection: p.Config.MqttPublisher.CleanSession,
		KeepAlive:                     20,
		ConnectRetryDelay:             1 * time.Second,
		OnConnectionUp: func(*autopaho.ConnectionManager, *paho.Connack) {
			log.Info("publisher connected", "id", p.Id)
		},
		OnConnectError: func(err error) {
			log.Info("publisher failed to connect ", "id", p.Id, "error", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: utils.InjectId(p.Config.PublisherId, p.Id),
			OnClientError: func(err error) {
				log.Error("publisher error", "id", p.Id, "error", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				log.Error("publisher disconnected", "id", p.Id, "reasonCode", d.ReasonCode, "reasonString", d.Properties.ReasonString)
			},
		},
	}
	return opts
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

	<-ctx.Done()
}

func (p Mqtt5Publisher) StartRateLimited(ctx context.Context) {
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

func (p Mqtt5Publisher) Send() {
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
	if p.Connection != nil {
		_ = p.Connection.Disconnect(context.TODO())
	}
}
