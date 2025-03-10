package mqtt

import (
	"context"
	"math/rand/v2"
	"strings"
	"sync/atomic"
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
	ctx        context.Context
	msg        []byte
}

func NewMqtt5Publisher(ctx context.Context, cfg config.Config, id int) Mqtt5Publisher {
	topic := publisherTopic(cfg.PublishTo, id)
	return Mqtt5Publisher{
		Id:         id,
		Connection: nil,
		Topic:      topic,
		Config:     cfg,
		ctx:        ctx,
	}
}

func (p *Mqtt5Publisher) Connect() {
	opts := p.connectionOptions()
	connection, err := autopaho.NewConnection(p.ctx, opts)
	if err != nil {
		log.Error("publisher connection failed", "id", p.Id, "error", err)
	}
	err = connection.AwaitConnection(p.ctx)
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
		SessionExpiryInterval:         uint32(p.Config.MqttPublisher.SessionExpiryInterval.Seconds()),
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

func (p Mqtt5Publisher) Start(publisherReady chan bool, startPublishing chan bool) {
	p.Connect()

	p.msg = utils.MessageBody(p.Config.Size)

	close(publisherReady)

	select {
	case <-p.ctx.Done():
		return
	case <-startPublishing:
		// short random delay to avoid all publishers publishing at the same time
		time.Sleep(time.Duration(rand.IntN(1000)) * time.Millisecond)
	}

	log.Info("publisher started", "id", p.Id, "rate", p.Config.Rate, "destination", p.Topic)

	var farewell string
	if p.Config.Rate == 0 {
		// idle connection
		<-p.ctx.Done()
		farewell = "context cancelled"
	} else {
		farewell = p.StartPublishing()
	}
	// TODO it seems that sometimes if we stop quickly after sending
	// a message, this message is not delivered, even though Publish
	// is supposed to by synchronous; to be investigated
	time.Sleep(500 * time.Millisecond)
	p.Stop(farewell)
}

func (p Mqtt5Publisher) StartPublishing() string {
	limiter := utils.RateLimiter(p.Config.Rate)

	var msgSent atomic.Int64
	for {
		select {
		case <-p.ctx.Done():
			return "time limit reached"
		default:
			if msgSent.Add(1) > int64(p.Config.PublishCount) {
				return "--pmessages value reached"
			}
			if p.Config.Rate > 0 {
				_ = limiter.Wait(p.ctx)
			}
			p.Send()
		}
	}
}

func (p Mqtt5Publisher) Send() {
	utils.UpdatePayload(p.Config.UseMillis, &p.msg)
	startTime := time.Now()
	_, err := p.Connection.Publish(p.ctx, &paho.Publish{
		QoS:     byte(p.Config.MqttPublisher.QoS),
		Topic:   p.Topic,
		Payload: p.msg,
	})
	if err != nil {
		// I couldn't find any way to prevent publishing just after omq
		// initiated the shutdown procedure, so we have to ignore this
		if !strings.Contains(err.Error(), "use of closed network connection") &&
			!strings.Contains(err.Error(), "context canceled") {
			log.Error("message sending failure", "id", p.Id, "error", err)
		}
		return
	}
	latency := time.Since(startTime)
	metrics.MessagesPublished.Inc()
	metrics.PublishingLatency.Update(latency.Seconds())
	log.Debug("message sent", "id", p.Id, "destination", p.Topic, "latency", latency)
}

func (p Mqtt5Publisher) Stop(reason string) {
	log.Debug("closing publisher connection", "id", p.Id, "reason", reason)
	if p.Connection != nil {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = p.Connection.Disconnect(disconnectCtx)
	}
}
