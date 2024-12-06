package mqtt

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"
	"golang.org/x/exp/rand"

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

func (p Mqtt5Publisher) Start(ctx context.Context, publisherReady chan bool, startPublishing chan bool) {
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
	// TODO it seems that sometimes if we stop quickly after sending
	// a message, this message is not delivered, even though Publish
	// is supposed to by synchronous; to be investigated
	time.Sleep(500 * time.Millisecond)
	p.Stop(farewell)
}

func (p Mqtt5Publisher) StartPublishing(ctx context.Context) string {
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
			p.Send(ctx)
		}
	}
}

func (p Mqtt5Publisher) Send(ctx context.Context) {
	utils.UpdatePayload(p.Config.UseMillis, &p.msg)
	startTime := time.Now()
	_, err := p.Connection.Publish(ctx, &paho.Publish{
		QoS:     byte(p.Config.MqttPublisher.QoS),
		Topic:   p.Topic,
		Payload: p.msg,
	})
	if err != nil {
		// I couldn't find any way to prevent publishing just after omq
		// initiated the shutdown procedure, so we have to ignore this
		if !strings.Contains(err.Error(), "use of closed network connection") {
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
		_ = p.Connection.Disconnect(context.TODO())
	}
}