package mqtt

import (
	"context"
	"crypto/tls"
	"os"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/rabbitmq/omq/pkg/metrics"
)

// Mqtt5Responder is the "consumer" side of MQTT5 request/response (RPC): it subscribes
// to a request topic and, for every request that carries a Response Topic, publishes a
// reply back to that topic echoing the request's Correlation Data.
type Mqtt5Responder struct {
	Id     int
	Topic  string // request topic
	Config config.Config
	ctx    context.Context
}

func NewMqtt5Responder(ctx context.Context, cfg config.Config, id int) Mqtt5Responder {
	return Mqtt5Responder{
		Id:     id,
		Topic:  publisherTopic(cfg.ConsumeFromTemplate, id),
		Config: cfg,
		ctx:    ctx,
	}
}

func (c Mqtt5Responder) Start(consumerReady chan bool) {
	var msgsHandled atomic.Int64
	subscribed := make(chan struct{}, 1)

	// set inside OnConnectionUp, before subscribing -- guaranteed to be populated
	// before any request can arrive on the subscription.
	var connMgr atomic.Pointer[autopaho.ConnectionManager]

	replyMsg := utils.MessageBody(c.Config.MqttRpc.ReplySize, c.Config.MqttRpc.ReplySizeTemplate, c.Id)

	handler := func(rcv paho.PublishReceived) (bool, error) {
		// incremented on return, i.e. only once the reply (if any) has actually been
		// sent -- otherwise a slow --consumer-latency could let Start's shutdown loop
		// disconnect while a reply is still in flight.
		defer msgsHandled.Add(1)

		payload := rcv.Packet.Payload
		timeSent, latency := utils.CalculateEndToEndLatency(&payload)
		metrics.RecordEndToEndLatency(latency)
		metrics.MessagesConsumedMetric(0).Inc()

		// Consumer latency: simulate processing time before the reply is sent.
		if c.Config.ConsumerLatencyTemplate != nil {
			latencyStr := utils.ExecuteTemplate(c.Config.ConsumerLatencyTemplate, c.Id)
			consumerLatency, err := time.ParseDuration(latencyStr)
			if err != nil {
				log.Error("failed to parse template-generated latency", "value", latencyStr, "error", err)
				os.Exit(1)
			}
			if consumerLatency > 0 {
				log.Debug("consumer latency", "id", c.Id, "latency", consumerLatency)
				time.Sleep(consumerLatency)
			}
		}

		if rcv.Packet.Properties == nil || rcv.Packet.Properties.ResponseTopic == "" {
			log.Debug("request without a response topic, dropping", "id", c.Id, "topic", c.Topic, "timeSent", timeSent)
			return true, nil
		}

		cm := connMgr.Load()
		if cm == nil {
			log.Error("responder not connected, can't send reply", "id", c.Id)
			return true, nil
		}

		var replyBody []byte
		if c.Config.MqttRpc.ReplySizeTemplate != nil {
			replyBody = utils.MessageBody(c.Config.MqttRpc.ReplySize, c.Config.MqttRpc.ReplySizeTemplate, c.Id)
		} else {
			replyBody = make([]byte, len(replyMsg))
			copy(replyBody, replyMsg)
		}

		reply := &paho.Publish{
			QoS:     byte(c.Config.MqttConsumer.QoS),
			Topic:   rcv.Packet.Properties.ResponseTopic,
			Payload: replyBody,
			Properties: &paho.PublishProperties{
				CorrelationData: rcv.Packet.Properties.CorrelationData,
			},
		}

		startTime := time.Now()
		_, err := cm.Publish(c.ctx, reply)
		if err != nil {
			log.Error("reply sending failure", "id", c.Id, "error", err)
			return true, nil
		}
		metrics.MessagesPublished.Inc()
		metrics.RecordPublishingLatency(time.Since(startTime))
		log.Debug("reply sent", "id", c.Id, "topic", reply.Topic, "latency", time.Since(startTime))

		return true, nil
	}

	urls := stringsToUrls(c.Config.ConsumerUri)
	reorderedUrls := utils.ReorderUrls(urls, c.Config.SpreadConnections, c.Id)

	pass, _ := urls[0].User.Password()
	opts := autopaho.ClientConfig{
		ServerUrls:                    reorderedUrls,
		ConnectUsername:               reorderedUrls[0].User.Username(),
		ConnectPassword:               []byte(pass),
		CleanStartOnInitialConnection: c.Config.MqttConsumer.CleanSession,
		SessionExpiryInterval:         uint32(c.Config.MqttConsumer.SessionExpiryInterval.Seconds()),
		KeepAlive:                     20,
		ReconnectBackoff:              autopaho.NewConstantBackoff(1 * time.Second),
		ConnectTimeout:                30 * time.Second,
		TlsCfg: &tls.Config{
			InsecureSkipVerify: c.Config.InsecureSkipTLSVerify,
		},
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			log.Info("responder connected", "id", c.Id, "topic", c.Topic)
			connMgr.Store(cm)
			subscribeWithRetry(c.ctx, cm, []paho.SubscribeOptions{
				{Topic: c.Topic, QoS: byte(c.Config.MqttConsumer.QoS)},
			}, subscribed, "responder", c.Id)
		},
		OnConnectError: func(err error) {
			log.Info("responder failed to connect", "id", c.Id, "error", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: utils.InjectId(c.Config.ConsumerId, c.Id),
			OnClientError: func(err error) {
				log.Error("responder error", "id", c.Id, "error", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				log.Error("responder disconnected", "id", c.Id, "reasonCode", d.ReasonCode, "reasonString", d.Properties.ReasonString)
			},
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				handler,
			},
		},
	}

	connection, err := autopaho.NewConnection(c.ctx, opts)
	if err != nil {
		log.Error("responder connection failed", "id", c.Id, "error", err)
		close(consumerReady)
		return
	}
	err = connection.AwaitConnection(c.ctx)
	if err != nil {
		// AwaitConnection only returns an error if the context is cancelled
		close(consumerReady)
		c.stop(connection, "context cancelled")
		return
	}

	// Don't signal readiness until the request-topic subscription is confirmed
	select {
	case <-subscribed:
		close(consumerReady)
	case <-c.ctx.Done():
		close(consumerReady)
		c.stop(connection, "context cancelled")
		return
	}

	for msgsHandled.Load() < int64(c.Config.ConsumeCount) {
		select {
		case <-c.ctx.Done():
			c.stop(connection, "time limit reached")
			return
		case <-time.After(100 * time.Millisecond):
			// Check more frequently to respond to context cancellation faster
		}
	}
	c.stop(connection, "--cmessages value reached")
}

func (c Mqtt5Responder) stop(connection *autopaho.ConnectionManager, reason string) {
	log.Debug("closing responder connection", "id", c.Id, "reason", reason)
	if connection != nil {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = connection.Disconnect(disconnectCtx)
	}
}
