package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/utils"
)

type Mqtt5Consumer struct {
	Id         int
	Connection *autopaho.ConnectionManager
	Topic      string
	Config     config.Config
	ctx        context.Context
}

func NewMqtt5Consumer(ctx context.Context, cfg config.Config, id int) Mqtt5Consumer {
	topic := publisherTopic(cfg.ConsumeFromTemplate, id)
	return Mqtt5Consumer{
		Id:         id,
		Connection: nil,
		Topic:      topic,
		Config:     cfg,
		ctx:        ctx,
	}
}

func (c Mqtt5Consumer) Start(consumerReady chan bool) {
	msgsReceived := 0
	var oooTracker *utils.OutOfOrderTracker
	if c.Config.DetectOutOfOrder || c.Config.DetectGaps {
		oooTracker = utils.NewOutOfOrderTracker()
	}

	handler := func(rcv paho.PublishReceived) (bool, error) {
		metrics.MessagesConsumedMetric(0).Inc()
		payload := rcv.Packet.Payload
		timeSent, latency := utils.CalculateEndToEndLatency(&payload)
		metrics.RecordEndToEndLatency(latency)

		if oooTracker != nil && rcv.Packet.Properties != nil {
			if pubID, seq, ok := extractMqtt5OrderingInfo(rcv.Packet.Properties.User); ok {
				result := oooTracker.Check(pubID, seq)
				switch result.Status {
				case utils.SequenceOutOfOrder:
					if c.Config.DetectOutOfOrder {
						metrics.MessagesConsumedOutOfOrderMetric(0).Inc()
						log.Info("out-of-order message",
							"publisher", pubID, "seq", seq, "lastSeq", result.LastSeq, "timeSent", timeSent)
					}
				case utils.SequenceGap:
					if c.Config.DetectGaps {
						metrics.MessagesConsumedGapsMetric(0).Inc()
						log.Info("gap in sequence (missing messages)",
							"publisher", pubID, "seq", seq, "lastSeq", result.LastSeq,
							"missed", seq-result.LastSeq-1, "timeSent", timeSent)
					}
				}
			}
		}

		msgsReceived++
		log.Debug("message received", "id", c.Id, "topic", c.Topic, "size", len(payload), "latency", latency)
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
		ConnectRetryDelay:             1 * time.Second,
		ConnectTimeout:                30 * time.Second,
		TlsCfg: &tls.Config{
			InsecureSkipVerify: c.Config.InsecureSkipTLSVerify,
		},
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			log.Info("consumer connected", "id", c.Id, "topic", c.Topic)
			subsPerConsumer := c.Config.MqttConsumer.SubscriptionsPerConsumer
			if subsPerConsumer == 0 {
				log.Info("consumer connected (no subscriptions)", "id", c.Id)
				return
			}
			subscriptions := make([]paho.SubscribeOptions, 0, subsPerConsumer)
			for i := 1; i <= subsPerConsumer; i++ {
				topic := c.Topic
				if subsPerConsumer > 1 {
					topic = fmt.Sprintf("%s/%d", c.Topic, i)
				}
				subscriptions = append(subscriptions, paho.SubscribeOptions{
					Topic: topic,
					QoS:   byte(c.Config.MqttConsumer.QoS),
				})
				log.Info("consumer subscribing", "id", c.Id, "topic", topic)
			}
			if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: subscriptions,
			}); err != nil {
				fmt.Printf("failed to subscribe (%s). This is likely to mean no messages will be received.", err)
			}
		},
		OnConnectError: func(err error) {
			log.Info("consumer failed to connect ", "id", c.Id, "error", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: utils.InjectId(c.Config.ConsumerId, c.Id),
			OnClientError: func(err error) {
				log.Error("consumer error", "id", c.Id, "error", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				log.Error("consumer disconnected", "id", c.Id, "reasonCode", d.ReasonCode, "reasonString", d.Properties.ReasonString)
			},
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				handler,
			},
		},
	}

	var err error
	connection, err := autopaho.NewConnection(c.ctx, opts)
	if err != nil {
		log.Error("consumer connection failed", "id", c.Id, "error", err)
		close(consumerReady)
		return
	}
	c.Connection = connection
	err = c.Connection.AwaitConnection(c.ctx)
	if err != nil {
		// AwaitConnection only returns an error if the context is cancelled
		close(consumerReady)
		c.Stop("context cancelled")
		return
	}
	close(consumerReady)

	// TODO: currently we can consume more than ConsumerCount messages
	for msgsReceived < c.Config.ConsumeCount {
		select {
		case <-c.ctx.Done():
			c.Stop("time limit reached")
			return
		case <-time.After(100 * time.Millisecond):
			// Check more frequently to respond to context cancellation faster
		}
	}
	c.Stop("--cmessages value reached")
}

func (c Mqtt5Consumer) Stop(reason string) {
	log.Debug("closing consumer connection", "id", c.Id, "reason", reason)
	if c.Connection != nil {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Connection.Disconnect(disconnectCtx)
	}
}

func extractMqtt5OrderingInfo(userProps []paho.UserProperty) (int, uint64, bool) {
	var pubIDStr, seqStr string
	for _, prop := range userProps {
		switch prop.Key {
		case utils.HeaderPublisherID:
			pubIDStr = prop.Value
		case utils.HeaderSequence:
			seqStr = prop.Value
		}
	}
	if pubIDStr == "" || seqStr == "" {
		return 0, 0, false
	}
	pubID, err1 := strconv.Atoi(pubIDStr)
	seq, err2 := strconv.ParseUint(seqStr, 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return pubID, seq, true
}
