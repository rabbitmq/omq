package mqtt_client

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/rabbitmq/omq/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus"
)

func Start(cfg config.Config) {
	var wg sync.WaitGroup

	if cfg.Consumers > 0 {
		for i := 1; i <= cfg.Consumers; i++ {
			subscribed := make(chan bool)
			n := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				Consumer(cfg, subscribed, n)
			}()

			// wait until we know the receiver has subscribed
			<-subscribed
		}
	}

	if cfg.Publishers > 0 {
		for i := 1; i <= cfg.Publishers; i++ {
			n := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				Publisher(cfg, n)
			}()
		}
	}

	wg.Wait()
}

func Publisher(cfg config.Config, n int) {
	var token mqtt.Token

	// sleep random interval to avoid all publishers publishing at the same time
	s := rand.Intn(cfg.Publishers)
	time.Sleep(time.Duration(s) * time.Millisecond)

	// open connection
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MqttUrl).
		SetUsername("guest").
		SetPassword("guest").
		SetClientID(fmt.Sprintf("omq-pub-%d", n)).
		SetAutoReconnect(true).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Info("connection lost", "protocol", "mqtt", "publisherId", n)
		}).
		SetProtocolVersion(4)

	c := mqtt.NewClient(opts)
	token = c.Connect()
	token.Wait()
	log.Info("publisher started", "protocol", "mqtt", "publisherId", n)

	topic := fmt.Sprintf("%s-%d", cfg.QueueNamePrefix, ((n-1)%cfg.QueueCount)+1)

	// message payload will be reused with the first bytes overwritten
	msg := utils.MessageBody(cfg)

	// main loop
	for i := 1; i <= cfg.PublishCount; i++ {
		utils.UpdatePayload(cfg.UseMillis, &msg)
		timer := prometheus.NewTimer(metrics.PublishingLatency.With(prometheus.Labels{"protocol": "mqtt"}))
		token = c.Publish(topic, 1, false, msg)
		token.Wait()
		timer.ObserveDuration()
		if token.Error() != nil {
			log.Error("message sending failure", "protocol", "mqtt", "publisherId", n, "error", token.Error())
		}
		log.Debug("message sent", "protocol", "mqtt", "publisherId", n)
		metrics.MessagesPublished.With(prometheus.Labels{"protocol": "mqtt"}).Inc()
		utils.WaitBetweenMessages(cfg.Rate)
	}

	log.Debug("publisher stopped", "protocol", "mqtt", "publisherId", n)
}

func Consumer(cfg config.Config, subscribed chan bool, n int) {
	// sleep random interval to avoid all consumers publishing at the same time
	s := rand.Intn(cfg.Consumers)
	time.Sleep(time.Duration(s) * time.Millisecond)

	// open connection
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MqttUrl).
		SetUsername("guest").
		SetPassword("guest").
		SetClientID(fmt.Sprintf("omq-sub-%d", n)).
		SetAutoReconnect(true).
		SetCleanSession(false).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Info("connection lost", "protocol", "mqtt", "consumerId", n)
		}).
		SetProtocolVersion(4)

	var token mqtt.Token
	c := mqtt.NewClient(opts)
	token = c.Connect()
	token.Wait()
	log.Info("consumer started", "protocol", "mqtt", "publisherId", n)

	topic := fmt.Sprintf("%s-%d", cfg.QueueNamePrefix, ((n-1)%cfg.QueueCount)+1)

	m := metrics.EndToEndLatency.With(prometheus.Labels{"protocol": "mqtt"})

	msgsReceived := 0

	handler := func(client mqtt.Client, msg mqtt.Message) {
		metrics.MessagesConsumed.With(prometheus.Labels{"protocol": "mqtt"}).Inc()
		payload := msg.Payload()
		m.Observe(utils.CalculateEndToEndLatency(&payload))
		msgsReceived++
		log.Debug("message received", "protocol", "mqtt", "subscriberId", n, "terminus", topic, "size", len(payload))
	}

	close(subscribed)
	token = c.Subscribe(topic, 1, handler)
	token.Wait()
	if token.Error() != nil {
		log.Error("failed to subscribe", "protocol", "mqtt", "publisherId", n, "error", token.Error())
	}

	for {
		time.Sleep(1 * time.Second)
		if msgsReceived >= cfg.ConsumeCount {
			break
		}
	}
	log.Debug("consumer finished", "protocol", "mqtt", "publisherId", n)
}
