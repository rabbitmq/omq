package stomp_client

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/go-stomp/stomp/v3"
	"github.com/prometheus/client_golang/prometheus"
)

// these are the default options that work with RabbitMQ
var options []func(*stomp.Conn) error = []func(*stomp.Conn) error{
	stomp.ConnOpt.Login("guest", "guest"),
	stomp.ConnOpt.Host("/"),
}

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
	// sleep random interval to avoid all publishers publishing at exactly the same time
	s := rand.Intn(cfg.Publishers)
	time.Sleep(time.Duration(s) * time.Millisecond)

	conn, err := stomp.Dial("tcp", cfg.StompUrl, options...)
	if err != nil {
		log.Error("publisher connection failed", "protocol", "STOMP", "publisherId", n, "error", err.Error())
		return
	}
	log.Info("publisher connected", "protocol", "STOMP", "publisherId", n)

	queue := fmt.Sprintf("/exchange/amq.topic/%s-%d", cfg.QueueNamePrefix, ((n-1)%cfg.QueueCount)+1)

	// message payload will be reused with the first bytes overwritten
	msg := utils.MessageBody(cfg)

	log.Info("publisher started", "protocol", "STOMP", "publisherId", n, "destination", queue)

	for i := 1; i <= cfg.PublishCount; i++ {
		utils.UpdatePayload(cfg.UseMillis, &msg)
		timer := prometheus.NewTimer(metrics.PublishingLatency.With(prometheus.Labels{"protocol": "stomp"}))
		err = conn.Send(queue, "", msg, stomp.SendOpt.Receipt)
		timer.ObserveDuration()
		if err != nil {
			log.Error("message sending failure", "protocol", "STOMP", "publisherId", n, "error", err)
			return
		}
		log.Debug("message sent", "protocol", "STOMP", "publisherId", n, "destination", queue)

		metrics.MessagesPublished.With(prometheus.Labels{"protocol": "stomp"}).Inc()
		utils.WaitBetweenMessages(cfg.Rate)

	}

	log.Debug("publisher finished", "publisherId", n)
}

func Consumer(cfg config.Config, subscribed chan bool, n int) {
	conn, err := stomp.Dial("tcp", cfg.StompUrl, options...)

	if err != nil {
		log.Error("consumer connection failed", "protocol", "STOMP", "consumerId", n, "error", err.Error())
		return
	}

	topic := fmt.Sprintf("/topic/%s-%d", cfg.QueueNamePrefix, ((n-1)%cfg.QueueCount)+1)
	sub, err := conn.Subscribe(topic, stomp.AckAuto)
	if err != nil {
		log.Error("subscription failed", "protocol", "STOMP", "consumerId", n, "queue", topic, "error", err.Error())
		return
	}
	close(subscribed)

	m := metrics.EndToEndLatency.With(prometheus.Labels{"protocol": "stomp"})

	log.Debug("consumer subscribed", "protocol", "STOMP", "consumerId", n, "destination", topic)

	for i := 1; i <= cfg.ConsumeCount; i++ {
		msg := <-sub.C
		if msg.Err != nil {
			log.Error("failed to receive a message", "protocol", "STOMP", "subscriberId", n, "topic", topic, "error", msg.Err)
			return
		}
		m.Observe(utils.CalculateEndToEndLatency(&msg.Body))
		log.Debug("message received", "protocol", "stomp", "subscriberId", n, "destination", topic, "size", len(msg.Body))
		metrics.MessagesConsumed.With(prometheus.Labels{"protocol": "stomp"}).Inc()
	}

	log.Debug("consumer finished", "protocol", "STOMP", "consumerId", n)

}
