package amqp10_client

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/topic"
	"github.com/rabbitmq/omq/pkg/utils"
	"github.com/relvacode/iso8601"

	"github.com/rabbitmq/omq/pkg/metrics"

	amqp "github.com/Azure/go-amqp"
	"github.com/prometheus/client_golang/prometheus"
)

type Amqp10Consumer struct {
	Id         int
	Connection *amqp.Conn
	Session    *amqp.Session
	Topic      string
	Config     config.Config
}

func NewConsumer(cfg config.Config, id int) *Amqp10Consumer {
	// open connection
	hostname, vhost := hostAndVHost(cfg.ConsumerUri)
	conn, err := amqp.Dial(context.TODO(), cfg.ConsumerUri, &amqp.ConnOptions{
		HostName: vhost,
		TLSConfig: &tls.Config{
			ServerName: hostname}})
	if err != nil {
		log.Error("consumer failed to connect", "protocol", "amqp-1.0", "consumerId", id, "error", err.Error())
		return nil
	}

	// open seesion
	session, err := conn.NewSession(context.TODO(), nil)
	if err != nil {
		log.Error("consumer failed to create a session", "protocol", "amqp-1.0", "consumerId", id, "error", err.Error())
		return nil
	}

	topic := topic.CalculateTopic(cfg.ConsumeFrom, id)

	return &Amqp10Consumer{
		Id:         id,
		Connection: conn,
		Session:    session,
		Topic:      topic,
		Config:     cfg,
	}

}

func (c Amqp10Consumer) Start(ctx context.Context, subscribed chan bool) {
	var durability amqp.Durability
	switch c.Config.QueueDurability {
	case config.None:
		durability = amqp.DurabilityNone
	case config.Configuration:
		durability = amqp.DurabilityConfiguration
	case config.UnsettledState:
		durability = amqp.DurabilityUnsettledState
	}
	receiver, err := c.Session.NewReceiver(ctx, c.Topic, &amqp.ReceiverOptions{SourceDurability: durability, Credit: int32(c.Config.ConsumerCredits), Properties: buildLinkProperties(c.Config), Filters: buildLinkFilters(c.Config)})
	if err != nil {
		log.Error("consumer failed to create a receiver", "protocol", "amqp-1.0", "consumerId", c.Id, "error", err.Error())
		return
	}
	close(subscribed)
	log.Debug("consumer subscribed", "protocol", "amqp-1.0", "consumerId", c.Id, "terminus", c.Topic, "durability", durability)

	m := metrics.EndToEndLatency

	log.Info("consumer started", "protocol", "amqp-1.0", "consumerId", c.Id, "terminus", c.Topic)
	previousMessageTimeSent := time.Unix(0, 0)

	for i := 1; i <= c.Config.ConsumeCount; i++ {
		select {
		case <-ctx.Done():
			c.Stop("time limit reached")
			return
		default:
			msg, err := receiver.Receive(ctx, nil)
			if err != nil {
				log.Error("failed to receive a message", "protocol", "amqp-1.0", "consumerId", c.Id, "terminus", c.Topic)
				return
			}

			payload := msg.GetData()
			priority := strconv.Itoa(int(msg.Header.Priority))
			timeSent, latency := utils.CalculateEndToEndLatency(&payload)
			m.With(prometheus.Labels{"protocol": "amqp-1.0"}).Observe(latency.Seconds())

			if c.Config.LogOutOfOrder && timeSent.Before(previousMessageTimeSent) {
				metrics.MessagesConsumedOutOfOrder.With(prometheus.Labels{"protocol": "amqp-1.0", "priority": priority}).Inc()
				log.Info("Out of order message received. This message was sent before the previous message", "this messsage", timeSent, "previous message", previousMessageTimeSent)
			}
			previousMessageTimeSent = timeSent

			log.Debug("message received", "protocol", "amqp-1.0", "consumerId", c.Id, "terminus", c.Topic, "size", len(payload), "priority", priority, "latency", latency)

			if c.Config.ConsumerLatency > 0 {
				log.Debug("consumer latency", "protocol", "amqp-1.0", "consumerId", c.Id, "latency", c.Config.ConsumerLatency)
				time.Sleep(c.Config.ConsumerLatency)
			}

			err = receiver.AcceptMessage(ctx, msg)
			if err != nil {
				log.Error("message NOT accepted", "protocol", "amqp-1.0", "consumerId", c.Id, "terminus", c.Topic)
			}
			metrics.MessagesConsumed.With(prometheus.Labels{"protocol": "amqp-1.0", "priority": priority}).Inc()
			log.Debug("message accepted", "protocol", "amqp-1.0", "consumerId", c.Id, "terminus", c.Topic)
		}
	}

	c.Stop("message count reached")
	log.Debug("consumer finished", "protocol", "amqp-1.0", "consumerId", c.Id)
}

func (c Amqp10Consumer) Stop(reason string) {
	log.Debug("closing connection", "protocol", "amqp-1.0", "consumerId", c.Id, "reason", reason)
	_ = c.Connection.Close()
}

func buildLinkProperties(cfg config.Config) map[string]any {
	props := map[string]any{
		"rabbitmq:priority": cfg.ConsumerPriority,
	}

	return props
}

func buildLinkFilters(cfg config.Config) []amqp.LinkFilter {
	var filters []amqp.LinkFilter

	if cfg.StreamOffset != "" {
		// parse stream offset
		offset, err := parseStreamOffset(cfg.StreamOffset)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
			os.Exit(1)
		}
		filters = append(filters, amqp.NewLinkFilter("rabbitmq:stream-offset-spec", 0, offset))
	}

	if cfg.StreamFilterValues != "" {
		filters = append(filters, amqp.NewLinkFilter("rabbitmq:stream-filter", 0, cfg.StreamFilterValues))
	}
	return filters
}

func parseStreamOffset(offset string) (any, error) {
	switch offset {
	case "":
		return nil, nil
	case "next", "first", "last":
		return offset, nil
	default:
		// check if streamOffset can be parsed as unsigned integer (chunkID)
		if chunkID, err := strconv.ParseUint(offset, 10, 64); err == nil {
			return chunkID, nil
		}
		// check if streamOffset can be parsed as an ISO 8601 timestamp
		if timestamp, err := iso8601.ParseString(offset); err == nil {
			return timestamp, nil
		}
	}
	return nil, fmt.Errorf("invalid stream offset: %s", offset)
}
