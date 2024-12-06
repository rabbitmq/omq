package mgmt

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/rabbitmq_amqp"
)

var (
	instance *Mgmt
	once     sync.Once
)

type Mgmt struct {
	ctx            context.Context
	conn           rmq.IConnection
	declaredQueues map[string]bool
	uris           []string
	cleanupQueues  bool
}

func Start(ctx context.Context, uris []string, cleanupQueues bool) *Mgmt {
	once.Do(func() {
		instance = &Mgmt{
			ctx:            ctx,
			uris:           uris,
			cleanupQueues:  cleanupQueues,
			declaredQueues: make(map[string]bool),
		}
	})
	return instance
}

func (m *Mgmt) connection() rmq.IConnection {
	if len(m.uris) == 0 {
		return nil
	}

	if m.conn != nil && m.conn.Status() == rmq.Open {
		return m.conn
	}

	for {
		// TODO support multiple URIs
		conn, err := rmq.Dial(context.TODO(), m.uris[0], &amqp.ConnOptions{
			SASLType:    amqp.SASLTypeAnonymous(),
			ContainerID: "omq-management",
		})
		if err == nil {
			m.conn = conn
			break
		}
		log.Error("can't establish a management connection; retrying...", "uri", m.uris[0], "error", err)
		select {
		case <-m.ctx.Done():
			return nil
		case <-time.After(time.Second):
			continue
		}
	}
	log.Debug("management connection established", "uri", m.uris[0])
	return m.conn
}

func (m *Mgmt) DeclareQueues(cfg config.Config) {
	log.Info("Declaring queues...")
	// declare queues for AMQP publishers
	if cfg.PublisherProto == config.AMQP && strings.HasPrefix(cfg.PublishTo, "/queues/") {
		queueName := strings.TrimPrefix(cfg.PublishTo, "/queues/")
		for i := 1; i <= cfg.Publishers; i++ {
			m.DeclareAndBind(cfg, utils.InjectId(queueName, i), i)
		}
	}
	// declare queues for AMQP consumers
	if cfg.ConsumerProto == config.AMQP {
		if strings.HasPrefix(cfg.ConsumeFrom, "/queues/") {
			for i := 1; i <= cfg.Consumers; i++ {
				queueName := strings.TrimPrefix(cfg.ConsumeFrom, "/queues/")
				m.DeclareAndBind(cfg, utils.InjectId(queueName, i), i)
			}
		} else {
			log.Info("Not declaring queues for AMQP consumers since the address doesn't start with /queues/")
		}
	}
	// declare queues for STOMP publishers
	if cfg.PublisherProto == config.STOMP && strings.HasPrefix(cfg.PublishTo, "/amq/queue/") {
		queueName := strings.TrimPrefix(cfg.PublishTo, "/amq/queue/")
		for i := 1; i <= cfg.Publishers; i++ {
			m.DeclareAndBind(cfg, utils.InjectId(queueName, i), i)
		}
	}
	// declare queues for STOMP consumers
	if cfg.ConsumerProto == config.STOMP && strings.HasPrefix(cfg.ConsumeFrom, "/amq/queue/") {
		queueName := strings.TrimPrefix(cfg.ConsumeFrom, "/amq/queue/")
		for i := 1; i <= cfg.Consumers; i++ {
			m.DeclareAndBind(cfg, utils.InjectId(queueName, i), i)
		}
	}
}

func (m *Mgmt) DeclareAndBind(cfg config.Config, queueName string, id int) rmq.IQueueInfo {
	if cfg.Queues == config.Predeclared || m.declaredQueues[queueName] {
		return nil
	}

	var queueType rmq.QueueType
	switch cfg.Queues {
	case config.Classic:
		queueType = rmq.QueueType{Type: rmq.Classic}
	case config.Quorum:
		queueType = rmq.QueueType{Type: rmq.Quorum}
	case config.Stream:
		queueType = rmq.QueueType{Type: rmq.Stream}
	}

	conn := instance.connection()
	if conn == nil {
		return nil
	}

	qi, err := conn.Management().DeclareQueue(context.TODO(), &rmq.QueueSpecification{
		Name:      queueName,
		QueueType: queueType,
	})
	if err != nil {
		log.Error("Failed to declare queue", "name", queueName, "error", err)
		os.Exit(1)
	}
	log.Debug("queue declared", "name", qi.Name(), "type", qi.Type())

	if m.cleanupQueues {
		m.declaredQueues[queueName] = true
	}

	exchangeName, routingKey := parsePublishTo(cfg.PublisherProto, cfg.PublishTo, id)

	// explicitly set routing key overrides everything else
	if cfg.BindingKey != "" {
		routingKey = utils.InjectId(cfg.BindingKey, id)
	}

	// explicitly set exchange overrides everything else
	if cfg.Exchange != "" {
		exchangeName = cfg.Exchange
	}

	if exchangeName != "amq.default" {
		_, err = instance.connection().Management().Bind(context.TODO(), &rmq.BindingSpecification{
			SourceExchange:   exchangeName,
			DestinationQueue: queueName,
			BindingKey:       routingKey,
		})
		if err != nil {
			log.Error("Failed to bind a queue", "exchange", exchangeName, "queue", queueName, "key", routingKey, "error", err)
			os.Exit(1)
		}
		log.Debug("binding declared", "exchange", exchangeName, "queue", queueName, "key", routingKey)
	}

	return qi
}

func parsePublishTo(proto config.Protocol, publishTo string, id int) (string, string) {
	parts := strings.Split(publishTo, "/")

	if len(parts) < 2 && proto != config.MQTT {
		return "amq.direct", utils.InjectId(parts[0], id)
	}

	exchange := ""
	routingKey := ""

	if proto == config.AMQP {
		if parts[1] == "queues" && len(parts) == 3 {
			exchange = "amq.default"
			routingKey = parts[2]
		}

		if parts[1] == "exchanges" && len(parts) == 3 {
			exchange = parts[2]
		}

		if parts[1] == "exchanges" && len(parts) == 4 {
			exchange = parts[2]
			routingKey = parts[3]
		}
	}

	if proto == config.MQTT {
		exchange = "amq.topic"
		routingKey = strings.TrimPrefix(publishTo, "/topic/")
	}

	if proto == config.STOMP {
		// STOMP publishing to /queue/:queue
		if parts[1] == "queue" && len(parts) == 3 {
			exchange = "amq.default"
			routingKey = parts[2]
		}

		// STOMP publishing to /amq/:queue
		if parts[1] == "amq" && parts[2] == "queue" && len(parts) == 4 {
			exchange = "amq.default"
			routingKey = parts[3]
		}

		// STOMP publishing to /topic/:key
		if parts[1] == "topic" && len(parts) == 3 {
			exchange = "amq.topic"
			routingKey = parts[2]
		}

		// STOMP publishing to /exchange/:exchange/:key
		if parts[1] == "exchange" && len(parts) == 4 {
			exchange = parts[2]
			routingKey = parts[3]
		}
	}

	return exchange, utils.InjectId(routingKey, id)
}

func (m *Mgmt) DeleteDeclaredQueues() {
	if len(m.declaredQueues) == 0 {
		return
	}

	log.Info("Deleting queues...")
	for queueName := range m.declaredQueues {
		if m.conn == nil || m.conn.Status() != rmq.Open {
			log.Info("Management connection lost; some queues were not deleted")
			return
		}
		log.Debug("deleting queue...", "name", queueName)
		err := m.conn.Management().DeleteQueue(context.TODO(), queueName)
		if err != nil {
			log.Info("Failed to delete a queue", "queue", queueName, "error", err)
		}
	}
}

func (m *Mgmt) DeleteQueue(ctx context.Context, name string) error {
	if m.conn == nil || m.conn.Status() != rmq.Open {
		return errors.New("management connection lost")
	}
	return m.conn.Management().DeleteQueue(ctx, name)
}

func (m *Mgmt) Stop() {
	m.DeleteDeclaredQueues()
	if m.conn != nil && m.conn.Status() == rmq.Open {
		m.conn.Close(context.Background())
	}
}
