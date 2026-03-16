package mgmt

import (
	"context"
	"crypto/tls"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

var (
	instance *Mgmt
	once     sync.Once
)

type Mgmt struct {
	ctx                   context.Context
	conn                  *rmq.AmqpConnection
	declaredQueues        map[string]bool
	uris                  []string
	cleanupQueues         bool
	insecureSkipTLSVerify bool
}

func Start(ctx context.Context, uris []string, cleanupQueues bool, insecureSkipTLSVerify bool) *Mgmt {
	once.Do(func() {
		instance = &Mgmt{
			ctx:                   ctx,
			uris:                  uris,
			cleanupQueues:         cleanupQueues,
			insecureSkipTLSVerify: insecureSkipTLSVerify,
			declaredQueues:        make(map[string]bool),
		}
	})
	return instance
}

func (m *Mgmt) connection() *rmq.AmqpConnection {
	if len(m.uris) == 0 {
		return nil
	}

	if m.conn != nil {
		return m.conn
	}

	for {
		// TODO support multiple URIs
		u, _ := url.Parse(m.uris[0])
		conn, err := rmq.Dial(m.ctx, m.uris[0], &rmq.AmqpConnOptions{
			SASLType:    amqp.SASLTypeAnonymous(),
			ContainerID: "omq-management",
			TLSConfig: &tls.Config{
				ServerName:         u.Hostname(),
				InsecureSkipVerify: m.insecureSkipTLSVerify,
			},
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
	// declare queues for AMQP 1.0 and 0.9.1 publishers
	if (cfg.PublisherProto == config.AMQP || cfg.PublisherProto == config.AMQP091) && strings.HasPrefix(cfg.PublishTo, "/queues/") {
		for i := 1; i <= cfg.Publishers; i++ {
			q := utils.ResolveTerminus(cfg.PublishToTemplate, i)
			queueName := strings.TrimPrefix(q, "/queues/")
			m.DeclareAndBind(cfg, queueName, i)
		}
	}
	// declare queues for AMQP 1.0 and 0.9.1 consumers
	if cfg.ConsumerProto == config.AMQP || cfg.ConsumerProto == config.AMQP091 {
		if _, ok := strings.CutPrefix(cfg.ConsumeFrom, "/queues/"); ok {
			for i := 1; i <= cfg.Consumers; i++ {
				q := utils.ResolveTerminus(cfg.ConsumeFromTemplate, i)
				queueName := strings.TrimPrefix(q, "/queues/")
				m.DeclareAndBind(cfg, queueName, i)
			}
		} else {
			log.Info("Not declaring queues for AMQP consumers since the address doesn't start with /queues/")
		}
	}
	// declare queues for STOMP publishers using /amq/queue/ destination
	if cfg.PublisherProto == config.STOMP && strings.HasPrefix(cfg.PublishTo, "/amq/queue/") {
		for i := 1; i <= cfg.Publishers; i++ {
			queueName := utils.ResolveTerminus(cfg.PublishToTemplate, i)
			queueName = strings.TrimPrefix(queueName, "/amq/queue/")
			m.DeclareAndBind(cfg, queueName, i)
		}
	}
	// declare queues for STOMP consumers using /amq/queue/ destination
	if cfg.ConsumerProto == config.STOMP && strings.HasPrefix(cfg.ConsumeFrom, "/amq/queue/") {
		for i := 1; i <= cfg.Consumers; i++ {
			q := utils.ResolveTerminus(cfg.ConsumeFromTemplate, i)
			queueName := strings.TrimPrefix(q, "/amq/queue/")
			m.DeclareAndBind(cfg, queueName, i)
		}
	}
}

func (m *Mgmt) DeclareAndBind(cfg config.Config, queueName string, id int) *rmq.AmqpQueueInfo {
	if cfg.Queues == config.Predeclared || cfg.Queues == config.Exclusive || m.declaredQueues[queueName] {
		return nil
	}

	var queueSpec rmq.IQueueSpecification
	switch cfg.Queues {
	case config.Classic:
		queueSpec = &rmq.ClassicQueueSpecification{Name: queueName, Arguments: cfg.QueueArgs}
	case config.Quorum:
		queueSpec = &rmq.QuorumQueueSpecification{Name: queueName, Arguments: cfg.QueueArgs}
	case config.Stream:
		queueSpec = &rmq.StreamQueueSpecification{Name: queueName, Arguments: cfg.QueueArgs}
	}

	conn := instance.connection()
	if conn == nil {
		return nil
	}

	qi, err := conn.Management().DeclareQueue(context.Background(), queueSpec)
	if err != nil {
		log.Error("Failed to declare queue", "name", queueName, "error", err)
		os.Exit(1)
	}
	log.Debug("queue declared", "name", qi.Name(), "type", qi.Type())

	if m.cleanupQueues {
		m.declaredQueues[queueName] = true
	}

	exchangeName, routingKey := parsePublishTo(cfg.PublisherProto, cfg.PublishToTemplate, id)

	// explicitly set routing key overrides everything else
	if cfg.BindingKey != "" {
		routingKey = utils.InjectId(cfg.BindingKey, id)
	}

	// explicitly set exchange overrides everything else
	if cfg.Exchange != "" {
		exchangeName = cfg.Exchange
	}

	if exchangeName != "amq.default" {
		_, err = instance.connection().Management().Bind(context.Background(), &rmq.ExchangeToQueueBindingSpecification{
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

func parsePublishTo(proto config.Protocol, publishToTemplate *template.Template, id int) (string, string) {
	resolvedPublishTo := utils.ResolveTerminus(publishToTemplate, id)

	parts := strings.Split(resolvedPublishTo, "/")

	if len(parts) < 2 && proto != config.MQTT {
		return "amq.direct", parts[0]
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

	if proto == config.AMQP091 {
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

		if len(parts) == 2 {
			exchange = "amq.default"
			routingKey = parts[1]
		}
	}

	if proto == config.MQTT {
		exchange = "amq.topic"
		routingKey = strings.TrimPrefix(resolvedPublishTo, "/topic/")
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

	return exchange, routingKey
}

func (m *Mgmt) DeleteDeclaredQueues() {
	if len(m.declaredQueues) == 0 {
		return
	}

	log.Info("Deleting queues...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for queueName := range m.declaredQueues {
		if m.conn == nil {
			log.Info("Management connection lost; some queues were not deleted")
			return
		}
		log.Debug("deleting queue...", "name", queueName)
		err := m.conn.Management().DeleteQueue(ctx, queueName)
		if err != nil {
			log.Info("Failed to delete a queue", "queue", queueName, "error", err)
		}
	}
}

func (m *Mgmt) DeleteQueue(ctx context.Context, name string) error {
	if m.conn == nil {
		return errors.New("management connection lost")
	}
	return m.conn.Management().DeleteQueue(ctx, name)
}

func (m *Mgmt) Stop() {
	m.DeleteDeclaredQueues()
	if m.conn != nil {
		_ = m.conn.Close(context.Background())
	}
}
