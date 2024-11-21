package mgmt

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/rabbitmq_amqp"
)

var mgmtConn *rmq.IConnection
var declaredQueues []string
var mgmtUri string

func Get() rmq.IManagement {
	var conn rmq.IConnection
	var err error
	if mgmtConn != nil && (*mgmtConn).Status() == rmq.Open {
		return (*mgmtConn).Management()
	}

	for {
		conn, err = rmq.Dial(context.TODO(), mgmtUri, nil)
		if err == nil {
			break
		}
		log.Error("can't establish a management connection; retrying...", "error", err)
		time.Sleep(time.Second)
	}
	log.Debug("management connection established", "uri", mgmtUri)
	mgmtConn = &conn
	return conn.Management()
}

func DeclareAndBind(cfg config.Config, queueName string, id int) rmq.IQueueInfo {
	if cfg.Queues == config.Predeclared {
		return nil
	}

	// we should allow multiple mgmt uris
	mgmtUri = cfg.ConsumerUri[0]
	mgmt := Get()

	var queueType rmq.QueueType
	switch cfg.Queues {
	case config.Classic:
		queueType = rmq.QueueType{Type: rmq.Classic}
	case config.Quorum:
		queueType = rmq.QueueType{Type: rmq.Quorum}
	case config.Stream:
		queueType = rmq.QueueType{Type: rmq.Stream}
	}

	queueName = strings.TrimPrefix(queueName, "/queues/")
	qi, err := mgmt.DeclareQueue(context.TODO(), &rmq.QueueSpecification{
		Name:      queueName,
		QueueType: queueType,
	})
	if err != nil {
		log.Error("Failed to declare queue", "name", queueName, "error", err)
		os.Exit(1)
	}
	log.Debug("queue declared", "name", qi.Name(), "type", qi.Type())

	if cfg.CleanupQueues {
		// if we don't need to delete at the end, there's no point in tracking declared queues
		// note: DeleteAll() is always called, so the empty list serves as the mechanism to skip deletion
		declaredQueues = append(declaredQueues, queueName)
	}

	var exchangeName, routingKey string
	if cfg.PublisherProto == config.MQTT {
		exchangeName = "amq.topic"
		routingKey = utils.InjectId(strings.TrimPrefix(cfg.PublishTo, "/topic/"), id)
	} else {
		exchangeName, routingKey = parsePublishTo(cfg.PublishTo, id)
	}

	// explicitly set routing key overrides everything else
	if cfg.Amqp.BindingKey != "" {
		routingKey = utils.InjectId(cfg.Amqp.BindingKey, id)
	}

	if exchangeName != "amq.default" {
		_, err = mgmt.Bind(context.TODO(), &rmq.BindingSpecification{
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

func parsePublishTo(publishTo string, id int) (string, string) {
	parts := strings.Split(publishTo, "/")

	if len(parts) < 2 {
		return "amq.direct", utils.InjectId(parts[0], id)
	}

	exchange := ""
	routingKey := ""

	if parts[1] == "queues" {
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

	// STOMP publishing to /topic/:key
	if parts[1] == "topic" && len(parts) == 3 {
		exchange = "amq.topic"
		routingKey = parts[2]
	}

	// STOMP publishing to /exchange/:exchange
	if parts[1] == "exchange" && len(parts) == 3 {
		exchange = parts[2]
	}

	// STOMP publishing to /exchange/:exchange/:key
	if parts[1] == "exchange" && len(parts) == 4 {
		exchange = parts[2]
		routingKey = parts[3]
	}

	return exchange, utils.InjectId(routingKey, id)
}

func DeleteDeclaredQueues() {
	for _, queueName := range declaredQueues {
		log.Debug("deleting queue...", "name", queueName)
		err := Get().DeleteQueue(context.TODO(), queueName)
		if err != nil {
			log.Info("Failed to delete a queue", "queue", queueName, "error", err)
		}
	}
}

func Disconnect() {
	if mgmtConn != nil && (*mgmtConn).Status() == rmq.Open {
		(*mgmtConn).Close(context.Background())
	}
}
