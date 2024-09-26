package mgmt

import (
	"context"
	"os"
	"strings"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/rabbitmq_amqp"
)

var management rmq.IManagement
var declaredQueues []string

func Get() rmq.IManagement {
	if management == nil {
		mgmtConn := rmq.NewAmqpConnection()
		err := mgmtConn.Open(context.Background(), rmq.NewConnectionSettings())
		if err != nil {
			panic(err)
		}
		management = mgmtConn.Management()
	}
	return management
}

func DeclareAndBind(cfg config.Config, queue string, id int) rmq.IQueueInfo {
	if cfg.Queues == config.Predeclared {
		return nil
	}

	mgmt := Get()

	var queueType rmq.TQueueType
	switch cfg.Queues {
	case config.Classic:
		queueType = rmq.Classic
	case config.Quorum:
		queueType = rmq.Quorum
	case config.Stream:
		queueType = rmq.Stream
	}

	queue = strings.TrimPrefix(queue, "/queues/")
	queueSpec := mgmt.Queue(queue).QueueType(rmq.QueueType{Type: queueType})
	qi, err := queueSpec.Declare(context.Background())
	if err != nil {
		log.Error("Failed to declare queue", "name", queue, "error", err)
		os.Exit(1)
	}
	if cfg.DeleteQueues {
		// if we don't need to delete at the end, there's no point in tracking them
		// note: DeleteAll() is always called, so the empty list serves as the mechanism to skip deletion
		declaredQueues = append(declaredQueues, queue)
	}

	exchange, routingKey := parsePublishTo(cfg.PublishTo, id)

	if exchange != "amq.default" {
		exchange := mgmt.Exchange(exchange)
		bindingSpec := mgmt.Binding().SourceExchange(exchange).DestinationQueue(queueSpec).Key(routingKey)
		err = bindingSpec.Bind(context.Background())
		if err != nil {
			log.Error("Failed to bind a queue", "exchange", exchange, "queue", queue, "key", routingKey, "error", err)
			os.Exit(1)
		}
	}

	return qi
}

func parsePublishTo(publishTo string, id int) (string, string) {
	parts := strings.Split(publishTo, "/")

	exchange := ""
	routingKey := ""

	if parts[1] == "/queues/" {
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

	return exchange, utils.InjectId(routingKey, id)
}

func DeleteDeclaredQueues() {
	for _, queue := range declaredQueues {
		queueSpec := Get().Queue(queue)
		err := queueSpec.Delete(context.Background())
		log.Debug("Deleted queue", "name", queue, "error", err)
	}
}

func Disconnect() {
	if management != nil {
		management.Close(context.Background())
	}
}
