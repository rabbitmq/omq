package topic

import (
	"fmt"

	"github.com/rabbitmq/omq/pkg/config"
)

func CalculateTopic(cfg config.Config, id int) string {
	topic := cfg.QueueNamePrefix
	if cfg.QueueCount > 1 {
		topic = topic + "-" + fmt.Sprint(((id-1)%cfg.QueueCount)+1)
	}
	return topic
}
