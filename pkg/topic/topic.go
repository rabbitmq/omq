package topic

import (
	"fmt"
	"strings"
)

func CalculateTopic(topic string, id int) string {
	return strings.Replace(topic, "%d", fmt.Sprintf("%d", id), -1)
}
