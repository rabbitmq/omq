package mgmt

import (
	"context"
	"testing"

	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/stretchr/testify/assert"
)

func TestDeclareAndBindPredeclared(t *testing.T) {
	cfg := config.Config{Queues: config.Predeclared, PublishTo: "foobar"}
	q := DeclareAndBind(cfg, "test", 0)
	assert.Nil(t, q)
}

func TestDeclareAndBindNoId(t *testing.T) {
	log.Setup()
	cfg := config.Config{Queues: config.Classic, PublishTo: "/queues/foobar"}
	q := DeclareAndBind(cfg, "foobar", 0)
	assert.Equal(t, "foobar", q.GetName())
	err := Get().Queue("foobar").Delete(context.Background())
	assert.Nil(t, err)
}

func TestDeclareAndBindWithId(t *testing.T) {
	log.Setup()
	cfg := config.Config{Queues: config.Classic, PublishTo: "/queues/foobar"}
	q := DeclareAndBind(cfg, "foobar-123", 123)
	assert.Equal(t, "foobar-123", q.GetName())
	err := Get().Queue("foobar-123").Delete(context.Background())
	assert.Nil(t, err)
}
