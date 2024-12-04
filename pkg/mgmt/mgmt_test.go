package mgmt_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/mgmt"
)

var (
	rmqMgmt *mgmt.Mgmt

	_ = Describe("DeclareAndBind", func() {
		BeforeEach(func() {
			log.Setup()
			rmqMgmt = mgmt.Start(context.Background(), []string{"amqp://localhost/"}, false)
		})
		It("should not declare anything when using predeclared queues", func() {
			cfg := config.Config{Queues: config.Predeclared, PublishTo: "foobar"}
			q := rmqMgmt.DeclareAndBind(cfg, "test", 0)
			Expect(q).To(BeNil())
		})

		It("should declare and bind a classic queue", func() {
			cfg := config.Config{Queues: config.Classic, PublishTo: "/queues/mgmt-classic", ConsumerUri: []string{"amqp://guest:guest@localhost:5672/"}}
			q := rmqMgmt.DeclareAndBind(cfg, "mgmt-classic", 0)
			Expect(q.Name()).To(Equal("mgmt-classic"))
			Expect(string(q.Type())).To(Equal("classic"))
			err := rmqMgmt.DeleteQueue(context.TODO(), "mgmt-classic")
			Expect(err).To(BeNil())
			// TOOD assert the binding
		})

		It("should declare and bind a quorum queue", func() {
			cfg := config.Config{Queues: config.Quorum, PublishTo: "/queues/mgmt-quorum", ConsumerUri: []string{"amqp://guest:guest@localhost:5672/"}}
			q := rmqMgmt.DeclareAndBind(cfg, "mgmt-quorum", 0)
			Expect(q.Name()).To(Equal("mgmt-quorum"))
			Expect(string(q.Type())).To(Equal("quorum"))
			err := rmqMgmt.DeleteQueue(context.TODO(), "mgmt-quorum")
			Expect(err).To(BeNil())
			// TOOD assert the binding
		})

		It("should declare and bind a stream queue", func() {
			cfg := config.Config{Queues: config.Stream, PublishTo: "/queues/mgmt-stream", ConsumerUri: []string{"amqp://guest:guest@localhost:5672/"}}
			q := rmqMgmt.DeclareAndBind(cfg, "mgmt-stream", 0)
			Expect(q.Name()).To(Equal("mgmt-stream"))
			Expect(string(q.Type())).To(Equal("stream"))
			err := rmqMgmt.DeleteQueue(context.TODO(), "mgmt-stream")
			Expect(err).To(BeNil())
			// TOOD assert the binding
		})
	})
)
