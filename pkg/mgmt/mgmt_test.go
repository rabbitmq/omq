package mgmt_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/mgmt"
)

var _ = Describe("DeclareAndBind", func() {
	It("should not declare anything when using predeclared queues", func() {
		cfg := config.Config{Queues: config.Predeclared, PublishTo: "foobar"}
		q := mgmt.DeclareAndBind(cfg, "test", 0)
		Expect(q).To(BeNil())
	})

	It("should declare and bind a classic queue", func() {
		log.Setup()
		cfg := config.Config{Queues: config.Classic, PublishTo: "/queues/mgmt-classic"}
		q := mgmt.DeclareAndBind(cfg, "mgmt-classic", 0)
		Expect(q.Name()).To(Equal("mgmt-classic"))
		Expect(string(q.Type())).To(Equal("classic"))
		err := mgmt.Get().DeleteQueue(context.TODO(), "mgmt-classic")
		Expect(err).To(BeNil())
		// TOOD assert the binding
	})

	It("should declare and bind a quorum queue", func() {
		log.Setup()
		cfg := config.Config{Queues: config.Quorum, PublishTo: "/queues/mgmt-quorum"}
		q := mgmt.DeclareAndBind(cfg, "mgmt-quorum", 0)
		Expect(q.Name()).To(Equal("mgmt-quorum"))
		Expect(string(q.Type())).To(Equal("quorum"))
		err := mgmt.Get().DeleteQueue(context.TODO(), "mgmt-quorum")
		Expect(err).To(BeNil())
		// TOOD assert the binding
	})

	It("should declare and bind a stream queue", func() {
		log.Setup()
		cfg := config.Config{Queues: config.Stream, PublishTo: "/queues/mgmt-stream"}
		q := mgmt.DeclareAndBind(cfg, "mgmt-stream", 0)
		Expect(q.Name()).To(Equal("mgmt-stream"))
		Expect(string(q.Type())).To(Equal("stream"))
		err := mgmt.Get().DeleteQueue(context.TODO(), "mgmt-stream")
		Expect(err).To(BeNil())
		// TOOD assert the binding
	})
})
