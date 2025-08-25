package main_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/rabbitmq/omq/pkg/testutil"
)

func TestOmq(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "OMQ Suite")
}

var (
	omqPath string
	_       = BeforeSuite(func() {
		// Check RabbitMQ connectivity before running any tests
		By("Checking RabbitMQ connectivity")
		if err := testutil.CheckRabbitMQConnectivity(); err != nil {
			Fail(testutil.RabbitMQFailureMessage(err))
		}

		By("Building omq binary")
		var err error
		omqPath, err = gexec.Build("github.com/rabbitmq/omq")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(gexec.CleanupBuildArtifacts)
	})
)
