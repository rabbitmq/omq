package mgmt_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/omq/pkg/testutil"
)

func TestMgmt(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MGMT Suite")
}

var _ = BeforeSuite(func() {
	// Check RabbitMQ connectivity before running any tests
	By("Checking RabbitMQ connectivity")
	if err := testutil.CheckRabbitMQConnectivity(); err != nil {
		Fail(testutil.RabbitMQFailureMessage(err))
	}
})
