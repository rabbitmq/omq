package main_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

func TestOmq(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "OMQ Suite")
}

var omqPath string
var _ = BeforeSuite(func() {
	var err error
	omqPath, err = gexec.Build("github.com/rabbitmq/omq")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(gexec.CleanupBuildArtifacts)
})
