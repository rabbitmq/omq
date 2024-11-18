package mgmt_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMgmt(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MGMT Suite")
}
