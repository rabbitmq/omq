package utils_test

import (
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/omq/pkg/utils"
)

var _ = Context("Utils", func() {
	DescribeTable("Latency calculation",
		func(units string, useMillis bool) {
			testMsg := utils.MessageBody(100)
			utils.UpdatePayload(useMillis, &testMsg)
			time.Sleep(10 * time.Millisecond)
			_, latency := utils.CalculateEndToEndLatency(&testMsg)
			// not very precise but we just care about the order of magnitude
			Expect(latency.Milliseconds()).Should(BeNumerically(">", 9))
			Expect(latency.Milliseconds()).Should(BeNumerically("<", 50))
		},
		Entry("When using nanoseconds", "nanoseconds", false),
		Entry("When using milliseconds", "milliseconds", true),
	)

	Describe("URI Parsing", func() {
		type test struct {
			rawURI        string
			defaultScheme string
			defaultPort   string
			broker        string
			username      string
			password      string
		}

		tests := []test{
			{rawURI: "mqtt://user:pass@name.com", defaultScheme: "mqtt", defaultPort: "1234", broker: "name.com:1234", username: "user", password: "pass"},
			{rawURI: "mqtt://name.com", defaultScheme: "mqtt", defaultPort: "1234", broker: "name.com:1234", username: "guest", password: "guest"},
			{rawURI: "mqtts://local:4321", defaultScheme: "mqtt", defaultPort: "1234", broker: "local:4321", username: "guest", password: "guest"},
			{rawURI: "local:4321", defaultScheme: "mqtt", defaultPort: "1234", broker: "local:4321", username: "guest", password: "guest"},
		}

		for _, tc := range tests {
			tc := tc
			It("should parse URI "+tc.rawURI+"-"+tc.defaultPort, func() {
				parsed := utils.ParseURI(tc.rawURI, tc.defaultScheme, tc.defaultPort)
				Expect(parsed.Broker).To(Equal(tc.broker))
				Expect(parsed.Username).To(Equal(tc.username))
				Expect(parsed.Password).To(Equal(tc.password))
			})
		}
	})

	Describe("Wrapped Sequence", func() {
		type test struct {
			length           int
			start            int
			expectedSequence []int
		}

		tests := []test{
			{length: 5, start: 0, expectedSequence: []int{0, 1, 2, 3, 4}},
			{length: 5, start: 1, expectedSequence: []int{1, 2, 3, 4, 0}},
			{length: 3, start: 1, expectedSequence: []int{1, 2, 0}},
			{length: 3, start: 4, expectedSequence: []int{1, 2, 0}},
			{length: 3, start: 5, expectedSequence: []int{2, 0, 1}},
			{length: 1, start: 2, expectedSequence: []int{0}},
		}

		for n, tc := range tests {
			tc := tc
			It("should generate wrapped sequence "+strconv.Itoa(n), func() {
				Expect(utils.WrappedSequence(tc.length, tc.start)).To(Equal(tc.expectedSequence))
			})
		}
	})
})
