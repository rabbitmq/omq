package utils_test

import (
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/utils"
)

var _ = Context("Utils", func() {
	DescribeTable("Latency calculation",
		func(units string, useMillis bool) {
			testMsg := utils.MessageBody(100, nil, 1)
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

	Describe("Delay accuracy calculation", func() {
		It("should calculate delay accuracy correctly for delayed messages", func() {
			testMsg := utils.MessageBody(100, nil, 1)
			utils.UpdatePayload(false, &testMsg)

			// Simulate a 100ms delay
			delayMs := int64(100)
			time.Sleep(50 * time.Millisecond) // Sleep less than the delay

			delayAccuracy, isDelayed := utils.CalculateDelayAccuracy(&testMsg, delayMs)

			Expect(isDelayed).To(BeTrue())
			// Should be negative (early) since we slept less than the delay
			Expect(delayAccuracy).To(BeNumerically("<", 0))
			// Should be around -50ms (we slept 50ms less than the 100ms delay)
			Expect(delayAccuracy.Milliseconds()).To(BeNumerically(">", -60))
			Expect(delayAccuracy.Milliseconds()).To(BeNumerically("<", -40))
		})

		It("should return false for messages without delay", func() {
			testMsg := utils.MessageBody(100, nil, 1)
			utils.UpdatePayload(false, &testMsg)

			delayAccuracy, isDelayed := utils.CalculateDelayAccuracy(&testMsg, 0)

			Expect(isDelayed).To(BeFalse())
			Expect(delayAccuracy).To(Equal(time.Duration(0)))
		})

		It("should return false for messages without latency tracking", func() {
			// Create a message that's too small for latency tracking (< 12 bytes)
			testMsg := make([]byte, 8)

			delayAccuracy, isDelayed := utils.CalculateDelayAccuracy(&testMsg, 100)

			Expect(isDelayed).To(BeFalse())
			Expect(delayAccuracy).To(Equal(time.Duration(0)))
		})

		It("should calculate positive delay accuracy for late messages", func() {
			testMsg := utils.MessageBody(100, nil, 1)
			utils.UpdatePayload(false, &testMsg)

			// Simulate a 50ms delay but sleep for 100ms
			delayMs := int64(50)
			time.Sleep(100 * time.Millisecond)

			delayAccuracy, isDelayed := utils.CalculateDelayAccuracy(&testMsg, delayMs)

			Expect(isDelayed).To(BeTrue())
			// Should be positive (late) since we slept more than the delay
			Expect(delayAccuracy).To(BeNumerically(">", 0))
			// Should be around +50ms (we slept 50ms more than the 50ms delay)
			Expect(delayAccuracy.Milliseconds()).To(BeNumerically(">", 40))
			Expect(delayAccuracy.Milliseconds()).To(BeNumerically("<", 60))
		})
	})

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
			It("should generate wrapped sequence "+strconv.Itoa(n), func() {
				Expect(utils.WrappedSequence(tc.length, tc.start)).To(Equal(tc.expectedSequence))
			})
		}
	})

	Describe("MessageBody with size", func() {
		It("should use default size when neither Size nor SizeTemplate is set", func() {
			msg := utils.MessageBody(0, nil, 1)
			Expect(len(msg)).To(Equal(12))
		})

		It("should use Size field for static values", func() {
			msg := utils.MessageBody(200, nil, 1)
			Expect(len(msg)).To(Equal(200))
		})

		It("should use SizeTemplate for dynamic values", func() {
			tmpl, err := config.ParseTemplateValue("300")
			Expect(err).To(BeNil())
			msg := utils.MessageBody(0, tmpl, 1)
			Expect(len(msg)).To(Equal(300))
		})

		It("should prefer Size field over SizeTemplate when both are set", func() {
			tmpl, err := config.ParseTemplateValue("300")
			Expect(err).To(BeNil())
			msg := utils.MessageBody(150, tmpl, 1)
			Expect(len(msg)).To(Equal(150))
		})

		It("should handle comma-separated values in template", func() {
			tmpl, err := config.ParseTemplateValue("100,200,300")
			Expect(err).To(BeNil())

			msg1 := utils.MessageBody(0, tmpl, 1)
			Expect(len(msg1)).To(Equal(100))
		})

		It("should handle size units in template", func() {
			tmpl, err := config.ParseTemplateValue("1kb")
			Expect(err).To(BeNil())
			msg := utils.MessageBody(0, tmpl, 1)
			Expect(len(msg)).To(Equal(1024))

			tmpl, err = config.ParseTemplateValue("10mb")
			Expect(err).To(BeNil())
			msg = utils.MessageBody(0, tmpl, 1)
			Expect(len(msg)).To(Equal(10 * 1024 * 1024))
		})
	})

	Describe("ParseSize", func() {
		It("should parse plain integer values (backward compatibility)", func() {
			size, err := utils.ParseSize("100")
			Expect(err).To(BeNil())
			Expect(size).To(Equal(100))
		})

		It("should parse values with 'b' unit", func() {
			size, err := utils.ParseSize("100b")
			Expect(err).To(BeNil())
			Expect(size).To(Equal(100))

			size, err = utils.ParseSize("100B")
			Expect(err).To(BeNil())
			Expect(size).To(Equal(100))
		})

		It("should parse values with 'kb' unit", func() {
			size, err := utils.ParseSize("1kb")
			Expect(err).To(BeNil())
			Expect(size).To(Equal(1024))

			size, err = utils.ParseSize("2KB")
			Expect(err).To(BeNil())
			Expect(size).To(Equal(2048))
		})

		It("should parse values with 'mb' unit", func() {
			size, err := utils.ParseSize("1mb")
			Expect(err).To(BeNil())
			Expect(size).To(Equal(1024 * 1024))

			size, err = utils.ParseSize("10MB")
			Expect(err).To(BeNil())
			Expect(size).To(Equal(10 * 1024 * 1024))
		})

		It("should handle decimal values", func() {
			size, err := utils.ParseSize("1.5kb")
			Expect(err).To(BeNil())
			Expect(size).To(Equal(int(1.5 * 1024)))

			size, err = utils.ParseSize("0.5mb")
			Expect(err).To(BeNil())
			Expect(size).To(Equal(int(0.5 * 1024 * 1024)))
		})

		It("should handle leading/trailing whitespace only", func() {
			size, err := utils.ParseSize("  100  ")
			Expect(err).To(BeNil())
			Expect(size).To(Equal(100))

			size, err = utils.ParseSize("  10mb  ")
			Expect(err).To(BeNil())
			Expect(size).To(Equal(10 * 1024 * 1024))
		})

		It("should reject whitespace between number and unit", func() {
			_, err := utils.ParseSize("10 mb")
			Expect(err).NotTo(BeNil())

			_, err = utils.ParseSize("100 kb")
			Expect(err).NotTo(BeNil())
		})

		It("should return error for invalid format", func() {
			_, err := utils.ParseSize("invalid")
			Expect(err).NotTo(BeNil())

			_, err = utils.ParseSize("10 20")
			Expect(err).NotTo(BeNil())
		})

		It("should return error for unknown unit", func() {
			_, err := utils.ParseSize("10tb")
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("unknown size unit"))

			_, err = utils.ParseSize("10xyz")
			Expect(err).NotTo(BeNil())

			// Full word units should not be supported
			_, err = utils.ParseSize("10kilobytes")
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("unknown size unit"))

			_, err = utils.ParseSize("10megabytes")
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("unknown size unit"))
		})
	})
})
