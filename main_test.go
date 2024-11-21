package main_test

import (
	"bufio"
	"bytes"
	"io"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	rabbithole "github.com/michaelklishin/rabbit-hole"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
)

var _ = Describe("OMQ CLI", func() {
	DescribeTable("supports any combination of protocols",
		func(publishProto string, publishToPrefix string, consumeProto string, consumeFromPrefix string) {
			publishTo := publishToPrefix + publishProto + consumeProto
			consumeFrom := consumeFromPrefix + publishProto + consumeProto
			args := []string{publishProto + "-" + consumeProto,
				"--pmessages=1",
				"--cmessages=1",
				"-t", publishTo,
				"-T", consumeFrom,
				"--queue-durability=none",
				"--time=3s",
				"--print-all-metrics",
			}
			if consumeProto == "amqp" {
				args = append(args, "--queues", "classic", "--cleanup-queues=true")
			}

			session := omq(args)
			Eventually(session).WithTimeout(4 * time.Second).Should(gexec.Exit(0))

			Eventually(session.Err).Should(gbytes.Say(`TOTAL PUBLISHED messages=1`))
			Eventually(session.Err).Should(gbytes.Say(`TOTAL CONSUMED messages=1`))
			Eventually(session).Should(gbytes.Say(`omq_messages_consumed_total{priority="normal"} 1`))
		},
		Entry("amqp -> amqp", "amqp", "/queues/", "amqp", "/queues/"), // https://github.com/Azure/go-amqp/issues/313
		Entry("stomp -> amqp", "stomp", "/topic/", "amqp", "/queues/"),
		Entry("mqtt -> amqp", "mqtt", "/topic/", "amqp", "/queues/"),
		Entry("amqp -> stomp", "amqp", "/exchanges/amq.topic/", "stomp", "/topic/"),
		Entry("amqp -> mqtt", "amqp", "/exchanges/amq.topic/", "mqtt", "/topic/"),
		Entry("stomp -> stomp", "stomp", "/topic/", "stomp", "/topic/"),
		Entry("stomp -> mqtt", "stomp", "/topic/", "mqtt", "/topic/"),
		Entry("mqtt -> mqtt", "mqtt", "/topic/", "mqtt", "/topic/"),
		Entry("mqtt -> stomp", "mqtt", "/topic/", "stomp", "/topic/"),
	)

	DescribeTable("supports message priorities for AMQP and STOMP",
		func(publishProto string, publishToPrefix string, consumeProto string, consumeFromPrefix string) {
			publishTo := publishToPrefix + publishProto + consumeProto
			consumeFrom := consumeFromPrefix + publishProto + consumeProto
			args := []string{publishProto + "-" + consumeProto,
				"-C", "1",
				"-D", "1",
				"-t", publishTo,
				"-T", consumeFrom,
				"--message-priority", "13",
				"--queue-durability", "none",
				"--time", "3s", // don't want to long in case of issues
				"--print-all-metrics",
			}
			if consumeProto == "amqp" {
				args = append(args, "--queues", "classic", "--cleanup-queues=true")
			}

			session := omq(args)
			Eventually(session).WithTimeout(4 * time.Second).Should(gexec.Exit(0))

			Eventually(session.Err).Should(gbytes.Say(`TOTAL PUBLISHED messages=1`))
			Eventually(session.Err).Should(gbytes.Say(`TOTAL CONSUMED messages=1`))
			Eventually(session).Should(gbytes.Say(`omq_messages_consumed_total{priority="high"} 1`))
		},
		Entry("amqp -> amqp", "amqp", "/queues/", "amqp", "/queues/"),
		Entry("stomp -> amqp", "stomp", "/topic/", "amqp", "/queues/"),
		Entry("amqp -> stomp", "amqp", "/exchanges/amq.topic/", "stomp", "/topic/"),
		Entry("stomp -> stomp", "stomp", "/topic/", "stomp", "/topic/"),
	)

	Describe("supports AMQP Stream Application Property Filters", func() {
		It("should filter messages based on app properties", func() {
			args := []string{"amqp",
				"--pmessages=6",
				"--publish-to=/queues/stream-with-app-property-filters",
				"--consume-from=/queues/stream-with-app-property-filters",
				"--amqp-app-property", "key1=foo,bar,baz",
				"--amqp-app-property-filter", "key1=$p:ba",
				"--queues=stream",
				"--cleanup-queues=true",
				"--time=2s",
			}

			session := omq(args)
			Eventually(session).WithTimeout(4 * time.Second).Should(gexec.Exit(0))
			Eventually(session.Err).Should(gbytes.Say(`TOTAL PUBLISHED messages=6`))
			Eventually(session.Err).Should(gbytes.Say(`TOTAL CONSUMED messages=4`))
		})
	})

	Describe("supports AMQP Stream Property Filters", func() {
		It("should filter messages based on app properties", func() {
			args := []string{"amqp",
				"--pmessages=3",
				"--publish-to=/queues/stream-with-property-filters",
				"--consume-from=/queues/stream-with-property-filters",
				"--amqp-subject=foo,bar,baz",
				"--amqp-property-filter", "subject=baz",
				"--queues=stream",
				"--cleanup-queues=true",
				"--time=2s",
			}

			session := omq(args)
			Eventually(session).WithTimeout(4 * time.Second).Should(gexec.Exit(0))
			Eventually(session.Err).Should(gbytes.Say(`TOTAL PUBLISHED messages=3`))
			Eventually(session.Err).Should(gbytes.Say(`TOTAL CONSUMED messages=1`))
		})
	})

	Describe("Fan-In from MQTT to AMQP", func() {
		It("should fan-in messages from MQTT to AMQP", func() {
			args := []string{"mqtt-amqp",
				"--publishers=3",
				"--consumers=1",
				"--pmessages=5",
				"--publish-to=sensor/%d",
				"--consume-from=/queues/sensors",
				"--amqp-binding-key=sensor.#",
				"--queues=classic",
				"--cleanup-queues=true",
				"--time=2s",
			}

			session := omq(args)
			Eventually(session).WithTimeout(4 * time.Second).Should(gexec.Exit(0))
			Eventually(session.Err).Should(gbytes.Say(`TOTAL PUBLISHED messages=15`))
			Eventually(session.Err).Should(gbytes.Say(`TOTAL CONSUMED messages=15`))
		})
	})

	Describe("supports --consumer-startup-delay", func() {
		It("should start consumers after the configured delay", func() {
			args := []string{"amqp",
				"-C", "1",
				"-D", "1",
				"--consumer-startup-delay=3s",
				"-t", "/queues/consumer-startup-delay",
				"-T", "/queues/consumer-startup-delay",
				"--queues", "classic",
				"--cleanup-queues=true",
				"--print-all-metrics"}

			session := omq(args)
			Eventually(session).WithTimeout(5 * time.Second).Should(gexec.Exit(0))
			Expect(metricValue(session.Out, `omq_end_to_end_latency_seconds{quantile="0.99"}`)).Should(BeNumerically(">", 2))
		})
	})

	Describe("supports `--max-in-flight` in AMQP", func() {
		It("Higher --max-in-flight value should lead to higher publishing rate", func() {
			publishWithMaxInFlight := func(maxInFlight string) *gexec.Session {
				args := []string{"amqp",
					"-z", "3s",
					"-t", "/queues/amqp-max-in-flight",
					"-T", "/queues/amqp-max-in-flight",
					"--queues", "stream",
					"--cleanup-queues=true",
					"--print-all-metrics",
					"--max-in-flight", maxInFlight}

				session := omq(args)
				Eventually(session).WithTimeout(5 * time.Second).Should(gexec.Exit(0))
				return session
			}

			session1 := publishWithMaxInFlight("1")
			publishedWithMaxInFlight1 := metricValue(session1.Out, `omq_messages_published_total`)

			session8 := publishWithMaxInFlight("8")
			publishedWithMaxInFlight8 := metricValue(session8.Out, `omq_messages_published_total`)

			Expect(publishedWithMaxInFlight1).Should(BeNumerically(">", 0))
			Expect(publishedWithMaxInFlight8).Should(BeNumerically(">", 0))
			// we don't expect 8x the throughput, but at least 2x
			Expect(publishedWithMaxInFlight8).Should(BeNumerically(">", publishedWithMaxInFlight1*2))
		})
	})

	DescribeTable("supports MQTT version 3.1, 3.1.1 and 5.0",
		func(versionFlag string, connectionVersion string) {
			rmqc, err := rabbithole.NewClient("http://127.0.0.1:15672", "guest", "guest")
			Expect(err).ShouldNot(HaveOccurred())
			args := []string{"mqtt",
				"--time=6s",
				"--publish-to=/topic/foo",
				"--consume-from=/topic/foo",
				"--consumer-id=omq-test-%r",
				"--publisher-id=omq-test-%r",
				"--rate", "1",
				"--print-all-metrics",
			}
			if (versionFlag) != "" {
				args = append(args, "--mqtt-publisher-version", versionFlag)
				args = append(args, "--mqtt-consumer-version", versionFlag)
			}
			session := omq(args)

			Eventually(func() bool {
				conns, err := rmqc.ListConnections()
				return err == nil &&
					len(conns) >= 2 &&
					slices.ContainsFunc(conns, func(conn rabbithole.ConnectionInfo) bool {
						return conn.Protocol == connectionVersion
					})
			}, 7*time.Second, 500*time.Millisecond).Should(BeTrue())
			Eventually(session).WithTimeout(7 * time.Second).Should(gexec.Exit(0))

			output, _ := io.ReadAll(session.Out)
			buf := bytes.NewReader(output)
			Expect(metricValue(buf, `omq_messages_consumed_total{priority="normal"}`)).Should((BeNumerically(">", 0)))
			buf.Reset(output)
			Expect(metricValue(buf, `omq_messages_published_total`)).Should((BeNumerically(">", 0)))
		},
		Entry("MQTT v3.1", "3", "MQTT 3-1"),
		Entry("MQTT v3.1.1", "4", "MQTT 3-1-1"),
		Entry("MQTT v5.0", "5", "MQTT 5-0"),
		Entry("default to MQTT v5.0", "", "MQTT 5-0"),
	)
})

func omq(args []string) *gexec.Session {
	GinkgoWriter.Println("omq", strings.Join(args, " "))

	cmd := exec.Command(omqPath, args...)
	session, err := gexec.Start(cmd, GinkgoWriter, GinkgoWriter)
	Expect(err).ShouldNot(HaveOccurred())
	return session
}

func metricValue(buf io.Reader, metric string) float64 {
	scanner := bufio.NewScanner(buf)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, metric) {
			v, err := strconv.ParseFloat(strings.Fields(line)[1], 64)
			if err != nil {
				return -1
			}
			GinkgoWriter.Println("The value of", metric, "is", v)
			return v
		}
	}
	return -1
}