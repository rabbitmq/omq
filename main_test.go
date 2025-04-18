package main_test

import (
	"bufio"
	"bytes"
	"io"
	"os"
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
	Describe("basic start/stop functionality", func() {
		It("executed without any commands/flags, displays its usage", func() {
			session := omq([]string{})
			Eventually(session).Should(gexec.Exit(0))
			Eventually(session.Out).Should(gbytes.Say(`Available Commands:`))
			Eventually(session.Out).Should(gbytes.Say(`Flags:`))
		})
		It("--time limit stops omq quickly", func() {
			args := []string{
				"mqtt",
				"--publishers=100",
				"--publish-to=time/%d",
				"--rate=1",
				"--consumers=100",
				"--consume-from=time/%d",
				"--time", "5s",
			}
			session := omq(args)
			// wait until metrics are printed (after consuemrs connected and publishers were spawned)
			Eventually(session.Err).WithTimeout(10 * time.Second).Should(gbytes.Say(`published=`))
			// from that moment, it should terminate roughly in 5 seconds
			Eventually(session).WithTimeout(6 * time.Second).Should(gexec.Exit(0))
		})
		It("^C can stop omq while it's trying to connect", func() {
			args := []string{
				"amqp",
				"--uri", "amqp://foobar:5672",
			}
			session := omq(args)
			Eventually(session.Err).WithTimeout(5 * time.Second).Should(gbytes.Say(`consumer failed to connect`))
			session.Signal(os.Signal(os.Interrupt))
			Eventually(session).WithTimeout(3 * time.Second).Should(gexec.Exit(0))
		})
		It("^C can stop omq while it's trying to create a sender", func() {
			args := []string{
				"amqp",
				"-y", "0",
				"-t", "/queues/no-such-queue",
			}
			session := omq(args)
			Eventually(session.Err).WithTimeout(5 * time.Second).Should(gbytes.Say(`publisher failed to create a sender `))
			session.Signal(os.Signal(os.Interrupt))
			Eventually(session).WithTimeout(3 * time.Second).Should(gexec.Exit(0))
		})
		It("^C can stop omq while it's publishing and consuming", func() {
			args := []string{
				"mqtt",
				"--publishers=2",
				"--publish-to=can-stop-me-now/%d",
				"--rate=1",
				"--consumers=2",
				"--consume-from=/queues/can-stop-me-now",
				"--binding-key=can-stop-me-now.#",
			}
			session := omq(args)
			// wait until metrics are printed (after consuemrs connected and publishers were spawned)
			Eventually(session.Err).WithTimeout(5 * time.Second).Should(gbytes.Say(`published=`))
			// from that moment, it should terminate roughly in 5 seconds
			time.Sleep(2 * time.Second)
			session.Signal(os.Signal(os.Interrupt))
			Eventually(session).WithTimeout(3 * time.Second).Should(gexec.Exit(0))
		})
	})

	DescribeTable("supports any combination of protocols",
		func(publishProto string, publishToPrefix string, consumeProto string, consumeFromPrefix string) {
			publishTo := publishToPrefix + publishProto + consumeProto
			consumeFrom := consumeFromPrefix + publishProto + consumeProto
			args := []string{
				publishProto + "-" + consumeProto,
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
		Entry("amqp -> amqp", "amqp", "/queues/", "amqp", "/queues/"),
		Entry("amqp -> amqp091", "amqp", "/queues/", "amqp", "/queues/"),
		Entry("amqp -> stomp", "amqp", "/exchanges/amq.topic/", "stomp", "/topic/"),
		Entry("amqp -> mqtt", "amqp", "/exchanges/amq.topic/", "mqtt", "/topic/"),
		Entry("amqp091 -> amqp", "amqp091", "/queues/", "amqp", "/queues/"),
		Entry("amqp091 -> amqp091", "amqp091", "/queues/", "amqp", "/queues/"),
		Entry("amqp091 -> mqtt", "amqp091", "/exchanges/amq.topic/", "mqtt", "/topic/"),
		Entry("amqp091 -> stomp", "amqp091", "/exchanges/amq.topic/", "stomp", "/topic/"),
		Entry("mqtt -> amqp", "mqtt", "/topic/", "amqp", "/queues/"),
		Entry("mqtt -> amqp091", "mqtt", "/topic/", "amqp", "/queues/"),
		Entry("mqtt -> mqtt", "mqtt", "/topic/", "mqtt", "/topic/"),
		Entry("mqtt -> stomp", "mqtt", "/topic/", "stomp", "/topic/"),
		Entry("stomp -> amqp", "stomp", "/topic/", "amqp", "/queues/"),
		Entry("stomp -> amqp091", "stomp", "/topic/", "amqp", "/queues/"),
		Entry("stomp -> stomp", "stomp", "/topic/", "stomp", "/topic/"),
		Entry("stomp -> mqtt", "stomp", "/topic/", "mqtt", "/topic/"),
	)

	DescribeTable("supports message priorities for AMQP and STOMP",
		func(publishProto string, publishToPrefix string, consumeProto string, consumeFromPrefix string) {
			publishTo := publishToPrefix + publishProto + consumeProto
			consumeFrom := consumeFromPrefix + publishProto + consumeProto
			args := []string{
				publishProto + "-" + consumeProto,
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
			args := []string{
				"amqp",
				"--pmessages=6",
				"--publish-to=/queues/stream-with-app-property-filters",
				"--consume-from=/queues/stream-with-app-property-filters",
				"--amqp-app-property", "key1=foo,bar,baz",
				"--amqp-app-property-filter", "key1=&p:ba",
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
			args := []string{
				"amqp",
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

	Describe("supports Fan-In from MQTT to AMQP", func() {
		It("should fan-in messages from MQTT to AMQP", func() {
			args := []string{
				"mqtt-amqp",
				"--publishers=3",
				"--consumers=1",
				"--pmessages=5",
				"--publish-to=fan-in-mqtt-amqp",
				"--consume-from=/queues/fan-in-mqtt-amqp",
				"--binding-key=fan-in-mqtt-amqp",
				"--queues=classic",
				"--cleanup-queues=true",
				"--time=5s",
			}

			session := omq(args)
			Eventually(session).WithTimeout(6 * time.Second).Should(gexec.Exit(0))
			Eventually(session.Err).Should(gbytes.Say(`TOTAL PUBLISHED messages=15`))
			Eventually(session.Err).Should(gbytes.Say(`TOTAL CONSUMED messages=15`))
		})
	})

	Describe("supports Fan-Out from AMQP to MQTT", func() {
		It("should fan-out messages from AMQP to MQTT", func() {
			args := []string{
				"amqp-mqtt",
				"--publishers=1",
				"--consumers=10",
				"--pmessages=5",
				"--publish-to=/exchanges/amq.topic/broadcast",
				"--consume-from=broadcast",
				"--time=3s",
			}

			session := omq(args)
			Eventually(session).WithTimeout(6 * time.Second).Should(gexec.Exit(0))
			Eventually(session.Err).Should(gbytes.Say(`TOTAL PUBLISHED messages=5`))
			Eventually(session.Err).Should(gbytes.Say(`TOTAL CONSUMED messages=50`))
		})
	})

	Describe("supports --consumer-startup-delay", func() {
		It("should start consumers after the configured delay", func() {
			args := []string{
				"amqp",
				"-C", "1",
				"-D", "1",
				"--consumer-startup-delay=3s",
				"-t", "/queues/consumer-startup-delay",
				"-T", "/queues/consumer-startup-delay",
				"--queues", "classic",
				"--cleanup-queues=true",
				"--print-all-metrics",
			}

			session := omq(args)
			Eventually(session).WithTimeout(5 * time.Second).Should(gexec.Exit(0))
			output, _ := io.ReadAll(session.Out)
			buf := bytes.NewReader(output)
			Expect(metricValue(buf, `omq_messages_consumed_total{priority="normal"}`)).Should(Equal(1.0))
			buf.Reset(output)
			Expect(metricValue(buf, `omq_end_to_end_latency_seconds{quantile="0.99"}`)).Should(BeNumerically(">", 2))
		})
	})

	Describe("supports `--max-in-flight` in AMQP", func() {
		It("Higher --max-in-flight value should lead to higher publishing rate", func() {
			publishWithMaxInFlight := func(maxInFlight string) *gexec.Session {
				args := []string{
					"amqp",
					"-z", "3s",
					"-t", "/queues/amqp-max-in-flight",
					"-T", "/queues/amqp-max-in-flight",
					"--queues", "stream",
					"--cleanup-queues=true",
					"--print-all-metrics",
					"--max-in-flight", maxInFlight,
				}

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
			args := []string{
				"mqtt",
				"--publish-to=/topic/omq-version-test-topic-" + versionFlag,
				"--consume-from=/topic/omq-version-test-topic-" + versionFlag,
				"--consumer-id=omq-version-test-consumer-" + versionFlag,
				"--publisher-id=omq-version-test-publisher-" + versionFlag,
				"--rate", "1",
				"--print-all-metrics",
			}
			if (versionFlag) != "" {
				args = append(args, "--mqtt-publisher-version", versionFlag)
				args = append(args, "--mqtt-consumer-version", versionFlag)
			}
			session := omq(args)
			Eventually(session.Err).WithTimeout(5 * time.Second).Should(gbytes.Say(`published=`))

			Eventually(func() bool {
				conns, err := rmqc.ListConnections()
				if err == nil &&
					len(conns) >= 2 &&
					slices.ContainsFunc(conns, func(conn rabbithole.ConnectionInfo) bool {
						return conn.Protocol == connectionVersion &&
							strings.HasPrefix(conn.ClientProperties["client_id"].(string), "omq-version-test")
					}) {
					return true
				} else {
					GinkgoWriter.Printf("\n--- time: %v    len: %v ---\n%+v\n---\n", time.Now(), len(conns), conns)
					return false
				}
			}, 7*time.Second, 500*time.Millisecond).Should(BeTrue())
			session.Signal(os.Signal(os.Interrupt))
			Eventually(session).WithTimeout(5 * time.Second).Should(gexec.Exit(0))

			output, _ := io.ReadAll(session.Out)
			buf := bytes.NewReader(output)
			Expect(metricValue(buf, `omq_messages_consumed_total{priority="normal"}`)).Should((BeNumerically(">", 0.0)))
			buf.Reset(output)
			Expect(metricValue(buf, `omq_messages_published_total`)).Should((BeNumerically(">", 0.0)))
		},
		Entry("MQTT v3.1", "3", "MQTT 3-1"),
		Entry("MQTT v3.1.1", "4", "MQTT 3-1-1"),
		Entry("MQTT v5.0", "5", "MQTT 5-0"),
		Entry("default to MQTT v5.0", "", "MQTT 5-0"),
	)

	Describe("declares queues for AMQP and STOMP clients", func() {
		It("declares queues for AMQP consumers with /queues/ address", func() {
			args := []string{
				"amqp",
				"-y", "2",
				"-x", "0",
				"-T", "/queues/declare-without-publishers-%d",
				"--queues", "classic",
				"--cleanup-queues=true",
				"--time", "10s",
			}

			rmqc, err := rabbithole.NewClient("http://127.0.0.1:15672", "guest", "guest")
			Expect(err).ShouldNot(HaveOccurred())
			session := omq(args)
			Eventually(func() bool {
				q1, err1 := rmqc.GetQueue("/", "declare-without-publishers-1")
				q2, err2 := rmqc.GetQueue("/", "declare-without-publishers-2")
				return err1 == nil && q1.Name == "declare-without-publishers-1" &&
					err2 == nil && q2.Name == "declare-without-publishers-2"
			}).WithTimeout(3 * time.Second).Should(BeTrue())

			session.Signal(os.Signal(os.Interrupt))

			// eventually the queue should be deleted
			Eventually(func() bool {
				_, err1 := rmqc.GetQueue("/", "declare-without-publishers-1")
				_, err2 := rmqc.GetQueue("/", "declare-without-publishers-2")
				return err1 != nil && strings.Contains(err1.Error(), "Object Not Found") &&
					err2 != nil && strings.Contains(err2.Error(), "Object Not Found")
			}).WithTimeout(3 * time.Second).Should(BeTrue())
		})

		It("declares queues for AMQP publishers with /queues/... address", func() {
			args := []string{
				"amqp",
				"-y", "0",
				"-r", "1",
				"-t", "/queues/declare-without-consumers",
				"--queues", "classic",
				"--cleanup-queues=true",
				"--time", "10s",
			}

			rmqc, err := rabbithole.NewClient("http://127.0.0.1:15672", "guest", "guest")
			Expect(err).ShouldNot(HaveOccurred())
			session := omq(args)
			Eventually(func() bool {
				q, err := rmqc.GetQueue("/", "declare-without-consumers")
				return err == nil && q.Name == "declare-without-consumers"
			}).WithTimeout(3 * time.Second).Should(BeTrue())

			session.Signal(os.Signal(os.Interrupt))

			// eventually the queue should be deleted
			Eventually(func() bool {
				_, err := rmqc.GetQueue("/", "declare-without-consumers")
				return err != nil
			}).WithTimeout(3 * time.Second).Should(BeTrue())
		})

		It("declares queues for STOMP publishers and consumers with /amq/queue/... addresses", func() {
			args := []string{
				"stomp",
				"-r", "1",
				"-t", "/amq/queue/stomp-declare-for-publisher",
				"-T", "/amq/queue/stomp-declare-for-consumer",
				"--queues", "quorum",
				"--cleanup-queues=true",
				"--time", "10s",
			}

			rmqc, err := rabbithole.NewClient("http://127.0.0.1:15672", "guest", "guest")
			Expect(err).ShouldNot(HaveOccurred())
			session := omq(args)
			Eventually(func() bool {
				q1, err1 := rmqc.GetQueue("/", "stomp-declare-for-consumer")
				q2, err2 := rmqc.GetQueue("/", "stomp-declare-for-publisher")
				return err1 == nil && q1.Name == "stomp-declare-for-consumer" &&
					err2 == nil && q2.Name == "stomp-declare-for-publisher"
			}).WithTimeout(3 * time.Second).Should(BeTrue())

			session.Signal(os.Signal(os.Interrupt))

			// eventually the queue should be deleted
			Eventually(func() bool {
				_, err1 := rmqc.GetQueue("/", "stomp-declare-for-consumer")
				_, err2 := rmqc.GetQueue("/", "stomp-declare-for-publisher")
				return err1 != nil && strings.Contains(err1.Error(), "Object Not Found") &&
					err2 != nil && strings.Contains(err2.Error(), "Object Not Found")
			}).WithTimeout(3 * time.Second).Should(BeTrue())
		})
	})

	Describe("exposes command line flags as a omq_args metric", func() {
		It("should print omq_args", func() {
			args := []string{
				"amqp",
				"-t", "/queues/omq-args",
				"-T", "/queues/omq-args",
				"-C", "0",
				"-D", "0",
				"--queues", "classic",
				"--cleanup-queues=true",
				"--print-all-metrics",
			}
			session := omq(args)
			Eventually(session).WithTimeout(3 * time.Second).Should(gexec.Exit(0))
			Eventually(session.Out).Should(gbytes.Say(`omq_args{command_line="amqp -t /queues/omq-args -T /queues/omq-args -C 0 -D 0 --queues classic"} 1`))
		})
	})
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
