package cmd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rabbitmq/omq/pkg/common"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/mgmt"
	"github.com/rabbitmq/omq/pkg/utils"
	"github.com/rabbitmq/omq/pkg/version"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/thediveo/enumflag/v2"

	"github.com/hashicorp/memberlist"
)

var cfg config.Config
var (
	amqp_amqp       = &cobra.Command{}
	amqp_amqp091    = &cobra.Command{}
	amqp_stomp      = &cobra.Command{}
	amqp_mqtt       = &cobra.Command{}
	stomp_stomp     = &cobra.Command{}
	stomp_amqp      = &cobra.Command{}
	stomp_amqp091   = &cobra.Command{}
	stomp_mqtt      = &cobra.Command{}
	mqtt_mqtt       = &cobra.Command{}
	mqtt_amqp       = &cobra.Command{}
	mqtt_amqp091    = &cobra.Command{}
	mqtt_stomp      = &cobra.Command{}
	amqp091_amqp091 = &cobra.Command{}
	amqp091_amqp    = &cobra.Command{}
	amqp091_mqtt    = &cobra.Command{}
	amqp091_stomp   = &cobra.Command{}
	versionCmd      = &cobra.Command{}
)

var (
	metricTags             []string
	amqpAppProperties      []string
	amqpAppPropertyFilters []string
	amqpPropertyFilters    []string
	streamOffset           string
)

var (
	rmqMgmt       *mgmt.Mgmt
	metricsServer *metrics.MetricsServer
)

func Execute() {
	rootCmd := RootCmd()
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func RootCmd() *cobra.Command {
	cfg = config.NewConfig()

	mqttConsumerFlags := pflag.NewFlagSet("mqtt-consumer", pflag.ContinueOnError)

	mqttConsumerFlags.IntVar(&cfg.MqttConsumer.Version, "mqtt-consumer-version", 5,
		"MQTT consumer protocol version (3, 4 or 5; default=5)")
	mqttConsumerFlags.IntVar(&cfg.MqttConsumer.QoS, "mqtt-consumer-qos", 0,
		"MQTT consumer QoS level (0, 1 or 2; default=0)")
	mqttConsumerFlags.BoolVar(&cfg.MqttConsumer.CleanSession, "mqtt-consumer-clean-session", true,
		"MQTT consumer clean session")
	mqttConsumerFlags.DurationVar(&cfg.MqttConsumer.SessionExpiryInterval, "mqtt-consumer-session-expiry-interval", 0,
		"MQTT consumer session expiry interval")

	mqttPublisherFlags := pflag.NewFlagSet("mqtt-publisher", pflag.ContinueOnError)
	mqttPublisherFlags.IntVar(&cfg.MqttPublisher.Version, "mqtt-publisher-version", 5,
		"MQTT consumer protocol version (3, 4 or 5; default=5)")
	mqttPublisherFlags.IntVar(&cfg.MqttPublisher.QoS, "mqtt-publisher-qos", 0,
		"MQTT publisher QoS level (0, 1 or 2; default=0)")
	mqttPublisherFlags.BoolVar(&cfg.MqttPublisher.CleanSession, "mqtt-publisher-clean-session", true,
		"MQTT publisher clean session")
	mqttPublisherFlags.DurationVar(&cfg.MqttPublisher.SessionExpiryInterval, "mqtt-publisher-session-expiry-interval", 0,
		"MQTT publisher session expiry interval")

	amqpPublisherFlags := pflag.NewFlagSet("amqp-publisher", pflag.ContinueOnError)

	amqpPublisherFlags.StringArrayVar(&amqpAppProperties, "amqp-app-property", []string{},
		"AMQP application properties, eg. key1=val1,val2")
	amqpPublisherFlags.StringSliceVar(&cfg.Amqp.Subjects, "amqp-subject", []string{},
		"AMQP 1.0 message subject(s)")
	amqpPublisherFlags.StringSliceVar(&cfg.Amqp.To, "amqp-to", []string{},
		"AMQP 1.0 message To field (required for the anonymous terminus)")
	amqpPublisherFlags.BoolVar(&cfg.Amqp.SendSettled, "amqp-send-settled", false,
		"Send settled messages (fire and forget)")

	amqpConsumerFlags := pflag.NewFlagSet("amqp-consumer", pflag.ContinueOnError)

	amqpConsumerFlags.IntVar(&cfg.Amqp.RejectRate, "amqp-reject-rate", 0,
		"Rate of messages to reject (0-100%)")
	amqpConsumerFlags.IntVar(&cfg.Amqp.ReleaseRate, "amqp-release-rate", 0,
		"Rate of messages to release without accepting (0-100%)")
	amqpConsumerFlags.StringArrayVar(&amqpAppPropertyFilters, "amqp-app-property-filter", []string{},
		"AMQP application property filters, eg. key1=&p:prefix")
	amqpConsumerFlags.StringArrayVar(&amqpPropertyFilters, "amqp-property-filter", []string{},
		"AMQP property filters, eg. key1=&p:prefix")

	amqp091PublisherFlags := pflag.NewFlagSet("amqp091-publisher", pflag.ContinueOnError)
	amqp091ConsumerFlags := pflag.NewFlagSet("amqp091-consumer", pflag.ContinueOnError)

	amqp_amqp = &cobra.Command{
		Use:     "amqp-amqp",
		Aliases: []string{"amqp"},
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.AMQP
			cfg.ConsumerProto = config.AMQP
			start(cfg)
		},
	}
	amqp_amqp.Flags().AddFlagSet(amqpPublisherFlags)
	amqp_amqp.Flags().AddFlagSet(amqpConsumerFlags)

	amqp_amqp091 = &cobra.Command{
		Use: "amqp-amqp091",
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.AMQP
			cfg.ConsumerProto = config.AMQP091
			start(cfg)
		},
	}
	amqp_amqp.Flags().AddFlagSet(amqpPublisherFlags)
	amqp_amqp.Flags().AddFlagSet(amqp091ConsumerFlags)

	amqp_stomp = &cobra.Command{
		Use: "amqp-stomp",
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.AMQP
			cfg.ConsumerProto = config.STOMP
			start(cfg)
		},
	}
	amqp_stomp.Flags().AddFlagSet(amqpPublisherFlags)

	amqp_mqtt = &cobra.Command{
		Use: "amqp-mqtt",
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.AMQP
			cfg.ConsumerProto = config.MQTT
			start(cfg)
		},
	}
	amqp_mqtt.Flags().AddFlagSet(amqpPublisherFlags)
	amqp_mqtt.Flags().AddFlagSet(mqttConsumerFlags)

	stomp_stomp = &cobra.Command{
		Use:     "stomp-stomp",
		Aliases: []string{"stomp"},
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.STOMP
			cfg.ConsumerProto = config.STOMP
			start(cfg)
		},
	}

	stomp_amqp = &cobra.Command{
		Use: "stomp-amqp",
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.STOMP
			cfg.ConsumerProto = config.AMQP
			start(cfg)
		},
	}
	stomp_amqp.Flags().AddFlagSet(amqpConsumerFlags)

	stomp_amqp091 = &cobra.Command{
		Use: "stomp-amqp091",
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.STOMP
			cfg.ConsumerProto = config.AMQP091
			start(cfg)
		},
	}
	stomp_amqp.Flags().AddFlagSet(amqp091ConsumerFlags)

	stomp_mqtt = &cobra.Command{
		Use: "stomp-mqtt",
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.STOMP
			cfg.ConsumerProto = config.MQTT
			start(cfg)
		},
	}
	stomp_mqtt.Flags().AddFlagSet(mqttConsumerFlags)

	mqtt_mqtt = &cobra.Command{
		Use:     "mqtt-mqtt",
		Aliases: []string{"mqtt"},
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.MQTT
			cfg.ConsumerProto = config.MQTT
			start(cfg)
		},
	}
	mqtt_mqtt.Flags().AddFlagSet(mqttPublisherFlags)
	mqtt_mqtt.Flags().AddFlagSet(mqttConsumerFlags)

	mqtt_amqp = &cobra.Command{
		Use: "mqtt-amqp",
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.MQTT
			cfg.ConsumerProto = config.AMQP
			start(cfg)
		},
	}
	mqtt_amqp.Flags().AddFlagSet(mqttPublisherFlags)
	mqtt_amqp.Flags().AddFlagSet(amqpConsumerFlags)

	mqtt_amqp091 = &cobra.Command{
		Use: "mqtt-amqp091",
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.MQTT
			cfg.ConsumerProto = config.AMQP091
			start(cfg)
		},
	}
	mqtt_amqp.Flags().AddFlagSet(mqttPublisherFlags)
	mqtt_amqp.Flags().AddFlagSet(amqp091ConsumerFlags)

	mqtt_stomp = &cobra.Command{
		Use: "mqtt-stomp",
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.MQTT
			cfg.ConsumerProto = config.STOMP
			start(cfg)
		},
	}
	mqtt_stomp.Flags().AddFlagSet(mqttPublisherFlags)

	amqp091_amqp091 = &cobra.Command{
		Use:     "amqp091-amqp091",
		Aliases: []string{"amqp091"},
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.AMQP091
			cfg.ConsumerProto = config.AMQP091
			start(cfg)
		},
	}
	amqp091_amqp091.Flags().AddFlagSet(amqp091PublisherFlags)
	amqp091_amqp091.Flags().AddFlagSet(amqp091ConsumerFlags)

	amqp091_amqp = &cobra.Command{
		Use: "amqp091-amqp",
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.AMQP091
			cfg.ConsumerProto = config.AMQP
			start(cfg)
		},
	}
	amqp091_amqp.Flags().AddFlagSet(amqp091PublisherFlags)
	amqp091_amqp.Flags().AddFlagSet(amqpConsumerFlags)

	amqp091_mqtt = &cobra.Command{
		Use: "amqp091-mqtt",
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.AMQP091
			cfg.ConsumerProto = config.MQTT
			start(cfg)
		},
	}
	amqp091_mqtt.Flags().AddFlagSet(amqp091PublisherFlags)

	amqp091_stomp = &cobra.Command{
		Use: "amqp091-stomp",
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.AMQP091
			cfg.ConsumerProto = config.STOMP
			start(cfg)
		},
	}
	amqp091_stomp.Flags().AddFlagSet(amqp091PublisherFlags)

	versionCmd = &cobra.Command{
		Use: "version",
		Run: func(cmd *cobra.Command, args []string) {
			version.Print()
			os.Exit(0)
		},
	}

	rootCmd := &cobra.Command{
		Use: "omq",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			log.Setup()

			err := sanitizeConfig(&cfg)
			if err != nil {
				fmt.Printf("ERROR: %s\n", err)
				os.Exit(1)
			}

			if cmd.Use != "version" {
				err := setUris(&cfg, cmd.Use)
				if err != nil {
					fmt.Printf("ERROR: %s\n", err)
					os.Exit(1)
				}
			}
		},
	}

	rootCmd.PersistentFlags().StringSliceVarP(&cfg.Uri, "uri", "", nil,
		"URI for both publishers and consumers")
	rootCmd.PersistentFlags().StringSliceVarP(&cfg.PublisherUri, "publisher-uri", "", nil,
		"URI for publishing")
	rootCmd.PersistentFlags().StringSliceVarP(&cfg.ConsumerUri, "consumer-uri", "", nil,
		"URI for consuming")
	rootCmd.PersistentFlags().StringSliceVarP(&cfg.ManagementUri, "management-uri", "", nil,
		"URI for declaring queues (must be AMQP)")
	rootCmd.PersistentFlags().BoolVar(&cfg.SpreadConnections, "spread-connections", true,
		"Spread connections across URIs")

	rootCmd.PersistentFlags().IntVarP(&cfg.Publishers, "publishers", "x", 1,
		"The number of publishers to start")
	rootCmd.PersistentFlags().IntVarP(&cfg.PublishCount, "pmessages", "C", math.MaxInt,
		"The number of messages to send per publisher")
	rootCmd.PersistentFlags().StringVarP(&cfg.PublishTo, "publish-to", "t", "/queues/omq-%d",
		"The topic/terminus to publish to (%d will be replaced with the publisher's id)")
	rootCmd.PersistentFlags().StringVar(&cfg.PublisherId, "publisher-id", "omq-publisher-%d",
		"Client ID for AMQP and MQTT publishers (%d => consumer's id, %r => random)")

	rootCmd.PersistentFlags().IntVarP(&cfg.Consumers, "consumers", "y", 1,
		"The number of consumers to start")
	rootCmd.PersistentFlags().IntVarP(&cfg.ConsumeCount, "cmessages", "D", math.MaxInt,
		"The number of messages to consume per consumer (default=MaxInt)")
	rootCmd.PersistentFlags().StringVarP(&cfg.ConsumeFrom, "consume-from", "T", "/queues/omq-%d",
		"The queue/topic/terminus to consume from (%d will be replaced with the consumer's id)")
	rootCmd.PersistentFlags().StringVar(&cfg.ConsumerId, "consumer-id", "omq-consumer-%d",
		"Client ID for AMQP and MQTT consumers (%d => consumer's id, %r => random)")
	rootCmd.PersistentFlags().StringVar(&streamOffset, "stream-offset", "",
		"Stream consumer offset specification (default=next)")
	rootCmd.PersistentFlags().Int32Var(&cfg.ConsumerPriority, "consumer-priority", 0, "Consumer priority")
	rootCmd.PersistentFlags().IntVar(&cfg.ConsumerCredits, "consumer-credits", 1,
		"AMQP-1.0 consumer credits / STOMP prefetch count")
	rootCmd.PersistentFlags().DurationVarP(&cfg.ConsumerLatency, "consumer-latency", "L", 0,
		"consumer latency (time to accept message)")
	rootCmd.PersistentFlags().BoolVar(&cfg.LogOutOfOrder, "log-out-of-order-messages", false,
		"Print a log line when a message is received that is older than the previously received message")
	rootCmd.PersistentFlags().DurationVar(&cfg.ConsumerStartupDelay, "consumer-startup-delay", 0,
		"Delay consumer startup to allow a backlog of messages to build up (eg. 10s)")

	rootCmd.PersistentFlags().VarP(enumflag.New(&cfg.Queues, "queues", config.QueueTypes, enumflag.EnumCaseInsensitive), "queues", "",
		"Type of queues to declare (or `predeclared` to use existing queues)")
	rootCmd.PersistentFlags().BoolVar(&cfg.CleanupQueues, "cleanup-queues", false,
		"Delete the queues at the end (omq only deletes the queues it explicitly declared)")
	rootCmd.PersistentFlags().VarP(enumflag.New(&cfg.QueueDurability, "queue-durability", config.AmqpDurabilityModes, enumflag.EnumCaseInsensitive), "queue-durability", "",
		"Queue durability (default: configuration - the queue definition is durable)")
	rootCmd.PersistentFlags().StringVar(&cfg.BindingKey, "binding-key", "",
		"Binding key for queue declarations")
	rootCmd.PersistentFlags().StringVar(&cfg.Exchange, "exchange", "",
		"Exchange for binding declarations")

	// messages
	rootCmd.PersistentFlags().IntVarP(&cfg.Size, "size", "s", 12, "Message payload size in bytes")
	rootCmd.PersistentFlags().Float32VarP(&cfg.Rate, "rate", "r", -1, "Messages per second (-1 = unlimited)")
	rootCmd.PersistentFlags().IntVarP(&cfg.MaxInFlight, "max-in-flight", "c", 1, "Maximum number of in-flight messages per publisher")
	rootCmd.PersistentFlags().BoolVarP(&cfg.MessageDurability, "message-durability", "d", true, "Mark messages as durable")
	rootCmd.PersistentFlags().StringVar(&cfg.MessagePriority, "message-priority", "", "Message priority (0-255, default=unset)")
	rootCmd.PersistentFlags().DurationVar(&cfg.MessageTTL, "message-ttl", 0, "Message TTL (not set by default)")

	rootCmd.PersistentFlags().StringSliceVar(&metricTags, "metric-tags", []string{},
		"Prometheus label-value pairs, eg. l1=v1,l2=v2")
	rootCmd.PersistentFlags().VarP(enumflag.New(&log.Level, "log-level", log.Levels, enumflag.EnumCaseInsensitive), "log-level", "l",
		"Log level (debug, info, error)")
	rootCmd.PersistentFlags().BoolVar(&cfg.PrintAllMetrics, "print-all-metrics", false,
		"Print all metrics before exiting")
	rootCmd.PersistentFlags().BoolVarP(&cfg.UseMillis, "use-millis", "m", false,
		"Use milliseconds for timestamps (automatically enabled when no publishers or no consumers)")
	rootCmd.PersistentFlags().DurationVarP(&cfg.Duration, "time", "z", 0,
		"Run duration (eg. 10s, 5m, 2h)")

	// instance synchronization
	rootCmd.PersistentFlags().IntVar(&cfg.ExpectedInstances, "expected-instances", 1,
		"The number of instances to synchronize")
	rootCmd.PersistentFlags().StringVar(&cfg.SyncName, "expected-instances-endpoint", "",
		"The DNS name that will return members to synchronize with")

	rootCmd.AddCommand(amqp_amqp)
	rootCmd.AddCommand(amqp_amqp091)
	rootCmd.AddCommand(amqp_stomp)
	rootCmd.AddCommand(amqp_mqtt)
	rootCmd.AddCommand(stomp_stomp)
	rootCmd.AddCommand(stomp_amqp)
	rootCmd.AddCommand(stomp_amqp091)
	rootCmd.AddCommand(stomp_mqtt)
	rootCmd.AddCommand(mqtt_mqtt)
	rootCmd.AddCommand(mqtt_amqp)
	rootCmd.AddCommand(mqtt_amqp091)
	rootCmd.AddCommand(mqtt_stomp)
	rootCmd.AddCommand(amqp091_amqp091)
	rootCmd.AddCommand(amqp091_amqp)
	rootCmd.AddCommand(amqp091_mqtt)
	rootCmd.AddCommand(amqp091_stomp)
	rootCmd.AddCommand(versionCmd)

	return rootCmd
}

func start(cfg config.Config) {
	// we can't do this in sanitizeConfig because we need to know
	// the publisher and consumer protocols and these are set later on
	if cfg.ConsumerLatency != 0 && cfg.ConsumerProto == config.MQTT {
		fmt.Println("Consumer latency is not supported for MQTT consumers")
		os.Exit(1)
	}

	if cfg.MaxInFlight > 1 && cfg.PublisherProto != config.AMQP {
		fmt.Println("max-in-flight > 1 is only supported for AMQP publishers")
		os.Exit(1)
	}

	if cfg.ExpectedInstances > 1 {
		joinCluster(cfg.ExpectedInstances, cfg.SyncName)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// handle ^C
	handleInterupt(ctx, cancel)

	metricsServer = metrics.Start(ctx, cfg)
	defer metricsServer.Stop()

	rmqMgmt = mgmt.Start(ctx, cfg.ManagementUri, cfg.CleanupQueues)
	defer rmqMgmt.Stop()

	var wg sync.WaitGroup

	if cfg.Queues != config.Predeclared {
		rmqMgmt.DeclareQueues(cfg)
	}
	if cfg.ConsumerStartupDelay == 0 {
		startConsumers(ctx, &wg)
	} else {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case <-time.After(cfg.ConsumerStartupDelay):
				startConsumers(ctx, &wg)
			}
		}()
	}

	startPublishing := make(chan bool)
	startPublishers(ctx, &wg, startPublishing)
	metricsServer.StartTime(time.Now())
	close(startPublishing)

	if cfg.Duration > 0 {
		time.AfterFunc(cfg.Duration, func() { cancel() })
		log.Debug("will stop all consumers and publishers at " + time.Now().Add(cfg.Duration).String())
	}

	// every  second, print the current values of the metrics
	wg.Wait()
}

func joinCluster(expectedInstance int, serviceName string) {
	if expectedInstance == 1 {
		return
	}

	if serviceName == "" {
		log.Error("when --expected-instances is set, --expected-instances-endpoint must be set")
		os.Exit(1)
	}

	var ips []string
	var err error
	for {
		// wait until endpoints returns the expected number of instances
		log.Info("getting endpoints", "name", serviceName)
		ips, err = utils.GetEndpoints(serviceName)
		if err != nil {
			log.Error("failed to retrieve endpoints; retrying...", "name", serviceName, "error", err)
			time.Sleep(time.Second)
			continue
		}
		if len(ips) >= expectedInstance {
			log.Info("reached the expected number of instances", "expected instances", expectedInstance, "current instances", len(ips))
			break
		}
		log.Info("waiting for the expected number of IPs to be returned from the endpoint", "exepcted", expectedInstance, "current", len(ips))
		time.Sleep(time.Second)
	}

	sort.Strings(ips)

	list, err := memberlist.Create(memberlist.DefaultLANConfig())
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	log.Info("joining all IPs found", "IPs", ips)

	time.Sleep(time.Duration(1000+rand.IntN(1000)) * time.Millisecond)

	// join the cluster
	for {
		var n int
		n, err = list.Join(ips)
		if err == nil {
			log.Info("successfully joined a cluster", "cluster size", n)
			break
		}
		log.Info("failed to join cluster; retrying...", "error", err)
		time.Sleep(time.Second)
	}

	// wait until the expected number of members is reached
	for {
		members := list.Members()
		if len(members) >= expectedInstance {
			log.Info("reached the expected number of instances", "expected instances", expectedInstance, "current cluster size", len(members))
			break
		}
		log.Info("waiting for more instances to join the cluster", "expected instances", expectedInstance, "current cluster size", len(members))
		time.Sleep(time.Second)
	}

	go func() {
		time.Sleep(30 * time.Second)
		log.Info("leaving the cluster")
		_ = list.Leave(time.Second)
		_ = list.Shutdown()
	}()
}

func startConsumers(ctx context.Context, wg *sync.WaitGroup) {
	for i := 1; i <= cfg.Consumers; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			consumerReady := make(chan bool)
			n := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, err := common.NewConsumer(ctx, cfg.ConsumerProto, cfg, n)
				if err != nil {
					log.Error("Error creating consumer: ", "error", err)
					os.Exit(1)
				}
				c.Start(consumerReady)
			}()
			// consumers are started one by one and synchronously
			<-consumerReady
		}
	}
}

func startPublishers(ctx context.Context, wg *sync.WaitGroup, startPublishing chan bool) {
	for i := 1; i <= cfg.Publishers; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			publisherReady := make(chan bool)
			n := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				p, err := common.NewPublisher(ctx, cfg, n)
				if err != nil {
					log.Error("Error creating publisher: ", "error", err)
					os.Exit(1)
				}
				// publisher are started one by one and synchronously
				// then we close the channel to allow all of them to start publishing "at once"
				// but each publisher sleeps for a random sub-second time, to avoid burts of
				// messages being published
				p.Start(publisherReady, startPublishing)
			}()
			<-publisherReady
		}
	}
}

func setUris(cfg *config.Config, command string) error {
	var managementUri []string
	if cfg.ManagementUri != nil {
		managementUri = cfg.ManagementUri
	} else {
		if cfg.Uri != nil && strings.HasPrefix(cfg.Uri[0], "amqp") {
			managementUri = cfg.Uri
		} else if cfg.PublisherUri == nil && cfg.ConsumerUri == nil {
			managementUri = []string{defaultUri("amqp")}
		} else {
			if cfg.Queues != config.Predeclared {
				return errors.New("Not sure where to connect to declare the queues; please set --management-uri")
			}
		}
	}
	log.Debug("setting management uri", "uri", managementUri)
	cfg.ManagementUri = managementUri

	if cfg.Uri != nil {
		if cfg.PublisherUri != nil || cfg.ConsumerUri != nil {
			fmt.Println("ERROR: can't specify both --uri and --publisher-uri/--consumer-uri")
			os.Exit(1)
		}
		cfg.PublisherUri = cfg.Uri
		cfg.ConsumerUri = cfg.Uri
	}

	if cfg.PublisherUri == nil {
		log.Debug("setting default publisher uri", "uri", defaultUri(strings.Split(command, "-")[0]))
		cfg.PublisherUri = []string{defaultUri(strings.Split(command, "-")[0])}
	}
	if cfg.ConsumerUri == nil {
		log.Debug("setting default consumer uri", "uri", defaultUri(strings.Split(command, "-")[1]))
		cfg.ConsumerUri = []string{defaultUri(strings.Split(command, "-")[1])}
	}
	return nil
}

func defaultUri(proto string) string {
	uri := "localhost"
	switch proto {
	case "amqp":
		uri = "amqp://localhost/"
	case "amqp091":
		uri = "amqp://localhost/"
	case "stomp":
		uri = "stomp://localhost:61613"
	case "mqtt":
		uri = "mqtt://localhost:1883"
	}
	return uri
}

func sanitizeConfig(cfg *config.Config) error {
	if cfg.Size < 12 {
		return fmt.Errorf("size can't be less than 12 bytes")
	}

	if cfg.Amqp.ReleaseRate > 100 {
		return fmt.Errorf("release rate can't be more than 100%%")
	}

	if cfg.Amqp.RejectRate > 100 {
		return fmt.Errorf("reject rate can't be more than 100%%")
	}

	if cfg.Amqp.ReleaseRate+cfg.Amqp.RejectRate > 100 {
		return fmt.Errorf("combined release and reject rate can't be more than 100%%")
	}

	if cfg.MaxInFlight < 1 {
		return fmt.Errorf("max-in-flight must be at least 1")
	}

	// go-amqp treats `0` as if the value was not set and uses 1 credit
	// this is confusing, so here we treat 0 as -1 (which go-amqp trets as 0...)
	if cfg.ConsumerCredits == 0 {
		cfg.ConsumerCredits = -1
	}
	// nanoseconds shouldn't be used across processes
	if cfg.Publishers == 0 || cfg.Consumers == 0 {
		cfg.UseMillis = true
	}

	if cfg.MessagePriority != "" {
		_, err := strconv.ParseUint(cfg.MessagePriority, 10, 8)
		if err != nil {
			return fmt.Errorf("invalid message priority: %s", cfg.MessagePriority)
		}
	}

	offset, err := parseStreamOffset(streamOffset)
	if err != nil {
		return fmt.Errorf("invalid stream offset value")
	}
	cfg.StreamOffset = offset

	// AMQP application properties
	cfg.Amqp.AppProperties = make(map[string][]string)
	for _, val := range amqpAppProperties {
		parts := strings.Split(val, "=")
		if len(parts) != 2 {
			return fmt.Errorf("invalid AMQP application property: %s, use key=v1,v2 format", val)
		}
		cfg.Amqp.AppProperties[parts[0]] = strings.Split(parts[1], ",")
	}

	// AMQP application property filters
	cfg.Amqp.AppPropertyFilters = make(map[string]string)
	for _, filter := range amqpAppPropertyFilters {
		parts := strings.SplitN(filter, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid AMQP application property filter: %s, use key=filterExpression format", filter)
		}
		cfg.Amqp.AppPropertyFilters[parts[0]] = parts[1]
	}

	// AMQP property filters
	cfg.Amqp.PropertyFilters = make(map[string]string)
	for _, filter := range amqpPropertyFilters {
		parts := strings.SplitN(filter, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid AMQP property filter: %s, use key=filterExpression format", filter)
		}
		cfg.Amqp.PropertyFilters[parts[0]] = parts[1]
	}

	// split metric tags into key-value pairs
	cfg.MetricTags = make(map[string]string)
	for _, tag := range metricTags {
		parts := strings.Split(tag, "=")
		if len(parts) != 2 {
			return fmt.Errorf("invalid metric tags: %s, use label=value format", tag)
		}
		cfg.MetricTags[parts[0]] = parts[1]
	}

	return nil
}

func parseStreamOffset(offset string) (any, error) {
	switch offset {
	case "":
		return "", nil
	case "next", "first", "last":
		return offset, nil
	default:
		// check if streamOffset can be parsed as unsigned integer (chunkID)
		if chunkID, err := strconv.ParseInt(offset, 10, 64); err == nil {
			return chunkID, nil
		}
		// check if streamOffset can be parsed as an ISO 8601 timestamp
		if timestamp, err := time.Parse(time.RFC3339, offset); err == nil {
			return timestamp, nil
		}
	}
	// return "", fmt.Errorf("invalid stream offset: %s", offset)
	return offset, nil //, fmt.Errorf("invalid stream offset: %s", offset)
}

func handleInterupt(ctx context.Context, cancel context.CancelFunc) {
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		select {
		case <-c:
			cancel()
			log.Print("Received SIGTERM, shutting down...")
			// PersistentPostRun does all the cleanup
			// this is just just a backup mechanism
			time.Sleep(5 * time.Second)
			os.Exit(0)
		case <-ctx.Done():
			return
		}
	}()
}
