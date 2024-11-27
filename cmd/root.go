package cmd

import (
	"context"
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

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/rabbitmq/omq/pkg/common"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/mgmt"
	"github.com/rabbitmq/omq/pkg/version"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/thediveo/enumflag/v2"

	"github.com/hashicorp/memberlist"
)

var cfg config.Config
var (
	amqp_amqp   = &cobra.Command{}
	amqp_stomp  = &cobra.Command{}
	amqp_mqtt   = &cobra.Command{}
	stomp_stomp = &cobra.Command{}
	stomp_amqp  = &cobra.Command{}
	stomp_mqtt  = &cobra.Command{}
	mqtt_mqtt   = &cobra.Command{}
	mqtt_amqp   = &cobra.Command{}
	mqtt_stomp  = &cobra.Command{}
	versionCmd  = &cobra.Command{}
)

var metricTags []string
var amqpAppProperties []string
var amqpAppPropertyFilters []string
var amqpPropertyFilters []string

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

	mqttPublisherFlags := pflag.NewFlagSet("mqtt-publisher", pflag.ContinueOnError)
	mqttPublisherFlags.IntVar(&cfg.MqttPublisher.Version, "mqtt-publisher-version", 5,
		"MQTT consumer protocol version (3, 4 or 5; default=5)")
	mqttPublisherFlags.IntVar(&cfg.MqttPublisher.QoS, "mqtt-publisher-qos", 0,
		"MQTT publisher QoS level (0, 1 or 2; default=0)")
	mqttPublisherFlags.BoolVar(&cfg.MqttPublisher.CleanSession, "mqtt-publisher-clean-session", true,
		"MQTT publisher clean session")

	amqpPublisherFlags := pflag.NewFlagSet("amqp-publisher", pflag.ContinueOnError)

	amqpPublisherFlags.StringArrayVar(&amqpAppProperties, "amqp-app-property", []string{},
		"AMQP application properties, eg. key1=val1,val2")
	amqpPublisherFlags.StringSliceVar(&cfg.Amqp.Subjects, "amqp-subject", []string{},
		"AMQP 1.0 message subject(s)")
	amqpPublisherFlags.BoolVar(&cfg.Amqp.SendSettled, "amqp-send-settled", false,
		"Send settled messages (fire and forget)")

	amqpConsumerFlags := pflag.NewFlagSet("amqp-consumer", pflag.ContinueOnError)

	amqpConsumerFlags.IntVar(&cfg.Amqp.RejectRate, "amqp-reject-rate", 0,
		"Rate of messages to reject (0-100%)")
	amqpConsumerFlags.IntVar(&cfg.Amqp.ReleaseRate, "amqp-release-rate", 0,
		"Rate of messages to release without accepting (0-100%)")
	amqpConsumerFlags.StringArrayVar(&amqpAppPropertyFilters, "amqp-app-property-filter", []string{},
		"AMQP application property filters, eg. key1=$p:prefix")
	amqpConsumerFlags.StringArrayVar(&amqpPropertyFilters, "amqp-property-filter", []string{},
		"AMQP property filters, eg. key1=$p:prefix")

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

	mqtt_stomp = &cobra.Command{
		Use: "mqtt-stomp",
		Run: func(cmd *cobra.Command, args []string) {
			cfg.PublisherProto = config.MQTT
			cfg.ConsumerProto = config.STOMP
			start(cfg)
		},
	}
	mqtt_stomp.Flags().AddFlagSet(mqttPublisherFlags)

	versionCmd = &cobra.Command{
		Use: "version",
		Run: func(cmd *cobra.Command, args []string) {
			version.Print()
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
				setUris(&cfg, cmd.Use)
			}
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			shutdown(cfg.CleanupQueues, cfg.PrintAllMetrics)
		},
	}

	rootCmd.PersistentFlags().StringSliceVarP(&cfg.Uri, "uri", "", nil,
		"URI for both publishers and consumers")
	rootCmd.PersistentFlags().StringSliceVarP(&cfg.PublisherUri, "publisher-uri", "", nil,
		"URI for publishing")
	rootCmd.PersistentFlags().StringSliceVarP(&cfg.ConsumerUri, "consumer-uri", "", nil,
		"URI for consuming")
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
	rootCmd.PersistentFlags().StringVar(&cfg.StreamOffset, "stream-offset", "",
		"Stream consumer offset specification (default=next)")
	rootCmd.PersistentFlags().Int32Var(&cfg.ConsumerPriority, "consumer-priority", 0, "Consumer priority")
	rootCmd.PersistentFlags().IntVar(&cfg.ConsumerCredits, "consumer-credits", 1,
		"AMQP-1.0 consumer credits / STOMP prefetch count")
	rootCmd.PersistentFlags().DurationVarP(&cfg.ConsumerLatency, "consumer-latency", "L", 0*time.Second,
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
	rootCmd.AddCommand(amqp_stomp)
	rootCmd.AddCommand(amqp_mqtt)
	rootCmd.AddCommand(stomp_stomp)
	rootCmd.AddCommand(stomp_amqp)
	rootCmd.AddCommand(stomp_mqtt)
	rootCmd.AddCommand(mqtt_mqtt)
	rootCmd.AddCommand(mqtt_amqp)
	rootCmd.AddCommand(mqtt_stomp)
	rootCmd.AddCommand(versionCmd)

	return rootCmd
}

func start(cfg config.Config) {
	if cfg.ConsumerLatency != 0 && cfg.ConsumerProto == config.MQTT {
		fmt.Printf("Consumer latency is not supported for MQTT consumers")
		os.Exit(1)
	}

	if cfg.MaxInFlight > 1 && cfg.PublisherProto != config.AMQP {
		fmt.Printf("max-in-flight > 1 is only supported for AMQP publishers")
		os.Exit(1)
	}

	join_cluster(cfg.ExpectedInstances, cfg.SyncName)

	start_metrics(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// handle ^C
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		select {
		case <-c:
			cancel()
			log.Print("Received SIGTERM, shutting down...")
			// PersistentPostRun does all the cleanup
			// this is just just a backup mechanism
			time.Sleep(30 * time.Second)
			os.Exit(0)
		case <-ctx.Done():
			return
		}
	}()

	var wg sync.WaitGroup
	wg.Add(cfg.Publishers + cfg.Consumers)

	// TODO
	// refactor; make consumer startup delay more accurate
	// clarfiy when queues are declared

	mgmt.DeclareQueues(cfg)
	// if --consumer-startup-delay is not set, we want to start
	// all the consumers before we start any publishers
	if cfg.ConsumerStartupDelay == 0 {
		startConsumers(ctx, cfg.ConsumerProto, &wg)
	} else {
		go func() {
			time.Sleep(cfg.ConsumerStartupDelay)
			startConsumers(ctx, cfg.ConsumerProto, &wg)
		}()
	}

	if cfg.Publishers > 0 {
		for i := 1; i <= cfg.Publishers; i++ {
			n := i
			go func() {
				defer wg.Done()
				p, err := common.NewPublisher(cfg.PublisherProto, cfg, n)
				if err != nil {
					log.Error("Error creating publisher: ", "error", err)
					os.Exit(1)
				}
				p.Start(ctx)
			}()
		}
	}

	if cfg.Duration > 0 {
		log.Debug("will stop all consumers and publishers at " + time.Now().Add(cfg.Duration).String())
		time.AfterFunc(cfg.Duration, func() { cancel() })
	}

	// every  second, print the current values of the metrics
	m := metrics.GetMetricsServer()
	m.PrintMessageRates(ctx)
	wg.Wait()
}

func join_cluster(expectedInstance int, serviceName string) {
	if expectedInstance == 1 {
		return
	}

	if serviceName == "" {
		log.Error("when --expected-instances is set, --expected-instances-endpoint must be set")
		os.Exit(1)
	}

	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	namespace := os.Getenv("MY_POD_NAMESPACE")
	var endpoints *v1.Endpoints
	var nodeCount int
	for {
		// wait until endpoints returns the expected number of instances
		log.Info("getting endpoints", "name", serviceName)
		endpoints, err = clientset.CoreV1().Endpoints(namespace).Get(context.TODO(), serviceName, metav1.GetOptions{})
		if err != nil || len(endpoints.Subsets) == 0 {
			log.Error("failed to retrieve endpoints; retrying...", "name", serviceName, "error", err)
			time.Sleep(time.Second)
			continue
		}
		nodeCount = len(endpoints.Subsets[0].Addresses)
		if nodeCount >= expectedInstance {
			log.Info("reached the expected number of instances", "expected instances", expectedInstance, "current instances", nodeCount)
			break
		}
		log.Info("waiting for the expected number of IPs to be returned from the endpoint", "exepcted", expectedInstance, "current", nodeCount)
		time.Sleep(time.Second)
	}

	ips := make([]string, len(endpoints.Subsets[0].Addresses))
	for i, node := range endpoints.Subsets[0].Addresses {
		ips[i] = node.IP
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

func start_metrics(cfg config.Config) {
	metrics.RegisterMetrics(cfg.MetricTags)
	metrics.RegisterCommandLineMetric(cfg, cfg.MetricTags)
	metricsServer := metrics.GetMetricsServer()
	metricsServer.Start()

}
func startConsumers(ctx context.Context, consumerProto config.Protocol, wg *sync.WaitGroup) {
	for i := 1; i <= cfg.Consumers; i++ {
		subscribed := make(chan bool)
		n := i
		go func() {
			defer wg.Done()
			c, err := common.NewConsumer(consumerProto, cfg, n)
			if err != nil {
				log.Error("Error creating consumer: ", "error", err)
				os.Exit(1)
			}
			c.Start(ctx, subscribed)
		}()

		// wait until we know the receiver has subscribed
		<-subscribed
	}
}

func setUris(cfg *config.Config, command string) {
	// --uri is a shortcut to set both --publisher-uri and --consumer-uri
	if cfg.Uri != nil {
		if cfg.PublisherUri != nil || cfg.ConsumerUri != nil {
			fmt.Printf("ERROR: can't specify both --uri and --publisher-uri/--consumer-uri")
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
}

func defaultUri(proto string) string {
	uri := "localhost"
	switch proto {
	case "amqp":
		uri = "amqp://localhost/"
	case "stomp":
		uri = "stomp://localhost:61613"
	case "mqtt":
		uri = "mqtt://localhost:1883"
	}
	return uri
}

func shutdown(deleteQueues bool, printAllMetrics bool) {
	if deleteQueues {
		mgmt.DeleteDeclaredQueues()
	}
	mgmt.Disconnect()
	metricsServer := metrics.GetMetricsServer()
	metricsServer.PrintSummary()

	if printAllMetrics {
		metricsServer.PrintAll()
	}
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

	if cfg.MaxInFlight < 1 || cfg.MaxInFlight > 256 {
		return fmt.Errorf("max-in-flight must be between 1 and 256")
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
