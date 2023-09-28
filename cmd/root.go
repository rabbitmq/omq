package cmd

import (
	"fmt"
	"math"
	"os"
	"strings"
	"sync"

	"github.com/rabbitmq/omq/pkg/common"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"

	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag/v2"
)

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
	rootCmd     = &cobra.Command{}
)

var metricsServer *metrics.MetricsServer

func Execute() {
	rootCmd := RootCmd()
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func RootCmd() *cobra.Command {
	var cfg config.Config

	amqp_amqp = &cobra.Command{
		Use:     "amqp-amqp",
		Aliases: []string{"amqp"},
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.AMQP, common.AMQP)
		},
	}
	amqp_amqp.Flags().IntVarP(&cfg.Amqp.ConsumerCredits, "amqp-consumer-credits", "", 1, "AMQP 1.0 consumer credits")
	amqp_amqp.Flags().VarP(enumflag.New(&cfg.Amqp.Durability, "amqp-durability", config.AmqpDurabilityModes, enumflag.EnumCaseInsensitive), "amqp-durability", "", "AMQP 1.0 durability mode")

	amqp_stomp = &cobra.Command{
		Use: "amqp-stomp",
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.AMQP, common.STOMP)
		},
	}

	amqp_mqtt = &cobra.Command{
		Use: "amqp-mqtt",
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.AMQP, common.MQTT)
		},
	}

	stomp_stomp = &cobra.Command{
		Use:     "stomp-stomp",
		Aliases: []string{"stomp"},
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.STOMP, common.STOMP)
		},
	}

	stomp_amqp = &cobra.Command{
		Use: "stomp-amqp",
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.STOMP, common.AMQP)
		},
	}
	stomp_amqp.Flags().IntVarP(&cfg.Amqp.ConsumerCredits, "amqp-consumer-credits", "", 1, "AMQP 1.0 consumer credits")

	stomp_mqtt = &cobra.Command{
		Use: "stomp-mqtt",
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.STOMP, common.MQTT)
		},
	}

	mqtt_mqtt = &cobra.Command{
		Use:     "mqtt-mqtt",
		Aliases: []string{"mqtt"},
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.MQTT, common.MQTT)
		},
	}

	mqtt_amqp = &cobra.Command{
		Use: "mqtt-amqp",
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.MQTT, common.AMQP)
		},
	}
	mqtt_amqp.Flags().IntVarP(&cfg.Amqp.ConsumerCredits, "amqp-consumer-credits", "", 1, "AMQP 1.0 consumer credits")

	mqtt_stomp = &cobra.Command{
		Use: "mqtt-stomp",
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.MQTT, common.STOMP)
		},
	}

	var rootCmd = &cobra.Command{Use: "omq",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if cfg.Size < 12 {
				_, _ = fmt.Fprintf(os.Stderr, "ERROR: size can't be less than 12 bytes\n")
				os.Exit(1)
			}
			setUris(&cfg, cmd.Use)
			metricsServer = metrics.NewMetricsServer()
			metricsServer.Start()
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			metrics.PrintMetrics()
			metricsServer.Stop()
		},
	}
	rootCmd.PersistentFlags().StringVarP(&cfg.PublisherUri, "publisher-uri", "", "", "URI for publishing")
	rootCmd.PersistentFlags().StringVarP(&cfg.ConsumerUri, "consumer-uri", "", "", "URI for consuming")
	rootCmd.PersistentFlags().IntVarP(&cfg.Publishers, "publishers", "x", 1, "The number of publishers to start")
	rootCmd.PersistentFlags().IntVarP(&cfg.Consumers, "consumers", "y", 1, "The number of consumers to start")
	rootCmd.PersistentFlags().IntVarP(&cfg.PublishCount, "pmessages", "C", math.MaxInt, "The number of messages to send per publisher (default=MaxInt)")
	rootCmd.PersistentFlags().IntVarP(&cfg.ConsumeCount, "cmessages", "D", math.MaxInt, "The number of messages to send per publisher (default=MaxInt)")
	rootCmd.PersistentFlags().StringVarP(&cfg.QueueNamePrefix, "queue-pattern", "q", "omq", "The queue name prefix")
	rootCmd.PersistentFlags().IntVarP(&cfg.QueueCount, "queue-count", "n", 1, "The number of queues to use")
	rootCmd.PersistentFlags().IntVarP(&cfg.Size, "size", "s", 12, "Message size in bytes")
	rootCmd.PersistentFlags().IntVarP(&cfg.Rate, "rate", "r", -1, "Messages per second (0 = unlimited)")
	rootCmd.PersistentFlags().DurationVarP(&cfg.Duration, "duration", "z", 0, "Duration (eg. 10s, 5m, 2h)")
	rootCmd.PersistentFlags().BoolVarP(&cfg.UseMillis, "use-millis", "m", false, "Use milliseconds for timestamps")

	rootCmd.AddCommand(amqp_amqp)
	rootCmd.AddCommand(amqp_stomp)
	rootCmd.AddCommand(amqp_mqtt)
	rootCmd.AddCommand(stomp_stomp)
	rootCmd.AddCommand(stomp_amqp)
	rootCmd.AddCommand(stomp_mqtt)
	rootCmd.AddCommand(mqtt_mqtt)
	rootCmd.AddCommand(mqtt_amqp)
	rootCmd.AddCommand(mqtt_stomp)

	return rootCmd
}

func start(cfg config.Config, publisherProto common.Protocol, consumerProto common.Protocol) {
	var wg sync.WaitGroup

	if cfg.Consumers > 0 {
		for i := 1; i <= cfg.Consumers; i++ {
			subscribed := make(chan bool)
			n := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, err := common.NewConsumer(consumerProto, cfg, n)
				if err != nil {
					log.Error("Error creating consumer: ", "error", err)
					os.Exit(1)
				}
				c.Start(subscribed)
			}()

			// wait until we know the receiver has subscribed
			<-subscribed
		}
	}

	if cfg.Publishers > 0 {
		for i := 1; i <= cfg.Publishers; i++ {
			n := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				p, err := common.NewPublisher(publisherProto, cfg, n)
				if err != nil {
					log.Error("Error creating publisher: ", "error", err)
					os.Exit(1)
				}
				p.Start()
			}()
		}
	}

	wg.Wait()
}

func setUris(cfg *config.Config, command string) {
	if cfg.PublisherUri == "" {
		println("setting publisher uri to ", defaultUri(strings.Split(command, "-")[0]))
		(*cfg).PublisherUri = defaultUri(strings.Split(command, "-")[0])
	}
	if cfg.ConsumerUri == "" {
		println("setting consumer uri to ", defaultUri(strings.Split(command, "-")[1]))
		(*cfg).ConsumerUri = defaultUri(strings.Split(command, "-")[1])
	}
}

func defaultUri(proto string) string {
	var uri = "localhost"
	switch proto {
	case "amqp":
		uri = "amqp://localhost"
	case "stomp":
		uri = "localhost:61613"
	case "mqtt":
		uri = "localhost:1883"
	}
	return uri
}
