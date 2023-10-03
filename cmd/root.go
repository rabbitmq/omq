package cmd

import (
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rabbitmq/omq/pkg/common"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/version"

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
	versionCmd  = &cobra.Command{}
)

func Execute() {
	rootCmd := RootCmd()
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func RootCmd() *cobra.Command {
	cfg := config.NewConfig()

	amqp_amqp = &cobra.Command{
		Use:     "amqp-amqp",
		Aliases: []string{"amqp"},
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.AMQP, common.AMQP)
		},
	}
	amqp_amqp.Flags().IntVarP(&cfg.Amqp.ConsumerCredits, "amqp-consumer-credits", "", 1, "AMQP 1.0 consumer credits")

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
	amqp_mqtt.Flags().IntVar(&cfg.MqttConsumer.QoS, "mqtt-consumer-qos", 0, "MQTT consumer QoS level (0, 1 or 2; default=0)")
	amqp_mqtt.Flags().BoolVar(&cfg.MqttConsumer.CleanSession, "mqtt-consumer-clean-session", true, "MQTT consumer clean session (default = true)")

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
	stomp_amqp.Flags().IntVar(&cfg.Amqp.ConsumerCredits, "amqp-consumer-credits", 1, "AMQP 1.0 consumer credits")

	stomp_mqtt = &cobra.Command{
		Use: "stomp-mqtt",
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.STOMP, common.MQTT)
		},
	}
	stomp_mqtt.Flags().IntVar(&cfg.MqttConsumer.QoS, "mqtt-consumer-qos", 0, "MQTT consumer QoS level (0, 1 or 2; default=0)")
	stomp_mqtt.Flags().BoolVar(&cfg.MqttConsumer.CleanSession, "mqtt-consumer-clean-session", true, "MQTT consumer clean session (default = true)")

	mqtt_mqtt = &cobra.Command{
		Use:     "mqtt-mqtt",
		Aliases: []string{"mqtt"},
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.MQTT, common.MQTT)
		},
	}
	mqtt_mqtt.Flags().IntVar(&cfg.MqttPublisher.QoS, "mqtt-publisher-qos", 0, "MQTT publisher QoS level (0, 1 or 2; default=0)")
	mqtt_mqtt.Flags().IntVar(&cfg.MqttConsumer.QoS, "mqtt-consumer-qos", 0, "MQTT consumer QoS level (0, 1 or 2; default=0)")
	mqtt_mqtt.Flags().BoolVar(&cfg.MqttPublisher.CleanSession, "mqtt-publisher-clean-session", true, "MQTT publisher clean session (default = true)")
	mqtt_mqtt.Flags().BoolVar(&cfg.MqttConsumer.CleanSession, "mqtt-consumer-clean-session", true, "MQTT consumer clean session (default = true)")

	mqtt_amqp = &cobra.Command{
		Use: "mqtt-amqp",
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.MQTT, common.AMQP)
		},
	}
	mqtt_amqp.Flags().IntVar(&cfg.Amqp.ConsumerCredits, "amqp-consumer-credits", 1, "AMQP 1.0 consumer credits")
	mqtt_amqp.Flags().IntVar(&cfg.MqttPublisher.QoS, "mqtt-qos", 0, "MQTT publisher QoS level (0, 1 or 2; default=0)")
	mqtt_amqp.Flags().BoolVar(&cfg.MqttPublisher.CleanSession, "mqtt-publisher-clean-session", true, "MQTT publisher clean session (default = true)")

	mqtt_stomp = &cobra.Command{
		Use: "mqtt-stomp",
		Run: func(cmd *cobra.Command, args []string) {
			start(cfg, common.MQTT, common.STOMP)
		},
	}
	mqtt_stomp.Flags().IntVar(&cfg.MqttPublisher.QoS, "mqtt-qos", 0, "MQTT publisher QoS level (0, 1 or 2; default=0)")
	mqtt_stomp.Flags().BoolVar(&cfg.MqttPublisher.CleanSession, "mqtt-publisher-clean-session", true, "MQTT publisher clean session (default = true)")

	versionCmd = &cobra.Command{
		Use: "version",
		Run: func(cmd *cobra.Command, args []string) {
			version.Print()
		},
	}

	var rootCmd = &cobra.Command{Use: "omq",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if cfg.Size < 12 {
				_, _ = fmt.Fprintf(os.Stderr, "ERROR: size can't be less than 12 bytes\n")
				os.Exit(1)
			}
			setUris(&cfg, cmd.Use)
		},
	}
	rootCmd.PersistentFlags().StringVarP(&cfg.PublisherUri, "publisher-uri", "", "", "URI for publishing")
	rootCmd.PersistentFlags().StringVarP(&cfg.ConsumerUri, "consumer-uri", "", "", "URI for consuming")
	rootCmd.PersistentFlags().IntVarP(&cfg.Publishers, "publishers", "x", 1, "The number of publishers to start")
	rootCmd.PersistentFlags().IntVarP(&cfg.Consumers, "consumers", "y", 1, "The number of consumers to start")
	rootCmd.PersistentFlags().IntVarP(&cfg.PublishCount, "pmessages", "C", math.MaxInt, "The number of messages to send per publisher (default=MaxInt)")
	rootCmd.PersistentFlags().IntVarP(&cfg.ConsumeCount, "cmessages", "D", math.MaxInt, "The number of messages to send per publisher (default=MaxInt)")
	rootCmd.PersistentFlags().StringVarP(&cfg.PublishTo, "publish-to", "t", "/topic/omq", "The topic/terminus to publish to (%d will be replaced with the publisher's id)")
	rootCmd.PersistentFlags().StringVarP(&cfg.ConsumeFrom, "consume-from", "T", "/topic/omq", "The queue/topic/terminus to consume from (%d will be replaced with the consumer's id)")
	rootCmd.PersistentFlags().IntVarP(&cfg.Size, "size", "s", 12, "Message size in bytes")
	rootCmd.PersistentFlags().IntVarP(&cfg.Rate, "rate", "r", -1, "Messages per second (0 = unlimited)")
	rootCmd.PersistentFlags().DurationVarP(&cfg.Duration, "duration", "z", 0, "Duration (eg. 10s, 5m, 2h)")
	rootCmd.PersistentFlags().BoolVarP(&cfg.UseMillis, "use-millis", "m", false, "Use milliseconds for timestamps")
	rootCmd.PersistentFlags().VarP(enumflag.New(&cfg.QueueDurability, "queue-durability", config.AmqpDurabilityModes, enumflag.EnumCaseInsensitive), "queue-durability", "", "Queue durability (default: configuration - the queue definition is durable)")
	rootCmd.PersistentFlags().StringVar(&cfg.Amqp.Subject, "amqp-subject", "", "AMQP 1.0 message subject")
	rootCmd.PersistentFlags().BoolVarP(&cfg.MessageDurability, "message-durability", "d", true, "Mark messages as durable (default=true)")

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

func start(cfg config.Config, publisherProto common.Protocol, consumerProto common.Protocol) {
	var wg sync.WaitGroup

	if cfg.Duration > 0 {
		go func() {
			time.Sleep(cfg.Duration)
			_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
		}()
	}

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
	if command == "version" {
		return
	}
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
