package cmd

import (
	"fmt"
	"math"
	"os"

	"github.com/rabbitmq/omq/pkg/amqp10_client"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/mqtt_client"
	"github.com/rabbitmq/omq/pkg/stomp_client"

	"github.com/spf13/cobra"
)

var (
	amqp    = &cobra.Command{}
	stomp   = &cobra.Command{}
	mqtt    = &cobra.Command{}
	rootCmd = &cobra.Command{}
)

func Execute() {
	rootCmd := RootCmd()
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func RootCmd() *cobra.Command {
	var cfg config.Config

	// omq amqp ...
	amqp = &cobra.Command{
		Use: "amqp",
		PreRun: func(cmd *cobra.Command, args []string) {
			_, _ = fmt.Fprintf(os.Stderr, "DEBUG: will start AMQP 1.0 client group\n")
		},
		Run: func(cmd *cobra.Command, args []string) {
			amqp10_client.Start(cfg)
		},
	}
	amqp.Flags().StringVarP(&cfg.AmqpUrl, "url", "H", "amqp://localhost:5672", "The address of the AMQP 1.0 server")
	amqp.Flags().IntVarP(&cfg.Publishers, "publishers", "x", 1, "The number of AMQP 1.0 publishers to start")
	amqp.Flags().IntVarP(&cfg.Consumers, "consumers", "y", 1, "The number of AMQP 1.0 consumers to start")
	amqp.Flags().IntVarP(&cfg.PublishCount, "pmessages", "C", math.MaxInt, "The number of messages to send per publisher (default=MaxInt)")
	amqp.Flags().IntVarP(&cfg.ConsumeCount, "cmessages", "D", math.MaxInt, "The number of messages to send per publisher (default=MaxInt)")
	amqp.Flags().StringVarP(&cfg.QueueNamePrefix, "queue-pattern", "q", "omq", "The queue name prefix")
	amqp.Flags().IntVarP(&cfg.QueueCount, "queue-count", "n", 1, "The number of queues to use")
	amqp.Flags().IntVarP(&cfg.Size, "size", "s", 12, "Message size in bytes")
	amqp.Flags().IntVarP(&cfg.Rate, "rate", "r", 0, "Messages per second (0 = unlimited)")
	amqp.Flags().DurationVarP(&cfg.Duration, "duration", "z", 0, "Duration (eg. 10s, 5m, 2h)")
	amqp.Flags().BoolVarP(&cfg.UseMillis, "use-millis", "m", false, "Use milliseconds for timestamps")

	// omq stomp ...
	stomp = &cobra.Command{
		Use: "stomp",
		PreRun: func(cmd *cobra.Command, args []string) {
			if cfg.Size < 12 {
				_, _ = fmt.Fprintf(os.Stderr, "ERROR: size can't be less than 12 bytes\n")
				os.Exit(1)
			}
			_, _ = fmt.Fprintf(os.Stderr, "DEBUG: will start a STOMP client group\n")
		},
		Run: func(cmd *cobra.Command, args []string) {
			stomp_client.Start(cfg)
		},
	}
	stomp.Flags().StringVarP(&cfg.StompUrl, "url", "H", "localhost:61613", "The affress of the STOMP server")
	stomp.Flags().IntVarP(&cfg.Publishers, "publishers", "x", 1, "The number of STOMP publishers to start")
	stomp.Flags().IntVarP(&cfg.Consumers, "consumers", "y", 1, "The number of STOMP consumers to start")
	stomp.Flags().IntVarP(&cfg.PublishCount, "pmessages", "C", math.MaxInt, "The number of messages to send per publisher (default=MazInt)")
	stomp.Flags().IntVarP(&cfg.ConsumeCount, "cmessages", "D", math.MaxInt, "The number of messages to send per publisher (default=MaxInt)")
	stomp.Flags().StringVarP(&cfg.QueueNamePrefix, "queue-pattern", "q", "omq", "The queue name prefix")
	stomp.Flags().IntVarP(&cfg.QueueCount, "queue-count", "n", 1, "The number of queues to use")
	stomp.Flags().IntVarP(&cfg.Size, "size", "s", 12, "Message size in bytes")
	stomp.Flags().IntVarP(&cfg.Rate, "rate", "r", 0, "Messages per second (0 = unlimited)")
	stomp.Flags().BoolVarP(&cfg.UseMillis, "use-millis", "m", false, "Use milliseconds for timestamps")

	// omq mqtt ...
	mqtt = &cobra.Command{
		Use: "mqtt",
		PreRun: func(cmd *cobra.Command, args []string) {
			if cfg.Size < 12 {
				_, _ = fmt.Fprintf(os.Stderr, "ERROR: size can't be less than 12 bytes\n")
				os.Exit(1)
			}
			_, _ = fmt.Fprintf(os.Stderr, "DEBUG: will start a mqtt client group\n")
		},
		Run: func(cmd *cobra.Command, args []string) {
			mqtt_client.Start(cfg)
		},
	}
	mqtt.Flags().StringVarP(&cfg.MqttUrl, "url", "H", "localhost:1883", "The affress of the MQTT server")
	mqtt.Flags().IntVarP(&cfg.Publishers, "publishers", "x", 1, "The number of MQTT publishers to start")
	mqtt.Flags().IntVarP(&cfg.Consumers, "consumers", "y", 1, "The number of MQTT consumers to start")
	mqtt.Flags().IntVarP(&cfg.PublishCount, "pmessages", "C", math.MaxInt, "The number of messages to send per publisher (default=MaxInt)")
	mqtt.Flags().IntVarP(&cfg.ConsumeCount, "cmessages", "D", math.MaxInt, "The number of messages to send per publisher (default=MaxInt)")
	mqtt.Flags().StringVarP(&cfg.QueueNamePrefix, "queue-pattern", "q", "omq", "The queue name prefix")
	mqtt.Flags().IntVarP(&cfg.QueueCount, "queue-count", "n", 1, "The number of queues to use")
	mqtt.Flags().IntVarP(&cfg.Size, "size", "s", 12, "Message size in bytes")
	mqtt.Flags().IntVarP(&cfg.Rate, "rate", "r", 0, "Messages per second (0 = unlimited)")
	mqtt.Flags().DurationVarP(&cfg.Duration, "duration", "z", 0, "Duration (eg. 10s, 5m, 2h)")
	mqtt.Flags().BoolVarP(&cfg.UseMillis, "use-millis", "m", false, "Use milliseconds for timestamps")

	var rootCmd = &cobra.Command{Use: "omq"}
	rootCmd.AddCommand(amqp)
	rootCmd.AddCommand(stomp)
	rootCmd.AddCommand(mqtt)

	return rootCmd
}
