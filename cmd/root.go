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

	amqp = &cobra.Command{
		Use:     "amqp-amqp",
		Aliases: []string{"amqp"},
		Run: func(cmd *cobra.Command, args []string) {
			amqp10_client.Start(cfg)
		},
		PreRun: func(cmd *cobra.Command, args []string) {
			if cfg.Size < 12 {
				_, _ = fmt.Fprintf(os.Stderr, "ERROR: size can't be less than 12 bytes\n")
				os.Exit(1)
			}
		},
	}

	stomp = &cobra.Command{
		Use:     "stomp-stomp",
		Aliases: []string{"stomp"},
		Run: func(cmd *cobra.Command, args []string) {
			stomp_client.Start(cfg)
		},
		PreRun: func(cmd *cobra.Command, args []string) {
			if cfg.Size < 12 {
				_, _ = fmt.Fprintf(os.Stderr, "ERROR: size can't be less than 12 bytes\n")
				os.Exit(1)
			}
		},
	}

	mqtt = &cobra.Command{
		Use:     "mqtt-mqtt",
		Aliases: []string{"mqtt"},
		Run: func(cmd *cobra.Command, args []string) {
			mqtt_client.Start(cfg)
		},
		PreRun: func(cmd *cobra.Command, args []string) {
			if cfg.Size < 12 {
				_, _ = fmt.Fprintf(os.Stderr, "ERROR: size can't be less than 12 bytes\n")
				os.Exit(1)
			}
		},
	}

	var rootCmd = &cobra.Command{Use: "omq"}
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

	rootCmd.AddCommand(amqp)
	rootCmd.AddCommand(stomp)
	rootCmd.AddCommand(mqtt)

	return rootCmd
}
