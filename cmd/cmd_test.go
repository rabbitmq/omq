package cmd

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("end-to-end latency uses nanoseconds only if we have both publishers and consumers", func() {
	It("should use nanoseconds when no -x / -y flags are used", func() {
		args := []string{"amqp", "-C", "0", "-D", "0", "-t", "/topic/foobar", "-T", "/topic/foobar"}
		rootCmd := RootCmd()
		rootCmd.SetArgs(args)
		_ = rootCmd.Execute()
		Expect(cfg.UseMillis).To(BeFalse())
	})

	It("should use millseconds when there are no publishers", func() {
		args := []string{"amqp", "-x", "0", "-D", "0", "-T", "/topic/foobar"}
		rootCmd := RootCmd()
		rootCmd.SetArgs(args)
		_ = rootCmd.Execute()
		Expect(cfg.UseMillis).To(BeTrue())
	})

	It("should use millseconds when there are no consumers", func() {
		args := []string{"amqp", "-y", "0", "-C", "0", "-t", "/topic/foobar"}
		rootCmd := RootCmd()
		rootCmd.SetArgs(args)
		_ = rootCmd.Execute()
		Expect(cfg.UseMillis).To(BeTrue())
	})
})
