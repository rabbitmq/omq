package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAmqpCmd(t *testing.T) {
	rootCmd := RootCmd()
	actual := new(bytes.Buffer)
	rootCmd.SetOut(actual)
	rootCmd.SetErr(actual)
	rootCmd.SetArgs([]string{"amqp", "-C", "1", "-D", "1"})
	err := rootCmd.Execute()

	assert.Nil(t, err)
}

func TestMqttCmd(t *testing.T) {
	rootCmd := RootCmd()
	actual := new(bytes.Buffer)
	rootCmd.SetOut(actual)
	rootCmd.SetErr(actual)
	rootCmd.SetArgs([]string{"mqtt", "-C", "1", "-D", "1"})
	err := rootCmd.Execute()

	assert.Nil(t, err)
}

func TestStompCmd(t *testing.T) {
	rootCmd := RootCmd()
	actual := new(bytes.Buffer)
	rootCmd.SetOut(actual)
	rootCmd.SetErr(actual)
	rootCmd.SetArgs([]string{"stomp", "-C", "1", "-D", "1"})
	err := rootCmd.Execute()

	assert.Nil(t, err)
}
