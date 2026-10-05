package stomp

import (
	"fmt"
	"net"
	"testing"
	"time"

	stompclient "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
	"github.com/rabbitmq/omq/pkg/log"
)

func TestStopDisconnectsSessionWhileDrainingDeliveries(t *testing.T) {
	log.Setup()
	client, broker := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = broker.Close() })
	brokerDone := make(chan error, 1)
	go func() {
		reader, writer := frame.NewReader(broker), frame.NewWriter(broker)
		if _, err := reader.Read(); err != nil {
			brokerDone <- err
			return
		}
		if err := writer.Write(frame.New(frame.CONNECTED, frame.Version, "1.2", frame.HeartBeat, "0,0")); err != nil {
			brokerDone <- err
			return
		}
		sub, err := reader.Read()
		if err != nil {
			brokerDone <- err
			return
		}
		if err := writer.Write(frame.New(frame.RECEIPT, frame.ReceiptId, sub.Header.Get(frame.Receipt))); err != nil {
			brokerDone <- err
			return
		}
		// The consumer is stopping, but a delivery is already in flight. Without
		// draining it, the client's I/O loop cannot read the DISCONNECT receipt.
		delivery := frame.New(frame.MESSAGE, frame.Subscription, sub.Header.Get(frame.Id), frame.Destination, "/topic/test", frame.MessageId, "1", frame.Ack, "1")
		if err := writer.Write(delivery); err != nil {
			brokerDone <- err
			return
		}
		disconnect, err := reader.Read()
		if err != nil {
			brokerDone <- err
			return
		}
		if disconnect.Command != frame.DISCONNECT {
			brokerDone <- fmt.Errorf("expected DISCONNECT, got %s", disconnect.Command)
			return
		}
		brokerDone <- writer.Write(frame.New(frame.RECEIPT, frame.ReceiptId, disconnect.Header.Get(frame.Receipt)))
	}()
	conn, err := stompclient.Connect(client, stompclient.ConnOpt.HeartBeat(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := conn.Subscribe("/topic/test", stompclient.AckClient, stompclient.SubscribeOpt.Receipt(""))
	if err != nil {
		t.Fatal(err)
	}
	consumer := &StompConsumer{Connection: conn, Subscription: sub}
	done := make(chan struct{})
	go func() { consumer.Stop("test"); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked with a delivery in flight")
	}
	select {
	case err := <-brokerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("broker did not receive DISCONNECT")
	}
}
