package mqtt

import (
	"context"
	"crypto/tls"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/utils"

	"github.com/rabbitmq/omq/pkg/metrics"
)

// Mqtt5Responder is the "consumer" side of MQTT5 request/response (RPC): it subscribes
// to a request topic and, for every request that carries a Response Topic, publishes a
// reply back to that topic echoing the request's Correlation Data.
type Mqtt5Responder struct {
	Id     int
	Topic  string // request topic
	Config config.Config
	ctx    context.Context
}

func NewMqtt5Responder(ctx context.Context, cfg config.Config, id int) Mqtt5Responder {
	return Mqtt5Responder{
		Id:     id,
		Topic:  publisherTopic(cfg.ConsumeFromTemplate, id),
		Config: cfg,
		ctx:    ctx,
	}
}

func (c Mqtt5Responder) Start(consumerReady chan bool) {
	var msgsHandled atomic.Int64
	subscribed := make(chan struct{}, 1)

	// set inside OnConnectionUp, before subscribing -- guaranteed to be populated
	// before any request can arrive on the subscription.
	var connMgr atomic.Pointer[autopaho.ConnectionManager]

	replyMsg := utils.MessageBody(c.Config.MqttRpc.ReplySize, c.Config.MqttRpc.ReplySizeTemplate, c.Id)
	// One worker for every reply. The handler must not publish: paho acks on the
	// router goroutine, and a QoS>0 Publish waits for a PUBACK that incoming()
	// cannot read if that goroutine is blocked. The queue does not block the
	// handler, so it cannot fill the receive buffer either.
	replies := newReplyQueue()
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for {
			job, ok := replies.pop()
			if !ok {
				return
			}
			c.sendReply(&connMgr, replyMsg, job.responseTopic, job.correlationData)
			msgsHandled.Add(1)
			replies.finished()
		}
	}()
	defer func() {
		replies.close()
		<-workerDone
	}()

	handler := func(rcv paho.PublishReceived) (bool, error) {
		payload := rcv.Packet.Payload
		timeSent, latency := utils.CalculateEndToEndLatency(&payload)
		// Request transit only. Do not fold this into end-to-end latency: the
		// reply is a different message and is measured on the requester.
		metrics.RecordRpcRequestLatency(latency)
		metrics.MessagesConsumedMetric(0).Inc()

		if rcv.Packet.Properties == nil || rcv.Packet.Properties.ResponseTopic == "" {
			log.Debug("request without a response topic, dropping", "id", c.Id, "topic", c.Topic, "timeSent", timeSent)
			msgsHandled.Add(1)
			return true, nil
		}

		// Copy before returning: the packet buffer can be reused once the handler returns.
		replies.push(replyJob{
			responseTopic:   rcv.Packet.Properties.ResponseTopic,
			correlationData: append([]byte(nil), rcv.Packet.Properties.CorrelationData...),
		})
		return true, nil
	}

	urls := stringsToUrls(c.Config.ConsumerUri)
	reorderedUrls := utils.ReorderUrls(urls, c.Config.SpreadConnections, c.Id)

	pass, _ := urls[0].User.Password()
	opts := autopaho.ClientConfig{
		ServerUrls:                    reorderedUrls,
		ConnectUsername:               reorderedUrls[0].User.Username(),
		ConnectPassword:               []byte(pass),
		CleanStartOnInitialConnection: c.Config.MqttConsumer.CleanSession,
		SessionExpiryInterval:         uint32(c.Config.MqttConsumer.SessionExpiryInterval.Seconds()),
		KeepAlive:                     uint16(c.Config.MqttConsumer.KeepAlive / time.Second),
		ReconnectBackoff:              autopaho.NewConstantBackoff(1 * time.Second),
		ConnectTimeout:                30 * time.Second,
		TlsCfg: &tls.Config{
			InsecureSkipVerify: c.Config.InsecureSkipTLSVerify,
		},
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			log.Info("responder connected", "id", c.Id, "topic", c.Topic)
			connMgr.Store(cm)
			subscribeWithRetry(c.ctx, cm, []paho.SubscribeOptions{
				{
					Topic: c.Topic,
					// Requests are published at the publisher QoS. Subscribe at that
					// QoS so --mqtt-publisher-qos and --mqtt-consumer-qos each control
					// one leg instead of both legs being min(publisher, consumer).
					QoS: byte(c.Config.MqttPublisher.QoS),
					// A reply published to the request topic must not be handled as
					// another request, and a retained request must not be answered
					// again when this responder subscribes.
					NoLocal:        true,
					RetainHandling: 2,
				},
			}, subscribed, "responder", c.Id)
		},
		OnConnectError: func(err error) {
			log.Info("responder failed to connect", "id", c.Id, "error", err)
		},
		ConnectPacketBuilder: connectReceiveMaximum(c.Config.MqttConsumer.ReceiveMaximum),
		ClientConfig: paho.ClientConfig{
			ClientID: utils.InjectId(c.Config.ConsumerId, c.Id),
			OnClientError: func(err error) {
				log.Error("responder error", "id", c.Id, "error", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				log.Error("responder disconnected", "id", c.Id, "reasonCode", d.ReasonCode, "reasonString", d.Properties.ReasonString)
			},
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				handler,
			},
		},
	}

	connection, err := autopaho.NewConnection(c.ctx, opts)
	if err != nil {
		log.Error("responder connection failed", "id", c.Id, "error", err)
		close(consumerReady)
		return
	}
	err = connection.AwaitConnection(c.ctx)
	if err != nil {
		// AwaitConnection only returns an error if the context is cancelled
		close(consumerReady)
		c.stop(connection, replies, "context cancelled")
		return
	}

	// Don't signal readiness until the request-topic subscription is confirmed
	select {
	case <-subscribed:
		close(consumerReady)
	case <-c.ctx.Done():
		close(consumerReady)
		c.stop(connection, replies, "context cancelled")
		return
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for msgsHandled.Load() < int64(c.Config.ConsumeCount) {
		select {
		case <-c.ctx.Done():
			c.stop(connection, replies, "time limit reached")
			return
		case <-ticker.C:
			// Check more frequently to respond to context cancellation faster
		}
	}
	c.stop(connection, replies, "--cmessages value reached")
}

func (c Mqtt5Responder) sendReply(connMgr *atomic.Pointer[autopaho.ConnectionManager], replyMsg []byte, responseTopic string, correlationData []byte) {
	if c.Config.ConsumerLatencyTemplate != nil {
		latencyStr := utils.ExecuteTemplate(c.Config.ConsumerLatencyTemplate, c.Id)
		consumerLatency, err := time.ParseDuration(latencyStr)
		if err != nil {
			log.Error("failed to parse template-generated latency", "value", latencyStr, "error", err)
		} else if consumerLatency > 0 {
			log.Debug("consumer latency", "id", c.Id, "latency", consumerLatency)
			timer := time.NewTimer(consumerLatency)
			select {
			case <-timer.C:
			case <-c.ctx.Done():
				// Shutdown should not wait out the simulated processing time, but
				// the request was already received, so still send the reply.
				timer.Stop()
			}
		}
	}

	cm := connMgr.Load()
	if cm == nil {
		log.Error("responder not connected, can't send reply", "id", c.Id)
		return
	}

	var replyBody []byte
	if c.Config.MqttRpc.ReplySizeTemplate != nil {
		replyBody = utils.MessageBody(c.Config.MqttRpc.ReplySize, c.Config.MqttRpc.ReplySizeTemplate, c.Id)
	} else {
		replyBody = make([]byte, len(replyMsg))
		copy(replyBody, replyMsg)
	}

	reply := &paho.Publish{
		QoS:     byte(c.Config.MqttConsumer.QoS),
		Topic:   responseTopic,
		Payload: replyBody,
		Properties: &paho.PublishProperties{
			CorrelationData: correlationData,
		},
	}

	// Detached from c.ctx so a reply already in flight still goes out on shutdown.
	pubCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// One worker publishes, so no publish lock. Stamp immediately before Publish
	// so reply latency is broker transit, not --consumer-latency.
	utils.UpdatePayload(c.Config.UseMillis, &replyBody)
	reply.Payload = replyBody
	startTime := time.Now()
	_, err := cm.Publish(pubCtx, reply)
	if err != nil {
		log.Error("reply sending failure", "id", c.Id, "error", err)
		return
	}
	metrics.MessagesPublished.Inc()
	metrics.RecordPublishingLatency(time.Since(startTime))
	log.Debug("reply sent", "id", c.Id, "topic", reply.Topic, "latency", time.Since(startTime))
}

func (c Mqtt5Responder) stop(connection *autopaho.ConnectionManager, replies *replyQueue, reason string) {
	if replies != nil {
		replies.waitIdle()
	}
	log.Debug("closing responder connection", "id", c.Id, "reason", reason)
	if connection != nil {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = connection.Disconnect(disconnectCtx)
	}
}

// replyJob is one response to publish. correlationData is already copied out of
// the packet buffer.
type replyJob struct {
	responseTopic   string
	correlationData []byte
}

// replyQueue is an unbounded queue drained by one worker. push must not block:
// it runs on paho's router goroutine.
type replyQueue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	jobs     []replyJob
	inFlight bool
	closed   bool
}

func newReplyQueue() *replyQueue {
	q := &replyQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *replyQueue) push(job replyJob) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.jobs = append(q.jobs, job)
	q.mu.Unlock()
	q.cond.Signal()
}

func (q *replyQueue) pop() (replyJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.jobs) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.jobs) == 0 {
		return replyJob{}, false
	}
	job := q.jobs[0]
	q.jobs[0] = replyJob{}
	q.jobs = q.jobs[1:]
	if len(q.jobs) == 0 {
		q.jobs = nil
	}
	q.inFlight = true
	return job, true
}

func (q *replyQueue) finished() {
	q.mu.Lock()
	q.inFlight = false
	idle := len(q.jobs) == 0
	q.mu.Unlock()
	if idle {
		q.cond.Broadcast()
	}
}

func (q *replyQueue) waitIdle() {
	q.mu.Lock()
	for len(q.jobs) > 0 || q.inFlight {
		q.cond.Wait()
	}
	q.mu.Unlock()
}

func (q *replyQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}
