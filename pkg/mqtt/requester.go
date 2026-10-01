package mqtt

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"math/rand/v2"
	"strings"
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

// Mqtt5Requester is the "publisher" side of MQTT5 request/response (RPC): it publishes
// requests carrying a Response Topic + Correlation Data, and awaits replies on its own
// (once-subscribed) response topic. Up to Config.MaxInFlight requests may be outstanding
// at once; unlike a plain publisher, the in-flight slot for a request is only released
// once a matching reply arrives or the request times out, not right after it's sent.
type Mqtt5Requester struct {
	Id            int
	Connection    *autopaho.ConnectionManager
	RequestTopic  string
	ResponseTopic string
	Config        config.Config
	ctx           context.Context
	msg           []byte
	sem           chan struct{}
	wg            sync.WaitGroup
	pendingMu     sync.Mutex
	pending       map[uint64]pendingReq
}

// pendingReq tracks one outstanding request. started is the timeout and
// round-trip clock; it is updated once Publish returns so both use the same
// instant. published is false while Publish is still in flight — those entries
// must not be expired, or the in-flight slot would be released early.
type pendingReq struct {
	started   time.Time
	published bool
}

func NewMqtt5Requester(ctx context.Context, cfg config.Config, id int) *Mqtt5Requester {
	return &Mqtt5Requester{
		Id:            id,
		RequestTopic:  publisherTopic(cfg.PublishToTemplate, id),
		ResponseTopic: publisherTopic(cfg.MqttRpc.ResponseTopicTemplate, id),
		Config:        cfg,
		ctx:           ctx,
		sem:           make(chan struct{}, cfg.MaxInFlight),
		pending:       make(map[uint64]pendingReq),
	}
}

func (r *Mqtt5Requester) Start(requesterReady chan bool, startRequesting chan bool) {
	// One sweeper for every in-flight request. Cancelled after Stop returns, so
	// the drain in Stop can still expire requests that never get a reply.
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	defer sweepCancel()
	go r.sweepTimeouts(sweepCtx)

	subscribed := make(chan struct{}, 1)

	urls := stringsToUrls(r.Config.PublisherUri)
	reorderedUrls := utils.ReorderUrls(urls, r.Config.SpreadConnections, r.Id)

	pass, _ := urls[0].User.Password()
	opts := autopaho.ClientConfig{
		ServerUrls:                    reorderedUrls,
		ConnectUsername:               reorderedUrls[0].User.Username(),
		ConnectPassword:               []byte(pass),
		CleanStartOnInitialConnection: r.Config.MqttPublisher.CleanSession,
		SessionExpiryInterval:         uint32(r.Config.MqttPublisher.SessionExpiryInterval.Seconds()),
		KeepAlive:                     20,
		ReconnectBackoff:              autopaho.NewConstantBackoff(1 * time.Second),
		ConnectTimeout:                30 * time.Second,
		TlsCfg: &tls.Config{
			InsecureSkipVerify: r.Config.InsecureSkipTLSVerify,
		},
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			log.Info("requester connected", "id", r.Id, "requestTopic", r.RequestTopic, "responseTopic", r.ResponseTopic)
			subscribeWithRetry(r.ctx, cm, []paho.SubscribeOptions{
				{
					Topic: r.ResponseTopic,
					// Replies are published at the consumer QoS. Subscribing lower
					// would downgrade them; subscribing at the publisher QoS would
					// not raise them.
					QoS: byte(r.Config.MqttConsumer.QoS),
					// Don't treat our own request as the reply if the request and
					// response topics are the same, and don't complete seq 0 from a
					// retained message left on the response topic.
					NoLocal:        true,
					RetainHandling: 2,
				},
			}, subscribed, "requester", r.Id)
		},
		OnConnectError: func(err error) {
			log.Info("requester failed to connect", "id", r.Id, "error", err)
		},
		ConnectPacketBuilder: connectReceiveMaximum(inFlightReceiveMaximum(r.Config.MaxInFlight)),
		ClientConfig: paho.ClientConfig{
			ClientID: utils.InjectId(r.Config.PublisherId, r.Id),
			OnClientError: func(err error) {
				log.Error("requester error", "id", r.Id, "error", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				log.Error("requester disconnected", "id", r.Id, "reasonCode", d.ReasonCode, "reasonString", d.Properties.ReasonString)
			},
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(rcv paho.PublishReceived) (bool, error) {
					r.handleReply(rcv)
					return true, nil
				},
			},
		},
	}

	connection, err := autopaho.NewConnection(r.ctx, opts)
	if err != nil {
		log.Error("requester connection failed", "id", r.Id, "error", err)
		close(requesterReady)
		return
	}
	r.Connection = connection

	err = r.Connection.AwaitConnection(r.ctx)
	if err != nil {
		// AwaitConnection only returns an error if the context is cancelled
		close(requesterReady)
		return
	}

	// Don't signal readiness until the response-topic subscription is confirmed
	select {
	case <-subscribed:
	case <-r.ctx.Done():
		close(requesterReady)
		r.Stop("context cancelled")
		return
	}

	r.msg = utils.MessageBody(r.Config.Size, r.Config.SizeTemplate, r.Id)

	close(requesterReady)

	select {
	case <-r.ctx.Done():
		return
	case <-startRequesting:
		// short random delay to avoid all requesters publishing at the same time
		time.Sleep(time.Duration(rand.IntN(1000)) * time.Millisecond)
	}

	log.Info("requester started", "id", r.Id, "rate", r.Config.Rate, "requestTopic", r.RequestTopic, "responseTopic", r.ResponseTopic)

	var farewell string
	if r.Config.Rate == 0 {
		// idle connection
		<-r.ctx.Done()
		farewell = "context cancelled"
	} else {
		farewell = r.StartRequesting()
	}
	time.Sleep(500 * time.Millisecond)
	r.Stop(farewell)
}

func (r *Mqtt5Requester) StartRequesting() string {
	limiter := utils.RateLimiter(r.Config.Rate)

	var msgSent atomic.Int64
	for {
		select {
		case <-r.ctx.Done():
			return "time limit reached"
		default:
			seq := uint64(msgSent.Add(1) - 1)
			if seq >= uint64(r.Config.PublishCount) {
				return "--pmessages value reached"
			}
			if r.Config.Rate > 0 {
				_ = limiter.Wait(r.ctx)
			}
			select {
			case r.sem <- struct{}{}:
			case <-r.ctx.Done():
				return "context cancelled"
			}
			r.wg.Add(1)
			// unlike a plain publisher, the in-flight slot (r.sem) is deliberately NOT
			// released here: it's held until handleReply or expireRequest releases it.
			go func(s uint64) {
				defer r.wg.Done()
				r.sendRequest(s)
			}(seq)
		}
	}
}

func (r *Mqtt5Requester) sendRequest(seq uint64) {
	if r.Connection == nil {
		<-r.sem
		return
	}

	var body []byte
	if r.Config.SizeTemplate != nil {
		body = utils.MessageBody(r.Config.Size, r.Config.SizeTemplate, r.Id)
	} else {
		body = make([]byte, len(r.msg))
		copy(body, r.msg)
	}
	utils.UpdatePayload(r.Config.UseMillis, &body)

	correlationData := encodeCorrelation(r.Id, seq)

	pub := &paho.Publish{
		QoS:     byte(r.Config.MqttPublisher.QoS),
		Topic:   r.RequestTopic,
		Payload: body,
		Properties: &paho.PublishProperties{
			CorrelationData: correlationData,
			ResponseTopic:   r.ResponseTopic,
		},
	}

	for key, tmpl := range r.Config.MqttPublisher.UserPropertyTemplates {
		val := utils.ExecuteTemplate(tmpl, r.Id, seq)
		pub.Properties.User = append(pub.Properties.User, paho.UserProperty{Key: key, Value: val})
	}

	if r.Config.MessageTTLTemplate != nil {
		ttlStr := utils.ExecuteTemplate(r.Config.MessageTTLTemplate, r.Id, seq)
		if ttl, err := time.ParseDuration(ttlStr); err == nil {
			expirySecs := uint32(ttl.Seconds())
			pub.Properties.MessageExpiry = &expirySecs
		} else {
			log.Error("failed to parse template-generated TTL", "value", ttlStr, "error", err)
		}
	}
	if pub.Properties.MessageExpiry == nil {
		// Drop the request at the broker shortly after the client gives up, so a
		// responder that reconnects does not answer a request the requester has
		// already timed out. MQTT expiry is whole seconds and 0 can mean "expire
		// immediately", so keep the message for at least one second past the timeout.
		secs := rpcMessageExpiry(r.Config.MqttRpc.Timeout)
		pub.Properties.MessageExpiry = &secs
	}

	// Register the request as pending before publishing so a very fast reply
	// can never race ahead of us recording it. The timeout clock starts only
	// once Publish returns (see below).
	r.pendingMu.Lock()
	r.pending[seq] = pendingReq{started: time.Now()}
	r.pendingMu.Unlock()

	startTime := time.Now()
	_, err := r.Connection.Publish(r.ctx, pub)
	if err != nil {
		r.pendingMu.Lock()
		_, stillPending := r.pending[seq]
		if stillPending {
			delete(r.pending, seq)
		}
		r.pendingMu.Unlock()
		if stillPending {
			<-r.sem
		}

		if !strings.Contains(err.Error(), "use of closed network connection") &&
			!strings.Contains(err.Error(), "context canceled") {
			log.Error("request sending failure", "id", r.Id, "error", err)
		}
		return
	}
	latency := time.Since(startTime)
	metrics.MessagesPublished.Inc()
	metrics.RecordPublishingLatency(latency)
	log.Debug("request sent", "id", r.Id, "destination", r.RequestTopic, "seq", seq, "latency", latency)

	// Same timestamp for the timeout window and the round-trip measurement.
	// If the reply already arrived, the entry is gone and must not be re-added.
	sentAt := time.Now()
	r.pendingMu.Lock()
	if p, ok := r.pending[seq]; ok {
		p.started = sentAt
		p.published = true
		r.pending[seq] = p
	}
	r.pendingMu.Unlock()
}

func (r *Mqtt5Requester) handleReply(rcv paho.PublishReceived) {
	if rcv.Packet.Properties == nil {
		log.Debug("reply without valid correlation data, ignoring", "id", r.Id)
		return
	}
	requesterID, seq, ok := decodeCorrelation(rcv.Packet.Properties.CorrelationData)
	if !ok {
		log.Debug("reply without valid correlation data, ignoring", "id", r.Id)
		return
	}
	if requesterID != r.Id {
		// A shared response topic delivers every publisher's replies to every
		// subscriber. Ignore anything that isn't this requester's own seq.
		log.Debug("reply for another requester, ignoring", "id", r.Id, "requesterID", requesterID, "seq", seq)
		return
	}

	r.pendingMu.Lock()
	pending, ok := r.pending[seq]
	if ok {
		delete(r.pending, seq)
	}
	r.pendingMu.Unlock()

	if !ok {
		// already timed out (or a duplicate/unexpected reply) -- the in-flight slot
		// was already released by the sweeper, so don't touch r.sem again.
		log.Debug("reply for unknown/expired request, ignoring", "id", r.Id, "seq", seq)
		return
	}

	latency := time.Since(pending.started)
	metrics.RecordRoundTripLatency(latency)
	_, replyLatency := utils.CalculateEndToEndLatency(&rcv.Packet.Payload)
	metrics.RecordRpcReplyLatency(replyLatency)
	metrics.MessagesConsumedMetric(0).Inc()
	log.Debug("reply received", "id", r.Id, "seq", seq, "latency", latency, "replyLatency", replyLatency)
	<-r.sem
}

// rpcMessageExpiry is the MQTT message expiry (whole seconds) for a request.
// It is one second longer than the client timeout so the broker does not drop
// the request before the requester has recorded a timeout, and it is never 0.
func rpcMessageExpiry(timeout time.Duration) uint32 {
	return uint32(timeout/time.Second) + 1
}

// correlationDataLen is requester id (uint32) + per-requester sequence (uint64).
// The responder echoes these bytes unchanged; the id stops publishers that share
// a response topic from accepting each other's replies.
const correlationDataLen = 12

func encodeCorrelation(requesterID int, seq uint64) []byte {
	b := make([]byte, correlationDataLen)
	binary.BigEndian.PutUint32(b[:4], uint32(requesterID))
	binary.BigEndian.PutUint64(b[4:], seq)
	return b
}

func decodeCorrelation(b []byte) (requesterID int, seq uint64, ok bool) {
	if len(b) != correlationDataLen {
		return 0, 0, false
	}
	return int(binary.BigEndian.Uint32(b[:4])), binary.BigEndian.Uint64(b[4:]), true
}

func (r *Mqtt5Requester) sweepTimeouts(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.expireDue()
		}
	}
}

func (r *Mqtt5Requester) expireDue() {
	now := time.Now()
	var expired []uint64
	r.pendingMu.Lock()
	for seq, pending := range r.pending {
		if !pending.published || now.Sub(pending.started) < r.Config.MqttRpc.Timeout {
			continue
		}
		expired = append(expired, seq)
		delete(r.pending, seq)
	}
	r.pendingMu.Unlock()

	for _, seq := range expired {
		if metrics.RpcTimeouts != nil {
			metrics.RpcTimeouts.Inc()
		}
		log.Info("request timed out waiting for reply", "id", r.Id, "seq", seq, "timeout", r.Config.MqttRpc.Timeout)
		<-r.sem
	}
}

func (r *Mqtt5Requester) Stop(reason string) {
	r.wg.Wait()

	// give any already-sent, still-pending requests a chance to be answered
	// (or time out and release their slot) before disconnecting.
	deadline := time.Now().Add(r.Config.MqttRpc.Timeout + time.Second)
	for {
		r.expireDue()
		r.pendingMu.Lock()
		remaining := len(r.pending)
		r.pendingMu.Unlock()
		if remaining == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	log.Debug("closing requester connection", "id", r.Id, "reason", reason)
	if r.Connection != nil {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = r.Connection.Disconnect(disconnectCtx)
	}
}
