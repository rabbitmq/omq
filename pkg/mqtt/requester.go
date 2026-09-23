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
	pending       map[uint64]time.Time
}

func NewMqtt5Requester(ctx context.Context, cfg config.Config, id int) *Mqtt5Requester {
	return &Mqtt5Requester{
		Id:            id,
		RequestTopic:  publisherTopic(cfg.PublishToTemplate, id),
		ResponseTopic: publisherTopic(cfg.MqttRpc.ResponseTopicTemplate, id),
		Config:        cfg,
		ctx:           ctx,
		sem:           make(chan struct{}, cfg.MaxInFlight),
		pending:       make(map[uint64]time.Time),
	}
}

func (r *Mqtt5Requester) Start(requesterReady chan bool, startRequesting chan bool) {
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
				{Topic: r.ResponseTopic, QoS: byte(r.Config.MqttPublisher.QoS)},
			}, subscribed, "requester", r.Id)
		},
		OnConnectError: func(err error) {
			log.Info("requester failed to connect", "id", r.Id, "error", err)
		},
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

	correlationData := make([]byte, 8)
	binary.BigEndian.PutUint64(correlationData, seq)

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

	// Register the request as pending before publishing so a very fast reply
	// can never race ahead of us recording it.
	r.pendingMu.Lock()
	r.pending[seq] = time.Now()
	r.pendingMu.Unlock()

	startTime := time.Now()
	_, err := r.Connection.Publish(r.ctx, pub)
	if err != nil {
		r.pendingMu.Lock()
		delete(r.pending, seq)
		r.pendingMu.Unlock()
		<-r.sem

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

	time.AfterFunc(r.Config.MqttRpc.Timeout, func() { r.expireRequest(seq) })
}

func (r *Mqtt5Requester) handleReply(rcv paho.PublishReceived) {
	if rcv.Packet.Properties == nil || len(rcv.Packet.Properties.CorrelationData) != 8 {
		log.Debug("reply without valid correlation data, ignoring", "id", r.Id)
		return
	}
	seq := binary.BigEndian.Uint64(rcv.Packet.Properties.CorrelationData)

	r.pendingMu.Lock()
	sendTime, ok := r.pending[seq]
	if ok {
		delete(r.pending, seq)
	}
	r.pendingMu.Unlock()

	if !ok {
		// already timed out (or a duplicate/unexpected reply) -- the in-flight slot
		// was already released by expireRequest, so don't touch r.sem again.
		log.Debug("reply for unknown/expired request, ignoring", "id", r.Id, "seq", seq)
		return
	}

	latency := time.Since(sendTime)
	metrics.RecordRoundTripLatency(latency)
	metrics.MessagesConsumedMetric(0).Inc()
	log.Debug("reply received", "id", r.Id, "seq", seq, "latency", latency)
	<-r.sem
}

func (r *Mqtt5Requester) expireRequest(seq uint64) {
	r.pendingMu.Lock()
	_, ok := r.pending[seq]
	if ok {
		delete(r.pending, seq)
	}
	r.pendingMu.Unlock()

	if ok {
		metrics.RpcTimeouts.Inc()
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
