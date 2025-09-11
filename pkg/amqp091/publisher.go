package amqp091

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/rabbitmq/omq/pkg/config"
	"github.com/rabbitmq/omq/pkg/log"
	"github.com/rabbitmq/omq/pkg/metrics"
	"github.com/rabbitmq/omq/pkg/utils"
)

type Amqp091Publisher struct {
	Id               int
	Connection       *amqp091.Connection
	Channel          *amqp091.Channel
	confirms         chan amqp091.Confirmation
	returns          chan amqp091.Return
	publishTimes     map[uint64]time.Time
	publishTimesLock sync.Mutex
	exchange         string
	routingKey       string
	Config           config.Config
	msg              []byte
	whichUri         int
	ctx              context.Context
}

func NewPublisher(ctx context.Context, cfg config.Config, id int) *Amqp091Publisher {
	exchange, routingKey := parseExchangeAndRoutingKey(cfg.PublishTo)
	publisher := &Amqp091Publisher{
		Id:           id,
		Connection:   nil,
		Config:       cfg,
		exchange:     exchange,
		routingKey:   utils.InjectId(routingKey, id),
		publishTimes: make(map[uint64]time.Time),
		whichUri:     0,
		ctx:          ctx,
	}

	if cfg.SpreadConnections {
		publisher.whichUri = (id - 1) % len(cfg.PublisherUri)
	}

	publisher.Connect()

	return publisher
}

func (p *Amqp091Publisher) Connect() {
	var err error

	if p.Connection != nil {
		_ = p.Connection.Close()
	}
	p.Connection = nil

	for p.Connection == nil {
		if p.whichUri >= len(p.Config.PublisherUri) {
			p.whichUri = 0
		}
		uri := p.Config.PublisherUri[p.whichUri]
		p.whichUri++
		config := amqp091.Config{
			Properties: amqp091.Table{
				"connection_name": fmt.Sprintf("omq-publisher-%d", p.Id),
			},
		}
		conn, err := amqp091.DialConfig(uri, config)
		if err != nil {
			log.Error("publisher connection failed", "id", p.Id, "error", err.Error())
			select {
			case <-time.After(1 * time.Second):
				continue
			case <-p.ctx.Done():
				return
			}
		} else {
			log.Debug("publisher connected", "id", p.Id, "uri", uri)
			p.Connection = conn
		}
	}
	for p.Channel, err = p.Connection.Channel(); err != nil; {
		log.Error("channel creation failed", "id", p.Id, "error", err.Error())
		time.Sleep(1 * time.Second)
	}
	p.confirms = make(chan amqp091.Confirmation) // p.Config.MaxInFlight ?
	p.returns = make(chan amqp091.Return)
	_ = p.Channel.Confirm(false)
	p.Channel.NotifyPublish(p.confirms)
	p.Channel.NotifyReturn(p.returns)
}

func (p *Amqp091Publisher) Start(publisherReady chan bool, startPublishing chan bool) {
	p.msg = utils.MessageBody(p.Config.Size)

	close(publisherReady)

	select {
	case <-p.ctx.Done():
		return
	case <-startPublishing:
		// short random delay to avoid all publishers publishing at the same time
		time.Sleep(time.Duration(rand.IntN(1000)) * time.Millisecond)
	}

	log.Info("publisher started", "id", p.Id, "rate", utils.Rate(p.Config.Rate), "exchange", p.exchange, "routing key", p.routingKey)
	var farewell string
	if p.Config.Rate == 0 {
		// idle connection
		<-p.ctx.Done()
		farewell = "context cancelled"
	} else {
		farewell = p.StartPublishing()
	}
	p.Stop(farewell)
}

func (p *Amqp091Publisher) StartPublishing() string {
	limiter := utils.RateLimiter(p.Config.Rate)

	go p.handleConfirms()
	go p.handleReturns()

	var msgSent atomic.Int64
	for {
		select {
		case <-p.ctx.Done():
			return "context cancelled"
		default:
			n := uint64(msgSent.Add(1))
			if n > uint64(p.Config.PublishCount) {
				return "--pmessages value reached"
			}
			if p.Config.Rate > 0 {
				_ = limiter.Wait(p.ctx)
			}
			err := p.SendAsync(n)
			if err != nil {
				p.Connect()
			} else {
				metrics.MessagesPublished.Inc()
				log.Debug("message sent", "id", p.Id, "deliveryTag", n)
			}
		}
	}
}

func (p *Amqp091Publisher) SendAsync(n uint64) error {
	msg := p.prepareMessage()

	p.setPublishTime(n, time.Now())
	err := p.Channel.PublishWithContext(p.ctx, p.exchange, p.routingKey, p.Config.Amqp091.Mandatory, false, msg)
	return err
}

func (p *Amqp091Publisher) handleConfirms() {
	for confirm := range p.confirms {
		if confirm.Ack {
			pubTime := p.getPublishTime(confirm.DeliveryTag)
			latency := time.Since(pubTime)
			metrics.PublishingLatency.Update(latency.Seconds())
			metrics.MessagesConfirmed.Inc()
			log.Debug("message confirmed", "id", p.Id, "delivery_tag", confirm.DeliveryTag, "latency", latency)
		} else {
			if confirm.DeliveryTag == 0 {
				log.Debug("handleConfirms completed (channel closed)")
				return
			}
			_ = p.getPublishTime(confirm.DeliveryTag)
			log.Debug("message not confirmed by the broker", "id", p.Id, "delivery_tag", confirm.DeliveryTag)
		}
	}
}

func (p *Amqp091Publisher) handleReturns() {
	for returned := range p.returns {
		metrics.MessagesReturned.Inc()
		log.Debug("message returned by broker (unroutable)",
			"id", p.Id,
			"reply_code", returned.ReplyCode,
			"reply_text", returned.ReplyText,
			"exchange", returned.Exchange,
			"routing_key", returned.RoutingKey)
	}
}

func (p *Amqp091Publisher) Stop(reason string) {
	log.Debug("closing publisher connection", "id", p.Id, "reason", reason)
	for len(p.publishTimes) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
	if p.Channel != nil {
		_ = p.Channel.Close()
	}
	if p.Connection != nil {
		_ = p.Connection.Close()
	}
}

func (p *Amqp091Publisher) prepareMessage() amqp091.Publishing {
	utils.UpdatePayload(p.Config.UseMillis, &p.msg)

	msg := amqp091.Publishing{
		DeliveryMode: amqp091.Persistent,
		Body:         p.msg,
	}

	// Handle template-based headers
	if len(p.Config.Amqp091.HeaderTemplates) > 0 {
		if msg.Headers == nil {
			msg.Headers = make(amqp091.Table)
		}
		for key, tmpl := range p.Config.Amqp091.HeaderTemplates {
			stringValue := utils.ExecuteTemplate(tmpl, "header "+key)
			// Convert to appropriate type like the original ParseHeaders function
			if intVal, err := strconv.ParseInt(stringValue, 10, 64); err == nil {
				msg.Headers[key] = intVal
			} else if floatVal, err := strconv.ParseFloat(stringValue, 64); err == nil {
				msg.Headers[key] = floatVal
			} else {
				msg.Headers[key] = stringValue
			}
		}
	}

	return msg
}

func (p *Amqp091Publisher) setPublishTime(deliveryTag uint64, t time.Time) {
	p.publishTimesLock.Lock()
	p.publishTimes[deliveryTag] = t
	p.publishTimesLock.Unlock()
}

func (p *Amqp091Publisher) getPublishTime(deliveryTag uint64) time.Time {
	p.publishTimesLock.Lock()
	t := p.publishTimes[deliveryTag]
	delete(p.publishTimes, deliveryTag)
	p.publishTimesLock.Unlock()
	return t
}

func parseExchangeAndRoutingKey(target string) (string, string) {
	exchange := ""
	routingKey := target

	parts := strings.SplitN(target, "/", 4)

	switch {
	case len(parts) == 3 && parts[0] == "" && parts[1] == "queues":
		// /queues/queueName -> default exchange and the queueName as routing key
		routingKey = parts[2]
	case len(parts) == 4 && parts[0] == "" && parts[1] == "exchanges":
		exchange = parts[2]
		routingKey = parts[3]
	case len(parts) == 3 && parts[0] == "" && parts[1] == "exchanges":
		exchange = parts[2]
		routingKey = ""
	}

	return exchange, routingKey
}
