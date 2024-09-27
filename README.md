## omq

`omq` is a messaging system client for testing purposes. It currently supports AMQP-1.0, STOMP and MQTT 3.1.1.
It is developed mostly for RabbitMQ but might be useful for other brokers as well (some tests against ActiveMQ
were performed).

`omq` starts a group of publishers and a group of consumers, in both cases all publishers/consumers are identical,
except for the target terminus/queue/routing key, which may be slightly different. The publishers can use
a different protocol than the consumers.

`omq` has subcommands for all protocol combinations. For example:
```
./omq stomp-amqp
```
will publish via STOMP and consume via AMQP 1.0. Note that starting with RabbitMQ 4.0, RabbitMQ doesn't automatically
declare queues for AMQP-1.0 subscriber, so a queue would need to exist for this example to work; if you want `omq` to
declare a queue, use `--queues`; see below for more about topic/queue/routing key details).

A more complex example:
```
./omq mqtt-amqp --publishers 10 --publish-to 'sensor/%d' --rate 1 --size 100 \
                --consumers 1 --consume-from /queues/sensors --amqp-binding-key 'sensor.#' --queues classic
```
will start 10 MQTT publishers, each publishing 1 message a second, with 100 bytes of payload, to the `amq.topic` exchange (default for the MQTT plugin)
with the topic/routing key of `sensor/%d`, where the `%d` is the ID of the publisher (from 1 to 10). It will also start a single AMQP 1.0 consumer that
consumes all those messages by declaring a classic queue `sensors` with a wildcard subscription.

If the publishing and consuming protocol is the same, you can use abbreviated commands: `amqp` instead of `amqp-amqp`, `stomp` instead of `stomp-stomp`
and `mqtt` instead of `mqtt-mqtt`.

### Installation

```
go install github.com/rabbitmq/omq@main
```

An [OCI image](https://hub.docker.com/r/pivotalrabbitmq/omq/tags) is also available: `pivotalrabbitmq/omq`.

### Connecting to the Broker

Both `--publisher-uri` and `--consumer-uri` can be repeated multiple times to set multiple
endpoints. If `omq` can't establish a connection or an existing connection is terminated,
it will try the next URI from the list. If the endpoints are the same for publishers and consumers,
you can use `--uri` instead (but can't mix `--uri` with `--publisher-uri` and `--consumer-uri`).

For example, here both publishers and consumers will connect to either of the 3 URIs:
```
omq mqtt --uri mqtt://localhost:1883 --uri mqtt://localhost:1884 --uri mqtt://localhost:1885
```
And in this case, all consumers will connect to port 1883, while publishers to 1884:

```
omq mqtt --consumer-uri mqtt://localhost:1883 --publisher-uri mqtt://localhost:1884
```

### Terminus/Topic/Queue/Routing Key

Different protocols refer to the targets / sources of messages differently and RabbitMQ handles each protocol differently as well.

`--publish-to` (or `-t`) refers to where to publish the messages - it is passed as-is to the publisher, except for MQTT (see below)
`--consume-from` (or `-T`) refers to where to consume the messages from - it is passed as-is to the consumer, except for MQTT (see below)

For convenience, if either `--publish-to` or `--consume-from` starts with `/exchange/amq.topic/` or `/topic/`, MQTT publisher/consumer
will remove that prefix. RabbitMQ only allows using a single topic exchange with MQTT (`amq.topic` by default), so this prefix doesn't make
much sense. Removing it makes it easier to use the same parameters across protocols.

Read more about how RabbitMQ handles sources and targets in different protocols:
* [AMQP 1.0](https://www.rabbitmq.com/docs/amqp#address-v1) format used by RabbitMQ 3.x
* [AMQP 1.0](https://www.rabbitmq.com/docs/amqp#address-v2) format used by RabbitMQ 4.0+ (the old format is still supported but deprecated)
* [MQTT](https://www.rabbitmq.com/docs/mqtt#topic-level-separator-and-wildcards)
* [STOMP](https://www.rabbitmq.com/docs/stomp#d)

### Metrics

`omq` exposes Prometheus metrics on port 8080, or the next available port if 8080 is in use (so 8081, 8082, and so on). This makes it easy to run multiple
`omq` instances on a single machine - just configure 8080 and the next few ports as Prometheus targets and it'll scrape them whenever metrics are available.
Sample Prometheus scrape config:
```
  - job_name: omq
    scrape_interval: 1s
    scrape_timeout: 1s
    static_configs:
      - targets:
        - localhost:8080
        - localhost:8081
        - localhost:8082
```

You can find [a simple dashboard](./dashboard/OMQ-Grafana.json) in this repo.

Additionally, metrics are printed to the console every second and a summary is printed upon termination.
Small differences between the values printed and exposed over HTTP are expected, since they are scraped
at a different point in time.

### Compatibility with perf-test

[perf-test](https://perftest.rabbitmq.com/) is the main testing tool used with RabbitMQ. It has many more options, but only supports AMQP 0.9.1
(historically, the main protocol used with RabbitMQ). `omq` uses the same message format for end-to-end latency measurment and therefore
messages published with perf-test can be consumed by `omq` or vice versa, and the end-to-end latency will be measured.

### Options

```
      --amqp-binding-key string             AMQP 1.0 consumer binding key
      --amqp-reject-rate int                Rate of messages to reject (0-100%)
      --amqp-release-rate int               Rate of messages to release without accepting (0-100%)
      --amqp-subject string                 AMQP 1.0 message subject
      --cleanup-queues                      Delete the queues at the end (only explicitly declared queues, not STOMP subscriptions)
  -D, --cmessages int                       The number of messages to consume per consumer (default=MaxInt) (default 9223372036854775807)
  -T, --consume-from string                 The queue/topic/terminus to consume from (%d will be replaced with the consumer's id) (default "/topic/omq")
      --consumer-credits int                AMQP-1.0 consumer credits / STOMP prefetch count (default 1)
  -L, --consumer-latency duration           consumer latency (time to accept message; not supported by MQTT)
      --consumer-priority int32             Consumer priority (AMQP 1.0 and STOMP)
      --consumer-uri strings                URI for consuming
  -y, --consumers int                       The number of consumers to start (default 1)
  -h, --help                                help for omq
  -l, --log-level log-level                 Log level (debug, info, error) (default info)
      --log-out-of-order-messages           Print a log line when a message is received that is older than the previously received message
  -d, --message-durability                  Mark messages as durable (default true)
      --message-priority string             Message priority (0-255, default=unset)
      --metric-tags strings                 Prometheus label-value pairs, eg. l1=v1,l2=v2
  -C, --pmessages int                       The number of messages to send per publisher (default 9223372036854775807)
  -t, --publish-to string                   The topic/terminus to publish to (%d will be replaced with the publisher's id) (default "/topic/omq")
      --publisher-uri strings               URI for publishing
  -x, --publishers int                      The number of publishers to start (default 1)
      --queues predeclared                  Type of queues to declare (or predeclared to use existing queues) (default predeclared)
      --queue-durability queue-durability   Queue durability (default: configuration - the queue definition is durable) (default configuration)
  -r, --rate float                          Messages per second (-1 = unlimited) (default -1)
  -s, --size int                            Message payload size in bytes (default 12)
      --spread-connections                  Spread connections across URIs (default true)
      --stream-filter-value-set string      Stream filter value for publisher
      --stream-filter-values string         Stream consumer filter
      --stream-offset string                Stream consumer offset specification (default=next)
  -z, --time duration                       Run duration (eg. 10s, 5m, 2h)
  -m, --use-millis                          Use milliseconds for timestamps (automatically enabled when no publishers or no consumers)
```
