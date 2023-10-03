## omq

`omq` is a messaging system client for testing purposes. It currently supports AMQP-1.0, STOMP and MQTT 3.1.
It is developed mostly for RabbitMQ but might be useful for other broker as well (some tests against ActiveMQ
were performed).

`omq` starts a group of publishers and a group of consumers, in both cases all publishers/consumers are identical,
except for the target termins/queue/routing key, which may be slightly different. The publishers can use
a different protocol than the consumers.

`omq` has subcommands for all protocol combinations. For example:
```
./omq stomp-amqp
```
will publish via STOMP and consume via AMQP (see below for the topic/queue/routing key details). A more complex example:
```
./omq mqtt-amqp --publishers 10 --consumers 1 --publish-to 'sensor/%d' --consume-from '/topic/sensor.#' --rate 1 --size 100
```
will start 10 MQTT publishers, each publishing 1 message a second, with a 100 bytes of payload, to the `amq.topic` exchange (default for the MQTT plugin)
with the topic/routing key of `sensor/%d`, where the `%d` is the ID of the publisher (from 1 to 10). It will also start a single AMQP 1.0 consumer that
consumes all those messages thanks to a wildcard subscription.

If the publishing and consuming protocol is the same, you can use abbreviated commands: `amqp` instead of `amqp-amqp`, `stomp` instead of `stomp-stomp`
and `mqtt` instead of `mqtt-mqtt`.

### Installation

```
go install github.com/rabbitmq/omq@main
```

### Metrics

`omq` exposes Prometheus metrics on port 8080 or the next available port if 8080 is in use (so 8081, 8082 and so on). This makes it easy to run multiple
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

Additionally, the final values of the metrics are printed when `omq` finishes. A nicer output or TUI will be available at some point.

### Comaptibility with perf-test

[perf-test](https://perftest.rabbitmq.com/) is the main testing tool used with RabbitMQ. It has many more options, but only supports AMQP 0.9.1
(historically, the main protocol used with RabbitMQ). `omq` uses the same message format for end-to-end latency measurment and therefore
messages published with perf-test can be consumed by `omq` or vice versa, and the end-to-end latency will be measured.

### Options

```
      --amqp-subject string                 AMQP 1.0 message subject
  -D, --cmessages int                       The number of messages to consume per consumer (default=MaxInt)
  -T, --consume-from string                 The queue/topic/terminus to consume from (%d will be replaced with the consumer's id) (default "/topic/omq")
      --consumer-uri string                 URI for consuming
  -y, --consumers int                       The number of consumers to start (default 1)
  -d, --message-durability                  Mark messages as durable (default=true) (default true)
  -C, --pmessages int                       The number of messages to send per publisher (default=MaxInt)
  -t, --publish-to string                   The topic/terminus to publish to (%d will be replaced with the publisher's id) (default "/topic/omq")
      --publisher-uri string                URI for publishing
  -x, --publishers int                      The number of publishers to start (default 1)
      --queue-durability queue-durability   Queue durability (default: configuration - the queue definition is durable) (default configuration)
  -r, --rate int                            Messages per second (-1 for unlimited; default=-1)
  -s, --size int                            Message payload size in bytes (default 12)
  -m, --use-millis                          Use milliseconds for timestamps (automatically enabled when no publishers or no consumers)
  -z, --time duration                       Run duration (eg. 10s, 5m, 2h)
  -h, --help                                help for omq
```
