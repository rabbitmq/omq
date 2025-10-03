## omq

`omq` is a messaging system client for testing purposes. It currently supports AMQP 1.0, AMQP 0.9.1, STOMP and MQTT 3.1/3.1.1/5.0. It is developed mostly for RabbitMQ but might be useful for other brokers
as well (some tests against ActiveMQ were performed).

`omq` starts a group of publishers and a group of consumers, in both cases all publishers/consumers are identical,
except for the target terminus/queue/routing key, which may be slightly different. The publishers can use
a different protocol than the consumers.

`omq` has subcommands for all protocol combinations. For example:
```shell
$ omq stomp-amqp
```
will publish via STOMP and consume via AMQP 1.0. Note that starting with RabbitMQ 4.0, RabbitMQ doesn't automatically
declare queues for AMQP-1.0 subscriber, so a queue would need to exist for this example to work; if you want `omq` to
declare a queue, use `--queues`; see below for more about topic/queue/routing key details).

A more complex example:
```shell
$ omq mqtt-amqp --publishers 10 --publish-to 'sensor/%d' --rate 1 --size 100 \
                --consumers 1 --consume-from /queues/sensors --binding-key 'sensor.#' --queues classic
```
will start 10 MQTT publishers, each publishing 1 message a second, with 100 bytes of payload, to the `amq.topic` exchange (default for the MQTT plugin)
with the topic/routing key of `sensor/%d`, where the `%d` is the ID of the publisher (from 1 to 10). It will also start a single AMQP 1.0 consumer that
consumes all those messages by declaring a classic queue `sensors` with a wildcard subscription.

If the publishing and consuming protocol is the same, you can use abbreviated commands: `amqp` instead of `amqp-amqp`, `stomp` instead of `stomp-stomp`
and `mqtt` instead of `mqtt-mqtt`.

### Installation

```shell
$ go install github.com/rabbitmq/omq@main
```

An [OCI image](https://hub.docker.com/r/pivotalrabbitmq/omq/tags) is also available: `pivotalrabbitmq/omq`.

### Connecting to the Broker

Both `--publisher-uri` and `--consumer-uri` can be repeated multiple times to set multiple
endpoints. If `omq` can't establish a connection or an existing connection is terminated,
it will try the next URI from the list. If the endpoints are the same for publishers and consumers,
you can use `--uri` instead (but can't mix `--uri` with `--publisher-uri` and `--consumer-uri`).

For example, here both publishers and consumers will connect to either of the 3 URIs:
```shell
$ omq mqtt --uri mqtt://localhost:1883 --uri mqtt://localhost:1884 --uri mqtt://localhost:1885
```

And in this case, all consumers will connect to port 1883, while publishers to 1884:

```shell
$ omq mqtt --consumer-uri mqtt://localhost:1883 --publisher-uri mqtt://localhost:1884
```

### Terminus/Topic/Queue/Routing Key

Different protocols refer to the targets / sources of messages differently and RabbitMQ handles each protocol differently as well.

`--publish-to` (or `-t`) refers to where to publish the messages - it is passed as-is to the publisher, except for MQTT (see below)
`--consume-from` (or `-T`) refers to where to consume the messages from - it is passed as-is to the consumer, except for MQTT (see below)

For convenience, if either `--publish-to` or `--consume-from` starts with `/exchange/amq.topic/` or `/topic/`, MQTT publisher/consumer
will remove that prefix. RabbitMQ only allows using a single topic exchange with MQTT (`amq.topic` by default), so this prefix doesn't make
much sense. Removing it makes it easier to use the same parameters across protocols.

AMQP 0.9.1 publishers use the same target address as AMQP 1.0:
* `/queues/foo` will publish to the default exchange with the routing key `foo`
* `/exchange/bar` will publish to the `bar` exchange with an empty routing key
* `/exchange/bar/baz` will publish to the `bar` exchange with the routing key `baz`
* any `--publish-to` value that doesn't match any of the above formats is treated as a routing key for the default exchange

Read more about how RabbitMQ handles sources and targets in different protocols:
* [AMQP 1.0](https://www.rabbitmq.com/docs/amqp#address-v1) format used by RabbitMQ 3.x
* [AMQP 1.0](https://www.rabbitmq.com/docs/amqp#address-v2) format used by RabbitMQ 4.0+ (the old format is still supported but deprecated)
* [MQTT](https://www.rabbitmq.com/docs/mqtt#topic-level-separator-and-wildcards)
* [STOMP](https://www.rabbitmq.com/docs/stomp#d)

### AMQP-1.0 Stream Filter Support

RabbitMQ 4.1 added [AMQP-1.0 stream filtering support](https://github.com/rabbitmq/rabbitmq-server/pull/12415).
Note that this is a separate feature from stream filtering of the Stream protocol.

`omq` supports AMQP-1.0 stream filtering in the following ways:
1. When publishing, you can specify application properties. If multiple values are provided, one of them is used for each message
   (so you get a mix of messages with different values). For example, `--amqp-app-property key=foo,bar,baz` will publish some messages
   with `key=foo`, some with `key=bar` and some with `key=baz` (in roughly equal proportions).
2. When consuming, you can apply a filter, for example, `--amqp-app-property-filter key=&p:ba` will tell RabbitMQ to only deliver
   messages where the `key` property starts with `ba` (`&p:` means that what follows is a prefix), so it'll return roughly 66%
   of the messages in the stream. You can filter on properties (eg. `subject`) or application properties.

Here's a full example, where we can see this in action:
```shell
$ omq amqp --queues stream -t /queues/stream -T /queues/stream --rate 100 --amqp-app-property key=foo,bar,baz --amqp-app-property-filter key=foo
2024/10/04 13:48:26 INFO consumer started id=1 terminus=/queues/stream
2024/10/04 13:48:26 INFO publisher started id=1 rate=100 destination=/queues/stream
2024/10/04 13:48:27 published=95/s consumed=32/s
2024/10/04 13:48:28 published=100/s consumed=33/s
2024/10/04 13:48:29 published=100/s consumed=34/s
```

We publish 100 messages per second with 3 different key values and then consume only messages with one of the values. Therefore, the consumption
rate is one third of the publishing rate.

### Templated Values

Some flags are parsed as Go text templates and provide additional functions from the [Sprig](https://masterminds.github.io/sprig/) library.
This allows setting random or otherwise generated values. For example:

* `--message-priority '{{ randInt 1 10 }}'` set a random message priority from the given range
* `--consumer-latency '{{ randInt 1 60 }}ms'` wait a random (between 1 and 60) number of milliseconds before acking a message
* `--publishers 10 --publish-to '/queues/cq-{{ mod .id 2 }}'` declare 2 queues and start 5 publishers per queue

### Metrics

`omq` exposes Prometheus metrics on port 8080, or the next available port if 8080 is in use (so 8081, 8082, and so on). This makes it easy to run multiple
`omq` instances on a single machine - just configure 8080 and the next few ports as Prometheus targets and it'll scrape them whenever metrics are available.
Sample Prometheus scrape config:
```yaml
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

Use `omq --help` for the full list of options. Keep in mind that some options are protocol-specific and therefore will only
be printed with the corresponding subcommand. For example `omq mqtt --help` will additionally show MQTT-specific options.
