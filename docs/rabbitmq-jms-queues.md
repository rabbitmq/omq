# Using With RabitMQ JMS Queues

## Prerequisites

- Tanzu RabbitMQ 4.3+ with the `rabbitmq_jms` plugin enabled
- `omq` 0.46.0 or newer

## Declaring JMS Queues

Use `--queues jms` to declare a JMS queue. The `x-selector-fields` argument controls
which message properties are held in memory for selector evaluation. Use `*` for all
application properties.

```bash
# Declare a JMS queue with all application properties indexed
omq amqp --queues jms \
  --queue-args 'x-selector-fields=*' \
  -t /queues/my-jms-queue -T /queues/my-jms-queue \
  -x 1 -C 10 -y 0

# Declare with specific fields indexed (comma-separated)
omq amqp --queues jms \
  --queue-args 'x-selector-fields=color,region,priority' \
  -t /queues/my-jms-queue -T /queues/my-jms-queue \
  -x 1 -C 10 -y 0

# Declare with selector field max bytes limit
omq amqp --queues jms \
  --queue-args 'x-selector-fields=*,x-selector-field-max-bytes=128' \
  -t /queues/my-jms-queue -T /queues/my-jms-queue \
  -x 1 -C 10 -y 0
```

**Important**: `x-selector-fields` must be a list. When using `--queues jms`, omq
automatically converts the comma-separated string to a list. If you declare the queue
via other means, ensure this argument is a proper list (e.g. `["*"]`), not a plain string.

## Publishing Messages with Application Properties

Use `--amqp-app-property` to set application properties on messages.
Values are cycled through comma-separated entries.

```bash
# Publish messages with different color property values
omq amqp --queues predeclared \
  -t /queues/my-jms-queue \
  -x 1 -C 10 -y 0 \
  --amqp-app-property 'color=red,blue,green,yellow'

# Publish with multiple properties
omq amqp --queues predeclared \
  -t /queues/my-jms-queue \
  -x 1 -C 10 -y 0 \
  --amqp-app-property 'color=red,blue,green' \
  --amqp-app-property 'size=small,large'
```

## Consuming with JMS Message Selectors

Use `--amqp-jms-selector` to filter messages using selector expressions.

### Basic Equality

```bash
# Consume only red messages
omq amqp --queues predeclared \
  -T /queues/my-jms-queue \
  -x 0 -y 1 -D 100 -z 10s \
  --amqp-jms-selector "color = 'red'"
```

### IS NULL / IS NOT NULL

```bash
# Consume messages where a property is set
omq amqp --queues predeclared \
  -T /queues/my-jms-queue \
  -x 0 -y 1 -D 100 -z 10s \
  --amqp-jms-selector "color IS NOT NULL"
```

## Message Priority

JMS queues support two priority levels: normal (0-4) and expedited (5-9).
Higher priority messages are delivered first.

```bash
# Publish with mixed priorities
omq amqp --queues jms \
  --queue-args 'x-selector-fields=*' \
  -t /queues/jms-priority -T /queues/jms-priority \
  -C 100 -y 0 --detect-out-of-order-messages \
  --message-priority '{{ randInt 0 10 }}'

# Consume and observe priority ordering
omq amqp --queues predeclared \
  -T /queues/jms-priority \
  -x 0 -D 100 -z 10s --log-level debug --detect-out-of-order-messages
```

Note: if you want to use `--detect-out-of-order-messages` when consuming, it also
has to be specified when publishing, since it sets annotations used for detecting
out-of-sequence messages.

Alternatively, you can skip this flag altogether and just look at the output - with
`--log-level debug` omq prints the priority of messages it received.

## Consumer Priority

JMS queues support three consumer priority levels: -1, 0, and 1.
Higher priority consumers receive messages first.

```bash
# Start priority 0 consumer
omq amqp --queues jms -x 0 \
  -T /queues/jms-consumer-priority \
  --consumer-priority 0

# Start a publisher
omq amqp --queues jms -y 0 \
  -t /queues/jms-consumer-priority --rate 1

# Start a higher priority consumer - you should see that the previous consumer
# no longer receives messages, while this higher-priority consumer takes over
omq amqp --queues jms -x 0 \
  -T /queues/jms-consumer-priority \
  --consumer-priority 1
```

## Queue Overflow (reject-publish)

JMS queues support `x-max-length` with `reject-publish` overflow strategy
(the `reject-publish` strategy is the default for JMS queues).

```bash
# Declare queue with max length 5 and reject-publish
omq amqp --queues jms \
  --queue-args 'x-selector-fields=*,x-max-length=5' \
  -t /queues/jms-overflow -T /queues/jms-overflow \
  -x 1 -C 20 -y 0
```

## AMQP 0-9-1 Interoperability

Messages published via AMQP 0-9-1 with headers can be consumed via AMQP 1.0
with JMS selectors. AMQP 0-9-1 headers are exposed as application properties
to AMQP 1.0 consumers.

```bash
# Publish with AMQP 0-9-1, consume with AMQP 1.0 JMS selector
omq amqp091-amqp --queues jms \
        --queue-args 'x-selector-fields=*' \
        -t /queues/jms-interop -T /queues/jms-interop \
        -x 1 --amqp091-header color=red,blue,green,yellow \
        -y 1 --amqp-jms-selector "color = 'red'"
```

The consumption rate should be roughly 25% of the publishing rate,
since the consumer only consumes messages with one of the 4 color values.

## Delayed Messages

Publish messages with a 30 second delay:
```
omq amqp --queues jms \
  --queue-args 'x-selector-fields=*' \
  -t /queues/jms-delay -T /queues/jms-delay \
  -x 1 -C 100 -y 0 \
  --amqp-msg-annotation 'x-opt-delivery-time={{ now | date_modify "+30s" | unixEpoch | mul 1000 }}'
```

The `x-opt-delivery-time` value must be in **milliseconds** since the Unix epoch.
`unixEpoch` sprig function returns seconds, so `| mul 1000` is required.
Alternative expression to delay by 30s would be `{{ now | unixEpoch | add 30 | mul 1000 }}`.

## Browsing Mode

RabbitMQ JMS queues support browsing mode - messages are dispatched
to the consumer but not removed from the queue. Use `--amqp-browse`
to enable this mode.
