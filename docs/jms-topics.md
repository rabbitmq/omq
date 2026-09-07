# Using With RabbitMQ JMS Topics

## Prerequisites

- Tanzu RabbitMQ 4.4+ with the `rabbitmq_jms` plugin enabled
- `omq` 0.55 or newer

`rabbitmq_jms` maps JMS Topics onto the [AMQP 1.0 JMS binding
map](https://groups.oasis-open.org/higherlogic/ws/public/document?document_id=65490)
on top of the `amq.topic` exchange. A connection is only treated as a JMS
client if it advertises a `product` connection property containing `jms`
(case-insensitive), the way Qpid JMS does; only then is it offered topic
subscriptions, `noLocal` filtering, temporary topics, etc. `omq` sets this
for you via `--amqp-jms-client`.

Without `--amqp-jms-client`, `--amqp-source-capability topic` and friends are
still sent, but the broker treats the connection as a plain AMQP 1.0 client and
rejects the link instead of creating a JMS-style subscription.

## Producing to a Topic

Use `--amqp-target-capability topic` to attach the sender's target directly to
a topic name (this is the AMQP 1.0 equivalent of a JMS `Topic` destination):

```bash
omq amqp \
  --amqp-jms-client \
  --amqp-target-capability topic \
  --publish-to orders.new \
  --consumers 0 -C 10
```

The topic name is used as-is as the `amq.topic` binding/routing key, so it
follows AMQP 0-9-1 topic conventions (`.`-separated words, `*`/`#`
wildcards on the subscribing side) rather than MQTT's `/`-separated ones.

## Subscribing to a Topic

`--amqp-source-capability topic` subscribes to a topic. Combine with `shared`
and/or `global` to mirror the JMS subscription types:

```bash
# Unshared, non-durable (the default "vanilla" topic subscription)
omq amqp \
  --amqp-jms-client \
  --amqp-source-capability topic \
  --queue-durability none \
  --consume-from orders.new \
  --publishers 0

# Shared, non-durable, scoped to this connection's client ID (container-id)
omq amqp \
  --amqp-jms-client \
  --amqp-source-capability topic --amqp-source-capability shared \
  --amqp-link-name "order-consumers|group1" \
  --queue-durability none \
  --consume-from orders.new \
  --publishers 0

# Shared, non-durable, global (no client ID set by the app)
omq amqp \
  --amqp-jms-client \
  --amqp-source-capability topic --amqp-source-capability shared --amqp-source-capability global \
  --amqp-link-name "order-consumers|group1" \
  --queue-durability none \
  --consume-from orders.new \
  --publishers 0
```

`--queue-durability` defaults to `configuration`, which (together with
`--amqp-jms-client`, which also forces expiry-policy to `never`) makes the
subscription **durable**. Pass `--queue-durability none` explicitly for
non-durable subscriptions, as shown above.

### Durable Subscriptions

A durable subscription needs a stable link name so it can be resumed later.
`--amqp-link-name` supports `%d` for the consumer's id, same as
`--consumer-id`:

```bash
# Create/resume an unshared durable subscription
omq amqp \
  --consumer-id order-app-1 \
  --amqp-jms-client \
  --amqp-source-capability topic \
  --amqp-link-name my-durable-sub \
  --queue-durability configuration \
  --consume-from orders.new \
  --publishers 0 -D 100
```

The resulting queue name encodes the subscription's shape, e.g.
`jms-sub~udl~order-app-1~my-durable-sub` (**u**nshared, **d**urable,
**l**ocal messages delivered). Detaching without closing the link (i.e.
without draining the last consumer) leaves the queue and its bindings in
place; run the same command again later to resume consuming from it.

### Temporary Topics

`omq` doesn't currently expose the dynamic-target flow necessary to
create temporary topics.

## `noLocal` Filtering

`--amqp-no-local` adds the JMS `no-local` filter to a subscription: messages
published on a connection with the *same container-id* (JMS client ID) as the
subscriber are not delivered back to it. It only applies to unshared
subscriptions.

```bash
# Subscriber, client ID "app-1", noLocal enabled
omq amqp \
  --consumer-id app-1 \
  --amqp-jms-client \
  --amqp-source-capability topic --amqp-no-local \
  --amqp-link-name sub1 \
  --queue-durability none \
  --consume-from orders.new \
  --publishers 0

# Publish from the SAME client ID -> filtered out, not delivered
omq amqp \
  --publisher-id app-1 \
  --amqp-jms-client \
  --amqp-target-capability topic \
  --publish-to orders.new \
  --consumers 0 -C 1

# Publish from a DIFFERENT client ID -> delivered
omq amqp \
  --publisher-id app-2 \
  --amqp-jms-client \
  --amqp-target-capability topic \
  --publish-to orders.new \
  --consumers 0 -C 1
```

A message dropped by `noLocal` (with no other matching queue) is settled with
the `RELEASED` outcome rather than silently disappearing; this is visible with
`--log-level debug` on the publisher.

## Selectors on Topic Subscriptions

`--amqp-jms-selector` works the same way as it does for [JMS
queues](rabbitmq-jms-queues.md): the subscription queue is bound indirectly,
via a generated `x-jms-selector` exchange sitting between `amq.topic` and the
queue.

```bash
# Subscriber, selector matches only color=red
omq amqp \
  --amqp-jms-client \
  --amqp-source-capability topic \
  --amqp-link-name sub-red \
  --queue-durability none \
  --consume-from orders.new \
  --amqp-jms-selector "color = 'red'" \
  --publishers 0

# Publish a matching message -> delivered
omq amqp \
  --amqp-jms-client \
  --amqp-target-capability topic \
  --publish-to orders.new \
  --amqp-app-property "color=red" \
  --consumers 0 -C 1

# Publish a non-matching message -> not delivered, released like the noLocal example above
omq amqp \
  --amqp-jms-client \
  --amqp-target-capability topic \
  --publish-to orders.new \
  --amqp-app-property "color=blue" \
  --consumers 0 -C 1
```

## Cross-Protocol Interoperability

JMS topic subscriptions are just bindings on `amq.topic`, so messages
published via AMQP 0-9-1, STOMP or MQTT are delivered to them like to any
other topic-exchange binding (subject to the routing key / topic name
translation each protocol applies):

```bash
# JMS topic subscriber ("/" in MQTT maps to "." in AMQP 0-9-1, so the topic
# name here must be given in its AMQP 0-9-1 form)
omq amqp \
  --amqp-jms-client \
  --amqp-source-capability topic \
  --amqp-link-name sub-interop \
  --queue-durability none \
  --consume-from test.interop \
  --publishers 0

# MQTT publisher, in another terminal, using the MQTT form of the same topic
omq mqtt --publish-to test/interop --consumers 0 -C 1
```
