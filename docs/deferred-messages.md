# Requesting Deferred Messages by Token

RabbitMQ quorum queues support parking an AMQP 1.0 message under a client-chosen
**deferral token** (by settling it with the MODIFIED outcome and both
`x-opt-deferral-token` and `x-opt-delivery-time` message annotations), then pulling it
back on demand by token instead of waiting for it to become eligible for normal
redelivery. See RabbitMQ's own documentation of the protocol for the full details.

This only works against **quorum queues**; the broker advertises support for it via the
`rabbitmq:deferral-tokens` symbol in the `offered-capabilities` of the ATTACH response.

`omq` doesn't create deferral tokens itself - parking a message under a token is just a
MODIFIED outcome with two annotations, so the existing `--amqp-modify` flag already
covers it (see below). What `omq` adds is the *retrieval* side: requesting back only the
messages parked under given token(s).

## Parking a message under a token

Use `--amqp-modify-rate 100` so every consumed message is settled with the MODIFIED
outcome, and `--amqp-modify` to set the token and a delivery time far in the future
(milliseconds since the Unix epoch):

```
omq amqp \
    --queues quorum \
    -t /queues/deferred-demo \
    -T /queues/deferred-demo \
    --pmessages 1 --cmessages 1 \
    --amqp-modify-rate 100 \
    --amqp-modify "x-opt-deferral-token=job-42,x-opt-delivery-time=9999999999999"
```

The message is now parked: it's neither ready nor unacknowledged, but still counted in
the queue's message count.

## Requesting deferred messages back

```
omq amqp \
    -T /queues/deferred-demo \
    --publishers 0 --consumers 1 \
    --amqp-request-deferred-token job-42 \
    --amqp-request-deferred-credit 1
```

`omq` sends a single FLOW frame carrying `rabbitmq:deferral-tokens` (with `job-42`) and
the requested link credit, then waits for exactly that many messages before finishing:

```
INFO requesting deferred messages id=0 terminus=/queues/deferred-demo tokens=[job-42] credit=1
INFO deferred retrieval finished id=0 terminus=/queues/deferred-demo tokens=[job-42] requested=1 received=1
```

Retrieved messages are accepted (settled) as they're received.

### Multiple tokens

Repeat `--amqp-request-deferred-token` to request several tokens at once - RabbitMQ
accepts an array of tokens in a single FLOW, so `omq` always combines them into one
request rather than issuing one FLOW per token. Size `--amqp-request-deferred-credit` to
the total number of messages expected across all the tokens:

```
omq amqp \
    -T /queues/deferred-demo \
    --publishers 0 --consumers 1 \
    --amqp-request-deferred-token job-42 \
    --amqp-request-deferred-token job-43 \
    --amqp-request-deferred-credit 2
```

### Unmatched tokens

A token that doesn't resolve to any parked message (already expired and redelivered
normally, or never issued) produces no delivery at all - there's no error to react to.
`--amqp-request-deferred-timeout` (default `5s`) bounds how long `omq` waits for the
requested credit's worth of messages before giving up and reporting how many were
actually received:

```
INFO deferred retrieval finished id=0 terminus=/queues/deferred-demo tokens=[unknown-token] requested=1 received=0
```

## Caveat: credit is shared with normal delivery

The credit granted alongside `rabbitmq:deferral-tokens` is ordinary link credit - it is
also available for the broker to dispatch normal, non-deferred ready messages on the same
link. If the queue has other ready messages competing for that credit, they can consume it
before the deferred assignment runs, and the retrieval will appear to return fewer
messages than expected. For predictable results, point this at a queue where the only
outstanding messages are the ones parked under the requested token(s).
