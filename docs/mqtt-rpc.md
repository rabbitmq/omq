# MQTT RPC without shared subscriptions

RabbitMQ does not support MQTT shared subscriptions (`$share/...`). With
`omq mqtt-rpc`, `--consumers` greater than 1 on a single request topic does
not load-balance: every responder receives every request and sends its own reply.

Shard on the client. Give each responder its own topic (`%d` is the responder
id) and pin each publisher to one responder with `mod`:

```shell
omq mqtt-rpc --consumers 2 --publishers 10 \
    --consume-from 'rpc/request/%d' \
    --publish-to 'rpc/request/{{ mod .id 2 }}'
```

The `2` in `mod .id 2` is the responder count. Publisher 0 and publisher 2 both
send to `rpc/request/0`; publisher 1 sends to `rpc/request/1`.

Topics that expand `%d` or `{{.id}}` on both `--publish-to` and `--consume-from`
pair publisher N with consumer N instead. Those counts need to match, or the
extra publishers time out and the extra responders sit idle.

`omq` prints a warning at startup in both of these cases.

## More examples

### A sharded service under realistic load

Four responders, each with its own request topic, serving 20 requesters. Every
requester keeps up to 5 calls outstanding and issues 50 calls per second. Requests
are 1 KB, replies are 4 KB, and each responder spends 20 ms "processing" a request
before it replies. Both legs use QoS 1, and a call is given up on after 2 seconds:

```shell
omq mqtt-rpc --publishers 20 --consumers 4 \
    --consume-from 'rpc/orders/%d' \
    --publish-to 'rpc/orders/{{ mod .id 4 }}' \
    --rate 50 --max-in-flight 5 \
    --size 1kb --mqtt-reply-size 4kb \
    --consumer-latency 20ms \
    --mqtt-publisher-qos 1 --mqtt-consumer-qos 1 \
    --mqtt-rpc-timeout 2s \
    --time 60s
```

`--consumer-latency` is applied by each responder on a single worker, so a responder
cannot sustain more than 50 replies per second here. Five requesters share each
responder (250 calls per second offered), so the calls queue up. Watch
`omq_roundtrip_latency_seconds` grow and `omq_rpc_timeouts_total` start to increase
once the offered load exceeds what the responders can handle. Lower `--rate` or add
responders (and raise the `mod` divisor to match) to bring it back down.

### One responder per requester, with a fixed response topic

When the request topic contains `%d` on both sides, requester N talks only to
responder N. This is useful for measuring the latency of a single client/server pair.
Here each of the 5 pairs makes 1000 sequential calls (`--max-in-flight` defaults to 1),
and the replies go to a response topic of your choosing instead of the generated one
(`%d` is the requester id):

```shell
omq mqtt-rpc --publishers 5 --consumers 5 \
    --consume-from 'rpc/pair/%d' \
    --publish-to 'rpc/pair/%d' \
    --mqtt-response-topic 'rpc/pair/%d/reply' \
    --pmessages 1000
```

Without `--mqtt-response-topic`, the topic includes a random per-invocation token so
that several `omq` processes can run side by side. A fixed topic is only safe if one
`omq` process uses it at a time.
