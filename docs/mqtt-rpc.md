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
