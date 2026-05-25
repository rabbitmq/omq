# Stream Filtering

## AMQP Properties

Publish with two properties (color, size), for each message set one of the 4 values
for a given property. Since `omq` cycles through a comma-separated list of values
and we have the same number of values for each property, they will go hand in hand:
all messages with `color=red` will also have `size=small` and so on.

```
omq amqp \
    --queues stream \
    -t /queues/prop-filter-1 \
    -T /queues/prop-filter-1 \
    --stream-offset first \
    --amqp-app-property "color=red,rose,blue,green" \
    --amqp-app-property "size=small,slim,large,huge"
    --amqp-app-property-filter "color=&p:r" \
    --amqp-app-property-filter "size=&p:s" \
```

To have random combinations of property values, we can use templating
to pick a random value from the list of possible values:

```
omq amqp \
    --queues stream \
    -t /queues/prop-filter-2 \
    -T /queues/prop-filter-2 \
    --stream-offset first \
    --amqp-app-property 'color={{ index (list "red" "rose" "blue" "green") (randInt 0 4) }}' \
    --amqp-app-property 'size={{ index (list "small" "slim" "large" "huge") (randInt 0 4) }}' \
    --amqp-app-property-filter "color=&p:r" \
    --amqp-app-property-filter "size=&p:s"
```
