# Watermill AMQP Pub/Sub
<img align="right" width="200" src="https://watermill.io/img/gopher.svg">

This is Pub/Sub for the [Watermill](https://watermill.io/) project.

## BigCommerce fork

Forked from [ThreeDotsLabs/watermill-amqp](https://github.com/ThreeDotsLabs/watermill-amqp) at
`v3.1.0`, with the module path rewritten to `github.com/bigcommerce/watermill-amqp/v3`. Import it as
that path; a `replace` directive is not enough, because Go honours `replace` only in the main
module's `go.mod` and so ignores one added by an intermediate library.

**What diverges from upstream:** the subscriber sends `basic.reject` rather than `basic.nack` when a
delivery attempt fails. From RabbitMQ 4.3, a quorum queue's `delivery-limit` is evaluated against
`delivery-count`, which `basic.nack` does not increment - so nacking a message the handler can never
process redelivers it forever and never dead-letters it. `basic.reject` increments both counters.
The shutdown paths still nack, so requeueing during a rolling deploy does not consume the queue's
`delivery-limit` budget. On brokers before 4.3 both verbs incremented the single counter, so this
matches how these queues behaved there.

Consumers should know that every `msg.Nack()` now counts as a failed delivery, transient failures
included, and that RabbitMQ 4.0+ defaults `delivery-limit` to 20 on quorum queues. A queue without a
dead-letter exchange therefore drops a message that keeps failing rather than looping it. Check your
queues have a dead-letter exchange before taking this fork.

### `DefaultMarshaler` needs `PreprocessDelivery` on quorum queues

**On a quorum queue, `DefaultMarshaler` cannot read a redelivered message unless you give it a
`PreprocessDelivery` that stringifies headers.** This is not specific to the fork, but the fork makes
it much easier to get burned by, so it is worth stating plainly.

A quorum queue attaches int-typed counter headers to a redelivery - `x-delivery-count` on any
version, plus `x-acquired-count` from 4.3. `DefaultMarshaler.Unmarshal` requires every header to be a
string and returns an error otherwise, so it fails on redelivery for that reason alone, before the
handler is reached. `processMessage` then rejects the message itself.

The consequence is that a *single* `msg.Nack()` is enough to exhaust `delivery-limit`, because each
redelivery fails at unmarshal in milliseconds rather than being retried by your handler. That
defeats the point of the limit, and it also defeats the shutdown paths above: a message requeued by a
rolling deploy comes back carrying a counter header, so the next consumer cannot read it either.

Fix it when you build the config:

```go
import (
	stdAmqp "github.com/rabbitmq/amqp091-go"
	"github.com/bigcommerce/watermill-amqp/v3/pkg/amqp"
)

config.Marshaler = amqp.DefaultMarshaler{
	PreprocessDelivery: func(delivery stdAmqp.Delivery) stdAmqp.Delivery {
		for key, value := range delivery.Headers {
			if _, ok := value.(string); !ok {
				delivery.Headers[key] = fmt.Sprintf("%v", value)
			}
		}

		return delivery
	},
}
```

A custom marshaler that tolerates non-string headers works equally well. If you are reaching this
library through an internal wrapper, check whether it already does this before adding your own.

The fix has been offered upstream, so expect rebases onto later ThreeDotsLabs releases: keep the
BigCommerce-specific changes (module path, CircleCI config) separate from the fix itself so it stays
cherry-pickable.


See [DEVELOPMENT.md](./DEVELOPMENT.md) for more information about running and testing.

Watermill is a Go library for working efficiently with message streams. It is intended
for building event driven applications, enabling event sourcing, RPC over messages,
sagas and basically whatever else comes to your mind. You can use conventional pub/sub
implementations like Kafka or RabbitMQ, but also HTTP or MySQL binlog if that fits your use case.

All Pub/Sub implementations can be found at [https://watermill.io/pubsubs/](https://watermill.io/pubsubs/).

Documentation: https://watermill.io/

Getting started guide: https://watermill.io/docs/getting-started/

Issues: https://github.com/ThreeDotsLabs/watermill/issues

## Contributing

All contributions are very much welcome. If you'd like to help with Watermill development,
please see [open issues](https://github.com/ThreeDotsLabs/watermill/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+)
and submit your pull request via GitHub.

## Support

If you didn't find the answer to your question in [the documentation](https://watermill.io/), feel free to ask us directly!

Please join us on the `#watermill` channel on the [Three Dots Labs Discord](https://discord.gg/QV6VFg4YQE).

## License

[MIT License](./LICENSE)
