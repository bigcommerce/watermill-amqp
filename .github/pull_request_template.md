<!--
This is BigCommerce's fork of ThreeDotsLabs/watermill-amqp.

Keep the fix itself separate from BigCommerce-specific changes (module path, CircleCI) so it stays
cherry-pickable upstream. See the fork section in README.md.

If this change belongs upstream, raise it against ThreeDotsLabs/watermill-amqp as well, branched
from upstream/master rather than from this fork's master.
-->

## What/Why?

### Human

<!-- Plain English: the problem, and what you did about it. No class names, paths or config keys. -->

### Agent

<!-- The full technical detail: what changed, why, and what you considered and rejected. -->

## Rollout/Rollback

<!--
Nothing deploys from this repo - it reaches services through the intermediate library that depends
on it. Note the tag you expect to cut, and how to roll back (normally a version pin downstream).
-->

## Testing

<!--
`make up` starts RabbitMQ, `make test` runs the suite, `make test_short` skips the broker-heavy
tests. For a change to ack/nack behaviour, say how you proved the test fails without the fix.
-->
