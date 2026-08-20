package amqp_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	stdAmqp "github.com/rabbitmq/amqp091-go"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/bigcommerce/watermill-amqp/v3/pkg/amqp"
)

// reconnectTimeout bounds re-dialling and the resubscribe that follows it. DefaultReconnectConfig
// backs off, so this is deliberately generous relative to how long a healthy reconnect takes.
const reconnectTimeout = 30 * time.Second

// TestReconnectAfterConnectionLoss asserts that a subscriber keeps delivering on the same channel
// after its connection drops, rather than going quiet.
//
// ConnectionWrapper detects loss through amqp091-go's Connection.NotifyClose and re-dials, and the
// subscriber's ReconnectLoop then rebuilds its topology and resumes consuming. amqp091-go v1.12
// grew its own connection and channel recovery, v1.13 added topology recovery, and v1.14 is mostly
// fixes to both - all of it around the close and notify paths this depends on. Nothing here opts
// into that machinery - Config.Recovery is a nil pointer unless a caller sets it - so the point of
// this test is that the library's own reconnect still works across those versions.
//
// Closing the underlying *amqp.Connection is the cheapest faithful trigger: it fires the same
// NotifyClose a broker-side eviction or a dropped TCP connection would. Upstream's
// TestPublishSubscribe_reconnect covers restarts instead, but it shells out to
// "docker-compose restart rabbitmq", so it cannot run against a CI service container.
func TestReconnectAfterConnectionLoss(t *testing.T) {
	logger := watermill.NewStdLogger(true, false)

	topic := "test_reconnect_" + watermill.NewShortUUID()

	config := amqp.NewDurableQueueConfig(amqpURI())
	config.Queue.Arguments = stdAmqp.Table{"x-queue-type": "quorum"}
	// Quorum queues attach int-typed counter headers on redelivery, and a dropped connection is
	// counted as a failed delivery - so anything requeued here comes back with them.
	config.Marshaler = amqp.DefaultMarshaler{PreprocessDelivery: stringifyHeaders}

	conn, err := amqp.NewConnection(config.Connection, logger)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, conn.Close())
	}()

	subscriber, err := amqp.NewSubscriberWithConnection(config, logger, conn)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, subscriber.Close())
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages, err := subscriber.Subscribe(ctx, topic)
	require.NoError(t, err)
	deleteQueueOnCleanup(t, topic)

	// Baseline, so a failure after the drop cannot be blamed on the subscription never working.
	publishOne(t, config, logger, topic, "before-drop")
	receiveWithin(t, messages, "before-drop", reconnectTimeout)

	require.NoError(t, conn.Connection().Close(), "could not drop the connection under test")

	// The assertion, and deliberately the only one: receiving here proves the wrapper re-dialled,
	// the subscriber rebuilt its consumer, and delivery resumed on the original channel. Polling
	// ConnectionWrapper.IsConnected would report the first of those sooner, but it races with
	// handleConnectionClose over the connected field, so the wait is folded into this timeout.
	publishOne(t, config, logger, topic, "after-drop")
	receiveWithin(t, messages, "after-drop", reconnectTimeout)
}

// publishOne publishes a single message on a connection of its own, so that dropping the
// subscriber's connection cannot affect the publish.
func publishOne(t *testing.T, config amqp.Config, logger watermill.LoggerAdapter, topic, payload string) {
	t.Helper()

	publisher, err := amqp.NewPublisher(config, logger)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, publisher.Close())
	}()

	require.NoError(t, publisher.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte(payload))))
}

// receiveWithin reads until the given payload arrives, acking everything it takes off the channel.
//
// Two kinds of unwanted delivery are expected after a drop, which is why this skips rather than
// asserting on the first message it sees:
//
//   - A redelivery of an earlier payload, because dropping a connection requeues whatever it had
//     already prefetched, and an in-flight ack can be lost with it.
//   - One or more zero-value messages - empty UUID, empty payload, empty metadata. ProcessMessages
//     receives from the deliveries channel without checking whether it is closed, so once
//     amqp091-go closes it that select case is permanently ready and yields the zero Delivery.
//     DefaultMarshaler turns that into an empty message and it reaches the handler. That is an
//     upstream defect in its own right; this test tolerates it rather than fixing it, so that it
//     stays a test of reconnection alone.
func receiveWithin(t *testing.T, messages <-chan *message.Message, payload string, within time.Duration) {
	t.Helper()

	deadline := time.Now().Add(within)

	// Counted and reported once rather than logged per message: the zero-value deliveries above
	// arrive as fast as this loop acks them, so logging each one buries the useful output.
	skipped := 0
	defer func() {
		if skipped > 0 {
			t.Logf("acked %d unwanted deliveries while waiting for %q", skipped, payload)
		}
	}()

	for {
		// Checked before each receive rather than raced against it as a select case. A select with
		// both cases ready picks one at random, so an uninterrupted run of zero-value deliveries
		// could keep winning and hang this until go test's package timeout instead of failing here.
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("did not receive %q within %s", payload, within)
		}

		timeout := time.NewTimer(remaining)

		select {
		case msg, ok := <-messages:
			timeout.Stop()

			if !ok {
				t.Fatalf("message channel closed while waiting for %q", payload)
			}

			received := string(msg.Payload)
			msg.Ack()

			if received == payload {
				return
			}

			skipped++
		case <-timeout.C:
			t.Fatalf("did not receive %q within %s", payload, within)
		}
	}
}
