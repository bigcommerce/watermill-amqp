package amqp_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	stdAmqp "github.com/rabbitmq/amqp091-go"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/bigcommerce/watermill-amqp/v3/pkg/amqp"
)

const (
	// testDeliveryLimit is the x-delivery-limit set on the quorum queue under test.
	testDeliveryLimit = 3

	// deliveryCountHeader is incremented by RabbitMQ on each failed delivery attempt, and is what
	// delivery-limit is evaluated against from 4.3 onwards.
	deliveryCountHeader = "x-delivery-count"

	// quietPeriod is how long to wait for a further redelivery before deciding the queue is done.
	quietPeriod = 5 * time.Second
)

// TestQuorumQueueDeliveryLimit asserts that a message the handler can never process is
// dead-lettered once the queue's delivery-limit is reached, rather than redelivered forever.
//
// RabbitMQ 4.3 split quorum queue poison-message tracking into two counters: acquired-count,
// incremented on every requeue, and delivery-count, incremented only on a failed delivery.
// delivery-limit is evaluated against delivery-count, which basic.nack does not increment -
// only basic.reject does. A subscriber that nacks on handler failure therefore loops the message
// indefinitely and never reaches the dead-letter exchange.
func TestQuorumQueueDeliveryLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping delivery-limit integration test in short mode")
	}

	logger := watermill.NewStdLogger(true, false)

	topic := "test_delivery_limit_" + watermill.NewShortUUID()
	deadLetterExchange := topic + "_dlx"
	deadLetterQueue := topic + "_dlq"

	declareDeadLetterTopology(t, deadLetterExchange, deadLetterQueue)

	config := amqp.NewDurableQueueConfig(amqpURI())
	config.Queue.Arguments = stdAmqp.Table{
		"x-queue-type":           "quorum",
		"x-delivery-limit":       testDeliveryLimit,
		"x-dead-letter-exchange": deadLetterExchange,
	}
	// RabbitMQ 4.3 attaches int-typed x-acquired-count and x-delivery-count headers on redelivery,
	// which DefaultMarshaler otherwise rejects as non-string metadata.
	config.Marshaler = amqp.DefaultMarshaler{PreprocessDelivery: stringifyHeaders}

	subscriber, err := amqp.NewSubscriber(config, logger)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, subscriber.Close())
	}()

	publisher, err := amqp.NewPublisher(config, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	messages, err := subscriber.Subscribe(ctx, topic)
	require.NoError(t, err)
	deleteQueueOnCleanup(t, topic)

	require.NoError(t, publisher.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte("poison"))))
	require.NoError(t, publisher.Close())

	// Nack every delivery, as a handler would for a message it can never process. Read until the
	// queue goes quiet, capped so an unbounded redelivery loop fails the test rather than hanging.
	var deliveryCounts []string
	maxDeliveries := testDeliveryLimit + 5

ReadLoop:
	for len(deliveryCounts) < maxDeliveries {
		select {
		case msg, ok := <-messages:
			if !ok {
				break ReadLoop
			}

			deliveryCounts = append(deliveryCounts, msg.Metadata.Get(deliveryCountHeader))
			msg.Nack()
		case <-time.After(quietPeriod):
			break ReadLoop
		}
	}

	// Asserting the values, not just the count: a climbing x-delivery-count is what proves the
	// reject incremented delivery-count, which is what delivery-limit is evaluated against, rather
	// than only acquired-count as a nack would.
	assert.Equal(
		t,
		expectedDeliveryCounts(),
		deliveryCounts,
		"expected the initial delivery plus %d redeliveries with a climbing %s",
		testDeliveryLimit,
		deliveryCountHeader,
	)

	assertDeadLettered(t, deadLetterQueue)
}

// TestQuorumQueueUnmarshalFailureDeliveryLimit covers the other reject path. A delivery that fails
// to unmarshal never reaches a handler, so processMessage rejects it itself - that reject must also
// count against delivery-limit and end in the dead-letter queue.
//
// The poison pill is an int-typed header, which DefaultMarshaler refuses as non-string metadata.
// That is not a contrived payload: from 4.3 the broker itself attaches int-typed x-acquired-count
// and x-delivery-count headers on redelivery.
func TestQuorumQueueUnmarshalFailureDeliveryLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping delivery-limit integration test in short mode")
	}

	logger := watermill.NewStdLogger(false, false)

	topic := "test_unmarshal_delivery_limit_" + watermill.NewShortUUID()
	deadLetterExchange := topic + "_dlx"
	deadLetterQueue := topic + "_dlq"

	declareDeadLetterTopology(t, deadLetterExchange, deadLetterQueue)

	config := amqp.NewDurableQueueConfig(amqpURI())
	config.Queue.Arguments = stdAmqp.Table{
		"x-queue-type":           "quorum",
		"x-delivery-limit":       testDeliveryLimit,
		"x-dead-letter-exchange": deadLetterExchange,
	}
	// Deliberately no PreprocessDelivery: the header below has to fail to unmarshal.

	subscriber, err := amqp.NewSubscriber(config, logger)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, subscriber.Close())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	messages, err := subscriber.Subscribe(ctx, topic)
	require.NoError(t, err)
	deleteQueueOnCleanup(t, topic)

	publishRaw(t, topic, stdAmqp.Table{"x-poison": 1})

	// Unmarshal fails before the message is passed on, so no handler should ever see it.
	select {
	case msg, ok := <-messages:
		if !ok {
			t.Fatal("subscriber channel closed before the message could be dead-lettered")
		}

		t.Fatalf("expected no message to reach the handler, got %q", msg.UUID)
	case <-time.After(quietPeriod):
	}

	assertDeadLettered(t, deadLetterQueue)
}

// assertDeadLettered waits for exactly one message to arrive in the dead-letter queue.
func assertDeadLettered(t *testing.T, deadLetterQueue string) {
	t.Helper()

	// tryQueueDepth rather than queueDepth: testify runs this callback in a worker goroutine, where
	// t.FailNow is not allowed, so a transient broker error has to read as a false condition.
	assert.Eventually(t, func() bool {
		depth, err := tryQueueDepth(deadLetterQueue)
		return err == nil && depth == 1
	}, 20*time.Second, time.Second, "message was not dead-lettered once delivery-limit was reached")
}

// publishRaw publishes straight to the queue behind a topic, bypassing the pub/sub's marshaler, so
// the subscriber receives a delivery it cannot unmarshal.
func publishRaw(t *testing.T, topic string, headers stdAmqp.Table) {
	t.Helper()

	channel, closeChannel := amqpChannel(t)
	defer closeChannel()

	require.NoError(t, channel.PublishWithContext(
		context.Background(),
		"",
		topic,
		false,
		false,
		stdAmqp.Publishing{
			DeliveryMode: stdAmqp.Persistent,
			Headers:      headers,
			Body:         []byte("poison"),
		},
	))
}

// stringifyHeaders converts non-string AMQP headers to strings, so that DefaultMarshaler accepts
// the numeric counters RabbitMQ adds on redelivery.
func stringifyHeaders(delivery stdAmqp.Delivery) stdAmqp.Delivery {
	for key, value := range delivery.Headers {
		if _, ok := value.(string); !ok {
			delivery.Headers[key] = fmt.Sprintf("%v", value)
		}
	}

	return delivery
}

// declareDeadLetterTopology creates the fanout exchange and quorum queue that the queue under test
// dead-letters into.
func declareDeadLetterTopology(t *testing.T, exchange, queue string) {
	t.Helper()

	channel, closeChannel := amqpChannel(t)
	defer closeChannel()

	require.NoError(t, channel.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil))

	_, err := channel.QueueDeclare(queue, true, false, false, false, stdAmqp.Table{
		"x-queue-type": "quorum",
	})
	require.NoError(t, err)

	require.NoError(t, channel.QueueBind(queue, "", exchange, false, nil))

	deleteQueueOnCleanup(t, queue)

	t.Cleanup(func() {
		cleanupChannel, closeConnection, err := tryAmqpChannel()
		if err != nil {
			t.Errorf("cleanup: cannot open a channel to delete exchange %s: %v", exchange, err)
			return
		}
		defer closeConnection()

		if err := cleanupChannel.ExchangeDelete(exchange, false, false); err != nil {
			t.Errorf("cleanup: cannot delete exchange %s: %v", exchange, err)
		}
	})
}

// tryQueueDepth returns the number of ready messages in a queue. It reports errors rather than
// asserting, so it is safe to call from an assert.Eventually callback.
func tryQueueDepth(queue string) (int, error) {
	channel, closeConnection, err := tryAmqpChannel()
	if err != nil {
		return 0, err
	}
	defer closeConnection()

	q, err := channel.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		return 0, err
	}

	return q.Messages, nil
}

func deleteQueueOnCleanup(t *testing.T, queue string) {
	t.Helper()

	t.Cleanup(func() {
		channel, closeConnection, err := tryAmqpChannel()
		if err != nil {
			t.Errorf("cleanup: cannot open a channel to delete queue %s: %v", queue, err)
			return
		}
		defer closeConnection()

		if _, err := channel.QueueDelete(queue, false, false, false); err != nil {
			t.Errorf("cleanup: cannot delete queue %s: %v", queue, err)
		}
	})
}

// amqpChannel opens a channel straight against the broker, bypassing the pub/sub under test.
func amqpChannel(t *testing.T) (*stdAmqp.Channel, func()) {
	t.Helper()

	channel, closeConnection, err := tryAmqpChannel()
	require.NoError(t, err)

	return channel, closeConnection
}

// tryAmqpChannel is the error-returning form of amqpChannel. Cleanup functions must use it: a
// require failure inside t.Cleanup calls FailNow, which abandons every cleanup still queued and
// leaves the test's quorum queues behind on the broker.
func tryAmqpChannel() (*stdAmqp.Channel, func(), error) {
	connection, err := stdAmqp.Dial(amqpURI())
	if err != nil {
		return nil, nil, err
	}

	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, nil, err
	}

	// Closing the connection closes its channels with it.
	return channel, func() {
		_ = connection.Close()
	}, nil
}

// expectedDeliveryCounts is the x-delivery-count sequence a correctly rejected message produces:
// absent on the first delivery, then one per failed delivery up to the limit.
func expectedDeliveryCounts() []string {
	counts := []string{""}
	for i := 1; i <= testDeliveryLimit; i++ {
		counts = append(counts, strconv.Itoa(i))
	}

	return counts
}
