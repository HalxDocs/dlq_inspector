// Package message defines the normalized message model shared by every broker
// adapter and the recovery engine. No broker-specific type (amqp.Delivery,
// kafka.ConsumerMessage, ...) ever crosses out of its adapter package;
// everything is normalized into Message before the command layer or recovery
// engine sees it.
package message

import "time"

// Message is the broker-agnostic representation of a failed message sitting in
// a dead-letter queue.
type Message struct {
	// ID is a stable identifier for the message. RabbitMQ has no native
	// message ID, so adapters prefer an x-message-id header when present and
	// fall back to a content hash (sha256 of payload + headers). This ID feeds
	// inspect, audit, and duplicate detection.
	ID string

	// Queue is the queue the message was read from (typically the DLQ).
	Queue string

	// Destination is where the message should be replayed to (the original
	// queue or exchange). Empty until the adapter can resolve it.
	Destination string

	// Payload is the raw message body.
	Payload []byte

	// Headers are normalized message headers/attributes (string values).
	Headers map[string]string

	// ContentType is the declared content type, if any.
	ContentType string

	// Timestamp is when the message was enqueued.
	Timestamp time.Time

	// RetryCount is how many times the message has been retried/redelivered.
	RetryCount int

	// FailureReason is the dead-letter reason or error detail, when available.
	FailureReason string

	// EventID is an application event identifier extracted for duplicate
	// detection, when present.
	EventID string

	// IdempotencyKey is an application idempotency key extracted for duplicate
	// detection, when present.
	IdempotencyKey string
}
