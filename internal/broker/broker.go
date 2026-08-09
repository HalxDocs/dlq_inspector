// Package broker defines the Broker contract that every message
// infrastructure adapter implements (RabbitMQ, Redis Streams, Kafka, SQS).
// The command layer and the recovery engine only ever talk to this interface
// and to message.Message — never to a broker SDK directly.
package broker

import (
	"context"
	"errors"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// ErrQueueNotFound is returned by queue-scoped operations — notably Stats,
// which doubles as the recovery engine's destination-existence probe — when
// the queue does not exist on the broker.
var ErrQueueNotFound = errors.New("queue not found")

// Broker is the contract every adapter implements.
type Broker interface {
	// Connect opens a connection using the given configuration.
	Connect(ctx context.Context, cfg ConnectionConfig) error
	// Close closes the connection.
	Close() error
	// ListQueues lists queues visible to the connection.
	ListQueues(ctx context.Context) ([]QueueSummary, error)
	// Inspect fetches a single message by its stable ID.
	Inspect(ctx context.Context, queue string, id string) (*message.Message, error)
	// Search fetches messages matching the filter.
	Search(ctx context.Context, queue string, f SearchFilter) ([]message.Message, error)
	// Publish sends a message to a destination (the replay target).
	Publish(ctx context.Context, destination string, msg *message.Message) error
	// Ack removes a message from a queue after it was successfully replayed.
	// The safety gate guarantees Ack is only ever called after a successful
	// Publish — never before. Adapters implement per-broker semantics
	// (RabbitMQ consumes until the message is found; SQS deletes by receipt
	// handle; ...).
	Ack(ctx context.Context, queue string, id string) error
	// Stats reports queue-level statistics. It is also the recovery engine's
	// destination-existence probe: adapters must return an error wrapping
	// ErrQueueNotFound when the queue does not exist.
	Stats(ctx context.Context, queue string) (QueueStats, error)
}

// ConnectionConfig carries the connection settings for an adapter.
type ConnectionConfig struct {
	// Broker is the adapter name, e.g. "rabbitmq".
	Broker string
	// URL is the connection URL.
	URL string
	// Queue is the default queue the CLI operates on, when set.
	Queue string
	// ManagementURL is the broker's management API base URL, when the broker
	// exposes one (e.g. RabbitMQ's management plugin). Empty means "derive or
	// not available".
	ManagementURL string
}

// QueueSummary describes a queue visible to the connection.
type QueueSummary struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Messages   int
	Consumers  int
	// Pending is the number of messages delivered to a consumer but not yet
	// acknowledged — unacknowledged deliveries (RabbitMQ) or the sum of
	// consumer-group PELs (Redis Streams). 0 when nothing is pending.
	Pending int
	// DLQ is the associated dead-letter queue, when the queue has a
	// dead-letter exchange binding.
	DLQ string
}

// QueueStats reports queue-level statistics.
type QueueStats struct {
	Queue     string
	Messages  int
	Consumers int
	// Pending is the total number of messages delivered to a consumer but not
	// yet acknowledged — e.g. the sum of Redis Streams consumer-group PELs.
	// 0 when the broker does not report it or nothing is pending.
	Pending   int
	OldestAge time.Duration
	NewestAge time.Duration
	// RetryCount is the average retry count across the queue's messages.
	RetryCount int
}

// SearchFilter narrows a Search call. Zero values mean "no constraint".
type SearchFilter struct {
	// ErrorText matches failure/error text (case-insensitive substring).
	ErrorText string
	// Since restricts to messages enqueued at or after this time.
	Since time.Time
	// Fields requires payload fields (dotted paths) to equal the given values.
	Fields map[string]string
	// MaxRetries filters to messages with retry count <= the value; nil means
	// no constraint.
	MaxRetries *int
	// Limit caps the number of returned messages; 0 means no limit.
	Limit int
	// Offset skips the first N matching messages; 0 means none.
	Offset int
}
