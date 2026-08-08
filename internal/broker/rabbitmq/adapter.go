// Package rabbitmq implements the Broker interface against RabbitMQ.
//
// Phase 1 of the build plan fills in Connect/ListQueues/Stats against a real
// broker; the recovery engine and command layer already depend only on the
// broker.Broker contract, so this stub keeps the module compiling while the
// adapter is developed.
package rabbitmq

import (
	"context"
	"errors"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// ErrNotImplemented is returned by methods that land in later build phases.
var ErrNotImplemented = errors.New("rabbitmq adapter: not implemented yet")

// Adapter implements broker.Broker against RabbitMQ.
type Adapter struct{}

// Compile-time assertion that Adapter satisfies the Broker contract.
var _ broker.Broker = (*Adapter)(nil)

func init() {
	broker.Register("rabbitmq", func() broker.Broker { return &Adapter{} })
}

// Connect opens a connection to the RabbitMQ instance.
func (a *Adapter) Connect(ctx context.Context, cfg broker.ConnectionConfig) error {
	return ErrNotImplemented
}

// Close closes the connection.
func (a *Adapter) Close() error {
	return ErrNotImplemented
}

// ListQueues lists queues visible to the connection.
func (a *Adapter) ListQueues(ctx context.Context) ([]broker.QueueSummary, error) {
	return nil, ErrNotImplemented
}

// Inspect fetches a single message by its stable ID.
func (a *Adapter) Inspect(ctx context.Context, queue, id string) (*message.Message, error) {
	return nil, ErrNotImplemented
}

// Search fetches messages matching the filter.
func (a *Adapter) Search(ctx context.Context, queue string, f broker.SearchFilter) ([]message.Message, error) {
	return nil, ErrNotImplemented
}

// Publish sends a message to a destination (the replay target).
func (a *Adapter) Publish(ctx context.Context, destination string, msg *message.Message) error {
	return ErrNotImplemented
}

// Stats reports queue-level statistics.
func (a *Adapter) Stats(ctx context.Context, queue string) (broker.QueueStats, error) {
	return broker.QueueStats{}, ErrNotImplemented
}
