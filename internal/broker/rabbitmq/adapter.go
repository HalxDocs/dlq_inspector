// Package rabbitmq implements the Broker interface against RabbitMQ.
//
// Phase 1 delivers Connect/Close/ListQueues/Stats. ListQueues requires the
// RabbitMQ management plugin (HTTP API); Stats works over AMQP via passive
// queue declare. Inspect/Search/Publish land in later phases. The recovery
// engine and command layer depend only on the broker.Broker contract, so this
// adapter remains a drop-in behind the interface.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// ErrNotImplemented is returned by methods that land in later build phases.
var ErrNotImplemented = errors.New("rabbitmq adapter: not implemented yet")

// ErrNotConnected is returned when a method is called before Connect.
var ErrNotConnected = errors.New("rabbitmq adapter: not connected — call Connect first")

// Adapter implements broker.Broker against RabbitMQ.
type Adapter struct {
	conn *connection
}

// Compile-time assertion that Adapter satisfies the Broker contract.
var _ broker.Broker = (*Adapter)(nil)

func init() {
	broker.Register("rabbitmq", func() broker.Broker { return &Adapter{} })
}

// Connect opens a connection to the RabbitMQ instance.
func (a *Adapter) Connect(ctx context.Context, cfg broker.ConnectionConfig) error {
	conn, err := dial(ctx, cfg)
	if err != nil {
		return err
	}
	a.conn = conn
	return nil
}

// Close closes the connection. It is safe to call without Connect.
func (a *Adapter) Close() error {
	if a.conn == nil {
		return nil
	}
	return a.conn.close()
}

// ListQueues lists queues visible to the connection via the RabbitMQ
// management HTTP API. Requires the management plugin and either a
// management_url in the profile or the standard 15672 port.
func (a *Adapter) ListQueues(ctx context.Context) ([]broker.QueueSummary, error) {
	if a.conn == nil {
		return nil, ErrNotConnected
	}
	client, err := newManagementClient(a.conn.cfg)
	if err != nil {
		return nil, err
	}
	return client.listQueues(ctx)
}

// Stats reports queue depth and consumer count via a passive queue declare,
// which works over plain AMQP. Oldest/newest message age is not exposed by
// the protocol and stays zero in this phase.
func (a *Adapter) Stats(ctx context.Context, queue string) (broker.QueueStats, error) {
	if a.conn == nil {
		return broker.QueueStats{}, ErrNotConnected
	}
	if queue == "" {
		return broker.QueueStats{}, fmt.Errorf("rabbitmq: empty queue name")
	}

	q, err := a.conn.ch.QueueDeclarePassive(queue, false, false, false, false, nil)
	if err != nil {
		return broker.QueueStats{}, friendlyQueueError(queue, err)
	}
	return broker.QueueStats{
		Queue:     q.Name,
		Messages:  q.Messages,
		Consumers: q.Consumers,
	}, nil
}

// friendlyQueueError maps common AMQP errors onto clear messages.
func friendlyQueueError(queue string, err error) error {
	var ae *amqp.Error
	if errors.As(err, &ae) && ae.Code == amqp.NotFound {
		return fmt.Errorf("queue %q not found", queue)
	}
	return fmt.Errorf("rabbitmq: queue %q: %w", queue, err)
}

// Inspect fetches a single message by its stable ID. Implemented in phase 2.
func (a *Adapter) Inspect(ctx context.Context, queue, id string) (*message.Message, error) {
	return nil, ErrNotImplemented
}

// Search fetches messages matching the filter. Implemented in phase 2.
func (a *Adapter) Search(ctx context.Context, queue string, f broker.SearchFilter) ([]message.Message, error) {
	return nil, ErrNotImplemented
}

// Publish sends a message to a destination (the replay target). Implemented in
// phase 3, behind the shared safety gate.
func (a *Adapter) Publish(ctx context.Context, destination string, msg *message.Message) error {
	return ErrNotImplemented
}
