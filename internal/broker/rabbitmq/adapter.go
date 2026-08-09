// Package rabbitmq implements the Broker interface against RabbitMQ.
//
// Phase 1 delivered Connect/Close/ListQueues/Stats. Phase 2 delivered
// Inspect/Search, which peek messages via the RabbitMQ management plugin
// (HTTP API) without consuming them. Phase 3 delivers Publish/Ack behind the
// shared safety gate. The recovery engine and command layer depend only on
// the broker.Broker contract, so this adapter remains a drop-in behind the
// interface.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
	"github.com/HalxDocs/dlq_inspector/internal/search"
)

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

// friendlyQueueError maps common AMQP errors onto clear messages. A missing
// queue wraps broker.ErrQueueNotFound so the recovery engine can distinguish
// "does not exist" (refuse to run) from "broker hiccup" (report the error).
func friendlyQueueError(queue string, err error) error {
	var ae *amqp.Error
	if errors.As(err, &ae) && ae.Code == amqp.NotFound {
		return fmt.Errorf("queue %q not found: %w", queue, broker.ErrQueueNotFound)
	}
	return fmt.Errorf("rabbitmq: queue %q: %w", queue, err)
}

// Inspect fetches a single message by its stable ID. RabbitMQ has no
// random-access get, so this peeks messages FIFO via the management API and
// matches on the shared ID rule, scanning up to a bounded number of messages.
func (a *Adapter) Inspect(ctx context.Context, queue, id string) (*message.Message, error) {
	if a.conn == nil {
		return nil, ErrNotConnected
	}
	if queue == "" {
		return nil, fmt.Errorf("rabbitmq: empty queue name")
	}
	if id == "" {
		return nil, fmt.Errorf("rabbitmq: empty message id")
	}
	client, err := newManagementClient(a.conn.cfg)
	if err != nil {
		return nil, err
	}
	raws, err := client.peekMessages(ctx, queue, 1000, 10000)
	if err != nil {
		return nil, err
	}
	for _, raw := range raws {
		m := toMessageFromMgmt(raw, queue)
		if m.ID == id {
			return &m, nil
		}
	}
	return nil, fmt.Errorf("message %q not found in queue %q (scanned %d messages)", id, queue, len(raws))
}

// Search peeks messages via the management API and applies the filter with
// the shared broker-agnostic search package. The scan is bounded so a search
// cannot page through an unbounded DLQ.
func (a *Adapter) Search(ctx context.Context, queue string, f broker.SearchFilter) ([]message.Message, error) {
	if a.conn == nil {
		return nil, ErrNotConnected
	}
	if queue == "" {
		return nil, fmt.Errorf("rabbitmq: empty queue name")
	}
	client, err := newManagementClient(a.conn.cfg)
	if err != nil {
		return nil, err
	}

	// Scan enough to satisfy limit+offset, but never more than the safety cap
	// (the filter may still leave fewer matches if the cap is hit).
	scanCap := 5000
	if needed := f.Limit + f.Offset; needed > scanCap {
		scanCap = needed
	}
	raws, err := client.peekMessages(ctx, queue, 500, scanCap)
	if err != nil {
		return nil, err
	}
	msgs := make([]message.Message, 0, len(raws))
	for _, raw := range raws {
		msgs = append(msgs, toMessageFromMgmt(raw, queue))
	}
	return search.Filter(msgs, f), nil
}
