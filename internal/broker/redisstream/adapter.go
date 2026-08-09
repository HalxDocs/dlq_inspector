// Package redisstream implements the Broker interface against Redis Streams.
// A DLQ is a stream; entries carry the payload plus dead-letter metadata
// (destination, error, retries, headers) as fields, which is how the shared
// message model is populated. Replay publishes the entry to the destination
// stream (XADD) and removes the DLQ copy (XDEL) — no consumer groups are
// involved, because the tool owns the DLQ outright.
//
// The command layer and the recovery engine depend only on broker.Broker, so
// this adapter is a drop-in behind the interface: the analyzer, classifier,
// planner, validator, and executor work here unchanged.
package redisstream

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
	"github.com/HalxDocs/dlq_inspector/internal/search"
)

// ErrNotConnected is returned when a method is called before Connect.
var ErrNotConnected = errors.New("redisstream adapter: not connected — call Connect first")

// Adapter implements broker.Broker against Redis Streams.
type Adapter struct {
	conn *connection
}

// Compile-time assertion that Adapter satisfies the Broker contract.
var _ broker.Broker = (*Adapter)(nil)

func init() {
	broker.Register("redisstream", func() broker.Broker { return &Adapter{} })
}

// Connect opens a connection to the Redis instance.
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

// ListQueues lists streams visible to the connection (SCAN TYPE STREAM) with
// each stream's depth and total consumers across its consumer groups.
func (a *Adapter) ListQueues(ctx context.Context) ([]broker.QueueSummary, error) {
	if a.conn == nil {
		return nil, ErrNotConnected
	}

	var out []broker.QueueSummary
	iter := a.conn.client.ScanType(ctx, 0, "*", 1000, "stream").Iterator()
	for iter.Next(ctx) {
		name := iter.Val()
		qs := broker.QueueSummary{Name: name}
		if n, err := a.conn.client.XLen(ctx, name).Result(); err == nil {
			qs.Messages = int(n)
		}
		if groups, err := a.conn.client.XInfoGroups(ctx, name).Result(); err == nil {
			for _, g := range groups {
				qs.Consumers += int(g.Consumers)
			}
		}
		out = append(out, qs)
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("redisstream: scan streams: %w", err)
	}
	return out, nil
}

// Stats reports stream depth (XLEN) and consumers across groups. Existence is
// probed with EXISTS first: a stream that does not exist must surface as
// broker.ErrQueueNotFound — Stats doubles as the recovery engine's
// destination-existence probe, and publishing into a nonexistent stream would
// silently create it, which is exactly the void the probe prevents.
func (a *Adapter) Stats(ctx context.Context, queue string) (broker.QueueStats, error) {
	if a.conn == nil {
		return broker.QueueStats{}, ErrNotConnected
	}
	if queue == "" {
		return broker.QueueStats{}, fmt.Errorf("redisstream: empty stream name")
	}

	exists, err := a.conn.client.Exists(ctx, queue).Result()
	if err != nil {
		return broker.QueueStats{}, fmt.Errorf("redisstream: exists %q: %w", queue, err)
	}
	if exists == 0 {
		return broker.QueueStats{}, fmt.Errorf("stream %q not found: %w", queue, broker.ErrQueueNotFound)
	}

	n, err := a.conn.client.XLen(ctx, queue).Result()
	if err != nil {
		return broker.QueueStats{}, fmt.Errorf("redisstream: xlen %q: %w", queue, err)
	}
	stats := broker.QueueStats{Queue: queue, Messages: int(n)}
	if groups, err := a.conn.client.XInfoGroups(ctx, queue).Result(); err == nil {
		for _, g := range groups {
			stats.Consumers += int(g.Consumers)
		}
	}
	return stats, nil
}

// Search ranges the stream (oldest first, bounded) and applies the shared
// broker-agnostic filter. The scan is capped so a search cannot page through
// an unbounded DLQ.
func (a *Adapter) Search(ctx context.Context, queue string, f broker.SearchFilter) ([]message.Message, error) {
	if a.conn == nil {
		return nil, ErrNotConnected
	}
	if queue == "" {
		return nil, fmt.Errorf("redisstream: empty stream name")
	}

	scanCap := int64(5000)
	if needed := int64(f.Limit + f.Offset); needed > scanCap {
		scanCap = needed
	}
	entries, err := a.conn.client.XRangeN(ctx, queue, "-", "+", scanCap).Result()
	if err != nil {
		return nil, fmt.Errorf("redisstream: xrange %q: %w", queue, err)
	}
	msgs := make([]message.Message, 0, len(entries))
	for _, e := range entries {
		msgs = append(msgs, toMessage(queue, e))
	}
	return search.Filter(msgs, f), nil
}

// Inspect fetches a single entry by its stream ID (an inclusive range of one).
func (a *Adapter) Inspect(ctx context.Context, queue, id string) (*message.Message, error) {
	if a.conn == nil {
		return nil, ErrNotConnected
	}
	if queue == "" {
		return nil, fmt.Errorf("redisstream: empty stream name")
	}
	if id == "" {
		return nil, fmt.Errorf("redisstream: empty message id")
	}

	entries, err := a.conn.client.XRange(ctx, queue, id, id).Result()
	if err != nil {
		return nil, fmt.Errorf("redisstream: xrange %q: %w", queue, err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("message %q not found in stream %q", id, queue)
	}
	m := toMessage(queue, entries[0])
	return &m, nil
}

// Publish adds the message to the destination stream (XADD), carrying the
// payload and metadata as fields. The entry ID is assigned by the stream; the
// original message ID is preserved in the message_id field for correlation.
func (a *Adapter) Publish(ctx context.Context, destination string, m *message.Message) error {
	if a.conn == nil {
		return ErrNotConnected
	}
	if destination == "" {
		return fmt.Errorf("redisstream: empty destination")
	}
	if m == nil {
		return fmt.Errorf("redisstream: nil message")
	}

	if err := a.conn.client.XAdd(ctx, &redis.XAddArgs{
		Stream: destination,
		Values: entryFields(m),
	}).Err(); err != nil {
		return fmt.Errorf("redisstream: xadd %q: %w", destination, err)
	}
	return nil
}

// Ack removes the DLQ copy: XDEL by the entry ID that Search/Inspect report.
func (a *Adapter) Ack(ctx context.Context, queue, id string) error {
	if a.conn == nil {
		return ErrNotConnected
	}
	if queue == "" {
		return fmt.Errorf("redisstream: empty stream name")
	}
	if id == "" {
		return fmt.Errorf("redisstream: empty message id")
	}

	n, err := a.conn.client.XDel(ctx, queue, id).Result()
	if err != nil {
		return fmt.Errorf("redisstream: xdel %q: %w", queue, err)
	}
	if n == 0 {
		return fmt.Errorf("message %q not found in stream %q", id, queue)
	}
	return nil
}
