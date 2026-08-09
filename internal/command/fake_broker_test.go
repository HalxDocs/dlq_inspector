package command

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/config"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// fakeBroker is an in-memory Broker used to test command rendering and error
// paths without a live broker.
type fakeBroker struct {
	mu     sync.Mutex
	queues []broker.QueueSummary
	// msgs are returned by Inspect/Search, keyed by queue.
	msgs map[string][]message.Message
	// searchErr, when set, is returned by Search.
	searchErr error
	// lastFilter records the filter passed to the most recent Search call.
	lastFilter broker.SearchFilter
	// lastInspectID records the id passed to the most recent Inspect call.
	lastInspectID string
	// published records every Publish call (destination, message ID).
	published []publishCall
	// acked records every Ack call (queue, message ID).
	acked []ackCall
	// publishErr, when set, is returned by Publish.
	publishErr error
	// ackErr, when set, is returned by Ack.
	ackErr error
	// statsErr, when set, is returned by Stats for any queue.
	statsErr error
}

// publishCall records one Publish invocation.
type publishCall struct {
	destination string
	id          string
}

// ackCall records one Ack invocation.
type ackCall struct {
	queue string
	id    string
}

func (f *fakeBroker) Connect(ctx context.Context, cfg broker.ConnectionConfig) error { return nil }

func (f *fakeBroker) Close() error { return nil }

func (f *fakeBroker) ListQueues(ctx context.Context) ([]broker.QueueSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]broker.QueueSummary(nil), f.queues...), nil
}

func (f *fakeBroker) Inspect(ctx context.Context, queue, id string) (*message.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastInspectID = id
	for _, m := range f.msgs[queue] {
		if m.ID == id {
			copy := m
			return &copy, nil
		}
	}
	return nil, fmt.Errorf("message %q not found in queue %q", id, queue)
}

func (f *fakeBroker) Search(ctx context.Context, queue string, flt broker.SearchFilter) ([]message.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastFilter = flt
	if f.searchErr != nil {
		return nil, f.searchErr
	}
	return append([]message.Message(nil), f.msgs[queue]...), nil
}

func (f *fakeBroker) Publish(ctx context.Context, destination string, msg *message.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, publishCall{destination: destination, id: msg.ID})
	return f.publishErr
}

func (f *fakeBroker) Ack(ctx context.Context, queue, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, ackCall{queue: queue, id: id})
	return f.ackErr
}

func (f *fakeBroker) Stats(ctx context.Context, queue string) (broker.QueueStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statsErr != nil {
		return broker.QueueStats{}, f.statsErr
	}
	for _, q := range f.queues {
		if q.Name == queue {
			return broker.QueueStats{Queue: q.Name, Messages: q.Messages, Consumers: q.Consumers}, nil
		}
	}
	return broker.QueueStats{}, fmt.Errorf("queue %q not found", queue)
}

// withFakeBroker swaps the command package's broker factory for fb for the
// duration of the test.
func withFakeBroker(t *testing.T, fb *fakeBroker) {
	t.Helper()
	orig := brokerFactory
	brokerFactory = func(name string) (broker.Broker, error) { return fb, nil }
	t.Cleanup(func() { brokerFactory = orig })
}

// writeTestConfig writes a config file with one profile to path.
func writeTestConfig(t *testing.T, path, profileName, defaultProfile string, p *config.Profile) {
	t.Helper()
	cfg := config.Defaults()
	cfg.DefaultProfile = defaultProfile
	cfg.Profiles = map[string]*config.Profile{profileName: p}
	if err := config.Save(path, cfg); err != nil {
		t.Fatal(err)
	}
}
