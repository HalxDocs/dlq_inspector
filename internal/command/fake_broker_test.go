package command

import (
	"context"
	"errors"
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
	// statsErr, when set, is returned by Stats for any queue.
	statsErr error
}

func (f *fakeBroker) Connect(ctx context.Context, cfg broker.ConnectionConfig) error { return nil }

func (f *fakeBroker) Close() error { return nil }

func (f *fakeBroker) ListQueues(ctx context.Context) ([]broker.QueueSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]broker.QueueSummary(nil), f.queues...), nil
}

func (f *fakeBroker) Inspect(ctx context.Context, queue, id string) (*message.Message, error) {
	return nil, errors.New("fakeBroker: Inspect unimplemented")
}

func (f *fakeBroker) Search(ctx context.Context, queue string, flt broker.SearchFilter) ([]message.Message, error) {
	return nil, errors.New("fakeBroker: Search unimplemented")
}

func (f *fakeBroker) Publish(ctx context.Context, destination string, msg *message.Message) error {
	return errors.New("fakeBroker: Publish unimplemented")
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
