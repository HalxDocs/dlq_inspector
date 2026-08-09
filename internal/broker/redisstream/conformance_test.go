package redisstream

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
)

// TestConformance runs the shared broker conformance suite against a real
// Redis. Requires DLQ_TEST_REDIS_URL; skips locally.
func TestConformance(t *testing.T) {
	a := &Adapter{}
	broker.RunConformance(t, a, broker.ConnectionConfig{
		URL: os.Getenv("DLQ_TEST_REDIS_URL"),
	}, func(t *testing.T, ctx context.Context) (string, func()) {
		name := fmt.Sprintf("dlq-inspector-conf-%d", time.Now().UnixNano())
		return name, func() { _ = a.conn.client.Del(ctx, name).Err() }
	})
}

// TestStatsMissingStream verifies the destination-existence probe: Stats on a
// stream that does not exist must wrap broker.ErrQueueNotFound so the
// recovery engine refuses instead of publishing into a stream that would be
// silently created.
func TestStatsMissingStream(t *testing.T) {
	url := os.Getenv("DLQ_TEST_REDIS_URL")
	if url == "" {
		t.Skip("DLQ_TEST_REDIS_URL not set")
	}

	ctx := context.Background()
	a := &Adapter{}
	if err := a.Connect(ctx, broker.ConnectionConfig{URL: url}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer a.Close()

	name := fmt.Sprintf("dlq-inspector-missing-%d", time.Now().UnixNano())
	_, err := a.Stats(ctx, name)
	if err == nil || !errors.Is(err, broker.ErrQueueNotFound) {
		t.Fatalf("Stats on missing stream = %v, want broker.ErrQueueNotFound", err)
	}
}
