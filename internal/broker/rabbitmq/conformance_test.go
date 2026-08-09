package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
)

// TestConformance runs the shared broker conformance suite against a real
// RabbitMQ. Requires DLQ_TEST_AMQP_URL; DLQ_TEST_MANAGEMENT_URL is needed for
// the queue-listing check.
func TestConformance(t *testing.T) {
	broker.RunConformance(t, &Adapter{}, broker.ConnectionConfig{
		URL:           os.Getenv("DLQ_TEST_AMQP_URL"),
		ManagementURL: os.Getenv("DLQ_TEST_MANAGEMENT_URL"),
	})
}

// TestStatsOnExistingQueue verifies Stats against a queue the test creates
// and removes afterwards.
func TestStatsOnExistingQueue(t *testing.T) {
	url := os.Getenv("DLQ_TEST_AMQP_URL")
	if url == "" {
		t.Skip("DLQ_TEST_AMQP_URL not set")
	}

	ctx := context.Background()
	a := &Adapter{}
	if err := a.Connect(ctx, broker.ConnectionConfig{URL: url}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	name := fmt.Sprintf("dlq-inspector-test-%d", time.Now().UnixNano())
	if _, err := a.conn.ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
		a.Close()
		t.Fatalf("declare queue: %v", err)
	}

	// Cleanup runs last-added first, so Close is registered first: the queue
	// is deleted while the channel is still open, then the connection closes.
	t.Cleanup(func() { _ = a.Close() })
	t.Cleanup(func() { _, _ = a.conn.ch.QueueDelete(name, false, false, false) })

	stats, err := a.Stats(ctx, name)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Queue != name {
		t.Errorf("Stats.Queue = %q, want %q", stats.Queue, name)
	}
	if stats.Messages != 0 || stats.Consumers != 0 {
		t.Errorf("Stats = %+v, want empty queue", stats)
	}
}
