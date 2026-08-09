package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// TestPublishAckRoundTrip exercises the full write path against a real
// RabbitMQ: publish a message, find it via Search, ack it, and verify it is
// gone. Skips when DLQ_TEST_AMQP_URL is not set (CI provides one).
func TestPublishAckRoundTrip(t *testing.T) {
	url := os.Getenv("DLQ_TEST_AMQP_URL")
	if url == "" {
		t.Skip("DLQ_TEST_AMQP_URL not set")
	}

	ctx := context.Background()
	a := &Adapter{}
	if err := a.Connect(ctx, broker.ConnectionConfig{
		URL:           url,
		ManagementURL: os.Getenv("DLQ_TEST_MANAGEMENT_URL"),
	}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer a.Close()

	name := fmt.Sprintf("dlq-inspector-write-%d", time.Now().UnixNano())
	if _, err := a.conn.ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	t.Cleanup(func() { a.conn.ch.QueueDelete(name, false, false, false) })

	msg := &message.Message{
		ID:             "write-test-1",
		Payload:        []byte(`{"event_id":"evt_w1"}`),
		ContentType:    "application/json",
		Headers:        map[string]string{"x-event-id": "evt_w1", "x-death": "should-be-stripped"},
		Timestamp:      time.Now(),
		IdempotencyKey: "key_w1",
	}
	if err := a.Publish(ctx, name, msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	found, err := a.Search(ctx, name, broker.SearchFilter{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("Search found %d messages, want 1", len(found))
	}
	if found[0].ID != "write-test-1" {
		t.Errorf("published ID = %q, want write-test-1", found[0].ID)
	}
	if _, stripped := found[0].Headers["x-death"]; stripped {
		t.Errorf("x-death header not stripped on publish: %+v", found[0].Headers)
	}
	if found[0].Headers["x-event-id"] != "evt_w1" {
		t.Errorf("x-event-id header lost: %+v", found[0].Headers)
	}

	if err := a.Ack(ctx, name, "write-test-1"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	after, err := a.Search(ctx, name, broker.SearchFilter{})
	if err != nil {
		t.Fatalf("Search after ack: %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("message still present after Ack: %+v", after)
	}
}

// TestAckNotFoundRequeries ensures acking a missing ID returns an error and
// leaves the queue intact.
func TestAckNotFoundRequeries(t *testing.T) {
	url := os.Getenv("DLQ_TEST_AMQP_URL")
	if url == "" {
		t.Skip("DLQ_TEST_AMQP_URL not set")
	}

	ctx := context.Background()
	a := &Adapter{}
	if err := a.Connect(ctx, broker.ConnectionConfig{URL: url}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer a.Close()

	name := fmt.Sprintf("dlq-inspector-ackmiss-%d", time.Now().UnixNano())
	if _, err := a.conn.ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	t.Cleanup(func() { a.conn.ch.QueueDelete(name, false, false, false) })

	if err := a.Publish(ctx, name, &message.Message{ID: "keep", Payload: []byte("stay")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if err := a.Ack(ctx, name, "missing-id"); err == nil {
		t.Fatal("Ack of missing ID expected error")
	}

	found, err := a.Search(ctx, name, broker.SearchFilter{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(found) != 1 || found[0].ID != "keep" {
		t.Fatalf("queue changed after failed Ack: %+v", found)
	}
}

// TestPublishHeaderStripping is a pure unit test of the header rules used by
// Publish, so the behavior is pinned even without a broker.
func TestPublishHeaderStripping(t *testing.T) {
	headers := map[string]string{
		"x-event-id":           "evt_1",
		"x-death":              "d",
		"x-death-extra":        "e",
		"x-first-death-reason": "rejected",
		"content-encoding":     "gzip",
	}
	got := publishHeaders(headers)
	for _, kept := range []string{"x-event-id", "content-encoding"} {
		if _, ok := got[kept]; !ok {
			t.Errorf("header %q should be preserved: %+v", kept, got)
		}
	}
	for _, stripped := range []string{"x-death", "x-death-extra", "x-first-death-reason"} {
		if _, ok := got[stripped]; ok {
			t.Errorf("dead-letter header %q should be stripped: %+v", stripped, got)
		}
	}
}
