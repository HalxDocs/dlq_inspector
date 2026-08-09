package broker

import (
	"context"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// FixtureFunc creates an isolated, empty queue (a stream for Redis, a queue
// for RabbitMQ) for the conformance suite and returns it with a cleanup. It
// runs in the adapter's own test package, so it can use adapter internals to
// declare the fixture.
type FixtureFunc func(t *testing.T, ctx context.Context) (queue string, cleanup func())

// RunConformance exercises the shared Broker contract against a live broker.
// The suite skips when no broker URL is configured, so it stays green on
// machines without a broker while still running in CI (see
// .github/workflows/ci.yml). Every new adapter must pass this suite — it is
// what guarantees the recovery engine behaves identically across brokers.
//
// Beyond Connect/ListQueues, the suite seeds a fixture queue through the
// public contract and proves the read/write/remove cycle: Publish, Search,
// Inspect, Stats, and Ack. It never assumes seeded IDs survive — Inspect and
// Ack use the IDs Search reports, which is the one contract the engine
// relies on.
func RunConformance(t *testing.T, b Broker, cfg ConnectionConfig, fixture FixtureFunc) {
	t.Helper()
	if cfg.URL == "" {
		t.Skip("no broker URL configured; skipping conformance suite")
	}

	ctx := context.Background()

	if err := b.Connect(ctx, cfg); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	t.Run("ListQueues", func(t *testing.T) {
		if _, err := b.ListQueues(ctx); err != nil {
			t.Fatalf("ListQueues: %v", err)
		}
	})

	t.Run("SearchInspectStatsAck", func(t *testing.T) {
		if fixture == nil {
			t.Skip("no fixture provided")
		}
		queue, cleanup := fixture(t, ctx)
		defer cleanup()

		// An empty queue searches to zero messages.
		msgs, err := b.Search(ctx, queue, SearchFilter{})
		if err != nil {
			t.Fatalf("Search (empty): %v", err)
		}
		if len(msgs) != 0 {
			t.Fatalf("empty queue search returned %d messages, want 0", len(msgs))
		}

		// Seed the DLQ through the public contract: Publish to the queue
		// itself, which is the DLQ in this suite.
		seeds := []*message.Message{
			{ID: "seed-1", ContentType: "application/json", Payload: []byte(`{"order_id":1,"status":"ok"}`)},
			{ID: "seed-2", ContentType: "application/json", Payload: []byte(`{"order_id":2,"status":"ok"}`)},
		}
		for _, s := range seeds {
			if err := b.Publish(ctx, queue, s); err != nil {
				t.Fatalf("Publish %s: %v", s.ID, err)
			}
		}

		// Search finds both, and payloads round-trip.
		msgs, err = b.Search(ctx, queue, SearchFilter{})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(msgs) != 2 {
			t.Fatalf("Search returned %d messages, want 2", len(msgs))
		}
		seen := map[string]bool{}
		for _, m := range msgs {
			seen[string(m.Payload)] = true
		}
		if !seen[`{"order_id":1,"status":"ok"}`] || !seen[`{"order_id":2,"status":"ok"}`] {
			t.Errorf("payloads did not round-trip: %v", msgs)
		}

		// Inspect by the ID Search reported.
		target := msgs[0].ID
		got, err := b.Inspect(ctx, queue, target)
		if err != nil {
			t.Fatalf("Inspect %s: %v", target, err)
		}
		if got.ID != target {
			t.Errorf("Inspect ID = %q, want %q", got.ID, target)
		}

		// Stats shows the depth.
		st, err := b.Stats(ctx, queue)
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		if st.Messages != 2 {
			t.Errorf("Stats.Messages = %d, want 2", st.Messages)
		}

		// Ack removes exactly the inspected message.
		if err := b.Ack(ctx, queue, target); err != nil {
			t.Fatalf("Ack %s: %v", target, err)
		}
		st2, err := b.Stats(ctx, queue)
		if err != nil {
			t.Fatalf("Stats after Ack: %v", err)
		}
		if st2.Messages != 1 {
			t.Errorf("Stats.Messages after Ack = %d, want 1", st2.Messages)
		}
	})
}
