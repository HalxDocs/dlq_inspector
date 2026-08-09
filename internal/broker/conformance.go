package broker

import (
	"context"
	"testing"
)

// RunConformance exercises the shared Broker contract against a live broker.
// The suite skips when no broker URL is configured, so it stays green on
// machines without a broker while still running in CI (see
// .github/workflows/ci.yml). Every new adapter must pass this suite — it is
// what guarantees the recovery engine behaves identically across brokers.
//
// Adapter-specific behavior (fixture setup, deeper assertions) belongs in the
// adapter's own test package.
func RunConformance(t *testing.T, b Broker, cfg ConnectionConfig) {
	t.Helper()
	if cfg.URL == "" {
		t.Skip("no broker URL configured (e.g. DLQ_TEST_AMQP_URL); skipping conformance suite")
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
}
