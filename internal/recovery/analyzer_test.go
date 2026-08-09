package recovery

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// fixtureMsgs is a known failure mix: two transient failures that differ
// only in IP (must merge into one group), one validation failure, one
// duplicate, and one message with no failure metadata.
func fixtureMsgs() []message.Message {
	ts := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	msg := func(id, reason, dest string, retries int, event string) message.Message {
		m := message.Message{
			ID:            id,
			Destination:   dest,
			FailureReason: reason,
			RetryCount:    retries,
			Timestamp:     ts,
		}
		if event != "" {
			m.Headers = map[string]string{"x-event-type": event}
		}
		return m
	}
	return []message.Message{
		msg("m1", "timeout connecting to 10.0.4.5:6432", "orders", 1, "order.placed"),
		msg("m2", "timeout connecting to 10.0.4.9:6432", "orders", 1, "order.placed"),
		msg("m3", "validation failed: customer_id is required", "orders", 0, "order.placed"),
		msg("m4", "duplicate event already processed", "orders", 0, "order.placed"),
		msg("m5", "", "orders", 0, ""),
		msg("m6", "timeout connecting to 10.0.4.11:6432", "orders", 2, "order.placed"),
	}
}

func TestAnalyzeGroupsByNormalizedSignature(t *testing.T) {
	groups := (Analyzer{}).Analyze(fixtureMsgs())

	if len(groups) != 4 {
		t.Fatalf("got %d groups, want 4:\n%+v", len(groups), groups)
	}

	bySig := make(map[string]FailureGroup, len(groups))
	for _, g := range groups {
		bySig[g.Signature] = g
	}

	timeout := bySig["timeout connecting to {ip}:{port}"]
	if timeout.Count != 3 {
		t.Errorf("timeout group count = %d, want 3 (IPs collapsed)", timeout.Count)
	}
	if timeout.Recommendation != Replayable {
		t.Errorf("timeout group recommendation = %s, want REPLAYABLE", timeout.Recommendation)
	}
	if timeout.Label != "Timeout connecting to" {
		t.Errorf("timeout group label = %q", timeout.Label)
	}
	if timeout.Percentage < 49.9 || timeout.Percentage > 50.1 {
		t.Errorf("timeout group percentage = %v, want ~50", timeout.Percentage)
	}

	validation := bySig["validation failed: customer_id is required"]
	if validation.Recommendation != RequiresFix {
		t.Errorf("validation group = %s, want REQUIRES_FIX", validation.Recommendation)
	}

	dup := bySig["duplicate event already processed"]
	if dup.Recommendation != DoNotReplay {
		t.Errorf("duplicate group = %s, want DO_NOT_REPLAY", dup.Recommendation)
	}

	unknown := bySig[noFailureSignature]
	if unknown.Recommendation != Investigate {
		t.Errorf("no-reason group = %s, want INVESTIGATE", unknown.Recommendation)
	}
}

func TestAnalyzeSplitsByDestinationAndEventType(t *testing.T) {
	// Same signature but different destination -> separate groups.
	msgs := []message.Message{
		{ID: "a1", Destination: "orders", FailureReason: "timeout"},
		{ID: "a2", Destination: "payments", FailureReason: "timeout"},
	}
	groups := (Analyzer{}).Analyze(msgs)
	if len(groups) != 2 {
		t.Fatalf("got %d groups, want 2 (destinations split)", len(groups))
	}

	// Same signature and destination but different event types -> split.
	msgs2 := []message.Message{
		{ID: "b1", Destination: "orders", FailureReason: "timeout", Headers: map[string]string{"x-event-type": "order.placed"}},
		{ID: "b2", Destination: "orders", FailureReason: "timeout", Headers: map[string]string{"x-event-type": "order.cancelled"}},
	}
	groups2 := (Analyzer{}).Analyze(msgs2)
	if len(groups2) != 2 {
		t.Fatalf("got %d groups, want 2 (event types split)", len(groups2))
	}
}

func TestAnalyzeEmpty(t *testing.T) {
	if got := (Analyzer{}).Analyze(nil); got != nil {
		t.Fatalf("Analyze(nil) = %+v, want nil", got)
	}
}

func TestAnalyzeOrderingLargestFirst(t *testing.T) {
	msgs := []message.Message{
		{ID: "s1", FailureReason: "timeout"},
		{ID: "s2", FailureReason: "timeout"},
		{ID: "s3", FailureReason: "timeout"},
		{ID: "s4", FailureReason: "invalid input"},
	}
	groups := (Analyzer{}).Analyze(msgs)
	if len(groups) != 2 {
		t.Fatalf("got %d groups", len(groups))
	}
	if groups[0].Count != 3 {
		t.Errorf("first group count = %d, want 3 (largest first)", groups[0].Count)
	}
}

func TestPayloadShape(t *testing.T) {
	cases := []struct {
		payload string
		want    string
	}{
		{`{"order_id": 1, "customer_id": 2}`, "customer_id,order_id"},
		{`not json`, "raw"},
		{``, "empty"},
	}
	for _, tc := range cases {
		if got := payloadShape([]byte(tc.payload)); got != tc.want {
			t.Errorf("payloadShape(%q) = %q, want %q", tc.payload, got, tc.want)
		}
	}
}

func TestGroupJSONRoundTrip(t *testing.T) {
	g := FailureGroup{
		ID:             "abc12345",
		Label:          "Timeout connecting to",
		Signature:      "timeout connecting to {ip}:{port}",
		MessageIDs:     []string{"m1", "m2"},
		Count:          2,
		Percentage:     66.7,
		Recommendation: Replayable,
		Confidence:     0.8,
		Destination:    "orders",
		EventType:      "order.placed",
		RetryBucket:    "1-2",
		PayloadShape:   "order_id",
		Breakdown:      map[Classification]int{Replayable: 2},
	}
	b, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	var back FailureGroup
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.ID != g.ID || back.Recommendation != Replayable || back.Count != 2 {
		t.Errorf("round trip mismatch: %+v", back)
	}
}
