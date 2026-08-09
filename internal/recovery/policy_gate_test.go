package recovery

import (
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/message"
	"github.com/HalxDocs/dlq_inspector/internal/policy"
)

func mustPolicy(t *testing.T, yaml string) *policy.Policy {
	t.Helper()
	p, err := policy.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse policy: %v", err)
	}
	return p
}

func TestClassifyWithPolicyOverridesInference(t *testing.T) {
	pol := mustPolicy(t, `
rules:
  - when: error contains validation
    action: replay
`)
	m := &message.Message{ID: "m1", FailureReason: "validation failed: customer_id required"}
	// The classifier alone says REQUIRES_FIX; the policy says replay.
	if res := Classify(m); res.Classification != RequiresFix {
		t.Fatalf("classifier = %s, want REQUIRES_FIX baseline", res.Classification)
	}
	got := ClassifyWithPolicy(m, pol)
	if got.Classification != Replayable {
		t.Errorf("ClassifyWithPolicy = %s, want REPLAYABLE from the policy", got.Classification)
	}
	if !strings.Contains(got.Reason, "policy rule") || !strings.Contains(got.Reason, "replay") {
		t.Errorf("reason = %q, want the policy rule named", got.Reason)
	}
	if got.Confidence != 0.9 {
		t.Errorf("confidence = %v, want 0.9 for an explicit rule", got.Confidence)
	}
}

func TestClassifyWithPolicyNoMatchFallsThrough(t *testing.T) {
	pol := mustPolicy(t, `
rules:
  - when: event_type == order.completed
    action: do_not_replay
`)
	m := &message.Message{ID: "m1", FailureReason: "rejected"}
	if got := ClassifyWithPolicy(m, pol); got.Classification != RequiresFix {
		t.Errorf("unmatched message = %s, want the classifier default REQUIRES_FIX", got.Classification)
	}
	if got := ClassifyWithPolicy(m, nil); got.Classification != RequiresFix {
		t.Errorf("nil policy = %s", got.Classification)
	}
}

func TestClassifyDuplicateHeaderOutranksPolicy(t *testing.T) {
	// The application marked THIS message as a duplicate; a general policy
	// rule must not force a replay over that.
	pol := mustPolicy(t, `
rules:
  - when: error == rejected
    action: replay
`)
	m := &message.Message{
		ID: "m1", FailureReason: "rejected",
		Headers: map[string]string{"x-duplicate-of": "evt_42"},
	}
	if got := ClassifyWithPolicy(m, pol); got.Classification != DoNotReplay {
		t.Errorf("ClassifyWithPolicy = %s, want DO_NOT_REPLAY (header outranks policy)", got.Classification)
	}
}

func TestClassifyPolicyMaxRetriesGate(t *testing.T) {
	pol := mustPolicy(t, `
rules:
  - when: error contains timeout
    action: do_not_replay
    params:
      max_retries: 2
`)
	// At 1 retry the rule applies and blocks the replay.
	low := &message.Message{ID: "m1", FailureReason: "timeout", RetryCount: 1}
	if got := ClassifyWithPolicy(low, pol); got.Classification != DoNotReplay {
		t.Errorf("low-retry = %s, want DO_NOT_REPLAY from the policy", got.Classification)
	}
	// At 3 retries the rule's gate is not satisfied, so it falls through to
	// the classifier (timeout below the high-retry threshold -> REPLAYABLE).
	high := &message.Message{ID: "m2", FailureReason: "timeout", RetryCount: 3}
	if got := ClassifyWithPolicy(high, pol); got.Classification != Replayable {
		t.Errorf("high-retry = %s, want REPLAYABLE from the classifier fallback", got.Classification)
	}
}

func TestAnalyzerPolicyOverrides(t *testing.T) {
	pol := mustPolicy(t, `
rules:
  - when: event_type == order.cancelled
    action: do_not_replay
`)
	msgs := []message.Message{
		{ID: "m1", Destination: "orders", FailureReason: "rejected", Headers: map[string]string{"x-event-type": "order.cancelled"}},
		{ID: "m2", Destination: "orders", FailureReason: "rejected", Headers: map[string]string{"x-event-type": "order.cancelled"}},
	}
	withPolicy := (Analyzer{Policy: pol}).Analyze(msgs)
	if len(withPolicy) != 1 || withPolicy[0].Recommendation != DoNotReplay {
		t.Errorf("with policy = %+v, want one DO_NOT_REPLAY group", withPolicy)
	}
	// Without the policy, the same messages are REQUIRES_FIX.
	without := (Analyzer{}).Analyze(msgs)
	if len(without) != 1 || without[0].Recommendation != RequiresFix {
		t.Errorf("without policy = %+v, want one REQUIRES_FIX group", without)
	}
}

func TestBuildPlanPolicyExcludes(t *testing.T) {
	pol := mustPolicy(t, `
rules:
  - when: event_type == order.cancelled
    action: do_not_replay
`)
	msgs := []message.Message{
		{ID: "m1", Queue: "orders-dlq", Destination: "orders", FailureReason: "rejected"},
		{ID: "m2", Queue: "orders-dlq", Destination: "orders", FailureReason: "rejected", Headers: map[string]string{"x-event-type": "order.cancelled"}},
	}
	p, err := BuildPlan(msgs, PlanOptions{Queue: "orders-dlq", Policy: pol})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.MessageIDs) != 1 || p.MessageIDs[0] != "m1" {
		t.Errorf("selected = %v, want only m1", p.MessageIDs)
	}
	if len(p.Excluded) != 1 || p.Excluded[0].MessageID != "m2" || p.Excluded[0].Classification != DoNotReplay {
		t.Fatalf("excluded = %+v, want m2 as DO_NOT_REPLAY", p.Excluded)
	}
	if !strings.Contains(p.Excluded[0].Reason, "policy rule") {
		t.Errorf("exclusion reason = %q, want the policy rule named", p.Excluded[0].Reason)
	}
	// The same messages without a policy are all selected.
	p2, err := BuildPlan(msgs, PlanOptions{Queue: "orders-dlq"})
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.MessageIDs) != 2 || len(p2.Excluded) != 0 {
		t.Errorf("no-policy plan = %d selected, %d excluded; want 2 and 0", len(p2.MessageIDs), len(p2.Excluded))
	}
}
