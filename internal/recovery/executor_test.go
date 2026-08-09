package recovery

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/time/rate"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

func execPlan(ids ...string) *RecoveryPlan {
	return &RecoveryPlan{
		ID:           "plan_exec",
		Queue:        "orders-dlq",
		MessageIDs:   ids,
		Destination:  "orders",
		Action:       "replay",
		Limits:       DefaultLimits(),
		SafetyChecks: []string{CheckSchema, CheckDuplicate, CheckDestination},
	}
}

func execMsgs(ids ...string) []message.Message {
	out := make([]message.Message, 0, len(ids))
	for _, id := range ids {
		out = append(out, message.Message{ID: id, Destination: "orders", Payload: []byte(`{}`)})
	}
	return out
}

func execStore(t *testing.T) *audit.Store {
	t.Helper()
	s, err := audit.Open(filepath.Join(t.TempDir(), "audit.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestExecuteReplaysAllAndAudits(t *testing.T) {
	store := execStore(t)
	b := &valBroker{msgs: map[string][]message.Message{"orders-dlq": execMsgs("m1", "m2", "m3", "m4", "m5")}}
	ex := Executor{Broker: b, Audit: store}

	res, err := ex.Execute(context.Background(), execPlan("m1", "m2", "m3", "m4", "m5"), ExecutorOptions{
		Confirm: true, BatchSize: 2, RateLimit: "0", Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Selected != 5 || res.Replayed != 5 || res.Skipped != 0 || res.Failed != 0 || res.Tripped {
		t.Errorf("result = %+v", res)
	}
	if res.Duration <= 0 {
		t.Errorf("duration = %v", res.Duration)
	}

	entries, err := store.Recent(100)
	if err != nil {
		t.Fatal(err)
	}
	// 5 per-message successes + 1 plan-level "completed".
	if len(entries) != 6 {
		t.Fatalf("audit entries = %d, want 6", len(entries))
	}
	success := 0
	for _, e := range entries {
		if e.Result == "success" {
			success++
		}
	}
	if success != 5 {
		t.Errorf("success entries = %d, want 5", success)
	}
}

func TestExecuteRefusesWithoutConfirm(t *testing.T) {
	b := &valBroker{msgs: map[string][]message.Message{"orders-dlq": execMsgs("m1")}}
	_, err := (Executor{Broker: b}).Execute(context.Background(), execPlan("m1"), ExecutorOptions{})
	if !errors.Is(err, ErrConfirmRequired) {
		t.Fatalf("got %v, want ErrConfirmRequired", err)
	}
}

func TestExecuteRefusesMissingDestination(t *testing.T) {
	// The destination-existence check is a hard invariant, independent of the
	// plan's declared checks: publishing into a nonexistent queue can be
	// confirmed and silently dropped, after which acking the DLQ copy would
	// lose the message. The run must refuse before any publish or ack.
	store := execStore(t)
	p := execPlan("m1", "m2")
	// Deliberately omit the declared check — the invariant must still hold.
	p.SafetyChecks = []string{CheckSchema, CheckDuplicate}
	b := &valBroker{
		msgs:          map[string][]message.Message{"orders-dlq": execMsgs("m1", "m2")},
		missingQueues: map[string]bool{"orders": true},
	}
	ex := Executor{Broker: b, Audit: store}

	_, err := ex.Execute(context.Background(), p, ExecutorOptions{
		Confirm: true, BatchSize: 2, RateLimit: "0",
	})
	if !errors.Is(err, ErrDestinationMissing) {
		t.Fatalf("got %v, want ErrDestinationMissing", err)
	}
	b.mu.Lock()
	published := b.publishes
	acked := b.acks
	b.mu.Unlock()
	if published != 0 || acked != 0 {
		t.Fatalf("refused run performed I/O: publishes=%d acks=%d", published, acked)
	}

	// The refusal itself is audited so the trail explains why nothing ran.
	entries, err := store.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Result != "refused" || entries[0].PlanID != "plan_exec" {
		t.Errorf("audit entries = %+v, want one refused plan entry", entries)
	}
}

func TestExecuteRejectsEmptySafetyChecks(t *testing.T) {
	p := execPlan("m1")
	p.SafetyChecks = nil
	b := &valBroker{msgs: map[string][]message.Message{"orders-dlq": execMsgs("m1")}}
	_, err := (Executor{Broker: b}).Execute(context.Background(), p, ExecutorOptions{Confirm: true})
	if !errors.Is(err, ErrNoSafetyChecks) {
		t.Fatalf("got %v, want ErrNoSafetyChecks", err)
	}
}

func TestExecutePublishFailureNeverAcks(t *testing.T) {
	store := execStore(t)
	b := &valBroker{
		msgs:        map[string][]message.Message{"orders-dlq": execMsgs("m1", "m2")},
		failPublish: map[string]int{"m2": 5}, // always fails with default retry=0
	}
	ex := Executor{Broker: b, Audit: store}

	res, err := ex.Execute(context.Background(), execPlan("m1", "m2"), ExecutorOptions{
		Confirm: true, BatchSize: 2, RateLimit: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 || res.Failed != 1 {
		t.Errorf("result = %+v", res)
	}
	// m2 must never be acked: publish failed, DLQ copy stays.
	b.mu.Lock()
	acked := b.acks
	published := b.publishes
	b.mu.Unlock()
	if acked != 1 || published != 2 {
		t.Errorf("publishes=%d acks=%d, want 2 publishes and 1 ack", published, acked)
	}

	entries, _ := store.Recent(100)
	for _, e := range entries {
		if e.MessageID == "m2" && e.Result != "publish_failed" {
			t.Errorf("m2 audit = %s, want publish_failed", e.Result)
		}
	}
}

func TestExecutePerMessageRetry(t *testing.T) {
	store := execStore(t)
	b := &valBroker{
		msgs:        map[string][]message.Message{"orders-dlq": execMsgs("m1")},
		failPublish: map[string]int{"m1": 1}, // fails once, then succeeds
	}
	ex := Executor{Broker: b, Audit: store}

	res, err := ex.Execute(context.Background(), execPlan("m1"), ExecutorOptions{
		Confirm: true, BatchSize: 1, RateLimit: "0", RetryPerMessage: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 || res.Failed != 0 {
		t.Errorf("result = %+v, want retry to succeed", res)
	}
}

func TestExecuteAckFailureCountsNewDLQEntry(t *testing.T) {
	store := execStore(t)
	b := &valBroker{
		msgs:    map[string][]message.Message{"orders-dlq": execMsgs("m1", "m2")},
		failAck: map[string]bool{"m2": true},
	}
	ex := Executor{Broker: b, Audit: store}

	res, err := ex.Execute(context.Background(), execPlan("m1", "m2"), ExecutorOptions{
		Confirm: true, BatchSize: 2, RateLimit: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	// m2 was published (counts as a failure for the breaker) but not acked.
	if res.Replayed != 1 || res.Failed != 1 || res.NewDLQEntries != 1 {
		t.Errorf("result = %+v", res)
	}
}

func TestCircuitBreakerTripsAndStops(t *testing.T) {
	store := execStore(t)
	// 10 messages, batch size 4: batch 1 = m1..m4 (all succeed); batch 2 =
	// m5..m8 where m5, m6, m7 fail -> 3/4 = 75% > 20% -> trip; batch 3 =
	// m9..m10 is never started.
	b := &valBroker{
		msgs: map[string][]message.Message{"orders-dlq": execMsgs("m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "m10")},
		failPublish: map[string]int{
			"m5": 5, "m6": 5, "m7": 5,
		},
	}
	ex := Executor{Broker: b, Audit: store}

	res, err := ex.Execute(context.Background(), execPlan("m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "m10"), ExecutorOptions{
		Confirm: true, BatchSize: 4, RateLimit: "0", Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Tripped {
		t.Fatalf("result = %+v, want tripped", res)
	}
	// m1..m4 replayed, m5..m7 failed, m8 (same batch) attempted and
	// succeeded; m9..m10 in the paused batch were never attempted.
	if res.Replayed != 5 || res.Failed != 3 {
		t.Errorf("result = %+v", res)
	}
	if len(res.Remaining) != 2 || res.Remaining[0] != "m9" || res.Remaining[1] != "m10" {
		t.Errorf("remaining = %v, want [m9 m10]", res.Remaining)
	}
	b.mu.Lock()
	attempted := b.publishes
	b.mu.Unlock()
	if attempted != 8 {
		t.Errorf("publishes = %d, want 8 (m9/m10 not attempted)", attempted)
	}
}

func TestExecuteCircuitBreakerOff(t *testing.T) {
	// When the failure rate stays under the threshold, the run completes:
	// 1 failure in a batch of 4 = 25%, under a 50% threshold.
	b := &valBroker{
		msgs:        map[string][]message.Message{"orders-dlq": execMsgs("m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8")},
		failPublish: map[string]int{"m5": 5},
	}
	res, err := (Executor{Broker: b}).Execute(context.Background(), execPlan("m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8"), ExecutorOptions{
		Confirm: true, BatchSize: 4, RateLimit: "0", FailureThreshold: 0.5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Tripped {
		t.Errorf("result = %+v, want no trip at 50%% threshold", res)
	}
	if res.Replayed != 7 || res.Failed != 1 {
		t.Errorf("result = %+v", res)
	}
}

func TestExecuteResumeSkipsCompleted(t *testing.T) {
	store := execStore(t)
	// m1 was already replayed successfully (audit evidence) — a resumed run
	// must not attempt it again.
	if err := store.Append(audit.Entry{
		Action: audit.ActionRecover, PlanID: "plan_exec", MessageID: "m1",
		SourceQueue: "orders-dlq", Destination: "orders", Confirmed: true, Result: "success",
	}); err != nil {
		t.Fatal(err)
	}
	b := &valBroker{msgs: map[string][]message.Message{"orders-dlq": execMsgs("m1", "m2", "m3")}}
	ex := Executor{Broker: b, Audit: store}

	res, err := ex.Execute(context.Background(), execPlan("m1", "m2", "m3"), ExecutorOptions{
		Confirm: true, Resume: true, BatchSize: 2, RateLimit: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 2 || res.Skipped != 1 {
		t.Errorf("result = %+v, want m1 skipped and m2/m3 replayed", res)
	}
	b.mu.Lock()
	published := b.publishes
	b.mu.Unlock()
	if published != 2 {
		t.Errorf("publishes = %d, want 2 (m1 not re-published)", published)
	}
}

func TestExecuteSkipsDuplicateEvidence(t *testing.T) {
	store := execStore(t)
	if err := store.Append(audit.Entry{
		Action: audit.ActionReplay, MessageID: "m1",
		SourceQueue: "orders-dlq", Destination: "orders", Confirmed: true, Result: "success",
	}); err != nil {
		t.Fatal(err)
	}
	b := &valBroker{msgs: map[string][]message.Message{"orders-dlq": execMsgs("m1", "m2")}}
	ex := Executor{Broker: b, Audit: store}

	res, err := ex.Execute(context.Background(), execPlan("m1", "m2"), ExecutorOptions{
		Confirm: true, BatchSize: 2, RateLimit: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 || res.Skipped != 1 {
		t.Errorf("result = %+v, want m1 skipped on duplicate evidence", res)
	}
}

func TestExecuteAuditsSkips(t *testing.T) {
	store := execStore(t)
	b := &valBroker{msgs: map[string][]message.Message{"orders-dlq": execMsgs("m1")}}
	ex := Executor{Broker: b, Audit: store}

	// "gone" fails validation (not in the queue) and must be audited as
	// skipped without ever being attempted.
	_, err := ex.Execute(context.Background(), execPlan("m1", "gone"), ExecutorOptions{
		Confirm: true, BatchSize: 2, RateLimit: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	entries, _ := store.Recent(100)
	found := false
	for _, e := range entries {
		if e.MessageID == "gone" && e.Result == "skipped" {
			found = true
		}
	}
	if !found {
		t.Errorf("no skipped audit entry for gone; entries = %+v", entries)
	}
}

func TestExecuteAuditsPlanExclusions(t *testing.T) {
	store := execStore(t)
	b := &valBroker{msgs: map[string][]message.Message{"orders-dlq": execMsgs("m1", "m2")}}
	p := execPlan("m1", "m2")
	p.Excluded = []ExcludedMessage{
		{MessageID: "m-dup", Classification: DoNotReplay, Reason: "message header x-duplicate-of marks it as a duplicate of evt_42"},
	}
	ex := Executor{Broker: b, Audit: store}

	res, err := ex.Execute(context.Background(), p, ExecutorOptions{Confirm: true, BatchSize: 2, RateLimit: "0"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 2 || res.Excluded != 1 {
		t.Errorf("result = %+v, want 2 replayed and 1 excluded", res)
	}

	entries, _ := store.Recent(100)
	found := false
	for _, e := range entries {
		if e.MessageID == "m-dup" && e.Result == "skipped" && e.PlanID == "plan_exec" {
			found = true
			if !strings.Contains(e.Reason, "x-duplicate-of") {
				t.Errorf("skip reason = %q, want the classifier reason", e.Reason)
			}
		}
	}
	if !found {
		t.Errorf("no skipped audit entry for the excluded message; entries = %+v", entries)
	}
}

func TestParseRateLimit(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{"10/s", false},
		{"100/m", false},
		{"0", false},
		{"", false},
		{"unlimited", false},
		{"bogus", true},
		{"-5/s", true},
	}
	for _, tc := range cases {
		_, err := ParseRateLimit(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("ParseRateLimit(%q) err = %v, wantErr %v", tc.in, err, tc.wantErr)
		}
	}
	if l, _ := ParseRateLimit("10/s"); l != 10 {
		t.Errorf("10/s = %v, want 10", l)
	}
	if l, _ := ParseRateLimit("100/m"); l != 100.0/60.0 {
		t.Errorf("100/m = %v, want %v", l, 100.0/60.0)
	}
	if l, _ := ParseRateLimit("0"); l != rate.Inf {
		t.Errorf("0 = %v, want rate.Inf", l)
	}
}
