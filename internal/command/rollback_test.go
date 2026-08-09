package command

import (
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/broker"
)

func seedSnapshots(t *testing.T, auditPath string, planID string) {
	t.Helper()
	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i, id := range []string{"m1", "m2"} {
		if err := store.SaveSnapshot(audit.Snapshot{
			PlanID:      planID,
			MessageID:   id,
			SourceQueue: "orders-dlq",
			Destination: "orders",
			Payload:     []byte(`{"order_id":1}`),
		}); err != nil {
			t.Fatalf("seed snapshot %d: %v", i, err)
		}
	}
}

func TestRollbackDryRunDefault(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	seedSnapshots(t, auditPath, "plan_rb")
	fb := &fakeBroker{queues: []broker.QueueSummary{{Name: "orders-dlq"}}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "rollback", "--plan", "plan_rb", "--config", cfgPath)
	if err != nil {
		t.Fatalf("rollback (dry-run): %v\n%s", err, out)
	}
	for _, want := range []string{"Rollback plan:", "plan_rb", "Snapshots:", "2", "orders-dlq", "Messages to restore:", "2", "Changes made: NONE"} {
		if !strings.Contains(out, want) {
			t.Errorf("dry-run output missing %q:\n%s", want, out)
		}
	}

	fb.mu.Lock()
	published := len(fb.published)
	fb.mu.Unlock()
	if published != 0 {
		t.Fatalf("dry-run published %d messages", published)
	}

	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entries, err := store.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Action != audit.ActionRollback || !entries[0].DryRun || entries[0].Result != "dry_run" {
		t.Fatalf("audit entries = %+v, want one rollback dry_run", entries)
	}
}

func TestRollbackDryRunSurfacesMissingDLQ(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	seedSnapshots(t, auditPath, "plan_rb")
	// No queues registered: the DLQ does not exist.
	fb := &fakeBroker{}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "rollback", "--plan", "plan_rb", "--config", cfgPath)
	if err != nil {
		t.Fatalf("rollback (dry-run): %v\n%s", err, out)
	}
	for _, want := range []string{"orders-dlq", "does not exist", "refused", "Changes made: NONE"} {
		if !strings.Contains(out, want) {
			t.Errorf("dry-run output missing %q:\n%s", want, out)
		}
	}

	fb.mu.Lock()
	published := len(fb.published)
	fb.mu.Unlock()
	if published != 0 {
		t.Fatalf("dry-run published %d messages", published)
	}
}

func TestRollbackConfirmRestoresAndAudits(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	seedSnapshots(t, auditPath, "plan_rb")
	fb := &fakeBroker{queues: []broker.QueueSummary{{Name: "orders-dlq"}}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "rollback", "--plan", "plan_rb", "--confirm", "--reason", "bad replay", "--config", cfgPath)
	if err != nil {
		t.Fatalf("rollback --confirm: %v\n%s", err, out)
	}
	for _, want := range []string{"Restored:", "2", "Failed:", "0", "orders-dlq"} {
		if !strings.Contains(out, want) {
			t.Errorf("confirm output missing %q:\n%s", want, out)
		}
	}

	fb.mu.Lock()
	published := append([]publishCall(nil), fb.published...)
	fb.mu.Unlock()
	if len(published) != 2 {
		t.Fatalf("published = %d, want 2", len(published))
	}
	for i, p := range published {
		if p.destination != "orders-dlq" || p.id != "m"+string(rune('1'+i)) {
			t.Errorf("publish %d = %+v, want the DLQ as destination", i, p)
		}
	}

	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entries, err := store.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	// 2 per-message rollback successes + 1 plan-level completed.
	if len(entries) != 3 {
		t.Fatalf("audit entries = %d, want 3\n%+v", len(entries), entries)
	}
	success := 0
	for _, e := range entries {
		if e.Action != audit.ActionRollback || !e.Confirmed {
			t.Errorf("entry = %+v, want a confirmed rollback", e)
		}
		if e.Result == "success" {
			success++
		}
		if !strings.Contains(e.Reason, "bad replay") {
			t.Errorf("reason = %q, want the operator reason", e.Reason)
		}
	}
	if success != 2 {
		t.Errorf("rollback success entries = %d, want 2", success)
	}
}

func TestRollbackRefusesMissingDLQ(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	seedSnapshots(t, auditPath, "plan_rb")
	// No queues registered: the DLQ does not exist.
	fb := &fakeBroker{}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "rollback", "--plan", "plan_rb", "--confirm", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "refusing to run") {
		t.Fatalf("expected refusal, got err=%v\n%s", err, out)
	}
	if !strings.Contains(err.Error(), "orders-dlq") {
		t.Errorf("refusal error = %q, want the DLQ name", err)
	}

	fb.mu.Lock()
	published := len(fb.published)
	fb.mu.Unlock()
	if published != 0 {
		t.Fatalf("refused rollback published %d messages", published)
	}

	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entries, err := store.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Result != "refused" || !entries[0].Confirmed {
		t.Fatalf("audit entries = %+v, want one confirmed refused entry", entries)
	}
}

func TestRollbackNoSnapshots(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{})

	_, err := runCommand(t, "rollback", "--plan", "plan_nope", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "no snapshots") {
		t.Fatalf("expected no-snapshots error, got %v", err)
	}
}

func TestRollbackRequiresPlan(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{})

	_, err := runCommand(t, "rollback", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "plan") {
		t.Fatalf("expected missing --plan error, got %v", err)
	}
}
