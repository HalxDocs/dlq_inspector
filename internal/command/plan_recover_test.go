package command

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/message"
	"github.com/HalxDocs/dlq_inspector/internal/recovery"
)

// planFixture is a known failure mix with a DO_NOT_REPLAY message.
func planFixture() []message.Message {
	return []message.Message{
		{ID: "m1", Queue: "orders-dlq", Destination: "orders", FailureReason: "timeout connecting to 10.0.4.5:6432", RetryCount: 1},
		{ID: "m2", Queue: "orders-dlq", Destination: "orders", FailureReason: "timeout connecting to 10.0.4.9:6432", RetryCount: 1},
		{ID: "m3", Queue: "orders-dlq", Destination: "orders", FailureReason: "validation failed: customer_id is required"},
		{ID: "m4", Queue: "orders-dlq", Destination: "orders", FailureReason: "duplicate event already processed"},
	}
}

// timeoutGroupID runs the analyzer over the fixture and returns the ID of the
// timeout group, so tests can reference a real group.
func timeoutGroupID(t *testing.T) string {
	t.Helper()
	groups := (recovery.Analyzer{}).Analyze(planFixture())
	for _, g := range groups {
		if strings.Contains(g.Signature, "timeout") {
			return g.ID
		}
	}
	t.Fatal("no timeout group in fixture")
	return ""
}

func TestPlanWritesJSON(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": planFixture()}})

	out, err := runCommand(t, "plan", "--config", cfgPath, "--output-file", planPath)
	if err != nil {
		t.Fatalf("plan: %v\n%s", err, out)
	}
	for _, want := range []string{"Plan written:", "3 messages selected", "dlq recover --plan"} {
		if !strings.Contains(out, want) {
			t.Errorf("plan output missing %q:\n%s", want, out)
		}
	}

	b, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	var p recovery.RecoveryPlan
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatalf("plan file is not valid JSON: %v\n%s", err, b)
	}
	// m4 (DO_NOT_REPLAY) excluded; m1, m2, m3 selected.
	if len(p.MessageIDs) != 3 {
		t.Errorf("message_ids = %v", p.MessageIDs)
	}
	for _, id := range p.MessageIDs {
		if id == "m4" {
			t.Error("DO_NOT_REPLAY message in plan")
		}
	}
	if p.Queue != "orders-dlq" || p.Destination != "orders" || p.Action != "replay" {
		t.Errorf("plan = %+v", p)
	}
	if len(p.SafetyChecks) == 0 {
		t.Error("plan has no safety checks")
	}
}

func TestPlanGroupFilter(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": planFixture()}})

	out, err := runCommand(t, "plan", "--group", timeoutGroupID(t), "--config", cfgPath, "--output-file", planPath)
	if err != nil {
		t.Fatalf("plan --group: %v\n%s", err, out)
	}
	if !strings.Contains(out, "2 messages selected") {
		t.Errorf("output = %q, want the 2 timeout messages", out)
	}

	b, _ := os.ReadFile(planPath)
	var p recovery.RecoveryPlan
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.MessageIDs) != 2 || p.GroupLabel != "Timeout connecting to" {
		t.Errorf("plan = %+v", p)
	}
}

func TestPlanUnknownGroupFails(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": planFixture()}})

	_, err := runCommand(t, "plan", "--group", "deadbeef", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "no messages selected") {
		t.Fatalf("expected no-messages error, got %v", err)
	}
}

func TestRecoverDryRunValidatesAndChangesNothing(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	fb := &fakeBroker{msgs: map[string][]message.Message{
		"orders-dlq": {
			{ID: "m1", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{"order_id":1}`)},
			{ID: "m2", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{"order_id":2}`)},
		},
	}}
	withFakeBroker(t, fb)

	// Build the plan file directly: two messages, both valid.
	p := recovery.RecoveryPlan{
		ID: "plan_test", Queue: "orders-dlq", MessageIDs: []string{"m1", "m2"},
		Destination: "orders", Action: "replay",
		Limits: recovery.DefaultLimits(), SafetyChecks: []string{recovery.CheckSchema, recovery.CheckDuplicate},
	}
	if err := writePlanFile(planPath, &p); err != nil {
		t.Fatal(err)
	}

	out, err := runCommand(t, "recover", "--plan", planPath, "--config", cfgPath)
	if err != nil {
		t.Fatalf("recover: %v\n%s", err, out)
	}
	for _, want := range []string{
		"Recovery plan:", planPath,
		"Selected:", "2 messages",
		"Destination:", "orders",
		"Payload validation:", "2/2 passed",
		"Messages to replay:", "2",
		"Changes made: NONE",
		"Dry run",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("recover output missing %q:\n%s", want, out)
		}
	}

	fb.mu.Lock()
	published := len(fb.published)
	acked := len(fb.acked)
	fb.mu.Unlock()
	if published != 0 || acked != 0 {
		t.Fatalf("dry-run performed mutating I/O: published=%d acked=%d", published, acked)
	}

	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entries, err := store.Recent(5)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !entries[0].DryRun || entries[0].Result != "dry_run" || entries[0].PlanID != "plan_test" {
		t.Errorf("audit entries = %+v, want one recover dry_run", entries)
	}
}

func TestRecoverFlagsDuplicateAndSkips(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")

	// Seed the audit with a prior replay of m1 -> duplicate evidence.
	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(audit.Entry{Action: audit.ActionReplay, MessageID: "m1", SourceQueue: "orders-dlq", Destination: "orders", Confirmed: true, Result: "success"}); err != nil {
		t.Fatal(err)
	}
	store.Close()

	fb := &fakeBroker{msgs: map[string][]message.Message{
		"orders-dlq": {
			{ID: "m1", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{"order_id":1}`)},
			{ID: "m2", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`not json`)},
			{ID: "m3", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{"order_id":3}`)},
		},
	}}
	withFakeBroker(t, fb)

	p := recovery.RecoveryPlan{
		ID: "plan_test", Queue: "orders-dlq", MessageIDs: []string{"m1", "m2", "m3", "gone"},
		Destination: "orders", Action: "replay",
		Limits: recovery.DefaultLimits(), SafetyChecks: []string{recovery.CheckSchema, recovery.CheckDuplicate},
	}
	if err := writePlanFile(planPath, &p); err != nil {
		t.Fatal(err)
	}

	out, err := runCommand(t, "recover", "--plan", planPath, "--config", cfgPath)
	if err != nil {
		t.Fatalf("recover: %v\n%s", err, out)
	}
	for _, want := range []string{
		"Selected:", "4 messages",
		"Payload validation:", "2/4 passed", // m1 and m3 valid; m2 invalid; gone missing
		"Duplicate warning:", "1",
		"Messages that will be skipped:", "3",
		"Messages to replay:", "1",
		"not_found_in_queue (1): gone",
		"payload_invalid (1): m2",
		"duplicate_evidence (1): m1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("recover output missing %q:\n%s", want, out)
		}
	}
}

func TestRecoverRejectsEmptySafetyChecks(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	p := recovery.RecoveryPlan{
		ID: "plan_test", Queue: "orders-dlq", MessageIDs: []string{"m1"},
		Destination: "orders", Action: "replay", Limits: recovery.DefaultLimits(),
	}
	if err := writePlanFile(planPath, &p); err != nil {
		t.Fatal(err)
	}
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {{ID: "m1", Payload: []byte(`{}`)}}}})

	_, err := runCommand(t, "recover", "--plan", planPath, "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "no safety checks") {
		t.Fatalf("expected no-safety-checks error, got %v", err)
	}
}

func TestRecoverConfirmNotYet(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	p := recovery.RecoveryPlan{
		ID: "plan_test", Queue: "orders-dlq", MessageIDs: []string{"m1"},
		Destination: "orders", Action: "replay",
		Limits: recovery.DefaultLimits(), SafetyChecks: []string{recovery.CheckSchema},
	}
	if err := writePlanFile(planPath, &p); err != nil {
		t.Fatal(err)
	}
	withFakeBroker(t, &fakeBroker{})

	_, err := runCommand(t, "recover", "--plan", planPath, "--confirm", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "next phase") {
		t.Fatalf("expected not-yet error for --confirm, got %v", err)
	}
}

func TestRecoverRequiresPlanFlag(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{})

	_, err := runCommand(t, "recover", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "plan") {
		t.Fatalf("expected missing --plan error, got %v", err)
	}
}

func TestRecoverJSON(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	p := recovery.RecoveryPlan{
		ID: "plan_test", Queue: "orders-dlq", MessageIDs: []string{"m1"},
		Destination: "orders", Action: "replay",
		Limits: recovery.DefaultLimits(), SafetyChecks: []string{recovery.CheckSchema, recovery.CheckDuplicate},
	}
	if err := writePlanFile(planPath, &p); err != nil {
		t.Fatal(err)
	}
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {{ID: "m1", Payload: []byte(`{}`)}}}})

	out, err := runCommand(t, "recover", "--plan", planPath, "--output", "json", "--config", cfgPath)
	if err != nil {
		t.Fatalf("recover --output json: %v\n%s", err, out)
	}
	var res struct {
		PlanID   string `json:"plan_id"`
		Selected int    `json:"selected"`
		ToReplay int    `json:"to_replay"`
		Checks   int    `json:"-"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	if res.PlanID != "plan_test" || res.Selected != 1 || res.ToReplay != 1 {
		t.Errorf("result = %+v", res)
	}
}
