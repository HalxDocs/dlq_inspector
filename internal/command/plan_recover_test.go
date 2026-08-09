package command

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/broker"
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

func TestPlanReportsExclusions(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	msgs := []message.Message{
		{ID: "m1", Queue: "orders-dlq", Destination: "orders", FailureReason: "rejected"},
		{ID: "m2", Queue: "orders-dlq", Destination: "orders", FailureReason: "rejected"},
		{ID: "m-dup", Queue: "orders-dlq", Destination: "orders", FailureReason: "rejected", Headers: map[string]string{"x-duplicate-of": "evt_42"}},
	}
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": msgs}})

	out, err := runCommand(t, "plan", "--config", cfgPath, "--output-file", planPath)
	if err != nil {
		t.Fatalf("plan: %v\n%s", err, out)
	}
	if !strings.Contains(out, "2 messages selected), 1 excluded (left in DLQ)") {
		t.Errorf("plan output = %q, want the exclusion count", out)
	}

	// The dry-run report shows the exclusion too.
	out, err = runCommand(t, "recover", "--plan", planPath, "--config", cfgPath)
	if err != nil {
		t.Fatalf("recover: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Excluded:") || !strings.Contains(out, "1") {
		t.Errorf("dry-run output missing exclusion line:\n%s", out)
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

func TestRecoverConfirmExecutesAndAudits(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	p := recovery.RecoveryPlan{
		ID: "plan_test", Queue: "orders-dlq", MessageIDs: []string{"m1", "m2", "m3"},
		Destination: "orders", Action: "replay",
		Limits: recovery.DefaultLimits(), SafetyChecks: []string{recovery.CheckSchema, recovery.CheckDuplicate},
	}
	if err := writePlanFile(planPath, &p); err != nil {
		t.Fatal(err)
	}
	fb := &fakeBroker{
		queues: []broker.QueueSummary{{Name: "orders"}},
		msgs: map[string][]message.Message{
			"orders-dlq": {
				{ID: "m1", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{}`)},
				{ID: "m2", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{}`)},
				{ID: "m3", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{}`)},
			},
		},
	}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "recover", "--plan", planPath, "--confirm", "--batch-size", "2", "--rate-limit", "0", "--config", cfgPath)
	if err != nil {
		t.Fatalf("recover --confirm: %v\n%s", err, out)
	}
	for _, want := range []string{"Selected:", "3", "Replayed:", "3", "Skipped:", "0", "Failed during replay:", "0", "New DLQ entries:", "0", "Duration:"} {
		if !strings.Contains(out, want) {
			t.Errorf("recover output missing %q:\n%s", want, out)
		}
	}

	fb.mu.Lock()
	published := len(fb.published)
	acked := len(fb.acked)
	fb.mu.Unlock()
	if published != 3 || acked != 3 {
		t.Fatalf("published=%d acked=%d, want 3 each", published, acked)
	}

	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entries, err := store.ByPlan("plan_test")
	if err != nil {
		t.Fatal(err)
	}
	// 3 per-message successes + 1 plan-level summary.
	if len(entries) != 4 {
		t.Fatalf("plan trail entries = %d, want 4: %+v", len(entries), entries)
	}
}

func TestRecoverConfirmCircuitBreakerTrips(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	p := recovery.RecoveryPlan{
		ID: "plan_test", Queue: "orders-dlq", MessageIDs: []string{"m1", "m2", "m3", "m4", "m5"},
		Destination: "orders", Action: "replay",
		Limits: recovery.DefaultLimits(), SafetyChecks: []string{recovery.CheckSchema, recovery.CheckDuplicate},
	}
	if err := writePlanFile(planPath, &p); err != nil {
		t.Fatal(err)
	}
	// publishErr fails every publish: the first batch (2 messages) trips the
	// breaker at 100% failure rate and the remaining messages are paused.
	fb := &fakeBroker{
		queues: []broker.QueueSummary{{Name: "orders"}},
		msgs: map[string][]message.Message{
			"orders-dlq": {
				{ID: "m1", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{}`)},
				{ID: "m2", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{}`)},
				{ID: "m3", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{}`)},
				{ID: "m4", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{}`)},
				{ID: "m5", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{}`)},
			},
		},
		publishErr: errBrokerDown,
	}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "recover", "--plan", planPath, "--confirm", "--batch-size", "2", "--rate-limit", "0", "--config", cfgPath)
	if err != nil {
		t.Fatalf("recover --confirm: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Circuit breaker tripped") {
		t.Errorf("missing circuit breaker message:\n%s", out)
	}
	if !strings.Contains(out, "--resume") {
		t.Errorf("missing --resume guidance:\n%s", out)
	}

	fb.mu.Lock()
	acked := len(fb.acked)
	fb.mu.Unlock()
	if acked != 0 {
		t.Fatalf("acked = %d after total publish failure, want 0", acked)
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

func TestRecoverDryRunSurfacesMissingDestination(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	p := recovery.RecoveryPlan{
		ID: "plan_test", Queue: "orders-dlq", MessageIDs: []string{"m1"},
		Destination: "vanished", Action: "replay",
		Limits:       recovery.DefaultLimits(),
		SafetyChecks: []string{recovery.CheckSchema, recovery.CheckDuplicate, recovery.CheckDestination},
	}
	if err := writePlanFile(planPath, &p); err != nil {
		t.Fatal(err)
	}
	// No queues registered: the destination does not exist.
	fb := &fakeBroker{msgs: map[string][]message.Message{
		"orders-dlq": {{ID: "m1", Queue: "orders-dlq", Destination: "vanished", Payload: []byte(`{}`)}},
	}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "recover", "--plan", planPath, "--config", cfgPath)
	if err != nil {
		t.Fatalf("recover: %v\n%s", err, out)
	}
	for _, want := range []string{
		"Destination warning:", "vanished", "does not exist",
		"Payload validation:", "1/1 passed",
		"Messages to replay:", "1",
		"Changes made: NONE",
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
}

func TestRecoverDryRunSurfacesPendingBacklog(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	p := recovery.RecoveryPlan{
		ID: "plan_test", Queue: "orders-dlq", MessageIDs: []string{"m1"},
		Destination: "orders", Action: "replay",
		Limits:       recovery.DefaultLimits(),
		SafetyChecks: []string{recovery.CheckSchema, recovery.CheckDuplicate, recovery.CheckDestination},
	}
	if err := writePlanFile(planPath, &p); err != nil {
		t.Fatal(err)
	}
	// The destination exists but its consumers already hold 2 unacknowledged
	// messages; a low threshold surfaces the backlog warning.
	fb := &fakeBroker{
		queues:       []broker.QueueSummary{{Name: "orders"}},
		statsPending: 2,
		msgs: map[string][]message.Message{
			"orders-dlq": {{ID: "m1", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{}`)}},
		},
	}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "recover", "--plan", planPath, "--pending-threshold", "2", "--config", cfgPath)
	if err != nil {
		t.Fatalf("recover: %v\n%s", err, out)
	}
	for _, want := range []string{
		"Pending warning:", "orders", "2 pending", "consumers may be backed up",
		"Payload validation:", "1/1 passed",
		"Messages to replay:", "1",
		"Changes made: NONE",
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

	// Without the threshold flag (default 100), the same 2 pending messages
	// are ordinary in-flight work and produce no warning.
	out, err = runCommand(t, "recover", "--plan", planPath, "--config", cfgPath)
	if err != nil {
		t.Fatalf("recover (default threshold): %v\n%s", err, out)
	}
	if strings.Contains(out, "Pending warning") {
		t.Errorf("default threshold warned for a small backlog:\n%s", out)
	}
}

func TestRecoverConfirmRefusesMissingDestination(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	// Deliberately a plan without the declared destination check: the
	// executor's invariant must still refuse.
	p := recovery.RecoveryPlan{
		ID: "plan_test", Queue: "orders-dlq", MessageIDs: []string{"m1"},
		Destination: "vanished", Action: "replay",
		Limits:       recovery.DefaultLimits(),
		SafetyChecks: []string{recovery.CheckSchema, recovery.CheckDuplicate},
	}
	if err := writePlanFile(planPath, &p); err != nil {
		t.Fatal(err)
	}
	fb := &fakeBroker{msgs: map[string][]message.Message{
		"orders-dlq": {{ID: "m1", Queue: "orders-dlq", Destination: "vanished", Payload: []byte(`{}`)}},
	}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "recover", "--plan", planPath, "--confirm", "--rate-limit", "0", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "refusing to run") {
		t.Fatalf("expected refusal, got err=%v\n%s", err, out)
	}
	fb.mu.Lock()
	published := len(fb.published)
	acked := len(fb.acked)
	fb.mu.Unlock()
	if published != 0 || acked != 0 {
		t.Fatalf("refused run performed I/O: published=%d acked=%d", published, acked)
	}

	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entries, err := store.ByPlan("plan_test")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Result != "refused" {
		t.Errorf("plan trail = %+v, want one refused entry", entries)
	}
}
