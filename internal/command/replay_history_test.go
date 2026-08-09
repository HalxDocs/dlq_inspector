package command

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/config"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

var errBrokerDown = errors.New("broker down")

// replayTestConfig writes a config whose audit store lives in a temp dir, so
// tests never touch the real ~/.dlq. Returns the config path and audit path.
func replayTestConfig(t *testing.T) (string, string) {
	t.Helper()
	auditPath := filepath.Join(t.TempDir(), "audit.db")
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := config.Defaults()
	cfg.Audit.Path = auditPath
	cfg.DefaultProfile = "dev"
	cfg.Profiles = map[string]*config.Profile{
		"dev": {Broker: "rabbitmq", URL: "amqp://guest:guest@localhost:5672/", DefaultQueue: "orders-dlq"},
	}
	if err := config.Save(path, cfg); err != nil {
		t.Fatal(err)
	}
	return path, auditPath
}

func replayMsg() message.Message {
	return message.Message{
		ID:          "m1",
		Queue:       "orders-dlq",
		Destination: "orders",
		Payload:     []byte(`{"order_id":123}`),
		RetryCount:  2,
	}
}

func TestReplayDryRunDefault(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	fb := &fakeBroker{
		queues: []broker.QueueSummary{{Name: "orders"}},
		msgs:   map[string][]message.Message{"orders-dlq": {replayMsg()}},
	}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "replay", "--id", "m1", "--config", cfgPath)
	if err != nil {
		t.Fatalf("replay (dry-run): %v\n%s", err, out)
	}
	for _, want := range []string{"m1", "orders-dlq", "orders", "Changes made: NONE", "Dry run"} {
		if !strings.Contains(out, want) {
			t.Errorf("dry-run output missing %q:\n%s", want, out)
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
	entries, err := store.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !entries[0].DryRun || entries[0].Result != "dry_run" {
		t.Fatalf("audit entries = %+v, want one dry_run", entries)
	}
}

func TestReplayConfirmExecutesAndAudits(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	fb := &fakeBroker{
		queues: []broker.QueueSummary{{Name: "orders"}},
		msgs:   map[string][]message.Message{"orders-dlq": {replayMsg()}},
	}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "replay", "--id", "m1", "--confirm", "--config", cfgPath)
	if err != nil {
		t.Fatalf("replay --confirm: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Replayed m1 -> orders") {
		t.Errorf("output = %q", out)
	}

	fb.mu.Lock()
	published := append([]publishCall(nil), fb.published...)
	acked := append([]ackCall(nil), fb.acked...)
	fb.mu.Unlock()
	if len(published) != 1 || published[0].destination != "orders" || published[0].id != "m1" {
		t.Fatalf("published = %+v", published)
	}
	if len(acked) != 1 || acked[0].id != "m1" {
		t.Fatalf("acked = %+v", acked)
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
	if len(entries) != 1 || entries[0].Result != "success" || !entries[0].Confirmed {
		t.Fatalf("audit entries = %+v, want confirmed success", entries)
	}
	if entries[0].Profile != "dev" || entries[0].Broker != "rabbitmq" || entries[0].SourceQueue != "orders-dlq" || entries[0].Destination != "orders" {
		t.Errorf("audit entry fields = %+v", entries[0])
	}
}

func TestReplayRequiresID(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{})

	_, err := runCommand(t, "replay", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "--id") {
		t.Fatalf("expected missing --id error, got %v", err)
	}
}

func TestReplayPublishFailureNoAck(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	fb := &fakeBroker{
		queues:     []broker.QueueSummary{{Name: "orders"}},
		msgs:       map[string][]message.Message{"orders-dlq": {replayMsg()}},
		publishErr: errBrokerDown,
	}
	withFakeBroker(t, fb)

	_, err := runCommand(t, "replay", "--id", "m1", "--confirm", "--config", cfgPath)
	if err == nil {
		t.Fatal("expected publish failure error")
	}

	fb.mu.Lock()
	acked := len(fb.acked)
	fb.mu.Unlock()
	if acked != 0 {
		t.Fatalf("Ack called after failed publish (acked=%d)", acked)
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
	if len(entries) != 1 || entries[0].Result != "publish_failed" {
		t.Fatalf("audit entries = %+v, want publish_failed", entries)
	}
}

func TestReplayDuplicateWarning(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(audit.Entry{
		Action: audit.ActionReplay, MessageID: "m1", SourceQueue: "orders-dlq",
		Destination: "orders", Confirmed: true, Result: "success",
	}); err != nil {
		t.Fatal(err)
	}
	store.Close()

	fb := &fakeBroker{
		queues: []broker.QueueSummary{{Name: "orders"}},
		msgs:   map[string][]message.Message{"orders-dlq": {replayMsg()}},
	}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "replay", "--id", "m1", "--config", cfgPath)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !strings.Contains(out, "POSSIBLE DUPLICATE") {
		t.Errorf("duplicate warning missing:\n%s", out)
	}
}

func TestReplayDryRunSurfacesMissingDestination(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	// No queues registered: the message's destination does not exist.
	fb := &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {replayMsg()}}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "replay", "--id", "m1", "--config", cfgPath)
	if err != nil {
		t.Fatalf("replay (dry-run): %v\n%s", err, out)
	}
	for _, want := range []string{"orders", "does not exist", "refused", "Changes made: NONE"} {
		if !strings.Contains(out, want) {
			t.Errorf("dry-run output missing %q:\n%s", want, out)
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
	entries, err := store.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Result != "dry_run" {
		t.Fatalf("audit entries = %+v, want one dry_run", entries)
	}
}

func TestReplayConfirmRefusesMissingDestination(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	fb := &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {replayMsg()}}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "replay", "--id", "m1", "--confirm", "--yes", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "refusing to replay") {
		t.Fatalf("expected refusal, got err=%v\n%s", err, out)
	}
	if !strings.Contains(err.Error(), "orders") {
		t.Errorf("refusal error = %q, want the destination name", err)
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
	entries, err := store.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Result != "refused" || !entries[0].Confirmed {
		t.Fatalf("audit entries = %+v, want one confirmed refused entry", entries)
	}
	if !strings.Contains(entries[0].Reason, "orders") {
		t.Errorf("refusal reason = %q, want the queue name", entries[0].Reason)
	}
}

func TestReplayNotFound(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {replayMsg()}}})

	_, err := runCommand(t, "replay", "--id", "nope", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not-found error, got %v", err)
	}
}

func TestHistoryEmpty(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	out, err := runCommand(t, "history", "--config", cfgPath)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if !strings.Contains(out, "No audit entries yet") {
		t.Errorf("output = %q", out)
	}
}

func TestHistoryListsEntries(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(audit.Entry{Action: audit.ActionReplay, MessageID: "m1", SourceQueue: "orders-dlq", Destination: "orders", Confirmed: true, Result: "success"}); err != nil {
		t.Fatal(err)
	}
	store.Close()

	out, err := runCommand(t, "history", "--config", cfgPath)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	for _, want := range []string{"replay", "m1", "orders-dlq", "orders", "success"} {
		if !strings.Contains(out, want) {
			t.Errorf("history output missing %q:\n%s", want, out)
		}
	}
}

func TestHistoryByPlanShowsFullTrail(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []audit.Entry{
		{Action: audit.ActionRecover, PlanID: "plan_abc", MessageID: "m1", SourceQueue: "orders-dlq", Destination: "orders", Confirmed: true, Result: "success"},
		{Action: audit.ActionRecover, PlanID: "plan_abc", MessageID: "m2", SourceQueue: "orders-dlq", Destination: "orders", Confirmed: true, Result: "publish_failed"},
		{Action: audit.ActionRecover, PlanID: "plan_abc", SourceQueue: "orders-dlq", Destination: "orders", Confirmed: true, Result: "completed"},
		{Action: audit.ActionReplay, PlanID: "plan_other", MessageID: "x", SourceQueue: "q", Destination: "d", Confirmed: true, Result: "success"},
	} {
		if err := store.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	store.Close()

	out, err := runCommand(t, "history", "--plan", "plan_abc", "--config", cfgPath)
	if err != nil {
		t.Fatalf("history --plan: %v", err)
	}
	for _, want := range []string{"Plan plan_abc", "m1", "m2", "publish_failed", "completed", "(plan)"} {
		if !strings.Contains(out, want) {
			t.Errorf("history --plan output missing %q:\n%s", want, out)
		}
	}
	// Entries from other plans must not appear.
	if strings.Contains(out, "plan_other") || strings.Contains(out, "\tx\t") {
		t.Errorf("history --plan leaked another plan's entries:\n%s", out)
	}
}

func TestHistoryByPlanEmpty(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	out, err := runCommand(t, "history", "--plan", "plan_nope", "--config", cfgPath)
	if err != nil {
		t.Fatalf("history --plan: %v", err)
	}
	if !strings.Contains(out, "No audit entries for plan plan_nope") {
		t.Errorf("output = %q", out)
	}
}

func TestHistoryJSONL(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(audit.Entry{Action: audit.ActionReplay, MessageID: "m1", Confirmed: true, Result: "success"}); err != nil {
		t.Fatal(err)
	}
	store.Close()

	out, err := runCommand(t, "history", "--output", "jsonl", "--config", cfgPath)
	if err != nil {
		t.Fatalf("history --output jsonl: %v", err)
	}
	var e audit.Entry
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &e); err != nil {
		t.Fatalf("jsonl output not JSON: %v\n%s", err, out)
	}
	if e.MessageID != "m1" || e.Result != "success" {
		t.Errorf("entry = %+v", e)
	}
}
