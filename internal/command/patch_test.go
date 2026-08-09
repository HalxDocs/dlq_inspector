package command

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

func patchMsg() message.Message {
	return message.Message{
		ID:          "m1",
		Queue:       "orders-dlq",
		Destination: "orders",
		Payload:     []byte(`{"order_id":1,"customer_id":1000}`),
	}
}

func patchBroker() *fakeBroker {
	return &fakeBroker{
		queues: []broker.QueueSummary{{Name: "orders"}},
		msgs:   map[string][]message.Message{"orders-dlq": {patchMsg()}},
	}
}

func TestPatchDryRunRendersDiff(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	fb := patchBroker()
	withFakeBroker(t, fb)

	out, err := runCommand(t, "patch", "--id", "m1", "--set", "customer_id=443", "--config", cfgPath)
	if err != nil {
		t.Fatalf("patch (dry-run): %v\n%s", err, out)
	}
	for _, want := range []string{
		"Payload diff:", "- customer_id: 1000", "+ customer_id: 443",
		"Changes made: NONE", "Dry run",
	} {
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

	// The dry-run is audited with the diff, so the trail shows the preview.
	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entries, err := store.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !entries[0].DryRun || entries[0].Result != "dry_run" || entries[0].Action != audit.ActionPatch {
		t.Fatalf("audit entries = %+v, want one patch dry_run", entries)
	}
	if !strings.Contains(entries[0].PayloadDiff, "+ customer_id: 443") {
		t.Errorf("dry-run audit diff = %q, want the rendered diff", entries[0].PayloadDiff)
	}
}

func TestPatchConfirmExecutesAndAudits(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	fb := patchBroker()
	withFakeBroker(t, fb)

	out, err := runCommand(t, "patch", "--id", "m1", "--set", "customer_id=443", "--confirm", "--yes", "--config", cfgPath)
	if err != nil {
		t.Fatalf("patch --confirm: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Patched and replayed m1 -> orders") {
		t.Errorf("output = %q", out)
	}

	fb.mu.Lock()
	published := append([]publishCall(nil), fb.published...)
	acked := append([]ackCall(nil), fb.acked...)
	fb.mu.Unlock()
	if len(published) != 1 || published[0].id != "m1" {
		t.Fatalf("published = %+v", published)
	}
	// The patched payload is published, not the original body.
	var payload map[string]any
	if err := json.Unmarshal([]byte(published[0].payload), &payload); err != nil {
		t.Fatalf("published payload not JSON: %v", err)
	}
	if payload["customer_id"] != float64(443) || payload["order_id"] != float64(1) {
		t.Errorf("published payload = %v, want customer_id 443 and order_id 1", payload)
	}
	if len(acked) != 1 || acked[0].id != "m1" {
		t.Fatalf("acked = %+v, want the DLQ copy acked after publish", acked)
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
	if len(entries) != 1 || entries[0].Action != audit.ActionPatch || entries[0].Result != "success" || !entries[0].Confirmed {
		t.Fatalf("audit entries = %+v, want one confirmed patch success", entries)
	}
	if !strings.Contains(entries[0].PayloadDiff, "- customer_id: 1000") || !strings.Contains(entries[0].PayloadDiff, "+ customer_id: 443") {
		t.Errorf("audit diff = %q, want the old->new diff", entries[0].PayloadDiff)
	}
}

func TestPatchNestedSet(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	fb := patchBroker()
	fb.msgs["orders-dlq"] = []message.Message{{ID: "m1", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{"billing":{"city":"Old","zip":"1111"}}`)}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "patch", "--id", "m1", "--set", "billing.city=Oslo", "--config", cfgPath)
	if err != nil {
		t.Fatalf("patch: %v\n%s", err, out)
	}
	if !strings.Contains(out, "- billing.city: \"Old\"") || !strings.Contains(out, "+ billing.city: \"Oslo\"") {
		t.Errorf("nested diff missing:\n%s", out)
	}
}

func TestPatchNoChangeRefused(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, patchBroker())

	_, err := runCommand(t, "patch", "--id", "m1", "--set", "customer_id=1000", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "no change") {
		t.Fatalf("expected no-change error, got %v", err)
	}
}

func TestPatchNonJSONPayload(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	fb := patchBroker()
	fb.msgs["orders-dlq"] = []message.Message{{ID: "m1", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`not json`)}}
	withFakeBroker(t, fb)

	_, err := runCommand(t, "patch", "--id", "m1", "--set", "a=1", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "JSON object") {
		t.Fatalf("expected non-JSON error, got %v", err)
	}
}

func TestPatchRequiresSet(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, patchBroker())

	_, err := runCommand(t, "patch", "--id", "m1", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "--set") {
		t.Fatalf("expected missing --set error, got %v", err)
	}
}

func TestPatchConfirmRefusesMissingDestination(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	// No queues registered: the replay destination does not exist.
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {patchMsg()}}})

	out, err := runCommand(t, "patch", "--id", "m1", "--set", "customer_id=443", "--confirm", "--yes", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "refusing to replay") {
		t.Fatalf("expected refusal, got err=%v\n%s", err, out)
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
	// The refusal is audited (confirmed, refused), with the diff recorded.
	if len(entries) != 1 || entries[0].Result != "refused" || !entries[0].Confirmed || entries[0].Action != audit.ActionPatch {
		t.Fatalf("audit entries = %+v, want one refused patch", entries)
	}
	if entries[0].PayloadDiff == "" {
		t.Error("refusal entry missing the diff")
	}
}

func TestHistoryShowsPayloadDiff(t *testing.T) {
	cfgPath, auditPath := replayTestConfig(t)
	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	diff := "- customer_id: 1000\n+ customer_id: 443"
	if err := store.Append(audit.Entry{
		Action: audit.ActionPatch, MessageID: "m1", SourceQueue: "orders-dlq", Destination: "orders",
		Confirmed: true, Result: "success", PayloadDiff: diff,
	}); err != nil {
		t.Fatal(err)
	}
	store.Close()

	out, err := runCommand(t, "history", "--config", cfgPath)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	for _, want := range []string{"Payload diff for m1", "- customer_id: 1000", "+ customer_id: 443"} {
		if !strings.Contains(out, want) {
			t.Errorf("history output missing %q:\n%s", want, out)
		}
	}
}
