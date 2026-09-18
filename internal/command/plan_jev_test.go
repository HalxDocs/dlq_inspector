package command

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/jev"
	"github.com/HalxDocs/dlq_inspector/internal/message"
	"github.com/HalxDocs/dlq_inspector/internal/recovery"
)

func TestPlanWithJevExcludesJevDoNotReplay(t *testing.T) {
	hits := 0
	srv := jevMockServer(t, "do_not_replay", 0.9, &hits)
	defer srv.Close()
	t.Setenv(jev.APIKeyEnv, "test-key")

	msgs := append(planFixture(), message.Message{
		ID: "m5", Queue: "orders-dlq", Destination: "orders",
		FailureReason: "the flux capacitor demagnetized",
	})
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": msgs}})
	planPath := filepath.Join(t.TempDir(), "recovery.json")

	out, err := runCommand(t, "plan", "--config", cfgPath,
		"--output-file", planPath, "--with-jev", "--jev-endpoint", srv.URL)
	if err != nil {
		t.Fatalf("plan --with-jev: %v\n%s", err, out)
	}
	if hits != 1 {
		t.Errorf("jev calls = %d, want 1 (only the INVESTIGATE message)", hits)
	}
	if !strings.Contains(out, "[jev: 1 calls]") {
		t.Errorf("output missing jev marker:\n%s", out)
	}
	b, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	var p recovery.RecoveryPlan
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatal(err)
	}
	for _, id := range p.MessageIDs {
		if id == "m5" {
			t.Errorf("jev DO_NOT_REPLAY message m5 selected: %v", p.MessageIDs)
		}
	}
	found := false
	for _, e := range p.Excluded {
		if e.MessageID == "m5" && e.Classification == recovery.DoNotReplay && strings.Contains(e.Reason, "jev") {
			found = true
		}
	}
	if !found {
		t.Errorf("m5 not excluded with jev reason: %+v", p.Excluded)
	}
}

func TestAppendJevAuditSuffix(t *testing.T) {
	if got := appendJevAuditSuffix("", 3); got != "[jev: 3 calls]" {
		t.Errorf("got %q", got)
	}
	if got := appendJevAuditSuffix("quarterly replay", 2); got != "quarterly replay [jev: 2 calls]" {
		t.Errorf("got %q", got)
	}
}
