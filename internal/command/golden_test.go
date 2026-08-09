package command

// Golden-file tests snapshot the terminal output of the read-only CLI surface
// — analyze, plan, and recover --dry-run — so output regressions (a garbled
// table, a dropped line, a wrong count) are caught the moment they happen.
// They run against the in-memory fake broker with a fixed fixture, so they are
// deterministic, need no live broker, and run in every CI test step.
//
// Regenerate after an intentional output change:
//
//	go test ./internal/command/ -run TestGolden -update

import (
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
	"github.com/HalxDocs/dlq_inspector/internal/recovery"
)

// update rewrites the golden files when passed as -update.
var update = flag.Bool("update", false, "update golden files")

// planIDRe matches generated plan IDs ("plan_" + 10 hex chars). Plan IDs are
// derived from a timestamp, so the golden comparison must pin them to a
// placeholder or every run would differ.
var planIDRe = regexp.MustCompile(`plan_[0-9a-f]{10}`)

// goldenPath is the testdata location for one golden.
func goldenPath(name string) string {
	return filepath.Join("testdata", name+".golden")
}

// sanitizeGoldens replaces run-to-run volatile values — generated plan IDs and
// the temp plan file path — with stable placeholders so the comparison is
// deterministic.
func sanitizeGoldens(out, planPath string) string {
	out = planIDRe.ReplaceAllString(out, "plan_XXXXXXXXXX")
	out = strings.ReplaceAll(out, planPath, "PLAN.json")
	return out
}

// golden compares actual output against the stored golden, or rewrites it when
// -update is given.
func golden(t *testing.T, name, actual string) {
	t.Helper()
	path := goldenPath(name)
	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(actual), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run with -update to create it)", path, err)
	}
	if string(want) != actual {
		t.Errorf("golden %s mismatch:\n--- want ---\n%s\n--- got ---\n%s", path, want, actual)
	}
}

func TestGoldenAnalyze(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": planFixture()}})

	out, err := runCommand(t, "analyze", "--config", cfgPath)
	if err != nil {
		t.Fatalf("analyze: %v\n%s", err, out)
	}
	golden(t, "analyze", out)
}

func TestGoldenPlan(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": planFixture()}})

	planPath := filepath.Join(t.TempDir(), "recovery.json")
	out, err := runCommand(t, "plan", "--config", cfgPath, "--output-file", planPath)
	if err != nil {
		t.Fatalf("plan: %v\n%s", err, out)
	}
	golden(t, "plan", sanitizeGoldens(out, planPath))
}

func TestGoldenRecoverDryRun(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	p := recovery.RecoveryPlan{
		ID: "plan_test", Queue: "orders-dlq",
		MessageIDs:  []string{"m1", "m2", "m3"},
		Destination: "orders", Action: "replay",
		Limits:       recovery.DefaultLimits(),
		SafetyChecks: []string{recovery.CheckSchema, recovery.CheckDuplicate, recovery.CheckDestination},
	}
	if err := writePlanFile(planPath, &p); err != nil {
		t.Fatal(err)
	}
	fb := &fakeBroker{
		queues: []broker.QueueSummary{{Name: "orders"}},
		msgs: map[string][]message.Message{
			"orders-dlq": {
				{ID: "m1", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{"order_id":1}`)},
				{ID: "m2", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{"order_id":2}`)},
				{ID: "m3", Queue: "orders-dlq", Destination: "orders", Payload: []byte(`{"order_id":3}`)},
			},
		},
	}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "recover", "--plan", planPath, "--config", cfgPath)
	if err != nil {
		t.Fatalf("recover: %v\n%s", err, out)
	}
	golden(t, "recover-dry-run", sanitizeGoldens(out, planPath))
}
