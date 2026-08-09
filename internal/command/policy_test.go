package command

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/config"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

const testPolicy = `
rules:
  - when: error contains timeout
    action: replay
    params:
      max_retries: 3
  - when: event_type == order.cancelled
    action: do_not_replay
`

const brokenPolicy = `
rules:
  - when: bogus == x
    action: replay_now
`

func writePolicy(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func policyTestConfig(t *testing.T, policyFile string) (string, string) {
	t.Helper()
	cfgPath, auditPath := replayTestConfig(t)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Profiles["dev"].PolicyFile = policyFile
	if err := config.Save(cfgPath, cfg); err != nil {
		t.Fatal(err)
	}
	return cfgPath, auditPath
}

func TestPolicyValidateValid(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	path := writePolicy(t, testPolicy)

	out, err := runCommand(t, "policy", "validate", path, "--config", cfgPath)
	if err != nil {
		t.Fatalf("validate: %v\n%s", err, out)
	}
	if !strings.Contains(out, "valid (2 rules)") {
		t.Errorf("output = %q", out)
	}
}

func TestPolicyValidateInvalid(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	path := writePolicy(t, brokenPolicy)

	_, err := runCommand(t, "policy", "validate", path, "--config", cfgPath)
	if err == nil {
		t.Fatal("expected validation error")
	}
	for _, want := range []string{"rule 1", "unknown field", "unknown action"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
}

func TestPolicyValidateJSON(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	path := writePolicy(t, testPolicy)

	out, err := runCommand(t, "policy", "validate", path, "--output", "json", "--config", cfgPath)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	var res struct {
		Valid bool `json:"valid"`
		Rules int  `json:"rules"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	if !res.Valid || res.Rules != 2 {
		t.Errorf("result = %+v", res)
	}
}

func TestPolicyApplyBindsToProfile(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	path := writePolicy(t, testPolicy)

	out, err := runCommand(t, "policy", "apply", path, "--profile", "dev", "--config", cfgPath)
	if err != nil {
		t.Fatalf("apply: %v\n%s", err, out)
	}
	if !strings.Contains(out, `Policy applied to profile "dev"`) {
		t.Errorf("output = %q", out)
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	abs, _ := filepath.Abs(path)
	if cfg.Profiles["dev"].PolicyFile != abs {
		t.Errorf("profile policy_file = %q, want %q (absolute)", cfg.Profiles["dev"].PolicyFile, abs)
	}
}

func TestPolicyApplyRejectsBroken(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	path := writePolicy(t, brokenPolicy)

	out, err := runCommand(t, "policy", "apply", path, "--profile", "dev", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "does not validate") {
		t.Fatalf("expected apply to refuse a broken policy, got err=%v\n%s", err, out)
	}
}

func TestAnalyzeHonorsPolicy(t *testing.T) {
	cfgPath, _ := policyTestConfig(t, writePolicy(t, testPolicy))
	msgs := []message.Message{
		{ID: "m1", Queue: "orders-dlq", Destination: "orders", FailureReason: "rejected"},
		{ID: "m2", Queue: "orders-dlq", Destination: "orders", FailureReason: "rejected", Headers: map[string]string{"x-event-type": "order.cancelled"}},
	}
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": msgs}})

	out, err := runCommand(t, "analyze", "--config", cfgPath)
	if err != nil {
		t.Fatalf("analyze: %v\n%s", err, out)
	}
	for _, want := range []string{"2 messages analyzed", "order.cancelled", "DO_NOT_REPLAY"} {
		if !strings.Contains(out, want) {
			t.Errorf("analyze output missing %q:\n%s", want, out)
		}
	}
}

func TestAnalyzeBrokenPolicyFails(t *testing.T) {
	cfgPath, _ := policyTestConfig(t, filepath.Join(t.TempDir(), "nope.yaml"))
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {replayMsg()}}})

	_, err := runCommand(t, "analyze", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "policy") {
		t.Fatalf("expected a policy error, got %v", err)
	}
}

func TestPlanHonorsPolicyExclusion(t *testing.T) {
	cfgPath, _ := policyTestConfig(t, writePolicy(t, testPolicy))
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	msgs := []message.Message{
		{ID: "m1", Queue: "orders-dlq", Destination: "orders", FailureReason: "rejected"},
		{ID: "m2", Queue: "orders-dlq", Destination: "orders", FailureReason: "rejected", Headers: map[string]string{"x-event-type": "order.cancelled"}},
	}
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": msgs}})

	out, err := runCommand(t, "plan", "--config", cfgPath, "--output-file", planPath)
	if err != nil {
		t.Fatalf("plan: %v\n%s", err, out)
	}
	if !strings.Contains(out, "1 messages selected") || !strings.Contains(out, "1 excluded (left in DLQ)") {
		t.Errorf("plan output = %q", out)
	}
}
