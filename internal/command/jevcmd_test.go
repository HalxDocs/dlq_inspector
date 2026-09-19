package command

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/config"
	"github.com/HalxDocs/dlq_inspector/internal/jev"
)

func TestJevEnableDisableRoundTrip(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	t.Setenv(jev.APIKeyEnv, "test-key")

	out, err := runCommand(t, "jev", "enable", "--config", cfgPath, "--threshold", "0.8")
	if err != nil {
		t.Fatalf("jev enable: %v\n%s", err, out)
	}
	if !strings.Contains(out, `Jev assist enabled for profile "dev"`) {
		t.Errorf("output:\n%s", out)
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	eff := cfg.Profiles["dev"].Jev.Effective()
	if !eff.Enabled || eff.Threshold != 0.8 || eff.Model != jev.DefaultModel {
		t.Errorf("stored settings: %+v", eff)
	}

	out, err = runCommand(t, "jev", "disable", "--config", cfgPath)
	if err != nil {
		t.Fatalf("jev disable: %v\n%s", err, out)
	}
	cfg, _ = config.Load(cfgPath)
	eff = cfg.Profiles["dev"].Jev.Effective()
	if eff.Enabled {
		t.Error("disable did not persist")
	}
	if eff.Threshold != 0.8 {
		t.Errorf("disable clobbered tuning: %+v", eff)
	}

	// Disabling twice is a no-op success.
	out, err = runCommand(t, "jev", "disable", "--config", cfgPath)
	if err != nil || !strings.Contains(out, "already off") {
		t.Errorf("second disable: err=%v out=%q", err, out)
	}
}

func TestJevEnableRejectsBadThreshold(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	_, err := runCommand(t, "jev", "enable", "--config", cfgPath, "--threshold", "5")
	if err == nil || !strings.Contains(err.Error(), "invalid --threshold") {
		t.Fatalf("want threshold error, got %v", err)
	}
}

func TestJevEnableMissingProfile(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	_, err := runCommand(t, "jev", "enable", "--config", cfgPath, "--profile", "nope")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("want profile error, got %v", err)
	}
}

func TestJevStatus(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	t.Setenv(jev.APIKeyEnv, "")

	out, err := runCommand(t, "jev", "status", "--config", cfgPath)
	if err != nil {
		t.Fatalf("jev status: %v\n%s", err, out)
	}
	if !strings.Contains(out, "off (rule-based)") || !strings.Contains(out, "(not set)") {
		t.Errorf("status:\n%s", out)
	}

	if _, err := runCommand(t, "jev", "enable", "--config", cfgPath); err != nil {
		t.Fatal(err)
	}
	t.Setenv(jev.APIKeyEnv, "k")
	out, err = runCommand(t, "jev", "status", "--config", cfgPath, "--output", "json")
	if err != nil {
		t.Fatalf("jev status json: %v\n%s", err, out)
	}
	var res struct {
		Enabled   bool   `json:"enabled"`
		APIKeyEnv string `json:"api_key_env"`
		APIKeySet bool   `json:"api_key_is_set"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("invalid JSON:\n%s", out)
	}
	if !res.Enabled || res.APIKeyEnv != jev.APIKeyEnv || !res.APIKeySet {
		t.Errorf("status json: %+v", res)
	}
}
