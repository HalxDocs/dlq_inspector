package config

import (
	"path/filepath"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/jev"
)

func TestJevEffectiveDefaults(t *testing.T) {
	var nilSettings *JevSettings
	got := nilSettings.Effective()
	if got.Enabled {
		t.Error("nil settings must not enable Jev")
	}
	if got.Threshold != jev.DefaultThreshold || got.Model != jev.DefaultModel ||
		got.Endpoint != jev.DefaultEndpoint || got.MaxCalls != jev.DefaultMaxCalls ||
		got.APIKeyEnv != jev.APIKeyEnv {
		t.Errorf("defaults not applied: %+v", got)
	}

	custom := &JevSettings{Enabled: true, All: true, Threshold: 0.85, Model: "jev-1.13.0",
		Endpoint: "https://gateway/x", MaxCalls: 5, APIKeyEnv: "MY_KEY"}
	got = custom.Effective()
	if !got.Enabled || !got.All || got.Threshold != 0.85 || got.Model != "jev-1.13.0" ||
		got.Endpoint != "https://gateway/x" || got.MaxCalls != 5 || got.APIKeyEnv != "MY_KEY" {
		t.Errorf("explicit values lost: %+v", got)
	}
}

func TestJevSettingsRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := Defaults()
	cfg.DefaultProfile = "dev"
	cfg.Profiles = map[string]*Profile{
		"dev": {Broker: "rabbitmq", URL: "amqp://x/", Jev: &JevSettings{Enabled: true, Threshold: 0.8}},
	}
	if err := Save(path, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	eff := got.Profiles["dev"].Jev.Effective()
	if !eff.Enabled || eff.Threshold != 0.8 || eff.Model != jev.DefaultModel {
		t.Errorf("jev settings lost: %+v", eff)
	}
}

func TestProfileWithoutJevIsRuleBased(t *testing.T) {
	p := &Profile{Broker: "rabbitmq"}
	if p.Jev.Effective().Enabled {
		t.Error("profile without jev block must default to rule-based")
	}
}
