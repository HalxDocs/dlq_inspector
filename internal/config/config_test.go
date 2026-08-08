package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaults(t *testing.T) {
	cfg := Defaults()
	if cfg.Audit.Path != DefaultAuditPath {
		t.Errorf("default audit path = %q, want %q", cfg.Audit.Path, DefaultAuditPath)
	}
	if cfg.Audit.RetentionDays != DefaultAuditRetentionDays {
		t.Errorf("default retention days = %d, want %d", cfg.Audit.RetentionDays, DefaultAuditRetentionDays)
	}
}

func TestLoadMissingFile(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "nope", "config.yaml"))
	if err != nil {
		t.Fatalf("Load of missing file: %v", err)
	}
	if cfg == nil || len(cfg.Profiles) != 0 {
		t.Fatalf("Load of missing file = %+v, want empty config", cfg)
	}
}

func TestLoadParse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := `default_profile: staging
profiles:
  staging:
    broker: rabbitmq
    url_env: DLQ_STAGING_AMQP_URL
    default_queue: orders-dlq
  production:
    broker: rabbitmq
    url: amqp://guest:guest@localhost:5672/
    require_confirm: true
audit:
  path: ~/.dlq/audit.db
  retention_days: 30
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DefaultProfile != "staging" {
		t.Errorf("default_profile = %q, want staging", cfg.DefaultProfile)
	}
	staging, ok := cfg.Profiles["staging"]
	if !ok {
		t.Fatal("staging profile missing")
	}
	if staging.Broker != "rabbitmq" || staging.URLEnv != "DLQ_STAGING_AMQP_URL" || staging.DefaultQueue != "orders-dlq" {
		t.Errorf("staging = %+v", staging)
	}
	prod := cfg.Profiles["production"]
	if prod == nil || !prod.RequireConfirm || prod.URL != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("production = %+v", prod)
	}
	if cfg.Audit.RetentionDays != 30 {
		t.Errorf("retention_days = %d, want 30", cfg.Audit.RetentionDays)
	}
}

func TestSaveRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	cfg := Defaults()
	cfg.DefaultProfile = "dev"
	cfg.Profiles = map[string]*Profile{
		"dev": {Broker: "rabbitmq", URL: "amqp://guest:guest@localhost:5672/", DefaultQueue: "orders-dlq"},
	}

	if err := Save(path, cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load after Save: %v", err)
	}
	if got.DefaultProfile != "dev" {
		t.Errorf("default_profile = %q, want dev", got.DefaultProfile)
	}
	dev, ok := got.Profiles["dev"]
	if !ok {
		t.Fatal("dev profile missing after round trip")
	}
	if dev.Broker != "rabbitmq" || dev.URL != "amqp://guest:guest@localhost:5672/" || dev.DefaultQueue != "orders-dlq" {
		t.Errorf("dev = %+v", dev)
	}
	if got.Audit.RetentionDays != DefaultAuditRetentionDays {
		t.Errorf("audit defaults lost on round trip: %+v", got.Audit)
	}
}

func TestProfileWithDefault(t *testing.T) {
	cfg := Defaults()
	cfg.DefaultProfile = "prod"
	cfg.Profiles = map[string]*Profile{"prod": {Broker: "rabbitmq"}}

	p, err := cfg.Profile("")
	if err != nil {
		t.Fatalf("Profile(\"\"): %v", err)
	}
	if p.Broker != "rabbitmq" {
		t.Errorf("Profile(\"\") = %+v", p)
	}

	if _, err := cfg.Profile("missing"); err == nil {
		t.Error("Profile(missing) expected error")
	}
}

func TestResolveURLFromEnv(t *testing.T) {
	t.Setenv("DLQ_TEST_AMQP_URL", "amqp://user:pass@broker:5672/")
	p := &Profile{URLEnv: "DLQ_TEST_AMQP_URL"}
	got, err := p.ResolveURL()
	if err != nil {
		t.Fatalf("ResolveURL: %v", err)
	}
	if got != "amqp://user:pass@broker:5672/" {
		t.Errorf("ResolveURL = %q", got)
	}
}

func TestResolveURLEnvUnset(t *testing.T) {
	t.Setenv("DLQ_TEST_MISSING_URL", "")
	p := &Profile{URLEnv: "DLQ_TEST_MISSING_URL"}
	if _, err := p.ResolveURL(); err == nil {
		t.Error("ResolveURL expected error when env var is unset")
	}
}

func TestResolveURLInline(t *testing.T) {
	p := &Profile{URL: "amqp://localhost:5672/"}
	got, err := p.ResolveURL()
	if err != nil {
		t.Fatalf("ResolveURL: %v", err)
	}
	if got != "amqp://localhost:5672/" {
		t.Errorf("ResolveURL = %q", got)
	}
}

func TestResolveURLNone(t *testing.T) {
	if _, err := (&Profile{}).ResolveURL(); err == nil {
		t.Error("ResolveURL expected error with no url and no url_env")
	}
}

func TestExpandPath(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ExpandPath("~/dlq/audit.db")
	if err != nil {
		t.Fatalf("ExpandPath: %v", err)
	}
	wantSuffix := filepath.Join("dlq", "audit.db")
	if !strings.HasPrefix(got, home) || !strings.HasSuffix(got, wantSuffix) {
		t.Errorf("ExpandPath(\"~/dlq/audit.db\") = %q, want prefix %q and suffix %q", got, home, wantSuffix)
	}

	plain := "relative/path.db"
	if got, err := ExpandPath(plain); err != nil || got != plain {
		t.Errorf("ExpandPath(%q) = %q, %v", plain, got, err)
	}
}
