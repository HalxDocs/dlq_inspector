package command

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runCommand executes the root command with the given args, capturing output.
func runCommand(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := NewRoot("test", "abc123", "2026-01-01")
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetArgs(args)
	err := root.Execute()
	return buf.String(), err
}

func TestVersion(t *testing.T) {
	out, err := runCommand(t, "version")
	if err != nil {
		t.Fatalf("version: %v", err)
	}
	if !strings.Contains(out, "dlq test") || !strings.Contains(out, "commit: abc123") {
		t.Errorf("version output = %q", out)
	}
}

func TestProfilesListEmpty(t *testing.T) {
	out, err := runCommand(t, "profiles", "list", "--config", filepath.Join(t.TempDir(), "config.yaml"))
	if err != nil {
		t.Fatalf("profiles list: %v", err)
	}
	if !strings.Contains(out, "No profiles configured") {
		t.Errorf("output = %q, want empty-state message", out)
	}
}

func TestConnectRejectsUnknownBroker(t *testing.T) {
	_, err := runCommand(t, "connect", "kafka", "--url", "amqp://localhost:5672/",
		"--config", filepath.Join(t.TempDir(), "config.yaml"))
	if err == nil || !strings.Contains(err.Error(), "unknown broker") {
		t.Fatalf("expected unknown broker error, got %v", err)
	}
}

func TestConnectRejectsBadScheme(t *testing.T) {
	_, err := runCommand(t, "connect", "rabbitmq", "--url", "http://localhost:5672/",
		"--config", filepath.Join(t.TempDir(), "config.yaml"))
	if err == nil || !strings.Contains(err.Error(), "unsupported URL scheme") {
		t.Fatalf("expected scheme error, got %v", err)
	}
}

func TestConnectRedisStreamAcceptsRedisScheme(t *testing.T) {
	out, err := runCommand(t, "connect", "redisstream", "--url", "redis://localhost:6379/0",
		"--profile", "dev", "--default-queue", "orders-dlq", "--config", filepath.Join(t.TempDir(), "config.yaml"))
	if err != nil {
		t.Fatalf("connect redisstream: %v\n%s", err, out)
	}
	if !strings.Contains(out, `Saved profile "dev"`) {
		t.Errorf("output = %q", out)
	}
}

func TestConnectRedisStreamRejectsAmqpScheme(t *testing.T) {
	_, err := runCommand(t, "connect", "redisstream", "--url", "amqp://localhost:5672/",
		"--config", filepath.Join(t.TempDir(), "config.yaml"))
	if err == nil || !strings.Contains(err.Error(), "unsupported URL scheme \"amqp\" for redisstream") {
		t.Fatalf("expected scheme error for redisstream, got %v", err)
	}
}

func TestConnectRabbitMQRejectsRedisScheme(t *testing.T) {
	_, err := runCommand(t, "connect", "rabbitmq", "--url", "redis://localhost:6379/0",
		"--config", filepath.Join(t.TempDir(), "config.yaml"))
	if err == nil || !strings.Contains(err.Error(), "unsupported URL scheme \"redis\" for rabbitmq") {
		t.Fatalf("expected scheme error for rabbitmq, got %v", err)
	}
}

func TestConnectRejectsBothURLAndEnv(t *testing.T) {
	_, err := runCommand(t, "connect", "rabbitmq", "--url", "amqp://localhost:5672/",
		"--url-env", "DLQ_URL", "--config", filepath.Join(t.TempDir(), "config.yaml"))
	if err == nil || !strings.Contains(err.Error(), "only one of") {
		t.Fatalf("expected both-set error, got %v", err)
	}
}

func TestConnectRejectsNeither(t *testing.T) {
	_, err := runCommand(t, "connect", "rabbitmq", "--config", filepath.Join(t.TempDir(), "config.yaml"))
	if err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("expected missing-url error, got %v", err)
	}
}

func TestConnectSavesProfileAndSetsDefault(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")

	out, err := runCommand(t, "connect", "rabbitmq", "--url", "amqp://guest:guest@localhost:5672/",
		"--profile", "dev", "--default-queue", "orders-dlq", "--config", cfgPath)
	if err != nil {
		t.Fatalf("connect: %v\n%s", err, out)
	}
	if !strings.Contains(out, `Saved profile "dev"`) {
		t.Errorf("output = %q", out)
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	body := string(data)
	for _, want := range []string{"default_profile: dev", "broker: rabbitmq", "url: amqp://guest:guest@localhost:5672/", "default_queue: orders-dlq"} {
		if !strings.Contains(body, want) {
			t.Errorf("config missing %q:\n%s", want, body)
		}
	}
}

func TestConnectSavesEnvVarNameNotValue(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	_, err := runCommand(t, "connect", "rabbitmq", "--url-env", "DLQ_PROD_AMQP_URL",
		"--profile", "prod", "--config", cfgPath)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	data, _ := os.ReadFile(cfgPath)
	body := string(data)
	if !strings.Contains(body, "url_env: DLQ_PROD_AMQP_URL") {
		t.Errorf("config missing url_env:\n%s", body)
	}
	if strings.Contains(body, "amqp://") {
		t.Errorf("config must not contain the URL value itself:\n%s", body)
	}
}

func TestProfilesListAfterConnect(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	if _, err := runCommand(t, "connect", "rabbitmq", "--url", "amqp://localhost:5672/",
		"--profile", "dev", "--config", cfgPath); err != nil {
		t.Fatal(err)
	}

	out, err := runCommand(t, "profiles", "list", "--config", cfgPath)
	if err != nil {
		t.Fatalf("profiles list: %v", err)
	}
	if !strings.Contains(out, "dev (default)") || !strings.Contains(out, "rabbitmq") {
		t.Errorf("profiles list output = %q", out)
	}
	// The URL value must never appear in profile listings.
	if strings.Contains(out, "amqp://") {
		t.Errorf("profiles list leaked the URL value: %q", out)
	}
}

func TestProfilesListJSON(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	if _, err := runCommand(t, "connect", "rabbitmq", "--url-env", "DLQ_STAGING_AMQP_URL",
		"--profile", "staging", "--config", cfgPath); err != nil {
		t.Fatal(err)
	}

	out, err := runCommand(t, "profiles", "list", "--output", "json", "--config", cfgPath)
	if err != nil {
		t.Fatalf("profiles list --output json: %v", err)
	}
	var entries []map[string]any
	if err := json.Unmarshal([]byte(out), &entries); err != nil {
		t.Fatalf("invalid JSON output %q: %v", out, err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	e := entries[0]
	if e["name"] != "staging" || e["broker"] != "rabbitmq" || e["default"] != true {
		t.Errorf("entry = %v", e)
	}
	if e["url_source"] != "env:DLQ_STAGING_AMQP_URL" {
		t.Errorf("url_source = %v", e["url_source"])
	}
}

func TestInvalidOutputFormat(t *testing.T) {
	_, err := runCommand(t, "version", "--output", "xml")
	if err == nil || !strings.Contains(err.Error(), "invalid --output") {
		t.Fatalf("expected output format error, got %v", err)
	}
}
