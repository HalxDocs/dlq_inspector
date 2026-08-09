package command

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/config"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// analyzeFixture is a known failure mix: three transient timeouts (two with
// identical signatures after normalization), one validation failure, one
// duplicate, and one message with no failure metadata.
func analyzeFixture() []message.Message {
	ts := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	return []message.Message{
		{ID: "m1", Queue: "orders-dlq", Destination: "orders", FailureReason: "timeout connecting to 10.0.4.5:6432", RetryCount: 1, Timestamp: ts},
		{ID: "m2", Queue: "orders-dlq", Destination: "orders", FailureReason: "timeout connecting to 10.0.4.9:6432", RetryCount: 1, Timestamp: ts},
		{ID: "m3", Queue: "orders-dlq", Destination: "orders", FailureReason: "timeout connecting to 10.0.4.11:6432", RetryCount: 2, Timestamp: ts},
		{ID: "m4", Queue: "orders-dlq", Destination: "orders", FailureReason: "validation failed: customer_id is required", Timestamp: ts},
		{ID: "m5", Queue: "orders-dlq", Destination: "orders", FailureReason: "duplicate event already processed", Timestamp: ts},
		{ID: "m6", Queue: "orders-dlq", Destination: "orders", Timestamp: ts},
	}
}

func TestAnalyzeEmpty(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {}}})

	out, err := runCommand(t, "analyze", "--config", cfgPath)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if !strings.Contains(out, "No messages to analyze") {
		t.Errorf("output = %q", out)
	}
}

func TestAnalyzeGroupsText(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": analyzeFixture()}})

	out, err := runCommand(t, "analyze", "--config", cfgPath)
	if err != nil {
		t.Fatalf("analyze: %v\n%s", err, out)
	}
	for _, want := range []string{
		"6 messages analyzed in orders-dlq",
		"GROUP 1 -- Timeout connecting to",
		"3 messages - 50.0%",
		"Recommendation: REPLAYABLE",
		"Recommendation: REQUIRES_FIX",
		"duplicate event already processed",
		"Recommendation: DO_NOT_REPLAY",
		"(no failure reason)",
		"Recommendation: INVESTIGATE",
		"timeout connecting to {ip}:{port}",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("analyze output missing %q:\n%s", want, out)
		}
	}
}

func TestAnalyzeJSON(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": analyzeFixture()}})

	out, err := runCommand(t, "analyze", "--output", "json", "--config", cfgPath)
	if err != nil {
		t.Fatalf("analyze --output json: %v\n%s", err, out)
	}
	var res struct {
		Queue  string `json:"queue"`
		Total  int    `json:"total"`
		Groups []struct {
			Label          string `json:"label"`
			Signature      string `json:"signature"`
			Count          int    `json:"count"`
			Recommendation string `json:"recommendation"`
		} `json:"groups"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	if res.Queue != "orders-dlq" || res.Total != 6 {
		t.Errorf("result = queue %q total %d", res.Queue, res.Total)
	}
	if len(res.Groups) != 4 {
		t.Fatalf("groups = %d, want 4", len(res.Groups))
	}
	// Largest group first.
	if res.Groups[0].Count != 3 || res.Groups[0].Recommendation != "REPLAYABLE" {
		t.Errorf("first group = %+v", res.Groups[0])
	}
	if res.Groups[0].Signature != "timeout connecting to {ip}:{port}" {
		t.Errorf("signature = %q", res.Groups[0].Signature)
	}
}

func TestAnalyzeNoQueue(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	// Clear the default queue so queue resolution fails.
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Profiles = map[string]*config.Profile{"dev": {Broker: "rabbitmq", URL: "amqp://guest:guest@localhost:5672/"}}
	if err := config.Save(cfgPath, cfg); err != nil {
		t.Fatal(err)
	}
	withFakeBroker(t, &fakeBroker{})

	_, err = runCommand(t, "analyze", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "no queue given") {
		t.Fatalf("expected no-queue error, got %v", err)
	}
}
