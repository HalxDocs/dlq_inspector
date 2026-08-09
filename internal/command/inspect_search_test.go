package command

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/config"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

func inspectTestMsg() message.Message {
	return message.Message{
		ID:            "msg-1",
		Queue:         "orders-dlq",
		Destination:   "orders",
		Payload:       []byte(`{"customer":{"email":"a@b.com"},"order_id":123}`),
		Headers:       map[string]string{"x-message-id": "msg-1"},
		ContentType:   "application/json",
		Timestamp:     time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC),
		RetryCount:    3,
		FailureReason: "rejected",
	}
}

func sensitiveTestConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeTestConfig(t, path, "dev", "dev", &config.Profile{
		Broker:          "rabbitmq",
		URL:             "amqp://guest:guest@localhost:5672/",
		DefaultQueue:    "orders-dlq",
		SensitiveFields: []string{"customer.email"},
	})
	return path
}

func TestInspectPretty(t *testing.T) {
	cfgPath := sensitiveTestConfig(t)
	fb := &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {inspectTestMsg()}}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "inspect", "--id", "msg-1", "--config", cfgPath)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	for _, want := range []string{"msg-1", "orders-dlq", "orders", "3", "rejected", "application/json"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	// Sensitive field masked by default; email never appears.
	if strings.Contains(out, "a@b.com") {
		t.Errorf("sensitive email leaked:\n%s", out)
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Errorf("redaction mask missing:\n%s", out)
	}
	if !strings.Contains(out, `"order_id": 123`) {
		t.Errorf("payload not pretty-printed:\n%s", out)
	}
}

func TestInspectShowSensitive(t *testing.T) {
	cfgPath := sensitiveTestConfig(t)
	fb := &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {inspectTestMsg()}}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "inspect", "--id", "msg-1", "--show-sensitive", "--config", cfgPath)
	if err != nil {
		t.Fatalf("inspect --show-sensitive: %v", err)
	}
	if !strings.Contains(out, "a@b.com") {
		t.Errorf("--show-sensitive should reveal email:\n%s", out)
	}
}

func TestInspectJSON(t *testing.T) {
	cfgPath := sensitiveTestConfig(t)
	fb := &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {inspectTestMsg()}}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "inspect", "--id", "msg-1", "--output", "json", "--config", cfgPath)
	if err != nil {
		t.Fatalf("inspect --output json: %v", err)
	}
	var got message.Message
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("invalid JSON %q: %v", out, err)
	}
	if got.ID != "msg-1" || got.RetryCount != 3 {
		t.Errorf("got %+v", got)
	}
	if strings.Contains(string(got.Payload), "a@b.com") {
		t.Errorf("json payload not redacted: %s", got.Payload)
	}
}

func TestInspectRequiresID(t *testing.T) {
	cfgPath := sensitiveTestConfig(t)
	withFakeBroker(t, &fakeBroker{})

	_, err := runCommand(t, "inspect", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "--id") {
		t.Fatalf("expected missing --id error, got %v", err)
	}
}

func TestInspectNotFound(t *testing.T) {
	cfgPath := sensitiveTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {inspectTestMsg()}}})

	_, err := runCommand(t, "inspect", "--id", "nope", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not-found error, got %v", err)
	}
}

func TestSearchTable(t *testing.T) {
	cfgPath := brokerTestConfig(t)
	msgs := []message.Message{
		{ID: "m1", Payload: []byte(`{"event_type":"payment.completed"}`), Timestamp: time.Now(), RetryCount: 1, FailureReason: "payment timeout"},
		{ID: "m2", Payload: []byte(`{"event_type":"payment.completed"}`), Timestamp: time.Now(), RetryCount: 5, FailureReason: "rejected"},
	}
	fb := &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": msgs}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "search", "--config", cfgPath)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	for _, want := range []string{"m1", "m2", "2 message(s) match"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestSearchPassesFilter(t *testing.T) {
	cfgPath := brokerTestConfig(t)
	fb := &fakeBroker{}
	withFakeBroker(t, fb)

	if _, err := runCommand(t, "search",
		"--error", "timeout",
		"--since", "2h",
		"--field", "event_type=payment.completed",
		"--field", "customer.id=443",
		"--max-retries", "3",
		"--limit", "25",
		"--config", cfgPath); err != nil {
		t.Fatalf("search with filters: %v", err)
	}

	f := fb.lastFilter
	if f.ErrorText != "timeout" {
		t.Errorf("ErrorText = %q, want timeout", f.ErrorText)
	}
	if f.Fields["event_type"] != "payment.completed" || f.Fields["customer.id"] != "443" {
		t.Errorf("Fields = %v", f.Fields)
	}
	if f.Limit != 25 {
		t.Errorf("Limit = %d, want 25", f.Limit)
	}
	if f.MaxRetries == nil || *f.MaxRetries != 3 {
		t.Errorf("MaxRetries = %v, want 3", f.MaxRetries)
	}
	// --since 2h must resolve to a time roughly two hours in the past.
	if f.Since.IsZero() {
		t.Error("Since not set")
	} else if age := time.Since(f.Since); age < 90*time.Minute || age > 150*time.Minute {
		t.Errorf("Since = %v, want ~2h ago", f.Since)
	}
}

func TestSearchRejectsMalformedField(t *testing.T) {
	cfgPath := brokerTestConfig(t)
	withFakeBroker(t, &fakeBroker{})

	_, err := runCommand(t, "search", "--field", "novalue", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "--field") {
		t.Fatalf("expected malformed --field error, got %v", err)
	}
}

func TestSearchRejectsBadSince(t *testing.T) {
	cfgPath := brokerTestConfig(t)
	withFakeBroker(t, &fakeBroker{})

	_, err := runCommand(t, "search", "--since", "not-a-time", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "--since") {
		t.Fatalf("expected bad --since error, got %v", err)
	}
}

func TestSearchJSON(t *testing.T) {
	cfgPath := brokerTestConfig(t)
	fb := &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {
		{ID: "m1", Payload: []byte(`{"a":1}`)},
	}}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "search", "--output", "json", "--config", cfgPath)
	if err != nil {
		t.Fatalf("search --output json: %v", err)
	}
	var got []message.Message
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("invalid JSON %q: %v", out, err)
	}
	if len(got) != 1 || got[0].ID != "m1" {
		t.Errorf("got %+v", got)
	}
}

func TestSearchEmpty(t *testing.T) {
	cfgPath := brokerTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": {}}})

	out, err := runCommand(t, "search", "--config", cfgPath)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if !strings.Contains(out, "No messages match") {
		t.Errorf("output = %q, want empty-state message", out)
	}
}
