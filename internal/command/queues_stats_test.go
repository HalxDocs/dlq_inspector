package command

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/config"
)

func brokerTestConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeTestConfig(t, path, "dev", "dev", &config.Profile{
		Broker:       "rabbitmq",
		URL:          "amqp://guest:guest@localhost:5672/",
		DefaultQueue: "orders-dlq",
	})
	return path
}

func TestQueuesListsSorted(t *testing.T) {
	cfgPath := brokerTestConfig(t)
	fb := &fakeBroker{queues: []broker.QueueSummary{
		{Name: "orders-dlq", Durable: true, Messages: 482, Consumers: 0, Pending: 12},
		{Name: "orders", Durable: true, Messages: 42, Consumers: 1, Pending: 3},
	}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "queues", "--config", cfgPath)
	if err != nil {
		t.Fatalf("queues: %v", err)
	}
	if strings.Index(out, "orders") > strings.Index(out, "orders-dlq") {
		t.Errorf("queues not sorted alphabetically:\n%s", out)
	}
	for _, want := range []string{"orders", "orders-dlq", "482", "42", "1", "true", "PENDING", "12", "3"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestQueuesJSON(t *testing.T) {
	cfgPath := brokerTestConfig(t)
	fb := &fakeBroker{queues: []broker.QueueSummary{{Name: "orders", Durable: true, Messages: 42}}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "queues", "--output", "json", "--config", cfgPath)
	if err != nil {
		t.Fatalf("queues --output json: %v", err)
	}
	var got []broker.QueueSummary
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("invalid JSON %q: %v", out, err)
	}
	if len(got) != 1 || got[0].Name != "orders" || got[0].Messages != 42 {
		t.Errorf("got %+v", got)
	}
}

func TestQueuesNoProfile(t *testing.T) {
	_, err := runCommand(t, "queues", "--config", filepath.Join(t.TempDir(), "config.yaml"))
	if err == nil || !strings.Contains(err.Error(), "no profile") {
		t.Fatalf("expected no-profile error, got %v", err)
	}
}

func TestStats(t *testing.T) {
	cfgPath := brokerTestConfig(t)
	fb := &fakeBroker{queues: []broker.QueueSummary{{Name: "orders-dlq", Messages: 482, Consumers: 0}}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "stats", "orders-dlq", "--config", cfgPath)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	for _, want := range []string{"orders-dlq", "482", "0"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestStatsShowsPending(t *testing.T) {
	cfgPath := brokerTestConfig(t)
	fb := &fakeBroker{queues: []broker.QueueSummary{{Name: "orders-dlq", Messages: 3, Consumers: 1}}, statsPending: 2}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "stats", "orders-dlq", "--config", cfgPath)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	for _, want := range []string{"Pending:", "2", "Consumers:", "1"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestStatsOmitsPendingWhenUnreported(t *testing.T) {
	// Brokers that do not track pending (e.g. RabbitMQ's AMQP path) must not
	// render a misleading "Pending: 0" line.
	cfgPath := brokerTestConfig(t)
	fb := &fakeBroker{queues: []broker.QueueSummary{{Name: "orders-dlq", Messages: 7}}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "stats", "orders-dlq", "--config", cfgPath)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if strings.Contains(out, "Pending") {
		t.Errorf("output shows Pending for a broker that does not report it:\n%s", out)
	}
}

func TestStatsUsesDefaultQueue(t *testing.T) {
	cfgPath := brokerTestConfig(t)
	fb := &fakeBroker{queues: []broker.QueueSummary{{Name: "orders-dlq", Messages: 7}}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "stats", "--config", cfgPath)
	if err != nil {
		t.Fatalf("stats (default queue): %v", err)
	}
	if !strings.Contains(out, "7") {
		t.Errorf("output = %q, want default queue stats", out)
	}
}

func TestStatsNoQueueAndNoDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeTestConfig(t, path, "dev", "dev", &config.Profile{Broker: "rabbitmq", URL: "amqp://localhost:5672/"})
	withFakeBroker(t, &fakeBroker{})

	_, err := runCommand(t, "stats", "--config", path)
	if err == nil || !strings.Contains(err.Error(), "default_queue") {
		t.Fatalf("expected missing-queue error, got %v", err)
	}
}

func TestStatsMissingQueue(t *testing.T) {
	cfgPath := brokerTestConfig(t)
	withFakeBroker(t, &fakeBroker{})

	_, err := runCommand(t, "stats", "nope", "--config", cfgPath)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not-found error, got %v", err)
	}
}

func TestStatsJSON(t *testing.T) {
	cfgPath := brokerTestConfig(t)
	fb := &fakeBroker{queues: []broker.QueueSummary{{Name: "orders-dlq", Messages: 482, Consumers: 0}}}
	withFakeBroker(t, fb)

	out, err := runCommand(t, "stats", "orders-dlq", "--output", "json", "--config", cfgPath)
	if err != nil {
		t.Fatalf("stats --output json: %v", err)
	}
	var got broker.QueueStats
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("invalid JSON %q: %v", out, err)
	}
	if got.Queue != "orders-dlq" || got.Messages != 482 {
		t.Errorf("got %+v", got)
	}
}
