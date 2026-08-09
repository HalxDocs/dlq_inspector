package rabbitmq

import (
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestToMessagePrefersMessageIDProperty(t *testing.T) {
	m := toMessage(amqp.Delivery{
		MessageId: "msg-1",
		Headers:   amqp.Table{"x-message-id": "header-id"},
		Body:      []byte(`{"a":1}`),
	}, "orders-dlq")

	if m.ID != "msg-1" {
		t.Errorf("ID = %q, want msg-1", m.ID)
	}
	if m.Queue != "orders-dlq" {
		t.Errorf("Queue = %q, want orders-dlq", m.Queue)
	}
}

func TestToMessageFallsBackToHeaderID(t *testing.T) {
	m := toMessage(amqp.Delivery{Headers: amqp.Table{"x-message-id": "header-id"}}, "q")
	if m.ID != "header-id" {
		t.Errorf("ID = %q, want header-id", m.ID)
	}
}

func TestToMessageContentHashID(t *testing.T) {
	d := amqp.Delivery{Headers: amqp.Table{"h": "v"}, Body: []byte("body")}

	m1 := toMessage(d, "q")
	if !strings.HasPrefix(m1.ID, "sha256:") {
		t.Fatalf("ID = %q, want sha256: prefix", m1.ID)
	}

	m2 := toMessage(d, "q")
	if m1.ID != m2.ID {
		t.Errorf("content hash not deterministic: %q vs %q", m1.ID, m2.ID)
	}

	d.Body = []byte("other")
	m3 := toMessage(d, "q")
	if m1.ID == m3.ID {
		t.Errorf("content hash did not change with payload: %q", m1.ID)
	}
}

func TestDeathInfo(t *testing.T) {
	h := amqp.Table{"x-death": []interface{}{
		amqp.Table{"queue": "orders", "reason": "rejected", "count": int64(3)},
		amqp.Table{"queue": "orders", "reason": "expired", "count": int64(1)},
	}}

	dest, retries, reason := deathInfo(h)
	if dest != "orders" || retries != 3 || reason != "rejected" {
		t.Errorf("deathInfo = (%q, %d, %q), want (orders, 3, rejected)", dest, retries, reason)
	}
}

func TestDeathInfoCountInt32(t *testing.T) {
	h := amqp.Table{"x-death": []interface{}{
		amqp.Table{"queue": "orders", "reason": "expired", "count": int32(7)},
	}}
	_, retries, _ := deathInfo(h)
	if retries != 7 {
		t.Errorf("retries = %d, want 7", retries)
	}
}

func TestDeathInfoMissing(t *testing.T) {
	dest, retries, reason := deathInfo(amqp.Table{})
	if dest != "" || retries != 0 || reason != "" {
		t.Errorf("deathInfo(empty) = (%q, %d, %q)", dest, retries, reason)
	}
}

func TestNormalizeHeaders(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tbl := amqp.Table{
		"str":    "s",
		"bool":   true,
		"num":    int64(42),
		"bytes":  []byte("raw"),
		"ts":     ts,
		"nested": amqp.Table{"k": "v"},
	}

	h := normalizeHeaders(tbl)
	if h["str"] != "s" || h["bool"] != "true" || h["num"] != "42" || h["bytes"] != "raw" {
		t.Errorf("headers = %v", h)
	}
	if !strings.Contains(h["ts"], "2026-01-02T03:04:05") {
		t.Errorf("ts header = %q", h["ts"])
	}
	if !strings.Contains(h["nested"], `"k":"v"`) {
		t.Errorf("nested header = %q", h["nested"])
	}
}

func TestExtractIDs(t *testing.T) {
	ev, idem := extractIDs(map[string]string{"event_id": "evt_1", "x-idempotency-key": "key_1"})
	if ev != "evt_1" || idem != "key_1" {
		t.Errorf("extractIDs = (%q, %q)", ev, idem)
	}
}

func TestExtractIDsMissing(t *testing.T) {
	ev, idem := extractIDs(map[string]string{"unrelated": "x"})
	if ev != "" || idem != "" {
		t.Errorf("extractIDs = (%q, %q), want empty", ev, idem)
	}
}
