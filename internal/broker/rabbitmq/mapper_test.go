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

// TestMessageIDStableAcrossReadPaths pins the contract the whole recovery
// loop depends on: a dead-lettered message must get the same ID whether it is
// read through the AMQP path (Ack, delivery scan) or the management API path
// (Inspect, Search). The x-death header serializes differently on each path
// (AMQP timestamps become time.Time, management returns unix floats), so it
// must not participate in the content hash.
func TestMessageIDStableAcrossReadPaths(t *testing.T) {
	payload := []byte(`{"order_id":1}`)
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	// AMQP path: x-death arrives as an amqp table with a timestamp value.
	amqpHeaders := map[string]string{
		"x-event-id": "evt_1",
		"x-death": headerString([]interface{}{amqp.Table{
			"queue": "orders", "reason": "rejected", "count": int32(1), "time": ts,
		}}),
	}
	// Management path: the same logical header, but "time" is a unix float
	// and counts are float64.
	mgmtHeaders := map[string]string{
		"x-event-id": "evt_1",
		"x-death": headerString([]interface{}{map[string]any{
			"queue": "orders", "reason": "rejected", "count": float64(1), "time": float64(ts.Unix()),
		}}),
	}

	idAMQP := messageIDFromParts("", amqpHeaders, payload)
	idMgmt := messageIDFromParts("", mgmtHeaders, payload)
	if idAMQP != idMgmt {
		t.Fatalf("ID differs across read paths:\n  amqp: %s\n  mgmt: %s", idAMQP, idMgmt)
	}

	// The ID must be stable across DLQ hops: a later hop changes the count
	// and time inside x-death but not the content identity.
	later := headerString([]interface{}{amqp.Table{
		"queue": "orders", "reason": "rejected", "count": int32(2), "time": ts.Add(time.Hour),
	}})
	idLater := messageIDFromParts("", map[string]string{"x-event-id": "evt_1", "x-death": later}, payload)
	if idLater != idAMQP {
		t.Errorf("ID changed across DLQ hops: %s vs %s", idAMQP, idLater)
	}

	// Application headers and payload still participate.
	otherPayload := messageIDFromParts("", amqpHeaders, []byte(`{"order_id":2}`))
	if otherPayload == idAMQP {
		t.Error("ID did not change with the payload")
	}
	otherHeader := messageIDFromParts("", map[string]string{"x-event-id": "evt_2", "x-death": amqpHeaders["x-death"]}, payload)
	if otherHeader == idAMQP {
		t.Error("ID did not change with an application header")
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

// TestToMessageDestinationFallback pins the rollback round-trip: a restored
// DLQ entry has no x-death (dead-letter bookkeeping is stripped on republish)
// but carries the original replay destination as an x-destination header, and
// the mapper must resolve it so a future plan can replay the message again.
func TestToMessageDestinationFallback(t *testing.T) {
	m := toMessage(amqp.Delivery{
		Headers: amqp.Table{"x-destination": "orders"},
		Body:    []byte(`{"order_id":1}`),
	}, "orders-dlq")

	if m.Destination != "orders" {
		t.Errorf("Destination = %q, want the x-destination fallback", m.Destination)
	}
}

func TestToMessageDestinationPrefersXDeath(t *testing.T) {
	// A real dead-lettered message has both x-death and (possibly) a stale
	// x-destination; x-death must win.
	m := toMessage(amqp.Delivery{
		Headers: amqp.Table{
			"x-death":       []interface{}{amqp.Table{"queue": "orders-v2", "reason": "rejected", "count": int64(1)}},
			"x-destination": "orders",
		},
	}, "orders-dlq")
	if m.Destination != "orders-v2" {
		t.Errorf("Destination = %q, want x-death to win", m.Destination)
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
