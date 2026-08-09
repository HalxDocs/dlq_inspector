package rabbitmq

import (
	"encoding/json"
	"testing"
)

func mgmtMsg(t *testing.T, raw string) mgmtMessage {
	t.Helper()
	var m mgmtMessage
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	return m
}

func TestToMessageFromMgmtTextPayload(t *testing.T) {
	m := toMessageFromMgmt(mgmtMsg(t, `{
		"payload": "{\"order_id\":123}",
		"payload_encoding": "string",
		"properties": {
			"message_id": "msg-1",
			"content_type": "application/json",
			"timestamp": 1700000000,
			"headers": {
				"x-death": [{"queue": "orders", "count": 3, "reason": "rejected"}]
			}
		}
	}`), "orders-dlq")

	if m.ID != "msg-1" {
		t.Errorf("ID = %q, want msg-1", m.ID)
	}
	if string(m.Payload) != `{"order_id":123}` {
		t.Errorf("Payload = %q", m.Payload)
	}
	if m.Queue != "orders-dlq" {
		t.Errorf("Queue = %q", m.Queue)
	}
	if m.Destination != "orders" {
		t.Errorf("Destination = %q, want orders (from x-death)", m.Destination)
	}
	if m.RetryCount != 3 {
		t.Errorf("RetryCount = %d, want 3", m.RetryCount)
	}
	if m.FailureReason != "rejected" {
		t.Errorf("FailureReason = %q, want rejected", m.FailureReason)
	}
	if m.Timestamp.Unix() != 1700000000 {
		t.Errorf("Timestamp = %v, want epoch 1700000000", m.Timestamp)
	}
	if m.ContentType != "application/json" {
		t.Errorf("ContentType = %q", m.ContentType)
	}
}

func TestToMessageFromMgmtBase64Payload(t *testing.T) {
	// "binary\x00data" is not valid UTF-8, so the management API base64-encodes it.
	m := toMessageFromMgmt(mgmtMsg(t, `{
		"payload": "YmluYXJ5AERhdGE=",
		"payload_encoding": "base64",
		"properties": {"headers": {}}
	}`), "q")

	want := []byte{'b', 'i', 'n', 'a', 'r', 'y', 0, 'D', 'a', 't', 'a'}
	if string(m.Payload) != string(want) {
		t.Errorf("Payload = %q, want %q", m.Payload, want)
	}
}

func TestToMessageFromMgmtIDRule(t *testing.T) {
	// No message_id property: falls back to x-message-id header.
	m := toMessageFromMgmt(mgmtMsg(t, `{
		"payload": "{}",
		"payload_encoding": "string",
		"properties": {"headers": {"x-message-id": "header-id"}}
	}`), "q")
	if m.ID != "header-id" {
		t.Errorf("ID = %q, want header-id", m.ID)
	}

	// Neither: sha256 content hash.
	m2 := toMessageFromMgmt(mgmtMsg(t, `{
		"payload": "{\"a\":1}",
		"payload_encoding": "string",
		"properties": {"headers": {}}
	}`), "q")
	if len(m2.ID) <= len("sha256:") || m2.ID[:7] != "sha256:" {
		t.Errorf("ID = %q, want sha256 prefix", m2.ID)
	}
}

func TestToMessageFromMgmtEventIDs(t *testing.T) {
	m := toMessageFromMgmt(mgmtMsg(t, `{
		"payload": "{}",
		"payload_encoding": "string",
		"properties": {"headers": {"x-event-id": "evt_1", "x-idempotency-key": "key_9"}}
	}`), "q")
	if m.EventID != "evt_1" || m.IdempotencyKey != "key_9" {
		t.Errorf("EventID=%q IdempotencyKey=%q", m.EventID, m.IdempotencyKey)
	}
}

func TestMgmtTimestampNullIsZero(t *testing.T) {
	if !mgmtTimestamp(json.RawMessage("null")).IsZero() {
		t.Error("null timestamp should map to zero time")
	}
	if !mgmtTimestamp(nil).IsZero() {
		t.Error("missing timestamp should map to zero time")
	}
}

func TestDecodeMgmtPayload(t *testing.T) {
	if got := decodeMgmtPayload("aGk=", "base64"); string(got) != "hi" {
		t.Errorf("base64 decode = %q, want hi", got)
	}
	if got := decodeMgmtPayload("plain", "string"); string(got) != "plain" {
		t.Errorf("string passthrough = %q", got)
	}
}
