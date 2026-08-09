package redisstream

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

func TestToMessageMapsFields(t *testing.T) {
	entry := redis.XMessage{
		ID: "1690000000000-0",
		Values: map[string]any{
			fieldPayload:     `{"order_id":1}`,
			fieldContentType: "application/json",
			fieldDestination: "orders",
			fieldError:       "timeout connecting",
			fieldRetries:     "3",
			fieldTimestamp:   "2023-07-22T01:26:40Z",
			fieldHeaders:     `{"x-event-type":"order.created"}`,
			fieldMessageID:   "1690000000000-0",
		},
	}
	m := toMessage("orders-dlq", entry)

	if m.ID != "1690000000000-0" || m.Queue != "orders-dlq" {
		t.Errorf("id/queue = %q/%q", m.ID, m.Queue)
	}
	if string(m.Payload) != `{"order_id":1}` || m.ContentType != "application/json" {
		t.Errorf("payload/content = %q/%q", m.Payload, m.ContentType)
	}
	if m.Destination != "orders" || m.FailureReason != "timeout connecting" || m.RetryCount != 3 {
		t.Errorf("death metadata = %q/%q/%d", m.Destination, m.FailureReason, m.RetryCount)
	}
	if m.Headers["x-event-type"] != "order.created" {
		t.Errorf("headers = %v", m.Headers)
	}
	want := time.Date(2023, 7, 22, 1, 26, 40, 0, time.UTC)
	if !m.Timestamp.Equal(want) {
		t.Errorf("timestamp = %v, want %v", m.Timestamp, want)
	}
}

func TestToMessageExtractsIDs(t *testing.T) {
	entry := redis.XMessage{ID: "1-0", Values: map[string]any{
		fieldHeaders: `{"x-event-id":"evt_1","x-idempotency-key":"key_1"}`,
	}}
	m := toMessage("q", entry)
	if m.EventID != "evt_1" || m.IdempotencyKey != "key_1" {
		t.Errorf("ids = %q/%q", m.EventID, m.IdempotencyKey)
	}
}

func TestToMessageMissingFields(t *testing.T) {
	m := toMessage("q", redis.XMessage{ID: "1690000000000-0"})
	if m.ID != "1690000000000-0" || m.Destination != "" || m.RetryCount != 0 || m.FailureReason != "" {
		t.Errorf("message = %+v", m)
	}
	if len(m.Payload) != 0 {
		t.Errorf("payload = %q, want empty", m.Payload)
	}
}

func TestEntryTimeFallbackToEntryID(t *testing.T) {
	m := toMessage("q", redis.XMessage{ID: "1750000000000-5"})
	want := time.UnixMilli(1750000000000).UTC()
	if !m.Timestamp.Equal(want) {
		t.Errorf("timestamp = %v, want %v", m.Timestamp, want)
	}
}

func TestEntryFieldsRoundTrip(t *testing.T) {
	in := message.Message{
		ID:            "1690000000000-0",
		Destination:   "orders",
		Payload:       []byte(`{"order_id":1}`),
		Headers:       map[string]string{"x-event-type": "order.created"},
		ContentType:   "application/json",
		FailureReason: "rejected",
		RetryCount:    2,
		Timestamp:     time.Date(2023, 7, 22, 1, 26, 40, 0, time.UTC),
	}
	fields := entryFields(&in)
	if fields[fieldMessageID] != "1690000000000-0" {
		t.Errorf("message_id = %v", fields[fieldMessageID])
	}
	if fields[fieldPayload] != `{"order_id":1}` || fields[fieldDestination] != "orders" {
		t.Errorf("payload/destination = %v/%v", fields[fieldPayload], fields[fieldDestination])
	}
	if fields[fieldError] != "rejected" || fields[fieldRetries] != "2" {
		t.Errorf("error/retries = %v/%v", fields[fieldError], fields[fieldRetries])
	}
	var h map[string]string
	if err := json.Unmarshal([]byte(fields[fieldHeaders].(string)), &h); err != nil {
		t.Fatalf("headers not JSON: %v", err)
	}
	if h["x-event-type"] != "order.created" {
		t.Errorf("headers = %v", h)
	}
}

func TestEntryFieldsSkipsZeroMetadata(t *testing.T) {
	fields := entryFields(&message.Message{ID: "1-0", Payload: []byte(`{}`)})
	for _, k := range []string{fieldDestination, fieldError, fieldRetries, fieldHeaders, fieldTimestamp} {
		if _, ok := fields[k]; ok {
			t.Errorf("field %q present for zero metadata: %v", k, fields)
		}
	}
}
