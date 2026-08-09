package search

import (
	"testing"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

func msg(id string, retries int, reason, payload string, ts time.Time) message.Message {
	return message.Message{
		ID:            id,
		Payload:       []byte(payload),
		Timestamp:     ts,
		RetryCount:    retries,
		FailureReason: reason,
	}
}

var samplePayload = `{"event_type":"payment.completed","customer":{"id":"443"},"amount":1299}`

func TestMatchErrorTextCaseInsensitive(t *testing.T) {
	m := msg("a", 1, "payment timeout connecting to 10.0.4.5:6432", samplePayload, time.Time{})
	if !Match(m, broker.SearchFilter{ErrorText: "TIMEOUT"}) {
		t.Error("case-insensitive error text should match")
	}
	if Match(m, broker.SearchFilter{ErrorText: "refused"}) {
		t.Error("unrelated error text should not match")
	}
}

func TestMatchErrorTextInPayload(t *testing.T) {
	m := msg("a", 1, "rejected", `{"error":"database connection refused"}`, time.Time{})
	if !Match(m, broker.SearchFilter{ErrorText: "refused"}) {
		t.Error("error text inside the payload should match")
	}
}

func TestMatchSince(t *testing.T) {
	now := time.Now()
	m := msg("a", 0, "", samplePayload, now.Add(-time.Hour))
	if Match(m, broker.SearchFilter{Since: now}) {
		t.Error("older message should be excluded by Since")
	}
	if !Match(m, broker.SearchFilter{Since: now.Add(-2 * time.Hour)}) {
		t.Error("newer message should pass Since")
	}
}

func TestMatchMaxRetries(t *testing.T) {
	three := 3
	m := msg("a", 5, "", samplePayload, time.Time{})
	if Match(m, broker.SearchFilter{MaxRetries: &three}) {
		t.Error("5 retries should fail MaxRetries=3")
	}
	ok := msg("a", 2, "", samplePayload, time.Time{})
	if !Match(ok, broker.SearchFilter{MaxRetries: &three}) {
		t.Error("2 retries should pass MaxRetries=3")
	}
}

func TestMatchFields(t *testing.T) {
	m := msg("a", 0, "", samplePayload, time.Time{})
	if !Match(m, broker.SearchFilter{Fields: map[string]string{"customer.id": "443", "event_type": "payment.completed"}}) {
		t.Error("matching field paths should pass")
	}
	if Match(m, broker.SearchFilter{Fields: map[string]string{"customer.id": "999"}}) {
		t.Error("mismatched field value should fail")
	}
	if Match(m, broker.SearchFilter{Fields: map[string]string{"nope.missing": "1"}}) {
		t.Error("missing field path should fail")
	}
}

func TestMatchFieldsRequiresJSONPayload(t *testing.T) {
	m := msg("a", 0, "", "not json at all", time.Time{})
	if Match(m, broker.SearchFilter{Fields: map[string]string{"a": "b"}}) {
		t.Error("non-JSON payload should never match a field constraint")
	}
	if !Match(m, broker.SearchFilter{}) {
		t.Error("empty filter should match non-JSON payload")
	}
}

func TestFilterOffsetAndLimit(t *testing.T) {
	msgs := []message.Message{
		msg("1", 0, "", samplePayload, time.Time{}),
		msg("2", 0, "", samplePayload, time.Time{}),
		msg("3", 0, "", samplePayload, time.Time{}),
		msg("4", 0, "", samplePayload, time.Time{}),
	}
	got := Filter(msgs, broker.SearchFilter{Limit: 2, Offset: 1})
	if len(got) != 2 || got[0].ID != "2" || got[1].ID != "3" {
		t.Fatalf("Filter(offset=1, limit=2) = %+v", got)
	}
}
