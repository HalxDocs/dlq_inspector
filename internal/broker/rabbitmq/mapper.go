package rabbitmq

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// toMessage normalizes an amqp.Delivery into the shared message model. This is
// the only place amqp types are allowed to exist — nothing derived from it
// crosses out of this package.
func toMessage(d amqp.Delivery, queue string) message.Message {
	headers := normalizeHeaders(d.Headers)
	destination, retries, reason := deathInfo(d.Headers)
	// Rollback-restored DLQ entries carry the original replay destination as
	// an x-destination header (they have no x-death — it is stripped on
	// republish), so a future plan can still replay them.
	if destination == "" {
		destination = headers["x-destination"]
	}
	eventID, idemKey := extractIDs(headers)

	return message.Message{
		ID:             messageID(d, headers),
		Queue:          queue,
		Destination:    destination,
		Payload:        d.Body,
		Headers:        headers,
		ContentType:    d.ContentType,
		Timestamp:      d.Timestamp,
		RetryCount:     retries,
		FailureReason:  reason,
		EventID:        eventID,
		IdempotencyKey: idemKey,
	}
}

// messageID implements the stable-ID rule from the build plan for AMQP
// deliveries: the amqp message-id property, else the x-message-id header,
// else a sha256 content hash. The delivery tag is deliberately not used — it
// is scoped to one connection and therefore unstable.
func messageID(d amqp.Delivery, headers map[string]string) string {
	return messageIDFromParts(d.MessageId, headers, d.Body)
}

// messageIDFromParts is the single shared implementation of the stable-ID
// rule, used by both the AMQP and the management API mapping paths so a
// message inspected either way gets the same ID.
//
// Dead-letter bookkeeping headers (x-death, x-first-death-*) are excluded
// from the hash: they change on every DLQ hop (count increments, the time
// moves) and the two read paths serialize them differently (AMQP decodes
// timestamps to time.Time, the management API returns unix floats), so
// including them would give a message a different ID per read path and per
// hop. The hash covers the payload and the application headers — the content
// identity — which is what a stable message ID should mean.
func messageIDFromParts(msgID string, headers map[string]string, payload []byte) string {
	if msgID != "" {
		return msgID
	}
	if id := headers["x-message-id"]; id != "" {
		return id
	}

	h := sha256.New()
	h.Write(payload)
	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if isDeathHeader(k) {
			continue
		}
		h.Write([]byte(k))
		h.Write([]byte{'='})
		h.Write([]byte(headers[k]))
		h.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// isDeathHeader reports whether a header is RabbitMQ dead-letter bookkeeping.
func isDeathHeader(k string) bool {
	return strings.HasPrefix(k, "x-death") || strings.HasPrefix(k, "x-first-death")
}

// normalizeHeaders converts an amqp.Table into plain string values.
func normalizeHeaders(t amqp.Table) map[string]string {
	out := make(map[string]string, len(t))
	for k, v := range t {
		out[k] = headerString(v)
	}
	return out
}

// headerString renders a header value as a string. Nested tables and arrays
// are JSON-encoded so structured headers stay inspectable.
func headerString(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	case []byte:
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339Nano)
	default:
		switch val.(type) {
		case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			return fmt.Sprintf("%v", val)
		}
		if b, err := json.Marshal(val); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", val)
	}
}

// deathInfo reads the x-death header that RabbitMQ attaches to dead-lettered
// messages. The first entry records the original queue (the replay
// destination), how many times the message has been dead-lettered, and why.
func deathInfo(h amqp.Table) (destination string, retries int, reason string) {
	v, ok := h["x-death"]
	if !ok {
		return "", 0, ""
	}
	arr, ok := v.([]interface{})
	if !ok || len(arr) == 0 {
		return "", 0, ""
	}
	first, ok := arr[0].(amqp.Table)
	if !ok {
		return "", 0, ""
	}
	if q, ok := first["queue"].(string); ok {
		destination = q
	}
	switch c := first["count"].(type) {
	case int32:
		retries = int(c)
	case int64:
		retries = int(c)
	}
	if r, ok := first["reason"].(string); ok {
		reason = r
	}
	return destination, retries, reason
}

// extractIDs pulls application event/idempotency identifiers from headers for
// later duplicate detection.
func extractIDs(headers map[string]string) (eventID, idemKey string) {
	for _, k := range []string{"x-event-id", "event_id", "event-id", "ce-id"} {
		if v := headers[k]; v != "" {
			eventID = v
			break
		}
	}
	for _, k := range []string{"x-idempotency-key", "idempotency_key", "idempotency-key"} {
		if v := headers[k]; v != "" {
			idemKey = v
			break
		}
	}
	return eventID, idemKey
}
