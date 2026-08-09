package redisstream

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// Entry field conventions for DLQ entries. Applications that dead-letter into
// a Redis stream DLQ (and the test fixtures) write these fields; the adapter
// maps them onto the shared message model. The payload is stored under
// "payload"; destination / error / retries are the dead-letter metadata that
// on RabbitMQ would live in x-death.
const (
	fieldPayload     = "payload"
	fieldContentType = "content_type"
	fieldDestination = "destination"
	fieldError       = "error"
	fieldRetries     = "retries"
	fieldHeaders     = "headers"
	fieldTimestamp   = "timestamp"
	// fieldMessageID preserves the original entry ID on replayed messages so
	// downstream consumers can correlate a replay to its DLQ entry.
	fieldMessageID = "message_id"
)

// toMessage normalizes a stream entry into the shared model. The message ID
// is the stream entry ID (e.g. "1690000000000-0") — the only stable handle a
// Redis stream gives us, and the one Search, Inspect, and Ack all agree on.
func toMessage(stream string, entry redis.XMessage) message.Message {
	get := func(k string) string {
		v, ok := entry.Values[k]
		if !ok || v == nil {
			return ""
		}
		return fmtString(v)
	}

	headers := map[string]string{}
	if h := get(fieldHeaders); h != "" {
		_ = json.Unmarshal([]byte(h), &headers) // malformed headers degrade to empty
	}
	eventID, idemKey := extractIDs(headers)

	return message.Message{
		ID:             entry.ID,
		Queue:          stream,
		Destination:    get(fieldDestination),
		Payload:        []byte(get(fieldPayload)),
		Headers:        headers,
		ContentType:    get(fieldContentType),
		Timestamp:      entryTime(get(fieldTimestamp), entry.ID),
		RetryCount:     atoi(get(fieldRetries)),
		FailureReason:  get(fieldError),
		EventID:        eventID,
		IdempotencyKey: idemKey,
	}
}

// entryFields renders the field map for an XADD: the payload plus the
// metadata the tool and future DLQ hops need. The message's own ID is stored
// as message_id for correlation; the stream assigns the entry ID.
func entryFields(m *message.Message) map[string]any {
	fields := map[string]any{fieldPayload: string(m.Payload)}
	if m.ContentType != "" {
		fields[fieldContentType] = m.ContentType
	}
	if m.Destination != "" {
		fields[fieldDestination] = m.Destination
	}
	if m.FailureReason != "" {
		fields[fieldError] = m.FailureReason
	}
	if m.RetryCount > 0 {
		fields[fieldRetries] = strconv.Itoa(m.RetryCount)
	}
	if len(m.Headers) > 0 {
		if b, err := json.Marshal(m.Headers); err == nil {
			fields[fieldHeaders] = string(b)
		}
	}
	if !m.Timestamp.IsZero() {
		fields[fieldTimestamp] = m.Timestamp.UTC().Format(time.RFC3339Nano)
	}
	fields[fieldMessageID] = m.ID
	return fields
}

// fmtString renders a stream field value as a string.
func fmtString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// entryTime prefers the stored timestamp, falling back to the stream entry
// ID's millisecond part — the Redis-native entry time.
func entryTime(ts, entryID string) time.Time {
	if ts != "" {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			return t
		}
	}
	if i := strings.Index(entryID, "-"); i > 0 {
		if ms, err := strconv.ParseInt(entryID[:i], 10, 64); err == nil {
			return time.UnixMilli(ms).UTC()
		}
	}
	return time.Time{}
}

// extractIDs pulls application event/idempotency identifiers from headers for
// later duplicate detection (mirrors the RabbitMQ mapper's conventions).
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
