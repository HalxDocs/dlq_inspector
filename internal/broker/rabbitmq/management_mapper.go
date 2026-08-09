package rabbitmq

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// mgmtMessage mirrors the subset of the management API's queue-get response
// payload we need.
type mgmtMessage struct {
	Payload         string         `json:"payload"`
	PayloadEncoding string         `json:"payload_encoding"`
	Exchange        string         `json:"exchange"`
	RoutingKey      string         `json:"routing_key"`
	Redelivered     bool           `json:"redelivered"`
	Properties      mgmtProperties `json:"properties"`
}

type mgmtProperties struct {
	MessageID   string          `json:"message_id"`
	Timestamp   json.RawMessage `json:"timestamp"`
	ContentType string          `json:"content_type"`
	Headers     map[string]any  `json:"headers"`
}

// toMessageFromMgmt normalizes a management API queue-get message into the
// shared model. The ID rule and header extraction match the AMQP mapping path,
// so a message read either way produces the same message.Message.
func toMessageFromMgmt(m mgmtMessage, queue string) message.Message {
	payload := decodeMgmtPayload(m.Payload, m.PayloadEncoding)
	headers := normalizeJSONHeaders(m.Properties.Headers)
	destination, retries, reason := deathInfoJSON(m.Properties.Headers)
	eventID, idemKey := extractIDs(headers)

	return message.Message{
		ID:             messageIDFromParts(m.Properties.MessageID, headers, payload),
		Queue:          queue,
		Destination:    destination,
		Payload:        payload,
		Headers:        headers,
		ContentType:    m.Properties.ContentType,
		Timestamp:      mgmtTimestamp(m.Properties.Timestamp),
		RetryCount:     retries,
		FailureReason:  reason,
		EventID:        eventID,
		IdempotencyKey: idemKey,
	}
}

// decodeMgmtPayload undoes the management API's base64 encoding when the
// payload is not valid UTF-8 text.
func decodeMgmtPayload(payload, encoding string) []byte {
	if encoding == "base64" {
		if b, err := base64.StdEncoding.DecodeString(payload); err == nil {
			return b
		}
	}
	return []byte(payload)
}

// normalizeJSONHeaders converts the management API's decoded header object
// into the same normalized string map the AMQP path produces.
func normalizeJSONHeaders(h map[string]any) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		out[k] = headerString(v)
	}
	return out
}

// deathInfoJSON reads the x-death header as returned by the management API,
// where it is a JSON array of objects rather than amqp tables.
func deathInfoJSON(h map[string]any) (destination string, retries int, reason string) {
	v, ok := h["x-death"]
	if !ok {
		return "", 0, ""
	}
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return "", 0, ""
	}
	first, ok := arr[0].(map[string]any)
	if !ok {
		return "", 0, ""
	}
	if q, ok := first["queue"].(string); ok {
		destination = q
	}
	switch c := first["count"].(type) {
	case float64:
		retries = int(c)
	case string:
		retries, _ = strconv.Atoi(c)
	}
	if r, ok := first["reason"].(string); ok {
		reason = r
	}
	return destination, retries, reason
}

// mgmtTimestamp converts the management API's unix-seconds timestamp to a
// time.Time. Missing or null timestamps map to the zero time.
func mgmtTimestamp(raw json.RawMessage) time.Time {
	if len(raw) == 0 || string(raw) == "null" {
		return time.Time{}
	}
	var secs int64
	if err := json.Unmarshal(raw, &secs); err == nil {
		return time.Unix(secs, 0)
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return time.Unix(int64(f), 0)
	}
	return time.Time{}
}
