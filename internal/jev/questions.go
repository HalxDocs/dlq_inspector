package jev

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// MaxErrorChars caps failure text in Jev state. Signatures already collapse
// dynamic values; this only guards against pathological error blobs.
const MaxErrorChars = 2000

// MaxGroupSamples caps error samples in group state.
const MaxGroupSamples = 5

// ClassificationQuestions returns the standard per-message question set.
// All three run in one Jev call in parallel: the Choice decides the class,
// the Noul isolates duplicate risk, the Score grades replay danger.
func ClassificationQuestions() map[string]Question {
	return map[string]Question{
		"classification": {
			Type:         "choice",
			Instructions: "Classify this dead-letter queue failure for recovery triage",
			Criteria: map[string]string{
				"replayable":    "Transient infrastructure failure worth retrying: timeout, connection reset/refused, throttling, 5xx",
				"requires_fix":  "Permanent failure needing a payload or config fix first: validation, schema/parse, auth, not-found, 4xx",
				"do_not_replay": "Replay would duplicate work or cause harm: duplicate, already processed, idempotency hit",
				"investigate":   "Missing, ambiguous, or conflicting signals: do not guess",
			},
		},
		"is_duplicate": {
			Type:         "noul",
			Instructions: "The failure indicates the event was already processed or is a duplicate",
		},
		"replay_risk": {
			Type:         "score",
			Instructions: "How risky is blindly replaying this message",
			Criteria: []string{
				"Safe to retry: clearly transient, low retries",
				"Needs care: unclear cause or several retries already",
				"Likely poison: permanent-looking, duplicate, or retried many times",
			},
		},
	}
}

// BuildMessageState renders metadata-only state for one message. It never
// includes payload bytes or raw header values (except the event type): only
// the failure reason, retry count/bucket, destination, event type,
// normalized signature, and payload shape.
func BuildMessageState(m *message.Message, signature string) string {
	if m == nil {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "error=%s", truncate(strings.TrimSpace(m.FailureReason), MaxErrorChars))
	fmt.Fprintf(&b, " | retries=%d (%s)", m.RetryCount, retryBucket(m.RetryCount))
	if m.Destination != "" {
		fmt.Fprintf(&b, " | destination=%s", truncate(m.Destination, 200))
	}
	if et := eventTypeOf(m); et != "" {
		fmt.Fprintf(&b, " | event_type=%s", truncate(et, 200))
	}
	if signature != "" {
		fmt.Fprintf(&b, " | signature=%s", truncate(signature, 500))
	}
	fmt.Fprintf(&b, " | payload_shape=%s", payloadShapeOf(m.Payload))
	return b.String()
}

// GroupSample is one failure group summarized for Jev review.
type GroupSample struct {
	Signature   string
	Destination string
	EventType   string
	RetryBucket string
	Count       int
	Errors      []string
}

// BuildGroupState renders metadata-only state for one failure group: the
// shared signature plus up to MaxGroupSamples truncated error samples.
func BuildGroupState(g GroupSample) string {
	var b strings.Builder
	fmt.Fprintf(&b, "signature=%s", truncate(g.Signature, 500))
	fmt.Fprintf(&b, " | count=%d", g.Count)
	if g.Destination != "" {
		fmt.Fprintf(&b, " | destination=%s", truncate(g.Destination, 200))
	}
	if g.EventType != "" {
		fmt.Fprintf(&b, " | event_type=%s", truncate(g.EventType, 200))
	}
	if g.RetryBucket != "" {
		fmt.Fprintf(&b, " | retries=%s", truncate(g.RetryBucket, 20))
	}
	n := len(g.Errors)
	if n > MaxGroupSamples {
		n = MaxGroupSamples
	}
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, " | sample%d=%s", i+1, truncate(strings.TrimSpace(g.Errors[i]), 500))
	}
	return b.String()
}

func truncate(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max]
}

func retryBucket(retries int) string {
	switch {
	case retries <= 0:
		return "0"
	case retries <= 2:
		return "1-2"
	case retries <= 5:
		return "3-5"
	default:
		return "6+"
	}
}

func eventTypeOf(m *message.Message) string {
	for _, k := range []string{"x-event-type", "event_type", "event-type", "ce-type"} {
		if v := m.Headers[k]; v != "" {
			return v
		}
	}
	return ""
}

func payloadShapeOf(payload []byte) string {
	if len(payload) == 0 {
		return "empty"
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		return "raw"
	}
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}
