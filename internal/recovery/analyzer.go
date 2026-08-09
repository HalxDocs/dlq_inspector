package recovery

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// FailureGroup clusters messages that failed the same way: they share a
// normalized error signature plus the destination, event type, and retry
// bucket the analyzer considers part of the cause.
type FailureGroup struct {
	// ID is a stable short hash of the group's signature.
	ID string
	// Label is a short human-readable name derived from the signature.
	Label string
	// Signature is the normalized failure signature the group is keyed on.
	Signature string
	// MessageIDs lists every message in the group (best-effort, in input order).
	MessageIDs []string
	// Count is the number of messages in the group.
	Count int
	// Percentage is Count / total analyzed * 100.
	Percentage float64
	// Recommendation is the majority classification of the group's messages.
	Recommendation Classification
	// Confidence is the mean confidence of messages in the majority class.
	Confidence float64
	// Destination is the replay destination shared by the group.
	Destination string
	// EventType is the application event type shared by the group, if any.
	EventType string
	// RetryBucket is the retry-count range shared by the group ("0", "1-2",
	// "3-5", "6+").
	RetryBucket string
	// FirstSeen / LastSeen bound the time window the group's messages arrived in.
	FirstSeen time.Time
	LastSeen  time.Time
	// PayloadShape describes the payload structure of the group's first
	// message (sorted top-level JSON keys, or "raw" for non-JSON bodies).
	PayloadShape string
	// Breakdown counts each classification within the group.
	Breakdown map[Classification]int
}

// groupKey is the tuple that defines one failure group. Signature is the
// dominant key; destination, event type, and retry bucket keep distinct
// causes from being merged just because their error text normalizes alike.
type groupKey struct {
	signature   string
	destination string
	eventType   string
	retryBucket string
}

// Analyzer groups a set of failed messages into failure patterns. It is
// broker-agnostic: it only reads message.Message.
type Analyzer struct{}

// Analyze clusters msgs into FailureGroups ordered by size (largest first).
func (Analyzer) Analyze(msgs []message.Message) []FailureGroup {
	total := len(msgs)
	if total == 0 {
		return nil
	}

	type acc struct {
		key      groupKey
		ids      []string
		classCnt map[Classification]int
		confSum  map[Classification]float64
		first    time.Time
		last     time.Time
		shape    string
	}
	groups := make(map[groupKey]*acc)
	for i := range msgs {
		m := &msgs[i]
		k := groupKey{
			signature:   NormalizeSignature(m.FailureReason),
			destination: m.Destination,
			eventType:   eventType(m),
			retryBucket: retryBucket(m.RetryCount),
		}
		a := groups[k]
		if a == nil {
			a = &acc{
				key:      k,
				classCnt: make(map[Classification]int),
				confSum:  make(map[Classification]float64),
			}
			groups[k] = a
		}
		a.ids = append(a.ids, m.ID)

		res := Classify(m)
		a.classCnt[res.Classification]++
		a.confSum[res.Classification] += res.Confidence

		if a.first.IsZero() || m.Timestamp.Before(a.first) {
			a.first = m.Timestamp
		}
		if m.Timestamp.After(a.last) {
			a.last = m.Timestamp
		}
		if a.shape == "" {
			a.shape = payloadShape(m.Payload)
		}
	}

	out := make([]FailureGroup, 0, len(groups))
	for _, a := range groups {
		rec, conf := majority(a.classCnt, a.confSum)
		g := FailureGroup{
			ID:             groupID(a.key.signature),
			Label:          groupLabel(a.key.signature),
			Signature:      a.key.signature,
			MessageIDs:     a.ids,
			Count:          len(a.ids),
			Percentage:     float64(len(a.ids)) / float64(total) * 100,
			Recommendation: rec,
			Confidence:     conf,
			Destination:    a.key.destination,
			EventType:      a.key.eventType,
			RetryBucket:    a.key.retryBucket,
			FirstSeen:      a.first,
			LastSeen:       a.last,
			PayloadShape:   a.shape,
			Breakdown:      a.classCnt,
		}
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Signature < out[j].Signature
	})
	return out
}

// majority picks the classification with the most votes. Ties resolve to
// INVESTIGATE — the honest default when the group's signals are split.
// Confidence is the mean confidence of the messages in the winning class.
func majority(counts map[Classification]int, confSum map[Classification]float64) (Classification, float64) {
	best := Investigate
	bestN := -1
	for c, n := range counts {
		if n > bestN {
			best, bestN = c, n
		}
	}
	if bestN <= 0 {
		return Investigate, 0.5
	}
	return best, confSum[best] / float64(bestN)
}

// retryBucket maps a retry count onto a coarse range used as a grouping key.
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

// eventType pulls an application event type out of common header conventions.
func eventType(m *message.Message) string {
	for _, k := range []string{"x-event-type", "event_type", "event-type", "ce-type"} {
		if v := m.Headers[k]; v != "" {
			return v
		}
	}
	return ""
}

// payloadShape describes a payload's structure for display: the sorted
// top-level keys when the body is JSON, or "raw" otherwise.
func payloadShape(payload []byte) string {
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

// groupID is a stable short identifier for a signature.
func groupID(signature string) string {
	h := sha256.Sum256([]byte(signature))
	return hex.EncodeToString(h[:])[:8]
}
