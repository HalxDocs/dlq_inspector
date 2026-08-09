// Package search implements broker-agnostic message filtering. Every adapter
// fetches raw messages its own way, then delegates the matching — and the CLI
// search command — to this package, so filter semantics are identical no
// matter which broker is underneath.
package search

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// Match reports whether m satisfies every constraint in f. Zero-value
// constraints always match.
func Match(m message.Message, f broker.SearchFilter) bool {
	if f.ErrorText != "" && !matchesError(m, f.ErrorText) {
		return false
	}
	if !f.Since.IsZero() && m.Timestamp.Before(f.Since) {
		return false
	}
	if f.MaxRetries != nil && m.RetryCount > *f.MaxRetries {
		return false
	}
	if len(f.Fields) > 0 && !matchesFields(m.Payload, f.Fields) {
		return false
	}
	return true
}

// matchesError is a case-insensitive substring match against the failure
// reason and the payload text, so an --error filter like "timeout" finds
// messages whose error detail lives inside the payload.
func matchesError(m message.Message, text string) bool {
	text = strings.ToLower(text)
	if strings.Contains(strings.ToLower(m.FailureReason), text) {
		return true
	}
	return strings.Contains(strings.ToLower(string(m.Payload)), text)
}

// matchesFields requires every dotted payload path to equal its configured
// value. Field constraints only apply to JSON payloads; non-JSON payloads
// never match a field constraint.
func matchesFields(payload []byte, fields map[string]string) bool {
	var doc any
	if err := json.Unmarshal(payload, &doc); err != nil {
		return false
	}
	for path, want := range fields {
		got, ok := lookupPath(doc, path)
		if !ok || fmt.Sprintf("%v", got) != want {
			return false
		}
	}
	return true
}

// lookupPath walks a dotted path (e.g. "customer.id") through a decoded JSON
// document.
func lookupPath(doc any, path string) (any, bool) {
	cur := doc
	for _, part := range strings.Split(path, ".") {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = obj[part]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// Filter returns the messages matching f, in order, with the filter's offset
// and limit applied.
func Filter(msgs []message.Message, f broker.SearchFilter) []message.Message {
	out := make([]message.Message, 0, len(msgs))
	for _, m := range msgs {
		if !Match(m, f) {
			continue
		}
		if f.Offset > 0 {
			f.Offset--
			continue
		}
		out = append(out, m)
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}
	return out
}
