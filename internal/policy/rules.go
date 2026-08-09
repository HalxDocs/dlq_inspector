package policy

import (
	"strconv"
	"strings"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// Match returns the first rule whose condition is true and whose params are
// satisfied for m. Rules are evaluated in order — the first match wins. When
// no rule applies, ok is false and the classifier's inference stands.
func (p *Policy) Match(m *message.Message) (Rule, bool) {
	if p == nil || m == nil {
		return Rule{}, false
	}
	for _, r := range p.Rules {
		if r.parsed.matches(m) && r.paramsSatisfied(m) {
			return r, true
		}
	}
	return Rule{}, false
}

// matches evaluates the parsed condition against a message.
func (c condition) matches(m *message.Message) bool {
	switch c.field {
	case "error":
		return matchText(m.FailureReason, c.op, c.value)
	case "event_type":
		return matchText(eventType(m), c.op, c.value)
	case "destination":
		return matchText(m.Destination, c.op, c.value)
	case "retries":
		n, _ := strconv.Atoi(c.value) // validated at parse time
		switch c.op {
		case "==":
			return m.RetryCount == n
		case "!=":
			return m.RetryCount != n
		case ">":
			return m.RetryCount > n
		case ">=":
			return m.RetryCount >= n
		case "<":
			return m.RetryCount < n
		case "<=":
			return m.RetryCount <= n
		}
	}
	return false
}

// matchText applies a text operator: equality and inequality are
// case-insensitive exact matches; contains is a case-insensitive substring.
func matchText(s, op, value string) bool {
	switch op {
	case "==":
		return strings.EqualFold(s, value)
	case "!=":
		return !strings.EqualFold(s, value)
	case "contains":
		return strings.Contains(strings.ToLower(s), strings.ToLower(value))
	}
	return false
}

// paramsSatisfied reports whether the rule's gates hold for m. A rule whose
// params are not satisfied does not apply and evaluation falls through.
func (r Rule) paramsSatisfied(m *message.Message) bool {
	if r.Params.MaxRetries != nil && m.RetryCount > *r.Params.MaxRetries {
		return false
	}
	if r.Params.RequireIdempotencyKey && m.IdempotencyKey == "" {
		return false
	}
	return true
}

// eventType pulls an application event type out of common header conventions
// (mirrors the analyzer's convention so policies see the same value).
func eventType(m *message.Message) string {
	for _, k := range []string{"x-event-type", "event_type", "event-type", "ce-type"} {
		if v := m.Headers[k]; v != "" {
			return v
		}
	}
	return ""
}
