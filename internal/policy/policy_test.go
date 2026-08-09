package policy

import (
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

const validPolicy = `
rules:
  - when: error == payment_timeout
    action: replay
    params:
      max_retries: 3
  - when: event_type == order.completed
    action: require_fix
    params:
      require_idempotency_key: true
  - when: error contains duplicate
    action: do_not_replay
`

func TestParseValidPolicy(t *testing.T) {
	p, err := Parse([]byte(validPolicy))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Rules) != 3 {
		t.Fatalf("rules = %d, want 3", len(p.Rules))
	}
	if p.Rules[0].Action != ActionReplay || p.Rules[0].Params.MaxRetries == nil || *p.Rules[0].Params.MaxRetries != 3 {
		t.Errorf("rule 1 = %+v", p.Rules[0])
	}
	if p.Rules[1].Action != ActionRequireFix || !p.Rules[1].Params.RequireIdempotencyKey {
		t.Errorf("rule 2 = %+v", p.Rules[1])
	}
}

func TestParseRejectsBrokenPolicies(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"no rules", "rules: []", "no rules"},
		{"missing when", "rules:\n  - action: replay", "missing when"},
		{"bad shape", "rules:\n  - when: only-two-tokens\n    action: replay", "must be"},
		{"unknown field", "rules:\n  - when: bogus == x\n    action: replay", "unknown field"},
		{"bad op", "rules:\n  - when: retries contains 3\n    action: replay", "not valid for retries"},
		{"retries non-integer", "rules:\n  - when: retries > many\n    action: replay", "not an integer"},
		{"unknown action", "rules:\n  - when: error == x\n    action: replay_now", "unknown action"},
		{"unknown param", "rules:\n  - when: error == x\n    action: replay\n    params:\n      when_happy: true", "unknown param"},
		{"bad max_retries", "rules:\n  - when: error == x\n    action: replay\n    params:\n      max_retries: many", "max_retries"},
		{"bad bool", "rules:\n  - when: error == x\n    action: replay\n    params:\n      require_idempotency_key: yes_please", "require_idempotency_key"},
		{"unterminated quote", "rules:\n  - when: error == \"abc\n    action: replay", "must be"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Parse error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestParseCollectsAllErrors(t *testing.T) {
	_, err := Parse([]byte(`
rules:
  - when: bogus == x
    action: nope
  - when: retries > many
    action: replay
`))
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"rule 1", "unknown field", "unknown action", "rule 2", "not an integer"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
}

func msg(overrides map[string]any) *message.Message {
	m := &message.Message{ID: "m1"}
	for k, v := range overrides {
		switch k {
		case "failure":
			m.FailureReason = v.(string)
		case "event":
			if m.Headers == nil {
				m.Headers = map[string]string{}
			}
			m.Headers["x-event-type"] = v.(string)
		case "retries":
			m.RetryCount = v.(int)
		case "idem":
			m.IdempotencyKey = v.(string)
		}
	}
	return m
}

func TestMatchFirstRuleWins(t *testing.T) {
	p, err := Parse([]byte(`
rules:
  - when: error contains timeout
    action: replay
  - when: error == timeout
    action: do_not_replay
`))
	if err != nil {
		t.Fatal(err)
	}
	r, ok := p.Match(msg(map[string]any{"failure": "payment timeout"}))
	if !ok || r.Action != ActionReplay {
		t.Errorf("match = %+v, %v; want the first (replay) rule", r, ok)
	}
}

func TestMatchTextOps(t *testing.T) {
	p, err := Parse([]byte(`
rules:
  - when: error == "Payment Timeout"
    action: replay
  - when: event_type == order.cancelled
    action: do_not_replay
  - when: destination != orders
    action: require_fix
`))
	if err != nil {
		t.Fatal(err)
	}
	// == is case-insensitive.
	if _, ok := p.Match(msg(map[string]any{"failure": "payment timeout"})); !ok {
		t.Error("case-insensitive equality did not match")
	}
	if _, ok := p.Match(msg(map[string]any{"event": "order.cancelled"})); !ok {
		t.Error("event_type equality did not match")
	}
	// != matches when the destination differs.
	if _, ok := p.Match(msg(map[string]any{"failure": "other", "idem": "k"})); !ok {
		t.Error("destination != did not match")
	}
}

func TestMatchRetriesOps(t *testing.T) {
	p, err := Parse([]byte(`
rules:
  - when: retries > 3
    action: do_not_replay
`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.Match(msg(map[string]any{"retries": 4})); !ok {
		t.Error("retries > 3 should match at 4")
	}
	if _, ok := p.Match(msg(map[string]any{"retries": 3})); ok {
		t.Error("retries > 3 must not match at 3")
	}
}

func TestMatchMaxRetriesGate(t *testing.T) {
	p, err := Parse([]byte(`
rules:
  - when: error contains timeout
    action: replay
    params:
      max_retries: 2
`))
	if err != nil {
		t.Fatal(err)
	}
	// Below the ceiling the rule applies...
	if _, ok := p.Match(msg(map[string]any{"failure": "timeout", "retries": 2})); !ok {
		t.Error("rule should apply at max_retries")
	}
	// ...above it the rule does not apply at all (falls through).
	if _, ok := p.Match(msg(map[string]any{"failure": "timeout", "retries": 3})); ok {
		t.Error("rule must not apply above max_retries")
	}
}

func TestMatchIdempotencyKeyGate(t *testing.T) {
	p, err := Parse([]byte(`
rules:
  - when: event_type == order.completed
    action: replay
    params:
      require_idempotency_key: true
`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.Match(msg(map[string]any{"event": "order.completed", "idem": "key-1"})); !ok {
		t.Error("rule should apply when the key is present")
	}
	if _, ok := p.Match(msg(map[string]any{"event": "order.completed"})); ok {
		t.Error("rule must not apply without an idempotency key")
	}
}

func TestNoMatchFallsThrough(t *testing.T) {
	p, err := Parse([]byte(`
rules:
  - when: event_type == order.completed
    action: do_not_replay
`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.Match(msg(map[string]any{"failure": "something else"})); ok {
		t.Error("unrelated message matched a policy rule")
	}
	if _, ok := (*Policy)(nil).Match(msg(nil)); ok {
		t.Error("nil policy matched")
	}
}
