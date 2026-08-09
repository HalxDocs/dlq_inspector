// Package policy implements recovery policies: YAML files, committed
// alongside the service they protect, that encode "what is safe to replay".
// A policy is an ordered list of rules; the first rule whose condition
// matches a message (and whose params are satisfied) decides that message's
// recovery action, overriding the classifier's inference.
package policy

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Action is the recovery decision a rule forces.
type Action string

// Supported actions.
const (
	ActionReplay      Action = "replay"        // classify REPLAYABLE
	ActionRequireFix  Action = "require_fix"   // classify REQUIRES_FIX
	ActionDoNotReplay Action = "do_not_replay" // classify DO_NOT_REPLAY
)

// Params are the conditional gates a rule carries. A rule only applies when
// its params are satisfied:
//
//	max_retries: N            — the rule does not apply above N retries
//	require_idempotency_key   — the rule requires an idempotency key
type Params struct {
	MaxRetries            *int `yaml:"max_retries,omitempty"`
	RequireIdempotencyKey bool `yaml:"require_idempotency_key,omitempty"`
}

// Rule is one when->action decision. When is a condition expression of the
// form "<field> <op> <value>", e.g. "error contains timeout" or
// "event_type == order.completed". Fields: error, event_type, destination,
// retries. Ops: ==, !=, contains (text); ==, !=, >, >=, <, <= (retries).
type Rule struct {
	When   string `yaml:"when"`
	Action Action `yaml:"action"`
	Params Params `yaml:"params,omitempty"`

	// parsed is the validated condition, set by Load.
	parsed condition
}

// Policy is a parsed policy document. Rules are evaluated in order; the
// first match wins.
type Policy struct {
	Rules []Rule `yaml:"rules"`
}

// Load reads, parses, and validates the policy file at path. A policy that
// does not parse cleanly is rejected wholesale — CI runs this on every
// commit, so a broken rule must never silently reach production.
func Load(path string) (*Policy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read policy %s: %w", path, err)
	}
	return Parse(data)
}

// Parse parses and validates policy YAML. The returned error joins every
// problem found, each prefixed with its rule number, so `dlq policy validate`
// reports all breakages in one pass.
func Parse(data []byte) (*Policy, error) {
	var raw struct {
		Rules []struct {
			When   string         `yaml:"when"`
			Action string         `yaml:"action"`
			Params map[string]any `yaml:"params"`
		} `yaml:"rules"`
	}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse policy: %w", err)
	}
	if len(raw.Rules) == 0 {
		return nil, errors.New("policy has no rules")
	}

	p := &Policy{}
	var errs []string
	for i, r := range raw.Rules {
		label := fmt.Sprintf("rule %d", i+1)
		if r.When == "" {
			errs = append(errs, fmt.Sprintf("%s: missing when", label))
			continue
		}
		cond, err := parseCondition(r.When)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", label, err))
		}
		var action Action
		switch Action(r.Action) {
		case ActionReplay, ActionRequireFix, ActionDoNotReplay:
			action = Action(r.Action)
		default:
			errs = append(errs, fmt.Sprintf("%s: unknown action %q (want replay, require_fix, or do_not_replay)", label, r.Action))
		}
		params, perr := parseParams(r.Params)
		if perr != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", label, perr))
		}
		p.Rules = append(p.Rules, Rule{
			When:   r.When,
			Action: action,
			Params: params,
			parsed: cond,
		})
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "; "))
	}
	return p, nil
}

// parseParams validates and normalizes the params map.
func parseParams(m map[string]any) (Params, error) {
	var p Params
	for k, v := range m {
		switch k {
		case "max_retries":
			n, ok := toInt(v)
			if !ok || n < 0 {
				return p, fmt.Errorf("max_retries must be a non-negative integer (got %v)", v)
			}
			p.MaxRetries = &n
		case "require_idempotency_key":
			b, ok := v.(bool)
			if !ok {
				return p, fmt.Errorf("require_idempotency_key must be true or false (got %v)", v)
			}
			p.RequireIdempotencyKey = b
		default:
			return p, fmt.Errorf("unknown param %q (want max_retries or require_idempotency_key)", k)
		}
	}
	return p, nil
}

// toInt converts a YAML/JSON number to an int.
func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		if n != float64(int(n)) {
			return 0, false
		}
		return int(n), true
	case string:
		i, err := strconv.Atoi(n)
		if err != nil {
			return 0, false
		}
		return i, true
	}
	return 0, false
}

// condition is the parsed form of a Rule.When expression.
type condition struct {
	field string
	op    string
	value string
}

// parseCondition parses "<field> <op> <value>". The value may be quoted.
func parseCondition(expr string) (condition, error) {
	parts := splitExpr(expr)
	if len(parts) != 3 {
		return condition{}, fmt.Errorf("when must be \"<field> <op> <value>\" (got %q)", expr)
	}
	field, op, value := parts[0], parts[1], unquote(parts[2])

	switch field {
	case "error", "event_type", "destination":
		switch op {
		case "==", "!=", "contains":
		default:
			return condition{}, fmt.Errorf("when %q: op %q is not valid for text field %q (want ==, !=, contains)", expr, op, field)
		}
	case "retries":
		switch op {
		case "==", "!=", ">", ">=", "<", "<=":
		default:
			return condition{}, fmt.Errorf("when %q: op %q is not valid for retries (want ==, !=, >, >=, <, <=)", expr, op)
		}
		if _, err := strconv.Atoi(value); err != nil {
			return condition{}, fmt.Errorf("when %q: retries value %q is not an integer", expr, value)
		}
	default:
		return condition{}, fmt.Errorf("when %q: unknown field %q (want error, event_type, destination, or retries)", expr, field)
	}

	return condition{field: field, op: op, value: value}, nil
}

// splitExpr splits on whitespace, honoring single and double quotes.
func splitExpr(expr string) []string {
	var out []string
	var cur strings.Builder
	quote := rune(0)
	for _, r := range expr {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
			cur.WriteRune(r)
		case r == '\'' || r == '"':
			quote = r
			cur.WriteRune(r)
		case r == ' ' || r == '\t':
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	if quote != 0 {
		return nil // unterminated quote
	}
	return out
}

// unquote strips surrounding quotes from a token.
func unquote(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
