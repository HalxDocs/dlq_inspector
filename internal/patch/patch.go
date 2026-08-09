// Package patch applies controlled edits to message payloads before replay
// and renders old->new diffs. Patching is the one place the recovery engine
// deliberately rewrites content; everything else is inspection and judgment.
// Patches are only ever applied to JSON object payloads — a message that is
// not JSON cannot be patched.
package patch

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// SetOp is one --set operation: assign a value at a dotted path.
type SetOp struct {
	// Path is a dotted path into the payload, e.g. "customer_id" or
	// "billing.address.city". Numeric segments index into arrays.
	Path string
	// Value is the raw text from the command line. It is parsed as JSON when
	// it is valid JSON (443 -> number, true -> boolean, ["a"] -> array) and
	// treated as a plain string otherwise (John -> "John").
	Value string
}

// ApplySet applies the operations to a JSON object payload and returns the
// new payload. Operations apply in order; later operations see earlier ones.
// Missing intermediate objects are created along the path. A payload that is
// not a JSON object, a path that walks through a type conflict, or an array
// index out of range is an error — a patch must never guess.
func ApplySet(payload []byte, ops []SetOp) ([]byte, error) {
	if len(ops) == 0 {
		return nil, fmt.Errorf("patch: no set operations")
	}
	var root map[string]any
	if err := json.Unmarshal(payload, &root); err != nil {
		return nil, fmt.Errorf("patch: payload is not a JSON object: %w", err)
	}

	for _, op := range ops {
		if op.Path == "" {
			return nil, fmt.Errorf("patch: empty path")
		}
		segments := strings.Split(op.Path, ".")
		if err := applySet(root, segments, op.Value); err != nil {
			return nil, err
		}
	}

	out, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("patch: encode patched payload: %w", err)
	}
	return out, nil
}

// applySet walks the segments into the payload, creating missing intermediate
// objects, and assigns the final value. Arrays are traversed by numeric
// segments; setting an element of an array requires an existing index.
func applySet(root map[string]any, segments []string, value string) error {
	v, err := parseValue(value)
	if err != nil {
		return err
	}

	var cur any = root
	for i := 0; i < len(segments)-1; i++ {
		seg := segments[i]
		idx, isIdx := indexSegment(seg)
		switch node := cur.(type) {
		case map[string]any:
			child, ok := node[seg]
			if !ok {
				child = map[string]any{}
				node[seg] = child
			}
			switch child.(type) {
			case map[string]any, []any:
				cur = child
			default:
				return fmt.Errorf("patch: %s: segment %q is not an object or array", strings.Join(segments[:i+1], "."), seg)
			}
		case []any:
			if !isIdx {
				return fmt.Errorf("patch: %s: segment %q on an array requires an index", strings.Join(segments[:i+1], "."), seg)
			}
			if idx < 0 || idx >= len(node) {
				return fmt.Errorf("patch: %s: index %d out of range (len %d)", strings.Join(segments[:i+1], "."), idx, len(node))
			}
			child, ok := node[idx].(map[string]any)
			if !ok {
				return fmt.Errorf("patch: %s: index %d is not an object", strings.Join(segments[:i+1], "."), idx)
			}
			cur = child
		default:
			return fmt.Errorf("patch: %s: cannot descend through %T", strings.Join(segments[:i+1], "."), cur)
		}
	}

	last := segments[len(segments)-1]
	switch node := cur.(type) {
	case map[string]any:
		node[last] = v
	case []any:
		idx, ok := indexSegment(last)
		if !ok {
			return fmt.Errorf("patch: %q: cannot set key %q on an array", strings.Join(segments, "."), last)
		}
		if idx < 0 || idx >= len(node) {
			return fmt.Errorf("patch: %q: index %d out of range (len %d)", strings.Join(segments, "."), idx, len(node))
		}
		node[idx] = v
	default:
		return fmt.Errorf("patch: %q: cannot set into %T", strings.Join(segments, "."), cur)
	}
	return nil
}

// indexSegment reports whether a path segment is an array index (all digits).
// Object keys that look like numbers are only reachable when the parent is an
// object, where the segment is treated as a plain key.
func indexSegment(seg string) (int, bool) {
	if seg == "" {
		return 0, false
	}
	for _, r := range seg {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	idx, err := strconv.Atoi(seg)
	if err != nil {
		return 0, false
	}
	return idx, true
}

// parseValue turns a command-line value into a JSON value: valid JSON is used
// as-is, anything else becomes a string.
func parseValue(raw string) (any, error) {
	if json.Valid([]byte(raw)) {
		var v any
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			return nil, fmt.Errorf("patch: parse value %q: %w", raw, err)
		}
		return v, nil
	}
	return raw, nil
}
