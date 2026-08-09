package patch

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// Diff renders a human-readable old->new diff of two JSON payloads. Both
// must be valid JSON. The output lists changed, removed, and added paths as
// "- path: old" / "+ path: new" lines with JSON-encoded values, in a
// deterministic order: shared keys are walked depth-first in sorted order,
// then keys that only exist in the new payload. Arrays compare as a whole
// (their path shows the full before/after). An empty string means the
// payloads are equal.
func Diff(old, new []byte) (string, error) {
	var o, n any
	if err := json.Unmarshal(old, &o); err != nil {
		return "", fmt.Errorf("patch: diff old payload: %w", err)
	}
	if err := json.Unmarshal(new, &n); err != nil {
		return "", fmt.Errorf("patch: diff new payload: %w", err)
	}

	var lines []string
	diffValue("", o, n, &lines)
	return strings.Join(lines, "\n"), nil
}

// diffValue appends the difference between o and n at the given dotted path.
func diffValue(path string, o, n any, out *[]string) {
	om, oIsObj := o.(map[string]any)
	nm, nIsObj := n.(map[string]any)
	if oIsObj && nIsObj {
		keys := sortedKeys(om, nm)
		for _, k := range keys {
			child := joinPath(path, k)
			ov, okO := om[k]
			nv, okN := nm[k]
			switch {
			case !okO:
				*out = append(*out, addLine(child, nv))
			case !okN:
				*out = append(*out, removeLine(child, ov))
			default:
				diffValue(child, ov, nv, out)
			}
		}
		return
	}
	if !reflect.DeepEqual(o, n) {
		*out = append(*out, removeLine(path, o), addLine(path, n))
	}
}

// sortedKeys returns the sorted union of keys in both objects, so the walk is
// deterministic and added/removed keys land in a stable position.
func sortedKeys(a, b map[string]any) []string {
	seen := map[string]bool{}
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func joinPath(base, seg string) string {
	if base == "" {
		return seg
	}
	return base + "." + seg
}

// valueText renders a JSON value compactly for display.
func valueText(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

func removeLine(path string, v any) string {
	return line("-", path, v)
}

func addLine(path string, v any) string {
	return line("+", path, v)
}

func line(op, path string, v any) string {
	if path == "" {
		return fmt.Sprintf("%s %s", op, valueText(v))
	}
	return fmt.Sprintf("%s %s: %s", op, path, valueText(v))
}
