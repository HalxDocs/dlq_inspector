package patch

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

func mustDecode(t *testing.T, payload []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		t.Fatalf("decode %s: %v", payload, err)
	}
	return m
}

func field(t *testing.T, payload []byte, path string) any {
	t.Helper()
	cur := any(mustDecode(t, payload))
	for _, seg := range strings.Split(path, ".") {
		switch node := cur.(type) {
		case map[string]any:
			cur = node[seg]
		case []any:
			idx, err := strconv.Atoi(seg)
			if err != nil {
				t.Fatalf("non-numeric segment %q on array", seg)
			}
			cur = node[idx]
		default:
			t.Fatalf("cannot descend through %T at %q", cur, seg)
		}
	}
	return cur
}

func TestApplySetRootLevel(t *testing.T) {
	out, err := ApplySet([]byte(`{"order_id":1,"customer_id":1000}`), []SetOp{{Path: "customer_id", Value: "443"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := field(t, out, "customer_id"); got != float64(443) {
		t.Errorf("customer_id = %v, want 443 (number)", got)
	}
	if got := field(t, out, "order_id"); got != float64(1) {
		t.Errorf("order_id changed to %v", got)
	}
}

func TestApplySetStringValue(t *testing.T) {
	// "John" is not valid JSON, so it becomes a string.
	out, err := ApplySet([]byte(`{"name":"old"}`), []SetOp{{Path: "name", Value: "John"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := field(t, out, "name"); got != "John" {
		t.Errorf("name = %v, want string John", got)
	}
}

func TestApplySetJSONValues(t *testing.T) {
	out, err := ApplySet([]byte(`{"flag":false,"tags":["a"],"meta":{}}`), []SetOp{
		{Path: "flag", Value: "true"},
		{Path: "tags", Value: `["x","y"]`},
		{Path: "meta.nested", Value: `{"deep":1}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := field(t, out, "flag"); got != true {
		t.Errorf("flag = %v, want true", got)
	}
	if got := field(t, out, "tags"); !jsonEqual(got, []any{"x", "y"}) {
		t.Errorf("tags = %v", got)
	}
	if got := field(t, out, "meta.nested.deep"); got != float64(1) {
		t.Errorf("meta.nested.deep = %v", got)
	}
}

func TestApplySetNestedPathCreatesObjects(t *testing.T) {
	out, err := ApplySet([]byte(`{"order_id":1}`), []SetOp{{Path: "billing.address.city", Value: "Oslo"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := field(t, out, "billing.address.city"); got != "Oslo" {
		t.Errorf("billing.address.city = %v", got)
	}
}

func TestApplySetArrayIndex(t *testing.T) {
	out, err := ApplySet([]byte(`{"items":[{"sku":"A"},{"sku":"B"}]}`), []SetOp{{Path: "items.1.sku", Value: "C"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := field(t, out, "items.1.sku"); got != "C" {
		t.Errorf("items.1.sku = %v, want C", got)
	}
	if got := field(t, out, "items.0.sku"); got != "A" {
		t.Errorf("items.0.sku changed to %v", got)
	}
}

func TestApplySetMultipleOpsInOrder(t *testing.T) {
	out, err := ApplySet([]byte(`{"a":1,"b":2}`), []SetOp{
		{Path: "a", Value: "10"},
		{Path: "b", Value: "20"},
		{Path: "a", Value: "99"}, // later op wins
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := field(t, out, "a"); got != float64(99) {
		t.Errorf("a = %v, want 99", got)
	}
	if got := field(t, out, "b"); got != float64(20) {
		t.Errorf("b = %v, want 20", got)
	}
}

func TestApplySetErrors(t *testing.T) {
	cases := []struct {
		name string
		pay  string
		op   SetOp
	}{
		{"non-json payload", "not json at all", SetOp{Path: "a", Value: "1"}},
		{"non-object payload", `[1,2]`, SetOp{Path: "a", Value: "1"}},
		{"empty path", `{}`, SetOp{Path: "", Value: "1"}},
		{"walk through scalar", `{"a":1}`, SetOp{Path: "a.b", Value: "1"}},
		{"index into non-object element", `{"a":[1]}`, SetOp{Path: "a.0.b", Value: "1"}},
		{"key on array", `{"items":[]}`, SetOp{Path: "items.x", Value: "1"}},
		{"index out of range", `{"items":[]}`, SetOp{Path: "items.0.x", Value: "1"}},
		{"no ops", `{}`, SetOp{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ops []SetOp
			if tc.op.Path != "" || tc.op.Value != "" {
				ops = []SetOp{tc.op}
			}
			if _, err := ApplySet([]byte(tc.pay), ops); err == nil {
				t.Errorf("ApplySet(%s, %+v) expected an error", tc.pay, tc.op)
			}
		})
	}
}

func TestDiffChangedNestedPaths(t *testing.T) {
	old := []byte(`{"customer_id":1000,"billing":{"city":"Old","zip":"1111"},"tags":["a","b"]}`)
	next := []byte(`{"customer_id":443,"billing":{"city":"New","zip":"1111"},"tags":["a","c"]}`)
	d, err := Diff(old, next)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"- customer_id: 1000", "+ customer_id: 443",
		"- billing.city: \"Old\"", "+ billing.city: \"New\"",
		"- tags: [\"a\",\"b\"]", "+ tags: [\"a\",\"c\"]",
	} {
		if !strings.Contains(d, want) {
			t.Errorf("diff missing %q:\n%s", want, d)
		}
	}
	// Unchanged paths must not appear.
	if strings.Contains(d, "zip") {
		t.Errorf("diff shows unchanged zip:\n%s", d)
	}
}

func TestDiffAddedRemoved(t *testing.T) {
	d, err := Diff([]byte(`{"keep":1,"gone":2}`), []byte(`{"keep":1,"fresh":3}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(d, "- gone: 2") || !strings.Contains(d, "+ fresh: 3") {
		t.Errorf("diff missing added/removed:\n%s", d)
	}
	if strings.Contains(d, "keep") {
		t.Errorf("diff shows unchanged keep:\n%s", d)
	}
}

func TestDiffEqual(t *testing.T) {
	d, err := Diff([]byte(`{"a":1,"b":[1,2]}`), []byte(`{"b":[1,2],"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if d != "" {
		t.Errorf("diff = %q, want empty for equal payloads (key order ignored)", d)
	}
}

func TestDiffTypeChange(t *testing.T) {
	d, err := Diff([]byte(`{"a":1}`), []byte(`{"a":"1"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(d, "- a: 1") || !strings.Contains(d, `+ a: "1"`) {
		t.Errorf("diff missing type change:\n%s", d)
	}
}

func TestDiffNonJSON(t *testing.T) {
	if _, err := Diff([]byte("nope"), []byte(`{}`)); err == nil {
		t.Error("expected an error for non-JSON payload")
	}
}

func jsonEqual(a, b any) bool {
	ab, err := json.Marshal(a)
	if err != nil {
		return false
	}
	bb, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return string(ab) == string(bb)
}
