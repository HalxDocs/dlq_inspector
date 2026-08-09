package message

import (
	"strings"
	"testing"
)

func TestRedactorMasksNestedField(t *testing.T) {
	payload := []byte(`{"customer":{"email":"a@b.com"},"order":42}`)
	got := string(Redactor{Fields: []string{"customer.email"}}.Apply(payload))
	if strings.Contains(got, "a@b.com") {
		t.Errorf("email not masked: %s", got)
	}
	if !strings.Contains(got, "[REDACTED]") {
		t.Errorf("mask missing: %s", got)
	}
	if !strings.Contains(got, `"order":42`) {
		t.Errorf("unrelated fields changed: %s", got)
	}
}

func TestRedactorMaskMultipleFields(t *testing.T) {
	payload := []byte(`{"card_number":"4111","ssn":"123-45"}`)
	got := string(Redactor{Fields: []string{"card_number", "ssn"}}.Apply(payload))
	if strings.Contains(got, "4111") || strings.Contains(got, "123-45") {
		t.Errorf("fields not masked: %s", got)
	}
}

func TestRedactorCustomMask(t *testing.T) {
	payload := []byte(`{"email":"a@b.com"}`)
	got := string(Redactor{Fields: []string{"email"}, Mask: "***"}.Apply(payload))
	if !strings.Contains(got, "***") || strings.Contains(got, "a@b.com") {
		t.Errorf("custom mask not applied: %s", got)
	}
}

func TestRedactorNoFieldsIsNoop(t *testing.T) {
	payload := []byte(`{"email":"a@b.com"}`)
	if got := (Redactor{}).Apply(payload); string(got) != string(payload) {
		t.Errorf("empty redactor changed payload: %s", got)
	}
}

func TestRedactorNonJSONPassthrough(t *testing.T) {
	payload := []byte("plain text")
	if got := (Redactor{Fields: []string{"email"}}).Apply(payload); string(got) != string(payload) {
		t.Errorf("non-JSON payload changed: %s", got)
	}
}

func TestRedactorMissingFieldNoop(t *testing.T) {
	payload := []byte(`{"a":1}`)
	if got := (Redactor{Fields: []string{"nope"}}).Apply(payload); string(got) != string(payload) {
		t.Errorf("missing field changed payload: %s", got)
	}
}

func TestPrettyJSON(t *testing.T) {
	got, err := PrettyJSON([]byte(`{"a":1,"b":[1,2]}`))
	if err != nil {
		t.Fatalf("PrettyJSON: %v", err)
	}
	if !strings.Contains(got, "\n  \"a\"") {
		t.Errorf("not indented: %q", got)
	}
}

func TestPrettyJSONRejectsNonJSON(t *testing.T) {
	if _, err := PrettyJSON([]byte("nope")); err == nil {
		t.Error("PrettyJSON on non-JSON should error")
	}
}
