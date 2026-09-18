package jev

import (
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

func TestBuildMessageStateIsMetadataOnly(t *testing.T) {
	m := &message.Message{
		ID:            "m1",
		FailureReason: "timeout connecting to 10.0.4.5:6432",
		Destination:   "orders",
		RetryCount:    2,
		Payload:       []byte(`{"customer":{"email":"a@b.com"},"card":"4111"}`),
		Headers:       map[string]string{"x-event-type": "order.created", "authorization": "secret"},
	}
	state := BuildMessageState(m, "timeout connecting to {ip}")
	for _, leak := range []string{"a@b.com", "4111", "secret"} {
		if strings.Contains(state, leak) {
			t.Fatalf("state leaks sensitive data %q: %q", leak, state)
		}
	}
	for _, want := range []string{"timeout", "retries=2", "orders", "order.created"} {
		if !strings.Contains(state, want) {
			t.Errorf("state missing %q: %q", want, state)
		}
	}
}

func TestBuildMessageStateTruncates(t *testing.T) {
	m := &message.Message{FailureReason: strings.Repeat("x", MaxErrorChars+100)}
	state := BuildMessageState(m, "")
	if len(state) > MaxErrorChars+300 {
		t.Fatalf("state not truncated: %d chars", len(state))
	}
}

func TestClassificationQuestionsShape(t *testing.T) {
	q := ClassificationQuestions()
	if q["classification"].Type != "choice" || q["is_duplicate"].Type != "noul" || q["replay_risk"].Type != "score" {
		t.Fatalf("bad question types: %+v", q)
	}
}

func TestBuildGroupStateSamples(t *testing.T) {
	g := GroupSample{Signature: "sig", Count: 301, Errors: []string{"e1", "e2", "e3", "e4", "e5", "e6", "e7"}}
	state := BuildGroupState(g)
	if !strings.Contains(state, "count=301") || !strings.Contains(state, "sample5=e5") {
		t.Errorf("bad group state: %q", state)
	}
	if strings.Contains(state, "sample6=") {
		t.Errorf("group state exceeds sample cap: %q", state)
	}
}
