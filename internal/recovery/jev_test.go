package recovery

import (
	"context"
	"errors"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/jev"
	"github.com/HalxDocs/dlq_inspector/internal/message"
	"github.com/HalxDocs/dlq_inspector/internal/policy"
)

type fakeJev struct {
	resp  *jev.Response
	err   error
	calls int
}

func (f *fakeJev) Evaluate(_ context.Context, _ string, _ map[string]jev.Question) (*jev.Response, error) {
	f.calls++
	return f.resp, f.err
}

func jevOK(choice string, conf float64) *jev.Response {
	return &jev.Response{
		Model: "jev-1.13.0",
		Answers: map[string]jev.Answer{
			"classification": {Type: "choice", Choice: choice, Confidence: conf,
				Probabilities: map[string]float64{choice: conf}},
			"is_duplicate": {Type: "noul", Noul: 0.01},
			"replay_risk":  {Type: "score", Score: 0.2},
		},
	}
}

func TestJevResolvesInvestigate(t *testing.T) {
	f := &fakeJev{resp: jevOK("replayable", 0.93)}
	a := &JevAssessor{Client: f, OnlyInvestigate: true, Cache: jev.NewCache()}
	m := &message.Message{ID: "m1", FailureReason: "the flux capacitor demagnetized"}
	got := ClassifyWithPolicyAndJev(context.Background(), m, nil, a)
	if got.Classification != Replayable || got.Source != "jev" {
		t.Fatalf("got %s/%s (%s)", got.Classification, got.Source, got.Reason)
	}
	if got.JevModel != "jev-1.13.0" {
		t.Errorf("model = %q", got.JevModel)
	}
	if f.calls != 1 {
		t.Errorf("calls = %d", f.calls)
	}
}

func TestJevSkippedForDecidedMessages(t *testing.T) {
	f := &fakeJev{resp: jevOK("requires_fix", 0.95)}
	a := &JevAssessor{Client: f, OnlyInvestigate: true}
	m := &message.Message{ID: "m1", FailureReason: "timeout connecting to x"}
	got := ClassifyWithPolicyAndJev(context.Background(), m, nil, a)
	if got.Source != "rules" || f.calls != 0 {
		t.Fatalf("Jev should be skipped, got %s calls=%d", got.Source, f.calls)
	}
}

func TestJevLosesToHeaderAndPolicy(t *testing.T) {
	f := &fakeJev{resp: jevOK("replayable", 0.99)}
	a := &JevAssessor{Client: f}
	dup := &message.Message{ID: "m1", FailureReason: "timeout",
		Headers: map[string]string{"x-duplicate-of": "evt_1"}}
	if got := ClassifyWithPolicyAndJev(context.Background(), dup, nil, a); got.Classification != DoNotReplay || got.Source != "rules" {
		t.Errorf("header lost: %+v", got)
	}
	pol, err := policy.Parse([]byte("rules:\n  - when: error contains timeout\n    action: do_not_replay\n"))
	if err != nil {
		t.Fatal(err)
	}
	m := &message.Message{ID: "m2", FailureReason: "timeout everywhere"}
	if got := ClassifyWithPolicyAndJev(context.Background(), m, pol, a); got.Source != "policy" {
		t.Errorf("policy lost: %+v", got)
	}
	if f.calls != 0 {
		t.Errorf("Jev consulted despite override, calls=%d", f.calls)
	}
}

func TestJevErrorFallsBack(t *testing.T) {
	f := &fakeJev{err: errors.New("boom")}
	a := &JevAssessor{Client: f}
	m := &message.Message{ID: "m1", FailureReason: "mystery failure"}
	got := ClassifyWithPolicyAndJev(context.Background(), m, nil, a)
	if got.Classification != Investigate || got.Source != "rules" || got.SkipReason != "jev-error" {
		t.Fatalf("bad fallback: %+v", got)
	}
}

func TestJevBelowThresholdFallsBack(t *testing.T) {
	f := &fakeJev{resp: jevOK("replayable", 0.4)}
	a := &JevAssessor{Client: f, Threshold: 0.7}
	m := &message.Message{ID: "m1", FailureReason: "mystery failure"}
	got := ClassifyWithPolicyAndJev(context.Background(), m, nil, a)
	if got.Source != "rules" || got.SkipReason != "below-threshold" {
		t.Fatalf("bad fallback: %+v", got)
	}
}

func TestJevCacheDedupes(t *testing.T) {
	f := &fakeJev{resp: jevOK("replayable", 0.9)}
	a := &JevAssessor{Client: f, Cache: jev.NewCache()}
	m := &message.Message{ID: "m1", FailureReason: "mystery failure"}
	for i := 0; i < 3; i++ {
		ClassifyWithPolicyAndJev(context.Background(), m, nil, a)
	}
	if f.calls != 1 {
		t.Fatalf("calls = %d, want 1 (cached)", f.calls)
	}
}

func TestJevMaxCallsCap(t *testing.T) {
	f := &fakeJev{resp: jevOK("replayable", 0.9)}
	a := &JevAssessor{Client: f, MaxCalls: 1}
	m1 := &message.Message{ID: "m1", FailureReason: "mystery one"}
	m2 := &message.Message{ID: "m2", FailureReason: "mystery two"}
	first := ClassifyWithPolicyAndJev(context.Background(), m1, nil, a)
	second := ClassifyWithPolicyAndJev(context.Background(), m2, nil, a)
	if first.Source != "jev" || second.Source != "rules" || second.SkipReason != "call-budget-exceeded" {
		t.Fatalf("cap broken: %+v %+v", first, second)
	}
}

func TestJevDisabledIsSilent(t *testing.T) {
	m := &message.Message{ID: "m1", FailureReason: "mystery failure"}
	got := ClassifyWithPolicyAndJev(context.Background(), m, nil, nil)
	if got.Classification != Investigate || got.Source != "rules" {
		t.Fatalf("bad disabled fallback: %+v", got)
	}
}
