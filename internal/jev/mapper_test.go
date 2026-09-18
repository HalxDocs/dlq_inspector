package jev

import (
	"testing"
)

func jevResp(choice string, conf, dup, risk float64) *Response {
	return &Response{
		Model: "jev-1.13.0",
		Answers: map[string]Answer{
			"classification": {Type: "choice", Choice: choice, Confidence: conf,
				Probabilities: map[string]float64{choice: conf}},
			"is_duplicate": {Type: "noul", Noul: dup},
			"replay_risk":  {Type: "score", Score: risk * 2},
		},
	}
}

func TestMapResponseConfident(t *testing.T) {
	v, ok, err := MapResponse(jevResp("replayable", 0.93, 0.01, 0.1), 0.7)
	if err != nil || !ok {
		t.Fatalf("want ok, got ok=%v err=%v", ok, err)
	}
	if v.Classification != "replayable" || v.Model != "jev-1.13.0" {
		t.Errorf("bad verdict: %+v", v)
	}
	if v.ReplayRisk < 0.09 || v.ReplayRisk > 0.11 {
		t.Errorf("risk = %v, want ~0.1", v.ReplayRisk)
	}
}

func TestMapResponseLowConfidenceFallsBack(t *testing.T) {
	_, ok, err := MapResponse(jevResp("replayable", 0.4, 0.0, 0.5), 0.7)
	if err != nil || ok {
		t.Fatalf("want fallback (ok=false), got ok=%v err=%v", ok, err)
	}
}

func TestMapResponseDuplicateOverride(t *testing.T) {
	v, ok, err := MapResponse(jevResp("replayable", 0.9, 0.95, 0.9), 0.7)
	if err != nil || !ok {
		t.Fatalf("want ok, got ok=%v err=%v", ok, err)
	}
	if v.Classification != "do_not_replay" {
		t.Errorf("duplicate did not override: %+v", v)
	}
}

func TestMapResponseUnknownChoice(t *testing.T) {
	_, ok, err := MapResponse(jevResp("maybe", 0.99, 0, 0), 0.7)
	if err != nil || ok {
		t.Fatalf("want fallback on unknown choice, got ok=%v err=%v", ok, err)
	}
}

func TestMapResponseMissingClassification(t *testing.T) {
	_, _, err := MapResponse(&Response{Model: "jev-1.13.0", Answers: map[string]Answer{}}, 0.7)
	if err == nil {
		t.Fatal("want error on missing classification")
	}
}
