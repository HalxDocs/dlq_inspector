package jev

import (
	"errors"
	"strings"
)

// DefaultThreshold is the minimum classification confidence required before
// a Jev verdict may replace the rule-based inference. Below it the caller
// keeps the rules and records the skip. Tunable per run via --jev-threshold.
const DefaultThreshold = 0.70

// DuplicateOverride is the is_duplicate probability at or above which the
// verdict is forced to do_not_replay, mirroring the rule-based contract
// that duplicate evidence outranks transient signals.
const DuplicateOverride = 0.8

// Verdict is the mapped outcome of one Jev evaluation call.
type Verdict struct {
	// Classification is one of replayable, requires_fix, do_not_replay,
	// investigate (lowercase Jev choice keys).
	Classification string
	// Confidence is the classification confidence (0-1, calibrated).
	Confidence float64
	// Probabilities is the full choice distribution for audit display.
	Probabilities map[string]float64
	// DuplicateProb is the is_duplicate noul probability (0-1).
	DuplicateProb float64
	// ReplayRisk is the replay_risk score normalized to 0-1
	// (raw Jev score is continuous around 0-2).
	ReplayRisk float64
	// Model is the versioned model ID that answered.
	Model string
}

// MapResponse maps a Jev response to a Verdict. It returns ok=false (no
// error) when the verdict is unusable or below threshold — the caller then
// falls back to rules silently.
func MapResponse(resp *Response, threshold float64) (v Verdict, ok bool, err error) {
	if resp == nil {
		return Verdict{}, false, errors.New("jev: nil response")
	}
	cls, found := resp.Answers["classification"]
	if !found {
		return Verdict{}, false, errors.New("jev: response missing classification answer")
	}
	choice := strings.ToLower(strings.TrimSpace(cls.Choice))
	switch choice {
	case "replayable", "requires_fix", "do_not_replay", "investigate":
	default:
		return Verdict{}, false, nil
	}
	conf := cls.Confidence
	if conf <= 0 {
		conf = maxProb(cls.Probabilities)
	}
	v = Verdict{
		Classification: choice,
		Confidence:     clamp01(conf),
		Probabilities:  cls.Probabilities,
		Model:          resp.Model,
	}
	if d, found := resp.Answers["is_duplicate"]; found {
		v.DuplicateProb = clamp01(d.Noul)
	}
	if r, found := resp.Answers["replay_risk"]; found {
		v.ReplayRisk = clamp01(r.Score / 2)
	}
	// Duplicate evidence outranks the choice, same as the rules.
	if v.DuplicateProb >= DuplicateOverride && v.Classification != "do_not_replay" {
		v.Classification = "do_not_replay"
		if v.DuplicateProb > v.Confidence {
			v.Confidence = v.DuplicateProb
		}
	}
	if threshold <= 0 {
		threshold = DefaultThreshold
	}
	if v.Confidence < threshold {
		return v, false, nil
	}
	return v, true, nil
}

func maxProb(p map[string]float64) float64 {
	best := 0.0
	for _, v := range p {
		if v > best {
			best = v
		}
	}
	return best
}

func clamp01(f float64) float64 {
	if f < 0 {
		return 0
	}
	if f > 1 {
		return 1
	}
	return f
}
