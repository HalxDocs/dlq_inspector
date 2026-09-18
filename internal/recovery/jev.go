package recovery

import (
	"context"
	"fmt"

	"github.com/HalxDocs/dlq_inspector/internal/jev"
	"github.com/HalxDocs/dlq_inspector/internal/message"
	"github.com/HalxDocs/dlq_inspector/internal/policy"
)

// JevEvaluator is the subset of *jev.Client used by the classifier. Tests
// substitute a fake; production passes a real client.
type JevEvaluator interface {
	Evaluate(ctx context.Context, state string, questions map[string]jev.Question) (*jev.Response, error)
}

// JevAssessor is an optional Jev-backed classifier pass. A nil assessor, or
// one with a nil Client, is disabled and every call falls back to the
// rule-based result silently.
type JevAssessor struct {
	Client JevEvaluator
	// Threshold is the minimum Jev confidence required to replace the
	// rule-based inference. Zero means jev.DefaultThreshold.
	Threshold float64
	// Cache deduplicates identical states within one run. Nil disables it.
	Cache *jev.Cache
	// MaxCalls caps Jev evaluations per assessor (spend guard). Zero means
	// no cap. Messages beyond the cap keep their rule-based result.
	MaxCalls int
	// OnlyInvestigate, when true, consults Jev only for messages the
	// rule-based classifier marked INVESTIGATE. This is the cheapest mode
	// and the default recommendation: rules handle the obvious, Jev
	// resolves the ambiguous.
	OnlyInvestigate bool

	calls int
}

// Enabled reports whether Jev evaluation can run.
func (a *JevAssessor) Enabled() bool {
	return a != nil && a.Client != nil
}

// Calls returns the number of Jev API calls made so far.
func (a *JevAssessor) Calls() int {
	if a == nil {
		return 0
	}
	return a.calls
}

func (a *JevAssessor) threshold() float64 {
	if a == nil || a.Threshold <= 0 {
		return jev.DefaultThreshold
	}
	return a.Threshold
}

// ClassifyWithPolicyAndJev applies the full precedence chain:
//
//  1. x-duplicate-of header (via Classify) — always wins, Jev never consulted.
//  2. Policy match — overrides inference, Jev never consulted.
//  3. Jev verdict — when the assessor is enabled, budget remains, and (in
//     OnlyInvestigate mode) the base result is INVESTIGATE.
//  4. Rule-based inference — the baseline and every fallback.
//
// ctx carries the call deadline. Any Jev error, timeout, unknown choice, or
// below-threshold verdict silently keeps the rule-based result with
// Source="rules".
func ClassifyWithPolicyAndJev(ctx context.Context, m *message.Message, p *policy.Policy, a *JevAssessor) ClassificationResult {
	base := ClassifyWithPolicy(m, p)
	if m == nil {
		return base
	}
	base.Source = "rules"
	if m.Headers["x-duplicate-of"] != "" {
		base.SkipReason = "header-duplicate"
		return base
	}
	if p != nil {
		if _, ok := p.Match(m); ok {
			base.Source = "policy"
			base.SkipReason = "policy-matched"
			return base
		}
	}
	if a == nil || !a.Enabled() {
		base.SkipReason = "jev-disabled"
		return base
	}
	if a.OnlyInvestigate && base.Classification != Investigate {
		base.SkipReason = "not-investigate"
		return base
	}
	if a.MaxCalls > 0 && a.calls >= a.MaxCalls {
		base.SkipReason = "call-budget-exceeded"
		return base
	}

	state := jev.BuildMessageState(m, NormalizeSignature(m.FailureReason))
	var resp *jev.Response
	if a.Cache != nil {
		if hit, ok := a.Cache.Get(state); ok {
			a.Cache.Hit()
			resp = hit
		}
	}
	if resp == nil {
		var err error
		resp, err = a.Client.Evaluate(ctx, state, jev.ClassificationQuestions())
		if err != nil {
			base.SkipReason = "jev-error"
			return base
		}
		a.calls++
		if a.Cache != nil {
			a.Cache.Put(state, resp)
		}
	}

	verdict, ok, err := jev.MapResponse(resp, a.threshold())
	if err != nil || !ok {
		base.SkipReason = "below-threshold"
		if err != nil {
			base.SkipReason = "jev-error"
		}
		return base
	}
	out := base
	out.Classification = jevChoiceToClassification(verdict.Classification)
	out.Confidence = verdict.Confidence
	out.Source = "jev"
	out.JevModel = verdict.Model
	out.ReplayRisk = verdict.ReplayRisk
	out.Reason = fmt.Sprintf("jev %s (p=%.2f, risk=%.2f): %s",
		verdict.Model, verdict.Confidence, verdict.ReplayRisk, jevChoiceWhy(verdict.Classification))
	if verdict.DuplicateProb >= jev.DuplicateOverride && verdict.Classification == "do_not_replay" {
		out.Reason += fmt.Sprintf("; duplicate probability %.2f", verdict.DuplicateProb)
	}
	return out
}

func jevChoiceToClassification(choice string) Classification {
	switch choice {
	case "replayable":
		return Replayable
	case "requires_fix":
		return RequiresFix
	case "do_not_replay":
		return DoNotReplay
	default:
		return Investigate
	}
}

func jevChoiceWhy(choice string) string {
	switch choice {
	case "replayable":
		return "failure looks transient — safe to retry"
	case "requires_fix":
		return "failure looks permanent — fix payload or config first"
	case "do_not_replay":
		return "event looks already processed — replay would duplicate work"
	default:
		return "signals remain ambiguous — investigate before replaying"
	}
}
