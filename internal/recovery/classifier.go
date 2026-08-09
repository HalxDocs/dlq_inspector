package recovery

import (
	"strings"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// Classification is the recovery category assigned to a failed message.
// The classifier never claims certainty it doesn't have: INVESTIGATE is the
// honest default when signals are missing or conflicting, rather than forcing
// every message into REPLAYABLE or DO_NOT_REPLAY.
type Classification string

const (
	// Replayable means the failure appears transient or recoverable.
	Replayable Classification = "REPLAYABLE"
	// RequiresFix means the payload or message data needs correction before
	// it can be replayed.
	RequiresFix Classification = "REQUIRES_FIX"
	// DoNotReplay means replaying could create duplicates or other harm.
	DoNotReplay Classification = "DO_NOT_REPLAY"
	// Investigate means the evidence is insufficient to decide safely.
	Investigate Classification = "INVESTIGATE"
)

// ClassificationResult is the per-message outcome of the classifier.
type ClassificationResult struct {
	MessageID      string
	Classification Classification
	Reason         string
	Confidence     float64 // 0.0-1.0, best-effort
	DuplicateOf    *string // set when a likely duplicate was found
}

// Classify applies the rule-based v1 classifier to a single message.
//
// The rules are deliberately conservative and code-defined (no user policy
// DSL yet — that arrives with the policy engine):
//
//   - Failure text that signals a duplicate (already processed, idempotency
//     hit) is DO_NOT_REPLAY — replaying would duplicate work.
//   - Transient-sounding failures (timeouts, connection resets, 5xx,
//     throttling) are REPLAYABLE — unless the retry count is already high,
//     which suggests the "transient" cause is actually persistent.
//   - Permanent-sounding failures (validation, authorization, not-found,
//     4xx, schema/parse errors) are REQUIRES_FIX — the payload needs
//     correction, not another attempt.
//   - Missing or conflicting signals default to INVESTIGATE.
func Classify(m *message.Message) ClassificationResult {
	reason := strings.ToLower(m.FailureReason)

	dup := hasAny(reason, dupKeywords)
	transient := hasAny(reason, transientKeywords)
	permanent := hasAny(reason, permanentKeywords)

	res := ClassificationResult{MessageID: m.ID}

	switch {
	case dup:
		res.Classification = DoNotReplay
		res.Reason = "failure text indicates a duplicate or already-processed event"
		res.Confidence = 0.9
		return res

	case transient && permanent:
		res.Classification = Investigate
		res.Reason = "failure text has both transient and permanent signals"
		res.Confidence = 0.5
		return res

	case transient:
		if m.RetryCount >= highRetry {
			res.Classification = Investigate
			res.Reason = "transient-looking failure but retry count is already high — the cause may be persistent"
			res.Confidence = 0.6
			return res
		}
		res.Classification = Replayable
		res.Reason = "failure appears transient (timeout, connection, throttling, 5xx)"
		res.Confidence = 0.8
		return res

	case permanent:
		res.Classification = RequiresFix
		res.Reason = "failure appears permanent (validation, authorization, schema, 4xx) — fix before replaying"
		res.Confidence = 0.8
		return res

	case m.FailureReason == "":
		res.Classification = Investigate
		res.Reason = "no failure reason recorded"
		res.Confidence = 0.4
		return res

	default:
		res.Classification = Investigate
		res.Reason = "failure text did not match any known signal"
		res.Confidence = 0.5
		return res
	}
}

// highRetry is the retry count beyond which a transient-looking failure is
// treated as suspicious: if it kept failing that many times, it probably
// isn't transient anymore.
const highRetry = 6

// transientKeywords signal failures worth retrying. "http 5" and "5xx" cover
// server-side HTTP statuses without the bare "5" matching every digit.
var transientKeywords = []string{
	"timeout", "timed out", "deadline", "temporary", "temporarily",
	"unavailable", "connection", "refused", "reset", "interrupted",
	"unreachable", "throttl", "rate limit", "too many requests",
	"circuit", "network", "try again", "transient",
	"5xx", "http 5", "502", "503", "504",
}

// permanentKeywords signal failures that need a fix, not another attempt.
var permanentKeywords = []string{
	"invalid", "validation", "malformed", "parse", "deserial",
	"schema", "constraint", "unauthorized", "forbidden", "permission",
	"denied", "not found", "unsupported", "rejected", "expired",
	"missing field", "required field", "unknown",
	"400", "401", "403", "404", "409", "422",
}

// dupKeywords signal that the event may already have been processed.
var dupKeywords = []string{
	"duplicate", "already", "idempotency", "replayed", "processed", "dedup",
}

// hasAny reports whether s contains any of the given substrings.
func hasAny(s string, needles []string) bool {
	for _, n := range needles {
		if strings.Contains(s, n) {
			return true
		}
	}
	return false
}
