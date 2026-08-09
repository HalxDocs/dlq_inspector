// Package dedupe surfaces duplicate/idempotency evidence before a replay or
// recovery executes. Replay is dangerous when the original operation may
// already have succeeded, so the tool warns — it never silently blocks; the
// operator decides. Evidence sources grow over time (audit trail now,
// destination-queue scans in later phases).
package dedupe

import (
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// Evidence describes a possible duplicate.
type Evidence struct {
	// MatchFound is true when a prior successful replay of this message was
	// found in the audit trail.
	MatchFound bool
	// MatchSource names where the matching record was found (e.g. "audit").
	MatchSource string
	// PriorReplayAt is when the prior replay happened, when known.
	PriorReplayAt *time.Time
	// EventID and IdempotencyKey echo the message's identifiers so the
	// operator sees what was matched.
	EventID        string
	IdempotencyKey string
}

// CheckAudit looks for a prior successful replay of this message in the local
// audit store. A message that was already replayed and came back is the
// strongest duplicate signal available to a local-first tool.
func CheckAudit(store *audit.Store, m *message.Message) (Evidence, error) {
	if store == nil {
		return Evidence{}, nil
	}
	entries, err := store.Replayed(m.ID)
	if err != nil {
		return Evidence{}, err
	}
	if len(entries) == 0 {
		return Evidence{EventID: m.EventID, IdempotencyKey: m.IdempotencyKey}, nil
	}
	at := entries[0].Timestamp
	return Evidence{
		MatchFound:     true,
		MatchSource:    "audit",
		PriorReplayAt:  &at,
		EventID:        m.EventID,
		IdempotencyKey: m.IdempotencyKey,
	}, nil
}
