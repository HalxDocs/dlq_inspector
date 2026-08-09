// Package safety is the shared safety pipeline every mutating operation
// passes through: dry-run -> duplicate evidence -> confirm -> execute -> audit.
// The command layer and the recovery engine never publish, ack, or delete a
// message directly — they call this gate, which enforces the invariants that
// make recovery safe:
//
//   - A dry-run performs zero mutating I/O (no publish, no ack, no delete).
//   - Execute refuses to run without an explicit confirm.
//   - The DLQ copy is acked only after a successful publish, so a failed
//     replay never loses the message (at-least-once semantics).
//   - Every outcome — dry-run or confirmed, success or failure — is written
//     to the append-only audit trail.
package safety

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/dedupe"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// Request describes one replay unit. Phase 6's batch recover drives the same
// gate once per message.
type Request struct {
	Broker broker.Broker
	Audit  *audit.Store

	// Queue is the DLQ the message sits in.
	Queue string
	// MessageID is the stable message ID to replay.
	MessageID string
	// Destination overrides the message's own destination (its original
	// queue, from x-death metadata). Empty means "use the message's".
	Destination string

	// BrokerName and Profile are recorded on audit entries.
	BrokerName string
	Profile    string
	// Reason is the operator-provided justification, recorded on audit entries.
	Reason string
	// Confirm must be true for Execute to run.
	Confirm bool
}

// ErrConfirmRequired is returned when Execute is called without an explicit
// confirm. The safety gate never performs mutating I/O without it.
var ErrConfirmRequired = errors.New("confirmation required: re-run with --confirm")

// ErrDestinationMissing is returned when the resolved replay destination does
// not exist on the broker. Publishing into a nonexistent queue can be
// confirmed and silently dropped, so the replay is refused before anything
// is published or acked — the DLQ copy is never at risk.
var ErrDestinationMissing = errors.New("destination queue does not exist — refusing to replay")

// Preview is the full dry-run picture: everything that would happen, with
// zero mutating I/O performed.
type Preview struct {
	Message      *message.Message
	Destination  string
	Duplicate    dedupe.Evidence
	SafetyChecks []string
	Warnings     []string
}

// Result is what actually happened during a confirmed execution.
type Result struct {
	MessageID   string
	Destination string
	Published   bool
	Acked       bool
	Audit       audit.Entry
}

// DryRun inspects the message, resolves the destination, gathers duplicate
// evidence, and reports the safety checks — all read-only. The dry-run itself
// is audited (Result "dry_run") so the trail shows what was previewed.
func DryRun(ctx context.Context, r Request) (*Preview, error) {
	m, err := r.Broker.Inspect(ctx, r.Queue, r.MessageID)
	if err != nil {
		return nil, err
	}
	dest, err := resolveDestination(r.Destination, m)
	if err != nil {
		return nil, err
	}
	dup, err := dedupe.CheckAudit(r.Audit, m)
	if err != nil {
		return nil, fmt.Errorf("duplicate check: %w", err)
	}

	checks := []string{"message_exists", "destination_resolved", "duplicate_checked"}
	warnings := duplicateWarnings(dup)

	// Destination existence is part of the dry-run picture: a publish into a
	// nonexistent queue is confirmed and silently dropped, after which acking
	// the DLQ copy would lose the message. Surface it here so the operator
	// sees it before --confirm; Execute refuses outright.
	if _, err := r.Broker.Stats(ctx, dest); err != nil {
		if errors.Is(err, broker.ErrQueueNotFound) {
			warnings = append(warnings, fmt.Sprintf("destination queue %q does not exist — a confirmed replay will be refused", dest))
		} else {
			return nil, fmt.Errorf("destination check for %q: %w", dest, err)
		}
	} else {
		checks = append(checks, "destination_exists")
	}

	if r.Audit != nil {
		_ = r.Audit.Append(dryRunEntry(r, m, dest))
	}

	return &Preview{
		Message:      m,
		Destination:  dest,
		Duplicate:    dup,
		SafetyChecks: checks,
		Warnings:     warnings,
	}, nil
}

// Execute replays the message: publish to the destination, ack the DLQ copy
// only after a successful publish, then write the audit entry. It refuses to
// run without Confirm and re-inspects the message so the execution matches
// what is actually in the queue, not just what a preview saw earlier.
func Execute(ctx context.Context, r Request) (*Result, error) {
	if !r.Confirm {
		return nil, ErrConfirmRequired
	}

	m, err := r.Broker.Inspect(ctx, r.Queue, r.MessageID)
	if err != nil {
		return nil, err
	}
	dest, err := resolveDestination(r.Destination, m)
	if err != nil {
		return nil, err
	}

	// Hard safety invariant: never publish into a destination that does not
	// exist. RabbitMQ confirms publishes to nonexistent queues and silently
	// drops them — acking the DLQ copy afterwards would lose the message.
	// Refuse before any publish or ack; the refusal itself is audited.
	if _, err := r.Broker.Stats(ctx, dest); err != nil {
		if errors.Is(err, broker.ErrQueueNotFound) {
			appendRefusal(r, m, dest, fmt.Sprintf("destination queue %q does not exist", dest))
			return nil, fmt.Errorf("%w: queue %q (DLQ message left in place)", ErrDestinationMissing, dest)
		}
		appendRefusal(r, m, dest, fmt.Sprintf("destination check failed: %v", err))
		return nil, fmt.Errorf("destination check for %q: %w (DLQ message left in place)", dest, err)
	}

	// Publish first. If this fails, the DLQ copy is untouched — never acked.
	if err := r.Broker.Publish(ctx, dest, m); err != nil {
		appendFailure(r, m, dest, "publish_failed")
		return nil, fmt.Errorf("replay failed at publish (DLQ message left in place): %w", err)
	}

	// Publish succeeded — now remove the DLQ copy.
	if err := r.Broker.Ack(ctx, r.Queue, r.MessageID); err != nil {
		appendFailure(r, m, dest, "ack_failed")
		// The message was published and is still in the DLQ: at-least-once
		// delivery means a duplicate is possible, but nothing was lost.
		return nil, fmt.Errorf("replay published but ack failed (message may be duplicated): %w", err)
	}

	entry := audit.Entry{
		Timestamp:   time.Now().UTC(),
		Action:      audit.ActionReplay,
		MessageID:   m.ID,
		SourceQueue: r.Queue,
		Destination: dest,
		Confirmed:   true,
		Result:      "success",
		Broker:      r.BrokerName,
		Profile:     r.Profile,
		Reason:      r.Reason,
	}
	if r.Audit != nil {
		_ = r.Audit.Append(entry)
	}

	return &Result{
		MessageID:   m.ID,
		Destination: dest,
		Published:   true,
		Acked:       true,
		Audit:       entry,
	}, nil
}

// resolveDestination uses the explicit override or the message's own
// destination recorded from x-death metadata.
func resolveDestination(override string, m *message.Message) (string, error) {
	if override != "" {
		return override, nil
	}
	if m.Destination != "" {
		return m.Destination, nil
	}
	return "", errors.New("cannot determine replay destination (message has no dead-letter metadata) — pass --destination")
}

// duplicateWarnings turns duplicate evidence into operator-facing warnings.
func duplicateWarnings(dup dedupe.Evidence) []string {
	if !dup.MatchFound {
		return nil
	}
	at := "previously"
	if dup.PriorReplayAt != nil {
		at = dup.PriorReplayAt.UTC().Format(time.RFC3339)
	}
	return []string{fmt.Sprintf("message was already replayed (%s); replaying again may duplicate work", at)}
}

func dryRunEntry(r Request, m *message.Message, dest string) audit.Entry {
	return audit.Entry{
		Timestamp:   time.Now().UTC(),
		Action:      audit.ActionReplay,
		MessageID:   m.ID,
		SourceQueue: r.Queue,
		Destination: dest,
		DryRun:      true,
		Result:      "dry_run",
		Broker:      r.BrokerName,
		Profile:     r.Profile,
		Reason:      r.Reason,
	}
}

func appendFailure(r Request, m *message.Message, dest, result string) {
	if r.Audit == nil {
		return
	}
	_ = r.Audit.Append(audit.Entry{
		Timestamp:   time.Now().UTC(),
		Action:      audit.ActionReplay,
		MessageID:   m.ID,
		SourceQueue: r.Queue,
		Destination: dest,
		Confirmed:   true,
		Result:      result,
		Broker:      r.BrokerName,
		Profile:     r.Profile,
		Reason:      r.Reason,
	})
}

// appendRefusal records a confirmed run that was refused before any publish
// or ack (e.g. the destination queue does not exist), so the trail explains
// why nothing happened. The operator's reason is preserved alongside the
// refusal detail.
func appendRefusal(r Request, m *message.Message, dest, detail string) {
	if r.Audit == nil {
		return
	}
	reason := r.Reason
	if detail != "" {
		reason = strings.TrimSpace(reason + " — " + detail)
	}
	_ = r.Audit.Append(audit.Entry{
		Timestamp:   time.Now().UTC(),
		Action:      audit.ActionReplay,
		MessageID:   m.ID,
		SourceQueue: r.Queue,
		Destination: dest,
		Confirmed:   true,
		Result:      "refused",
		Broker:      r.BrokerName,
		Profile:     r.Profile,
		Reason:      reason,
	})
}
