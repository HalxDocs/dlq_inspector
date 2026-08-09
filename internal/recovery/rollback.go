package recovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// RollbackOptions control one rollback run.
type RollbackOptions struct {
	// Confirm must be true — rollback refuses to restore otherwise.
	Confirm bool
	// Reason is the operator-provided justification, recorded on every audit
	// entry of the run.
	Reason string
	// BrokerName and Profile are recorded on audit entries.
	BrokerName string
	Profile    string
}

// RollbackResult summarizes one rollback run.
type RollbackResult struct {
	PlanID     string        `json:"plan_id"`
	Snapshots  int           `json:"snapshots"`
	Restored   int           `json:"restored"`
	Failed     int           `json:"failed"`
	DLQ        string        `json:"dlq"`
	MissingDLQ string        `json:"missing_dlq,omitempty"`
	Duration   time.Duration `json:"duration"`
}

// Rollback restores snapshotted messages back to the DLQ they were replayed
// from — the reversal of a bad recovery. Without Confirm it is a dry run: it
// probes the DLQ (existence) and reports what would be restored. With Confirm
// it republishes every snapshot to its source queue and audits each restore.
//
// The same hard invariant as recovery holds: a publish into a queue that does
// not exist can be confirmed and silently dropped, so a rollback whose DLQ has
// vanished refuses before publishing anything.
func Rollback(ctx context.Context, b broker.Broker, a *audit.Store, snaps []audit.Snapshot, opts RollbackOptions) (*RollbackResult, error) {
	if len(snaps) == 0 {
		return nil, fmt.Errorf("rollback: no snapshots to restore")
	}
	dlq := snaps[0].SourceQueue
	start := time.Now()
	res := &RollbackResult{PlanID: snaps[0].PlanID, Snapshots: len(snaps), DLQ: dlq}

	// All snapshots of one plan share the DLQ; probe it once. A missing DLQ is
	// reported in the dry run and refuses a confirmed run, with the refusal
	// itself audited.
	if _, err := b.Stats(ctx, dlq); err != nil {
		if errors.Is(err, broker.ErrQueueNotFound) {
			res.MissingDLQ = dlq
			if !opts.Confirm {
				return res, nil
			}
			rollbackPlanAudit(a, &snaps[0], "refused", opts, fmt.Sprintf("DLQ %q does not exist", dlq))
			return res, fmt.Errorf("%w: queue %q", ErrDestinationMissing, dlq)
		}
		return nil, fmt.Errorf("rollback: check DLQ %q: %w", dlq, err)
	}
	if !opts.Confirm {
		return res, nil
	}

	for i := range snaps {
		s := &snaps[i]
		if err := b.Publish(ctx, s.SourceQueue, snapshotMessage(s)); err != nil {
			rollbackAudit(a, s, "failed", opts, "publish back to the DLQ failed; the message is not restored")
			res.Failed++
			continue
		}
		rollbackAudit(a, s, "success", opts, "")
		res.Restored++
	}
	res.Duration = time.Since(start)
	rollbackPlanAudit(a, &snaps[0], "completed", opts, "")
	return res, nil
}

// snapshotMessage rebuilds the broker-agnostic message a snapshot captured, so
// rollback republishes exactly what was replayed. The replay destination is
// carried on the restored entry (x-destination header on RabbitMQ, the
// destination field on Redis) so a future plan can replay it again; the
// restored entry itself targets the DLQ.
func snapshotMessage(s *audit.Snapshot) *message.Message {
	headers := s.Headers
	if headers == nil {
		headers = map[string]string{}
	}
	if s.Destination != "" {
		headers["x-destination"] = s.Destination
	}
	return &message.Message{
		ID:          s.MessageID,
		Queue:       s.SourceQueue,
		Destination: s.Destination,
		Payload:     s.Payload,
		Headers:     headers,
		ContentType: s.ContentType,
		Timestamp:   s.Timestamp,
	}
}

func rollbackAudit(a *audit.Store, s *audit.Snapshot, result string, opts RollbackOptions, detail string) {
	if a == nil {
		return
	}
	reason := opts.Reason
	if detail != "" {
		reason = strings.TrimSpace(reason + " — " + detail)
	}
	_ = a.Append(audit.Entry{
		Timestamp:   time.Now().UTC(),
		Action:      audit.ActionRollback,
		PlanID:      s.PlanID,
		MessageID:   s.MessageID,
		SourceQueue: s.SourceQueue,
		Destination: s.SourceQueue,
		Confirmed:   true,
		Result:      result,
		Broker:      opts.BrokerName,
		Profile:     opts.Profile,
		Reason:      reason,
	})
}

func rollbackPlanAudit(a *audit.Store, s *audit.Snapshot, result string, opts RollbackOptions, reason string) {
	if a == nil {
		return
	}
	reason = strings.TrimSpace(opts.Reason + " — " + reason)
	_ = a.Append(audit.Entry{
		Timestamp:   time.Now().UTC(),
		Action:      audit.ActionRollback,
		PlanID:      s.PlanID,
		SourceQueue: s.SourceQueue,
		Destination: s.SourceQueue,
		Confirmed:   true,
		Result:      result,
		Broker:      opts.BrokerName,
		Profile:     opts.Profile,
		Reason:      reason,
	})
}
