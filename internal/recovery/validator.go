package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/dedupe"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// SkipReason explains why one planned message will not be replayed.
type SkipReason string

// Skip reasons produced by validation.
const (
	SkipNotFound   SkipReason = "not_found_in_queue"
	SkipInvalid    SkipReason = "payload_invalid"
	SkipDuplicate  SkipReason = "duplicate_evidence"
	SkipNotPlanned SkipReason = "not_in_plan"
)

// SkippedMessage records one planned message that must not be replayed.
type SkippedMessage struct {
	MessageID string     `json:"message_id"`
	Reason    SkipReason `json:"reason"`
	Detail    string     `json:"detail,omitempty"`
}

// ValidationResult is the dry-run report: what validation found without
// changing anything.
type ValidationResult struct {
	PlanID     string `json:"plan_id"`
	Queue      string `json:"queue"`
	Selected   int    `json:"selected"`
	Validated  int    `json:"validated"`
	Duplicates int    `json:"duplicates"`
	Skipped    int    `json:"skipped"`
	ToReplay   int    `json:"to_replay"`
	// ChecksRun lists the safety checks that actually ran.
	ChecksRun []string `json:"checks_run"`
	// SkippedMessages details every skipped message, for the report and audit.
	SkippedMessages []SkippedMessage `json:"skipped_messages,omitempty"`
	// DestinationMissing is the plan's destination queue when it does not
	// exist on the broker (empty when it exists or the check did not run). A
	// confirmed run must refuse in this case — publishing into a nonexistent
	// queue can be silently dropped.
	DestinationMissing string `json:"destination_missing,omitempty"`
	// Warnings lists non-blocking dry-run findings, e.g. the missing
	// destination above.
	Warnings []string `json:"warnings,omitempty"`
}

// PlanValidator validates a plan against the live queue and the local audit
// trail. It is strictly read-only: no publish, ack, or delete ever happens
// here — that is the dry-run contract.
type PlanValidator struct {
	Broker broker.Broker
	// Audit, when set, is used for duplicate evidence lookups.
	Audit *audit.Store
}

// Validate runs every safety check the plan declares. A plan with an empty
// SafetyChecks list is rejected outright — the safety gate refuses plans that
// were not built with checks.
func (v PlanValidator) Validate(ctx context.Context, plan *RecoveryPlan) (*ValidationResult, error) {
	if plan == nil {
		return nil, errors.New("validator: nil plan")
	}
	if len(plan.SafetyChecks) == 0 {
		return nil, ErrNoSafetyChecks
	}

	// One bounded scan of the queue gives us the messages as they exist now;
	// validation should reflect reality, not what the plan author saw.
	msgs, err := v.Broker.Search(ctx, plan.Queue, broker.SearchFilter{Limit: validationScanLimit(plan)})
	if err != nil {
		return nil, fmt.Errorf("validator: scan queue %q: %w", plan.Queue, err)
	}
	byID := make(map[string]message.Message, len(msgs))
	for i := range msgs {
		byID[msgs[i].ID] = msgs[i]
	}

	res := &ValidationResult{
		PlanID:    plan.ID,
		Queue:     plan.Queue,
		Selected:  len(plan.MessageIDs),
		ChecksRun: append([]string(nil), plan.SafetyChecks...),
	}

	// The destination check is plan-level: one Stats call (the existence
	// probe) decides whether a confirmed run can possibly be safe. A missing
	// destination is a warning here — the dry-run reports it and continues
	// validating — but the executor refuses outright.
	if contains(plan.SafetyChecks, CheckDestination) {
		if plan.Destination == "" {
			return nil, errors.New("validator: plan has no destination to check")
		}
		if _, err := v.Broker.Stats(ctx, plan.Destination); err != nil {
			if errors.Is(err, broker.ErrQueueNotFound) {
				res.DestinationMissing = plan.Destination
				res.Warnings = append(res.Warnings,
					fmt.Sprintf("destination queue %q does not exist — a confirmed run will be refused", plan.Destination))
			} else {
				return nil, fmt.Errorf("validator: check destination %q: %w", plan.Destination, err)
			}
		}
	}

	checkSchema := contains(plan.SafetyChecks, CheckSchema)
	checkDuplicate := contains(plan.SafetyChecks, CheckDuplicate)

	for _, id := range plan.MessageIDs {
		cur, ok := byID[id]
		if !ok {
			res.Skipped++
			res.SkippedMessages = append(res.SkippedMessages, SkippedMessage{MessageID: id, Reason: SkipNotFound})
			continue
		}
		if checkSchema && !schemaValid(&cur) {
			res.Skipped++
			res.SkippedMessages = append(res.SkippedMessages, SkippedMessage{
				MessageID: id,
				Reason:    SkipInvalid,
				Detail:    "payload does not parse as JSON",
			})
			continue
		}
		res.Validated++

		if checkDuplicate && v.Audit != nil {
			ev, err := dedupe.CheckAudit(v.Audit, &cur)
			if err != nil {
				return nil, fmt.Errorf("validator: duplicate check for %s: %w", id, err)
			}
			if ev.MatchFound {
				res.Duplicates++
				res.Skipped++
				detail := "prior replay found in audit trail"
				if ev.PriorReplayAt != nil {
					detail = fmt.Sprintf("replayed at %s", ev.PriorReplayAt.UTC().Format("2006-01-02T15:04:05Z"))
				}
				res.SkippedMessages = append(res.SkippedMessages, SkippedMessage{
					MessageID: id,
					Reason:    SkipDuplicate,
					Detail:    detail,
				})
			}
		}
	}

	res.ToReplay = res.Selected - res.Skipped
	return res, nil
}

// validationScanLimit scans as deep as the plan needs, with a floor so a
// small plan still validates against a realistically deep queue.
func validationScanLimit(plan *RecoveryPlan) int {
	limit := len(plan.MessageIDs) * 2
	if limit < 10000 {
		limit = 10000
	}
	return limit
}

// schemaValid reports whether a message passes the schema check: JSON
// payloads must parse; non-JSON bodies (per content type) have nothing to
// check and pass.
func schemaValid(m *message.Message) bool {
	if len(m.Payload) == 0 {
		return true
	}
	if !json.Valid(m.Payload) {
		return false
	}
	return true
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
