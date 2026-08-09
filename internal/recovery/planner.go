package recovery

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// PlanLimits constrain how a plan executes (Phase 6). They are recorded on
// the plan so the exact intended operation is reviewable before it runs.
type PlanLimits struct {
	// BatchSize is how many messages to process per batch.
	BatchSize int `json:"batch_size"`
	// RateLimit is the maximum execution rate, e.g. "10/s".
	RateLimit string `json:"rate_limit"`
	// Concurrency is how many messages to process in parallel.
	Concurrency int `json:"concurrency"`
}

// DefaultLimits are the execution limits proposed when the operator does not
// override them.
func DefaultLimits() PlanLimits {
	return PlanLimits{BatchSize: 25, RateLimit: "10/s", Concurrency: 1}
}

// RecoveryPlan is the exportable, reviewable description of one recovery
// operation: which messages, where to, under what limits, and which safety
// checks must pass before anything runs. A plan is JSON and is meant to be
// reviewed, diffed, or stored before execution.
type RecoveryPlan struct {
	// ID uniquely identifies the plan (audit entries reference it).
	ID string `json:"id"`
	// CreatedAt is when the plan was built.
	CreatedAt time.Time `json:"created_at"`
	// Queue is the DLQ the messages are recovered from.
	Queue string `json:"queue"`
	// GroupID narrows the plan to one failure group, when built with --group.
	GroupID string `json:"group_id,omitempty"`
	// GroupLabel is the failure group's display name, when applicable.
	GroupLabel string `json:"group_label,omitempty"`
	// MessageIDs are the exact messages selected for recovery.
	MessageIDs []string `json:"message_ids"`
	// Destination is where the messages will be replayed to.
	Destination string `json:"destination"`
	// Action is "replay" today; "patch_and_replay" arrives with payload
	// patching (Phase 7).
	Action string `json:"action"`
	// Limits constrain execution.
	Limits PlanLimits `json:"limits"`
	// SafetyChecks lists the checks the validator must run before execution.
	// A plan with an empty SafetyChecks list is rejected.
	SafetyChecks []string `json:"safety_checks"`
}

// PlanChecks are the safety checks a plan declares and the validator performs.
const (
	CheckSchema    = "schema_validated"
	CheckDuplicate = "duplicate_checked"
)

// ErrNoSafetyChecks is returned when a plan declares no safety checks — the
// plan is refused rather than validated.
var ErrNoSafetyChecks = errors.New("plan has no safety checks — refusing to validate")

// PlanOptions control which messages a plan selects.
type PlanOptions struct {
	// Queue is the DLQ to plan from.
	Queue string
	// GroupID, when set, restricts selection to that failure group.
	GroupID string
	// Destination overrides the messages' own replay destination. When empty,
	// the messages' dead-letter metadata is used (all selected messages must
	// agree on it).
	Destination string
	// Limits are the execution limits recorded on the plan.
	Limits PlanLimits
	// IncludeDoNotReplay, when true, keeps DO_NOT_REPLAY messages in the
	// selection. The default is to exclude them — replaying them risks
	// duplicates or harm.
	IncludeDoNotReplay bool
}

// BuildPlan turns a set of failed messages into a RecoveryPlan. Selection
// follows the group filter (when given) and excludes DO_NOT_REPLAY messages
// unless explicitly overridden.
func BuildPlan(msgs []message.Message, opts PlanOptions) (*RecoveryPlan, error) {
	if len(msgs) == 0 {
		return nil, errors.New("no messages to plan")
	}
	if opts.Limits.BatchSize <= 0 {
		opts.Limits.BatchSize = DefaultLimits().BatchSize
	}
	if opts.Limits.RateLimit == "" {
		opts.Limits.RateLimit = DefaultLimits().RateLimit
	}
	if opts.Limits.Concurrency <= 0 {
		opts.Limits.Concurrency = DefaultLimits().Concurrency
	}

	destinations := map[string]bool{}
	var ids []string
	firstSig := ""
	for i := range msgs {
		m := &msgs[i]
		sig := NormalizeSignature(m.FailureReason)
		if opts.GroupID != "" && groupID(sig) != opts.GroupID {
			continue
		}
		if !opts.IncludeDoNotReplay && Classify(m).Classification == DoNotReplay {
			continue
		}
		if firstSig == "" {
			firstSig = sig
		}
		destinations[m.Destination] = true
		ids = append(ids, m.ID)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("no messages selected (group %q found nothing, or all were DO_NOT_REPLAY)", opts.GroupID)
	}

	dest := opts.Destination
	if dest == "" {
		if len(destinations) != 1 {
			return nil, fmt.Errorf("cannot determine a single replay destination (found %d) — pass --destination", len(destinations))
		}
		for d := range destinations {
			dest = d
		}
	}
	if dest == "" {
		return nil, errors.New("cannot determine replay destination (messages have no dead-letter metadata) — pass --destination")
	}

	// Preserve deterministic ordering for reviewability.
	sort.Strings(ids)

	label := ""
	if opts.GroupID != "" && firstSig != "" {
		label = groupLabel(firstSig)
	}

	now := time.Now().UTC()
	return &RecoveryPlan{
		ID:           newPlanID(opts.Queue, opts.GroupID, now),
		CreatedAt:    now,
		Queue:        opts.Queue,
		GroupID:      opts.GroupID,
		GroupLabel:   label,
		MessageIDs:   ids,
		Destination:  dest,
		Action:       "replay",
		Limits:       opts.Limits,
		SafetyChecks: []string{CheckSchema, CheckDuplicate},
	}, nil
}

// newPlanID derives a stable, readable plan identifier.
func newPlanID(queue, group string, at time.Time) string {
	h := sha256.New()
	h.Write([]byte(queue))
	h.Write([]byte{0})
	h.Write([]byte(group))
	h.Write([]byte{0})
	h.Write([]byte(at.Format(time.RFC3339Nano)))
	return "plan_" + hex.EncodeToString(h.Sum(nil))[:10]
}
