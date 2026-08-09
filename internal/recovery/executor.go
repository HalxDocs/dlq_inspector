package recovery

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// DefaultFailureThreshold is the failure rate inside a batch that trips the
// circuit breaker. Crossing it pauses the run; continuation requires an
// explicit re-confirmation (--resume).
const DefaultFailureThreshold = 0.20

// ErrConfirmRequired is returned when the executor is asked to run without
// an explicit confirm. Execution never happens without it.
var ErrConfirmRequired = errors.New("confirmation required: re-run with --confirm")

// ErrResumeRequired is returned by a continuation attempt that did not pass
// --resume after a trip.
var ErrResumeRequired = errors.New("circuit breaker tripped — re-run with --resume to continue")

// ErrDestinationMissing is returned when the plan's destination queue does
// not exist. Publishing into a nonexistent queue can be confirmed and
// silently dropped (RabbitMQ drops unroutable publishes), after which acking
// the DLQ copy would lose the message — so the run is refused before
// anything is published or acked.
var ErrDestinationMissing = errors.New("destination queue does not exist — refusing to run")

// Executor runs a confirmed recovery plan: republish each selected message to
// its destination and ack the DLQ copy only after a successful publish. It
// enforces the safety gate (no execution without Confirm), the plan's limits
// (batch size, rate limit, concurrency), per-message retry, and the circuit
// breaker — a bad plan must not re-flood the DLQ before an operator notices.
type Executor struct {
	Broker broker.Broker
	Audit  *audit.Store
}

// ExecutorOptions control one execution run. Zero values fall back to the
// plan's own limits.
type ExecutorOptions struct {
	// Confirm must be true — the executor refuses to run otherwise.
	Confirm bool
	// Resume continues a previously tripped run, skipping messages that
	// already have a successful replay in the audit trail.
	Resume bool
	// BatchSize overrides the plan's batch size.
	BatchSize int
	// Concurrency overrides the plan's concurrency.
	Concurrency int
	// RateLimit overrides the plan's rate limit (e.g. "10/s", "100/m", "0").
	RateLimit string
	// RetryPerMessage is how many extra publish attempts each message gets
	// before it counts as failed.
	RetryPerMessage int
	// FailureThreshold overrides the circuit-breaker threshold (0.0-1.0).
	FailureThreshold float64
	// BrokerName and Profile are recorded on audit entries.
	BrokerName string
	Profile    string
	// Reason is the operator-provided justification, recorded on audit entries.
	Reason string
} // ExecutionResult summarizes one confirmed run.
type ExecutionResult struct {
	PlanID   string `json:"plan_id"`
	Selected int    `json:"selected"`
	Replayed int    `json:"replayed"`
	Skipped  int    `json:"skipped"`
	// Excluded counts messages the plan deliberately left in the DLQ
	// (DO_NOT_REPLAY); each gets a skipped audit entry with the reason.
	Excluded           int           `json:"excluded"`
	Failed             int           `json:"failed"`
	NewDLQEntries      int           `json:"new_dlq_entries"`
	Tripped            bool          `json:"tripped"`
	TrippedFailureRate float64       `json:"tripped_failure_rate,omitempty"`
	Remaining          []string      `json:"remaining,omitempty"`
	Duration           time.Duration `json:"duration"`
}

// Execute validates the plan against the current queue state, then replays
// every message that passed validation, in batches under the configured rate
// and concurrency limits. It never acks without a successful publish.
func (e Executor) Execute(ctx context.Context, plan *RecoveryPlan, opts ExecutorOptions) (*ExecutionResult, error) {
	if !opts.Confirm {
		return nil, ErrConfirmRequired
	}
	if len(plan.SafetyChecks) == 0 {
		return nil, ErrNoSafetyChecks
	}

	// Hard safety invariant, independent of the plan's declared checks: never
	// publish into a destination that does not exist. A publish to a
	// nonexistent queue can be confirmed and silently dropped — acking the
	// DLQ copy afterwards would lose the message. Refuse before any publish,
	// ack, or per-message audit write; the refusal itself is recorded.
	if plan.Destination != "" {
		if _, err := e.Broker.Stats(ctx, plan.Destination); err != nil {
			if errors.Is(err, broker.ErrQueueNotFound) {
				e.auditPlan(plan, "refused", opts)
				return nil, fmt.Errorf("%w: queue %q", ErrDestinationMissing, plan.Destination)
			}
			return nil, fmt.Errorf("executor: check destination %q: %w", plan.Destination, err)
		}
	}
	start := time.Now()

	if opts.BatchSize <= 0 {
		opts.BatchSize = plan.Limits.BatchSize
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = plan.Limits.Concurrency
	}
	if opts.RateLimit == "" {
		opts.RateLimit = plan.Limits.RateLimit
	}
	if opts.FailureThreshold <= 0 || opts.FailureThreshold > 1 {
		opts.FailureThreshold = DefaultFailureThreshold
	}
	limit, err := ParseRateLimit(opts.RateLimit)
	if err != nil {
		return nil, err
	}
	// Burst per batch so a batch can start promptly; the steady-state rate
	// still holds across batches and workers share the same limiter.
	limiter := rate.NewLimiter(limit, opts.BatchSize)

	// Re-validate now: the queue may have changed since the plan or a
	// previous dry-run. Messages that fail validation are skipped and audited.
	vres, err := (PlanValidator{Broker: e.Broker, Audit: e.Audit}).Validate(ctx, plan)
	if err != nil {
		return nil, err
	}
	skipped := map[string]SkippedMessage{}
	for _, s := range vres.SkippedMessages {
		skipped[s.MessageID] = s
	}

	// Attempt list: planned messages that passed validation.
	attempt := make([]string, 0, len(plan.MessageIDs))
	for _, id := range plan.MessageIDs {
		if _, ok := skipped[id]; ok {
			continue
		}
		attempt = append(attempt, id)
	}

	res := &ExecutionResult{
		PlanID:   plan.ID,
		Selected: len(plan.MessageIDs),
	}

	// On resume, messages that already completed (successful replay in the
	// audit) are not attempted again; they are already done.
	if opts.Resume {
		attempt = e.filterCompleted(ctx, plan, attempt, res)
	}

	// Audit the validator's skips so the trail shows what was not replayed
	// and why. (Resumed runs re-skip the same ones only if validation
	// re-discovered them; already-completed messages already have entries.)
	for _, s := range vres.SkippedMessages {
		res.Skipped++
		e.audit(plan, s.MessageID, "skipped", opts, detailFor(s))
	}

	// Audit the plan's exclusions: messages deliberately left in the DLQ
	// (DO_NOT_REPLAY). They never reached validation, but the trail must
	// show that the recovery chose not to touch them, and why.
	for _, x := range plan.Excluded {
		res.Excluded++
		e.audit(plan, x.MessageID, "skipped", opts, x.Reason)
	}

	// Execute in batches; the circuit breaker checks each batch.
	remaining := append([]string(nil), attempt...)
	for len(remaining) > 0 {
		n := opts.BatchSize
		if len(remaining) < n {
			n = len(remaining)
		}
		batch := remaining[:n]
		remaining = remaining[n:]

		batchFailures, ackFailures, replayed := e.runBatch(ctx, plan, batch, limiter, opts)
		res.Replayed += replayed
		res.Failed += batchFailures
		res.NewDLQEntries += ackFailures

		rate := float64(batchFailures) / float64(len(batch))
		if rate > opts.FailureThreshold {
			res.Tripped = true
			res.TrippedFailureRate = rate
			res.Remaining = append([]string(nil), remaining...)
			e.auditPlan(plan, "tripped", opts)
			return res, nil
		}
	}

	res.Duration = time.Since(start)
	e.auditPlan(plan, "completed", opts)
	return res, nil
}

// outcome is one message's execution result within a batch.
type outcome struct {
	state string // "replayed" | "publish_failed" | "ack_failed"
}

// runBatch processes one batch, honoring the rate limiter and concurrency
// cap. Returns (publishFailures, ackFailures, replayed).
func (e Executor) runBatch(ctx context.Context, plan *RecoveryPlan, ids []string, limiter *rate.Limiter, opts ExecutorOptions) (int, int, int) {
	results := make([]outcome, len(ids))

	workers := opts.Concurrency
	if workers <= 0 {
		workers = 1
	}
	if workers > len(ids) {
		workers = len(ids)
	}

	work := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				// Rate limit: one token per message attempt, shared by all
				// workers, so the configured rate holds regardless of
				// concurrency.
				if err := limiter.Wait(ctx); err != nil {
					results[i] = outcome{state: "publish_failed"}
					continue
				}
				results[i] = e.process(ctx, plan, ids[i], opts)
			}
		}()
	}
	for i := range ids {
		work <- i
	}
	close(work)
	wg.Wait()

	publishFailures, ackFailures, replayed := 0, 0, 0
	for _, r := range results {
		switch r.state {
		case "replayed":
			replayed++
		case "publish_failed":
			publishFailures++
		case "ack_failed":
			publishFailures++ // still a failure for the circuit breaker
			ackFailures++
		}
	}
	return publishFailures, ackFailures, replayed
}

// process replays one message: publish (with per-message retry), then ack
// only after a successful publish. Every outcome is audited.
func (e Executor) process(ctx context.Context, plan *RecoveryPlan, id string, opts ExecutorOptions) outcome {
	// Inspect the message fresh so we publish what is actually in the queue.
	m, err := e.Broker.Inspect(ctx, plan.Queue, id)
	if err != nil {
		e.audit(plan, id, "publish_failed", opts, "message vanished before replay")
		return outcome{state: "publish_failed"}
	}

	published := false
	for attempt := 0; attempt <= opts.RetryPerMessage; attempt++ {
		if err := e.Broker.Publish(ctx, plan.Destination, m); err == nil {
			published = true
			break
		}
	}
	if !published {
		e.audit(plan, id, "publish_failed", opts, "publish failed after retries; DLQ copy left in place")
		return outcome{state: "publish_failed"}
	}

	if err := e.Broker.Ack(ctx, plan.Queue, id); err != nil {
		// Published but not acked: at-least-once means a duplicate is
		// possible, but nothing was lost — and the DLQ copy will likely
		// re-enter as a new DLQ entry.
		e.audit(plan, id, "ack_failed", opts, "published but ack failed; message may be duplicated")
		return outcome{state: "ack_failed"}
	}

	// Snapshot the message exactly as it was replayed, so a bad recovery can
	// be rolled back: rollback republishes the snapshot to the DLQ. Only
	// successful replays are snapshotted — a message left in the DLQ needs no
	// snapshot to be recovered again.
	e.snapshot(plan, m, opts)

	e.audit(plan, id, "success", opts, "")
	return outcome{state: "replayed"}
}

// snapshot records the replayed message in the audit store. Best-effort like
// the audit writes: the replay has already happened at this point, so a local
// storage failure must not misreport the recovery outcome.
func (e Executor) snapshot(plan *RecoveryPlan, m *message.Message, opts ExecutorOptions) {
	if e.Audit == nil {
		return
	}
	_ = e.Audit.SaveSnapshot(audit.Snapshot{
		PlanID:      plan.ID,
		MessageID:   m.ID,
		SourceQueue: plan.Queue,
		Destination: plan.Destination,
		Payload:     m.Payload,
		ContentType: m.ContentType,
		Headers:     m.Headers,
		Timestamp:   time.Now().UTC(),
	})
}

// filterCompleted drops messages that already have a successful replay in the
// audit trail (a resumed run continuing after a trip). They count as skipped,
// not replayed again. The audit is queried directly by message ID — no extra
// broker round-trips.
func (e Executor) filterCompleted(ctx context.Context, plan *RecoveryPlan, attempt []string, res *ExecutionResult) []string {
	if e.Audit == nil {
		return attempt
	}
	out := attempt[:0]
	for _, id := range attempt {
		entries, err := e.Audit.Replayed(id)
		if err == nil && len(entries) > 0 {
			res.Skipped++
			continue
		}
		out = append(out, id)
	}
	return out
}

func (e Executor) audit(plan *RecoveryPlan, messageID, result string, opts ExecutorOptions, detail string) {
	if e.Audit == nil {
		return
	}
	reason := opts.Reason
	if detail != "" {
		reason = strings.TrimSpace(reason + " — " + detail)
	}
	_ = e.Audit.Append(audit.Entry{
		Timestamp:   time.Now().UTC(),
		Action:      audit.ActionRecover,
		PlanID:      plan.ID,
		MessageID:   messageID,
		SourceQueue: plan.Queue,
		Destination: plan.Destination,
		Confirmed:   true,
		Result:      result,
		Broker:      opts.BrokerName,
		Profile:     opts.Profile,
		Reason:      reason,
	})
}

func (e Executor) auditPlan(plan *RecoveryPlan, result string, opts ExecutorOptions) {
	if e.Audit == nil {
		return
	}
	_ = e.Audit.Append(audit.Entry{
		Timestamp:   time.Now().UTC(),
		Action:      audit.ActionRecover,
		PlanID:      plan.ID,
		SourceQueue: plan.Queue,
		Destination: plan.Destination,
		Confirmed:   true,
		Result:      result,
		Broker:      opts.BrokerName,
		Profile:     opts.Profile,
		Reason:      opts.Reason,
	})
}

// ParseRateLimit parses "10/s", "100/m", "0" (unlimited), or a bare number
// (per second) into a rate.Limit.
func ParseRateLimit(s string) (rate.Limit, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	switch s {
	case "", "0", "unlimited":
		return rate.Inf, nil
	}
	per := time.Second
	switch {
	case strings.HasSuffix(s, "/s"):
		s = strings.TrimSuffix(s, "/s")
	case strings.HasSuffix(s, "/m"):
		s = strings.TrimSuffix(s, "/m")
		per = time.Minute
	default:
		// bare number = per second
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid rate limit %q (want e.g. 10/s, 100/m, or 0 for unlimited)", s)
	}
	return rate.Limit(n / per.Seconds()), nil
}

func detailFor(s SkippedMessage) string {
	if s.Detail != "" {
		return s.Detail
	}
	return string(s.Reason)
}
