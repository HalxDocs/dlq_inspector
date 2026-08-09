package command

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/recovery"
)

func newRecoverCmd(opts *GlobalOptions) *cobra.Command {
	var (
		planPath         string
		dryRun           bool
		confirm          bool
		resume           bool
		batchSize        int
		rateLimit        string
		concurrency      int
		retry            int
		failureThreshold float64
		reason           string
	)

	cmd := &cobra.Command{
		Use:   "recover --plan <file>",
		Short: "Validate and execute a recovery plan",
		Long: `Validate a recovery plan against the live queue — and execute it once
confirmed.

Without --confirm this is a dry run: it re-checks every planned message
against the queue (existence, payload schema, duplicate evidence) and
reports what would happen. Changes made: NONE. Zero mutating I/O.

With --confirm the plan executes in batches under the rate limit and
concurrency cap. If a batch's failure rate crosses the circuit-breaker
threshold (default 20%%), the run pauses and --resume is required to
continue. Every message outcome is written to the audit trail.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := commandContext(cmd.Context())
			defer cancel()

			p, err := loadPlanFile(planPath)
			if err != nil {
				return err
			}

			b, profile, err := openBroker(ctx, opts)
			if err != nil {
				return err
			}
			defer b.Close()

			store, err := openAuditStore(opts)
			if err != nil {
				return err
			}
			defer store.Close()

			if confirm {
				res, err := (recovery.Executor{Broker: b, Audit: store}).Execute(ctx, p, recovery.ExecutorOptions{
					Confirm:          true,
					Resume:           resume,
					BatchSize:        batchSize,
					Concurrency:      concurrency,
					RateLimit:        rateLimit,
					RetryPerMessage:  retry,
					FailureThreshold: failureThreshold,
					BrokerName:       profile.Broker,
					Profile:          effectiveProfileName(opts),
					Reason:           reason,
				})
				if err != nil {
					return err
				}
				if opts.Output == "json" {
					return writeJSON(cmd, res)
				}
				return renderRecoverSummary(cmd, planPath, p, res)
			}

			res, err := (recovery.PlanValidator{Broker: b, Audit: store}).Validate(ctx, p)
			if err != nil {
				return err
			}

			_ = store.Append(audit.Entry{
				Timestamp:   time.Now().UTC(),
				Action:      audit.ActionRecover,
				PlanID:      p.ID,
				SourceQueue: p.Queue,
				Destination: p.Destination,
				DryRun:      true,
				Result:      "dry_run",
				Broker:      profile.Broker,
				Profile:     effectiveProfileName(opts),
				Reason:      reason,
			})

			if opts.Output == "json" {
				return writeJSON(cmd, res)
			}
			return renderRecoverReport(cmd, planPath, p, res)
		},
	}

	cmd.Flags().StringVar(&planPath, "plan", "", "path to the recovery plan JSON (required)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate the plan without executing it (the default)")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "execute the plan")
	cmd.Flags().BoolVar(&resume, "resume", false, "continue a tripped run, skipping already-replayed messages")
	cmd.Flags().IntVar(&batchSize, "batch-size", 0, "messages per batch (defaults to the plan's limit)")
	cmd.Flags().StringVar(&rateLimit, "rate-limit", "", "execution rate limit, e.g. 10/s (defaults to the plan's limit)")
	cmd.Flags().IntVar(&concurrency, "concurrency", 0, "parallel workers (defaults to the plan's limit)")
	cmd.Flags().IntVar(&retry, "retry", 1, "extra publish attempts per message before it counts as failed")
	cmd.Flags().Float64Var(&failureThreshold, "failure-threshold", 0, "circuit-breaker failure rate 0.0-1.0 (default 0.20)")
	cmd.Flags().StringVar(&reason, "reason", "", "operator-provided reason, recorded in the audit trail")
	_ = cmd.MarkFlagRequired("plan")
	return cmd
}

// renderRecoverReport prints the dry-run report: what validation found and
// the explicit "Changes made: NONE" line.
func renderRecoverReport(cmd *cobra.Command, planPath string, p *recovery.RecoveryPlan, res *recovery.ValidationResult) error {
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Recovery plan:\t%s\n", planPath)
	fmt.Fprintf(tw, "Plan ID:\t%s\n", p.ID)
	fmt.Fprintf(tw, "Selected:\t%d messages\n", res.Selected)
	fmt.Fprintf(tw, "Destination:\t%s\n", p.Destination)
	fmt.Fprintf(tw, "Payload validation:\t%d/%d passed\n", res.Validated, res.Selected)
	if res.Duplicates > 0 {
		fmt.Fprintf(tw, "Duplicate warning:\t%d\n", res.Duplicates)
	}
	fmt.Fprintf(tw, "Messages that will be skipped:\t%d\n", res.Skipped)
	fmt.Fprintf(tw, "Messages to replay:\t%d\n", res.ToReplay)
	if len(res.ChecksRun) > 0 {
		fmt.Fprintf(tw, "Safety checks run:\t%s\n", joinComma(res.ChecksRun))
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	if len(res.SkippedMessages) > 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "\nSkipped messages:")
		byReason := map[recovery.SkipReason][]string{}
		for _, s := range res.SkippedMessages {
			byReason[s.Reason] = append(byReason[s.Reason], s.MessageID)
		}
		for _, reason := range []recovery.SkipReason{recovery.SkipNotFound, recovery.SkipInvalid, recovery.SkipDuplicate} {
			ids := byReason[reason]
			if len(ids) == 0 {
				continue
			}
			fmt.Fprintf(cmd.OutOrStdout(), "  %s (%d): %s\n", reason, len(ids), joinComma(ids))
		}
	}

	fmt.Fprintln(cmd.OutOrStdout(), "\nChanges made: NONE")
	fmt.Fprintln(cmd.OutOrStdout(), "Dry run — validation only, no message was published or acked.")
	return nil
}

// renderRecoverSummary prints the confirmed-execution report.
func renderRecoverSummary(cmd *cobra.Command, planPath string, p *recovery.RecoveryPlan, res *recovery.ExecutionResult) error {
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Recovery plan:\t%s\n", planPath)
	fmt.Fprintf(tw, "Plan ID:\t%s\n", p.ID)
	fmt.Fprintf(tw, "Selected:\t%d\n", res.Selected)
	fmt.Fprintf(tw, "Replayed:\t%d\n", res.Replayed)
	fmt.Fprintf(tw, "Skipped:\t%d\n", res.Skipped)
	fmt.Fprintf(tw, "Failed during replay:\t%d\n", res.Failed)
	fmt.Fprintf(tw, "New DLQ entries:\t%d\n", res.NewDLQEntries)
	fmt.Fprintf(tw, "Duration:\t%s\n", res.Duration.Round(time.Millisecond))
	if err := tw.Flush(); err != nil {
		return err
	}

	if res.Tripped {
		fmt.Fprintf(cmd.OutOrStdout(), "\nCircuit breaker tripped (failure rate %.0f%% exceeded %.0f%%) — paused.\n",
			res.TrippedFailureRate*100, recovery.DefaultFailureThreshold*100)
		fmt.Fprintf(cmd.OutOrStdout(), "Remaining: %d messages (%s)\n", len(res.Remaining), joinComma(res.Remaining))
		fmt.Fprintf(cmd.OutOrStdout(), "Re-run with --confirm --resume to continue from where it stopped.\n")
	}
	return nil
}

// joinComma joins strings with ", ".
func joinComma(items []string) string {
	out := ""
	for i, it := range items {
		if i > 0 {
			out += ", "
		}
		out += it
	}
	return out
}
