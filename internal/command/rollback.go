package command

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/recovery"
)

func newRollbackCmd(opts *GlobalOptions) *cobra.Command {
	var (
		planID  string
		dryRun  bool
		confirm bool
		reason  string
	)

	cmd := &cobra.Command{
		Use:   "rollback --plan <id>",
		Short: "Restore snapshotted messages back to the DLQ",
		Long: `Restore messages that a confirmed recovery replayed back to the DLQ they
came from.

Every confirmed recovery snapshots each replayed message first (payload,
headers, destination). If the replay turned out badly, rollback republishes
those snapshots to the DLQ so the messages can be inspected and re-planned —
with every restore audited against the same plan, tagged with your reason.

Without --confirm this is a dry run: it verifies the DLQ still exists and
reports what would be restored. Changes made: NONE.

Rollback refuses to run if the DLQ no longer exists: publishing into a
nonexistent queue can be confirmed and silently dropped, which would lose
the messages a second time.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := commandContext(cmd.Context())
			defer cancel()

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

			snaps, err := store.Snapshots(planID)
			if err != nil {
				return err
			}
			if len(snaps) == 0 {
				return fmt.Errorf("no snapshots found for plan %s (was it recovered with a confirmed run of this tool?)", planID)
			}

			if confirm {
				res, err := recovery.Rollback(ctx, b, store, snaps, recovery.RollbackOptions{
					Confirm:    true,
					Reason:     reason,
					BrokerName: profile.Broker,
					Profile:    effectiveProfileName(opts),
				})
				if err != nil {
					return err
				}
				if opts.Output == "json" {
					return writeJSON(cmd, res)
				}
				return renderRollbackSummary(cmd, res)
			}

			res, err := recovery.Rollback(ctx, b, store, snaps, recovery.RollbackOptions{})
			if err != nil {
				return err
			}

			_ = store.Append(audit.Entry{
				Timestamp:   time.Now().UTC(),
				Action:      audit.ActionRollback,
				PlanID:      planID,
				SourceQueue: res.DLQ,
				DryRun:      true,
				Result:      "dry_run",
				Broker:      profile.Broker,
				Profile:     effectiveProfileName(opts),
				Reason:      reason,
			})

			if opts.Output == "json" {
				return writeJSON(cmd, res)
			}
			return renderRollbackReport(cmd, res)
		},
	}

	cmd.Flags().StringVar(&planID, "plan", "", "plan ID whose snapshots to restore (required)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "report what would be restored without publishing (the default)")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "restore the snapshotted messages to the DLQ")
	cmd.Flags().StringVar(&reason, "reason", "", "operator-provided reason, recorded in the audit trail")
	_ = cmd.MarkFlagRequired("plan")
	return cmd
}

// renderRollbackReport prints the dry-run report for a rollback.
func renderRollbackReport(cmd *cobra.Command, res *recovery.RollbackResult) error {
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Rollback plan:\t%s\n", res.PlanID)
	fmt.Fprintf(tw, "Snapshots:\t%d\n", res.Snapshots)
	fmt.Fprintf(tw, "DLQ:\t%s\n", res.DLQ)
	if res.MissingDLQ != "" {
		fmt.Fprintf(tw, "Destination warning:\tqueue %q does not exist — a confirmed rollback will be refused\n", res.MissingDLQ)
	}
	fmt.Fprintf(tw, "Messages to restore:\t%d\n", res.Snapshots)
	if err := tw.Flush(); err != nil {
		return err
	}

	fmt.Fprintln(cmd.OutOrStdout(), "\nChanges made: NONE")
	fmt.Fprintln(cmd.OutOrStdout(), "Dry run — validation only, no message was published.")
	return nil
}

// renderRollbackSummary prints the confirmed-execution report for a rollback.
func renderRollbackSummary(cmd *cobra.Command, res *recovery.RollbackResult) error {
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Rollback plan:\t%s\n", res.PlanID)
	fmt.Fprintf(tw, "Snapshots:\t%d\n", res.Snapshots)
	fmt.Fprintf(tw, "DLQ:\t%s\n", res.DLQ)
	fmt.Fprintf(tw, "Restored:\t%d\n", res.Restored)
	fmt.Fprintf(tw, "Failed:\t%d\n", res.Failed)
	fmt.Fprintf(tw, "Duration:\t%s\n", res.Duration.Round(time.Millisecond))
	return tw.Flush()
}
