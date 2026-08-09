package command

import (
	"errors"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/recovery"
)

// errConfirmLater is returned when --confirm is passed before the executor
// lands (Phase 6). The dry-run path is complete now; execution is not.
var errConfirmLater = errors.New("confirmed execution lands in the next phase — for now dlq recover only validates (drop --confirm)")

func newRecoverCmd(opts *GlobalOptions) *cobra.Command {
	var (
		planPath string
		dryRun   bool
		confirm  bool
		reason   string
	)

	cmd := &cobra.Command{
		Use:   "recover --plan <file>",
		Short: "Validate and execute a recovery plan",
		Long: `Validate a recovery plan against the live queue — and, once confirmed
execution exists, run it.

Without --confirm this is a dry run: it re-checks every planned message
against the queue (existence, payload schema, duplicate evidence) and
reports what would happen. Changes made: NONE. Zero mutating I/O.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := commandContext(cmd.Context())
			defer cancel()

			p, err := loadPlanFile(planPath)
			if err != nil {
				return err
			}
			if confirm {
				return errConfirmLater
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
	cmd.Flags().BoolVar(&confirm, "confirm", false, "execute the plan (arrives with the executor)")
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
