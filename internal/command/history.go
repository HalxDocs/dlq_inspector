package command

import (
	"encoding/json"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
)

func newHistoryCmd(opts *GlobalOptions) *cobra.Command {
	var (
		limit  int
		planID string
	)

	cmd := &cobra.Command{
		Use:   "history",
		Short: "List recent audit entries or a recovery plan's full trail",
		Long: `List the most recent audit entries: what the tool did, when, and with what
result. The audit trail is the source of truth for what was replayed, patched,
or recovered.

With --plan <id>, shows the full trail of one recovery: every per-message
outcome in execution order plus the plan-level summary.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := openAuditStore(opts)
			if err != nil {
				return err
			}
			defer store.Close()

			var entries []audit.Entry
			if planID != "" {
				entries, err = store.ByPlan(planID)
			} else {
				entries, err = store.Recent(limit)
			}
			if err != nil {
				return err
			}

			switch opts.Output {
			case "json":
				return writeJSON(cmd, entries)
			case "jsonl":
				enc := json.NewEncoder(cmd.OutOrStdout())
				for _, e := range entries {
					if err := enc.Encode(e); err != nil {
						return err
					}
				}
				return nil
			}

			if len(entries) == 0 {
				if planID != "" {
					fmt.Fprintf(cmd.OutOrStdout(), "No audit entries for plan %s.\n", planID)
				} else {
					fmt.Fprintln(cmd.OutOrStdout(), "No audit entries yet.")
				}
				return nil
			}

			if planID != "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Plan %s — full recovery trail\n", planID)
			}

			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "TIMESTAMP\tACTION\tMESSAGE\tQUEUE\tDESTINATION\tCONFIRMED\tRESULT\tREASON")
			for _, e := range entries {
				msg := e.MessageID
				if msg == "" {
					msg = "(plan)"
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%t\t%s\t%s\n",
					e.Timestamp.Format(time.RFC3339),
					e.Action,
					msg,
					e.SourceQueue,
					e.Destination,
					e.Confirmed,
					e.Result,
					e.Reason)
			}
			return tw.Flush()
		},
	}

	cmd.Flags().IntVar(&limit, "limit", 20, "maximum number of entries to list")
	cmd.Flags().StringVar(&planID, "plan", "", "show the full trail for this plan ID")
	return cmd
}
