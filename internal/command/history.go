package command

import (
	"encoding/json"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

func newHistoryCmd(opts *GlobalOptions) *cobra.Command {
	var limit int

	cmd := &cobra.Command{
		Use:   "history",
		Short: "List recent audit entries",
		Long: `List the most recent audit entries: what the tool did, when, and with what
result. The audit trail is the source of truth for what was replayed, patched,
or recovered.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := openAuditStore(opts)
			if err != nil {
				return err
			}
			defer store.Close()

			entries, err := store.Recent(limit)
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
				fmt.Fprintln(cmd.OutOrStdout(), "No audit entries yet.")
				return nil
			}

			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "TIMESTAMP\tACTION\tMESSAGE\tQUEUE\tDESTINATION\tCONFIRMED\tRESULT")
			for _, e := range entries {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%t\t%s\n",
					e.Timestamp.Format(time.RFC3339),
					e.Action,
					e.MessageID,
					e.SourceQueue,
					e.Destination,
					e.Confirmed,
					e.Result)
			}
			return tw.Flush()
		},
	}

	cmd.Flags().IntVar(&limit, "limit", 20, "maximum number of entries to list")
	return cmd
}
