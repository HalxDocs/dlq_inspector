package command

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/recovery"
)

// analyzeResult is the structured JSON payload of dlq analyze --output json.
type analyzeResult struct {
	Queue       string                  `json:"queue"`
	Total       int                     `json:"total"`
	GeneratedAt time.Time               `json:"generated_at"`
	Groups      []recovery.FailureGroup `json:"groups"`
}

func newAnalyzeCmd(opts *GlobalOptions) *cobra.Command {
	var limit int

	cmd := &cobra.Command{
		Use:   "analyze [queue]",
		Short: "Group failed messages by failure pattern and classify each group",
		Long: `Analyze a dead-letter queue and turn a pile of messages into grouped
failure patterns: each group shares a normalized error signature, destination,
event type, and retry range, and carries a recovery recommendation
(REPLAYABLE / REQUIRES_FIX / DO_NOT_REPLAY / INVESTIGATE).

The queue defaults to the profile's default_queue. Analysis is read-only —
no message is published, acked, or moved.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := commandContext(cmd.Context())
			defer cancel()

			b, profile, err := openBroker(ctx, opts)
			if err != nil {
				return err
			}
			defer b.Close()

			queue, err := resolveQueue(args, profile.DefaultQueue)
			if err != nil {
				return err
			}

			msgs, err := b.Search(ctx, queue, broker.SearchFilter{Limit: limit})
			if err != nil {
				return err
			}
			if len(msgs) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No messages to analyze.")
				return nil
			}

			groups := (recovery.Analyzer{}).Analyze(msgs)

			if opts.Output == "json" {
				return writeJSON(cmd, analyzeResult{
					Queue:       queue,
					Total:       len(msgs),
					GeneratedAt: time.Now().UTC(),
					Groups:      groups,
				})
			}
			return renderAnalyze(cmd, queue, len(msgs), groups)
		},
	}

	cmd.Flags().IntVar(&limit, "limit", 1000, "maximum number of messages to analyze")
	return cmd
}

// renderAnalyze prints the summary line and one block per failure group.
func renderAnalyze(cmd *cobra.Command, queue string, total int, groups []recovery.FailureGroup) error {
	fmt.Fprintf(cmd.OutOrStdout(), "%d messages analyzed in %s\n", total, queue)
	for i, g := range groups {
		fmt.Fprintf(cmd.OutOrStdout(), "\nGROUP %d -- %s [%s]\n", i+1, g.Label, g.ID)
		fmt.Fprintf(cmd.OutOrStdout(), "%d messages - %.1f%%\n", g.Count, g.Percentage)
		fmt.Fprintf(cmd.OutOrStdout(), "Recommendation: %s (confidence %.2f)\n", g.Recommendation, g.Confidence)

		tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		fmt.Fprintf(tw, "Signature:\t%s\n", g.Signature)
		if g.Destination != "" {
			fmt.Fprintf(tw, "Destination:\t%s\n", g.Destination)
		}
		if g.EventType != "" {
			fmt.Fprintf(tw, "Event type:\t%s\n", g.EventType)
		}
		fmt.Fprintf(tw, "Retries:\t%s\n", g.RetryBucket)
		if g.PayloadShape != "" {
			fmt.Fprintf(tw, "Payload shape:\t%s\n", g.PayloadShape)
		}
		if !g.FirstSeen.IsZero() {
			fmt.Fprintf(tw, "Time window:\t%s to %s\n",
				g.FirstSeen.UTC().Format(time.RFC3339), g.LastSeen.UTC().Format(time.RFC3339))
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	return nil
}
