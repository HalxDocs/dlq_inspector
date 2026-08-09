package command

import (
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func newStatsCmd(opts *GlobalOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "stats [queue]",
		Short: "Show statistics for a queue",
		Long: `Show statistics for a queue. When no queue is given, the profile's
default_queue is used.`,
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

			stats, err := b.Stats(ctx, queue)
			if err != nil {
				return err
			}

			if opts.Output == "json" {
				return writeJSON(cmd, stats)
			}

			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintf(tw, "Queue:\t%s\n", stats.Queue)
			fmt.Fprintf(tw, "Messages:\t%d\n", stats.Messages)
			fmt.Fprintf(tw, "Consumers:\t%d\n", stats.Consumers)
			// Pending (delivered but unacknowledged, e.g. Redis consumer-group
			// PELs) is only rendered by brokers that report it — a missing line
			// means the broker does not track it, not that it is zero.
			if stats.Pending > 0 {
				fmt.Fprintf(tw, "Pending:\t%d\n", stats.Pending)
			}
			// Message age is not exposed by the AMQP protocol in this phase.
			fmt.Fprintf(tw, "Oldest age:\tn/a\n")
			fmt.Fprintf(tw, "Newest age:\tn/a\n")
			return tw.Flush()
		},
	}
}
