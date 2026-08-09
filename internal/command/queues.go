package command

import (
	"fmt"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func newQueuesCmd(opts *GlobalOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "queues",
		Short: "List queues on the connected broker",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := commandContext(cmd.Context())
			defer cancel()

			b, _, err := openBroker(ctx, opts)
			if err != nil {
				return err
			}
			defer b.Close()

			queues, err := b.ListQueues(ctx)
			if err != nil {
				return err
			}
			sort.Slice(queues, func(i, j int) bool { return queues[i].Name < queues[j].Name })

			if opts.Output == "json" {
				return writeJSON(cmd, queues)
			}
			if len(queues) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No queues found.")
				return nil
			}

			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "NAME\tDURABLE\tAUTO-DELETE\tMESSAGES\tCONSUMERS")
			for _, q := range queues {
				fmt.Fprintf(tw, "%s\t%t\t%t\t%d\t%d\n", q.Name, q.Durable, q.AutoDelete, q.Messages, q.Consumers)
			}
			return tw.Flush()
		},
	}
}
