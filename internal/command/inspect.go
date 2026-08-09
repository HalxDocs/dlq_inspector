package command

import (
	"fmt"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

func newInspectCmd(opts *GlobalOptions) *cobra.Command {
	var (
		id            string
		format        string
		showSensitive bool
	)

	cmd := &cobra.Command{
		Use:   "inspect [queue]",
		Short: "Inspect a single failed message by ID",
		Long: `Inspect a single failed message from a dead-letter queue.

The queue defaults to the profile's default_queue. Sensitive payload fields
configured in the profile are masked unless --show-sensitive is given.`,
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
			if id == "" {
				return fmt.Errorf("--id is required")
			}
			switch format {
			case "pretty", "raw":
			default:
				return fmt.Errorf("invalid --format %q (want pretty or raw)", format)
			}

			m, err := b.Inspect(ctx, queue, id)
			if err != nil {
				return err
			}
			if !showSensitive {
				m.Payload = message.Redactor{Fields: profile.SensitiveFields}.Apply(m.Payload)
			}

			if opts.Output == "json" {
				return writeJSON(cmd, m)
			}
			return renderInspect(cmd, m, format)
		},
	}

	cmd.Flags().StringVar(&id, "id", "", "message ID to inspect")
	cmd.Flags().StringVar(&format, "format", "pretty", "payload format: pretty or raw")
	cmd.Flags().BoolVar(&showSensitive, "show-sensitive", false, "reveal configured sensitive fields (audit-logged in a later phase)")
	return cmd
}

// renderInspect prints the message metadata block followed by the payload.
func renderInspect(cmd *cobra.Command, m *message.Message, format string) error {
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "ID:\t%s\n", m.ID)
	fmt.Fprintf(tw, "Queue:\t%s\n", m.Queue)
	if m.Destination != "" {
		fmt.Fprintf(tw, "Destination:\t%s\n", m.Destination)
	}
	if !m.Timestamp.IsZero() {
		fmt.Fprintf(tw, "Timestamp:\t%s\n", m.Timestamp.Format(time.RFC3339))
	}
	fmt.Fprintf(tw, "Retries:\t%d\n", m.RetryCount)
	if m.FailureReason != "" {
		fmt.Fprintf(tw, "Failure reason:\t%s\n", m.FailureReason)
	}
	if m.ContentType != "" {
		fmt.Fprintf(tw, "Content type:\t%s\n", m.ContentType)
	}
	if m.EventID != "" {
		fmt.Fprintf(tw, "Event ID:\t%s\n", m.EventID)
	}
	if m.IdempotencyKey != "" {
		fmt.Fprintf(tw, "Idempotency key:\t%s\n", m.IdempotencyKey)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	if len(m.Headers) > 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "\nHeaders:")
		keys := make([]string, 0, len(m.Headers))
		for k := range m.Headers {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(cmd.OutOrStdout(), "  %s: %s\n", k, m.Headers[k])
		}
	}

	fmt.Fprintln(cmd.OutOrStdout())
	if format == "raw" {
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n", string(m.Payload))
		return nil
	}
	pretty, err := message.PrettyJSON(m.Payload)
	if err != nil {
		// Not JSON — fall back to the raw bytes.
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n", string(m.Payload))
		return nil
	}
	fmt.Fprintln(cmd.OutOrStdout(), pretty)
	return nil
}
