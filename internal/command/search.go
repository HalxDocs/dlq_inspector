package command

import (
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

func newSearchCmd(opts *GlobalOptions) *cobra.Command {
	var (
		errorText     string
		since         string
		fields        []string
		maxRetries    int
		limit         int
		showSensitive bool
	)

	cmd := &cobra.Command{
		Use:   "search [queue]",
		Short: "Search messages in a queue",
		Long: `Search messages in a queue, filtering by error text, enqueue time,
payload field values, and retry count.

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

			f := broker.SearchFilter{
				ErrorText: errorText,
				Limit:     limit,
				Fields:    make(map[string]string, len(fields)),
			}
			for _, kv := range fields {
				k, v, ok := strings.Cut(kv, "=")
				if !ok || k == "" {
					return fmt.Errorf("invalid --field %q (want key=value)", kv)
				}
				f.Fields[k] = v
			}
			if since != "" {
				t, err := parseSince(since)
				if err != nil {
					return err
				}
				f.Since = t
			}
			if maxRetries >= 0 {
				f.MaxRetries = &maxRetries
			}

			msgs, err := b.Search(ctx, queue, f)
			if err != nil {
				return err
			}
			if !showSensitive {
				red := message.Redactor{Fields: profile.SensitiveFields}
				for i := range msgs {
					msgs[i].Payload = red.Apply(msgs[i].Payload)
				}
			}

			if opts.Output == "json" {
				return writeJSON(cmd, msgs)
			}
			return renderSearchResults(cmd, msgs)
		},
	}

	cmd.Flags().StringVar(&errorText, "error", "", "match failure text or payload containing this string (case-insensitive)")
	cmd.Flags().StringVar(&since, "since", "", "only messages enqueued within the last duration (e.g. 2h) or after an RFC3339 timestamp")
	cmd.Flags().StringArrayVar(&fields, "field", nil, "require a payload field to equal a value (key=value, repeatable)")
	cmd.Flags().IntVar(&maxRetries, "max-retries", -1, "only messages with retry count <= N")
	cmd.Flags().IntVar(&limit, "limit", 50, "maximum number of results")
	cmd.Flags().BoolVar(&showSensitive, "show-sensitive", false, "reveal configured sensitive fields (audit-logged in a later phase)")
	return cmd
}

// parseSince accepts a relative duration ("2h", "30m") or an absolute RFC3339
// timestamp.
func parseSince(s string) (time.Time, error) {
	if d, err := time.ParseDuration(s); err == nil {
		return time.Now().Add(-d), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid --since %q (want a duration like 2h or an RFC3339 timestamp)", s)
}

// renderSearchResults prints a compact results table with a one-line payload
// snippet, followed by a count summary.
func renderSearchResults(cmd *cobra.Command, msgs []message.Message) error {
	if len(msgs) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "No messages match.")
		return nil
	}

	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tTIMESTAMP\tRETRIES\tREASON\tPAYLOAD")
	for _, m := range msgs {
		ts := "-"
		if !m.Timestamp.IsZero() {
			ts = m.Timestamp.Format(time.RFC3339)
		}
		reason := oneLine(truncate(m.FailureReason, 30))
		snippet := oneLine(truncate(string(m.Payload), 60))
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\n", m.ID, ts, m.RetryCount, reason, snippet)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "\n%d message(s) match.\n", len(msgs))
	return nil
}

// truncate shortens s to at most n runes, appending an ellipsis.
func truncate(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "…"
}

// oneLine collapses newlines and tabs so payload snippets stay on one row.
func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	return strings.TrimSpace(s)
}
