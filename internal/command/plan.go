package command

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/recovery"
)

func newPlanCmd(opts *GlobalOptions) *cobra.Command {
	var (
		groupID            string
		destination        string
		output             string
		batchSize          int
		rateLimit          string
		concurrency        int
		limit              int
		includeDoNotReplay bool
		reason             string
	)

	cmd := &cobra.Command{
		Use:   "plan [queue]",
		Short: "Build a recovery plan from analyzed failure groups",
		Long: `Build a recovery plan: the exact set of messages, destination, execution
limits, and safety checks that a later dlq recover will run. The plan is
written as JSON for review and diffing — nothing is executed here.

By default every replayable message is selected. Use --group <id> (an ID
shown by dlq analyze) to plan one failure group. Messages classified
DO_NOT_REPLAY are excluded unless --include-do-not-replay is given.`,
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
				return fmt.Errorf("no messages found in %q to plan", queue)
			}

			p, err := recovery.BuildPlan(msgs, recovery.PlanOptions{
				Queue:              queue,
				GroupID:            groupID,
				Destination:        destination,
				Limits:             recovery.PlanLimits{BatchSize: batchSize, RateLimit: rateLimit, Concurrency: concurrency},
				IncludeDoNotReplay: includeDoNotReplay,
			})
			if err != nil {
				return err
			}

			if err := writePlanFile(output, p); err != nil {
				return err
			}

			store, err := openAuditStore(opts)
			if err != nil {
				return err
			}
			defer store.Close()
			if err := store.Append(audit.Entry{
				Timestamp:   p.CreatedAt,
				Action:      audit.ActionPlan,
				PlanID:      p.ID,
				SourceQueue: queue,
				Destination: p.Destination,
				Result:      "written",
				Broker:      profile.Broker,
				Profile:     effectiveProfileName(opts),
				Reason:      reason,
			}); err != nil {
				return fmt.Errorf("record plan in audit: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Plan written: %s (%d messages selected)\n", output, len(p.MessageIDs))
			fmt.Fprintf(cmd.OutOrStdout(), "Plan ID: %s\n", p.ID)
			fmt.Fprintf(cmd.OutOrStdout(), "Review it, then run: dlq recover --plan %s --dry-run\n", output)
			return nil
		},
	}

	cmd.Flags().StringVar(&groupID, "group", "", "plan only this failure group (ID shown by dlq analyze)")
	cmd.Flags().StringVar(&destination, "destination", "", "override the replay destination")
	cmd.Flags().StringVar(&output, "output-file", "recovery.json", "path to write the plan JSON to")
	cmd.Flags().IntVar(&batchSize, "batch-size", 25, "messages per batch during execution")
	cmd.Flags().StringVar(&rateLimit, "rate-limit", "10/s", "execution rate limit (e.g. 10/s)")
	cmd.Flags().IntVar(&concurrency, "concurrency", 1, "parallel execution workers")
	cmd.Flags().IntVar(&limit, "limit", 1000, "maximum number of messages to consider")
	cmd.Flags().BoolVar(&includeDoNotReplay, "include-do-not-replay", false, "also select messages classified DO_NOT_REPLAY")
	cmd.Flags().StringVar(&reason, "reason", "", "operator-provided reason, recorded in the audit trail")
	return cmd
}

// writePlanFile writes the plan as indented JSON for review and diffing.
func writePlanFile(path string, p *recovery.RecoveryPlan) error {
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("encode plan: %w", err)
	}
	b = append(b, '\n')
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write plan %s: %w", path, err)
	}
	return nil
}

// loadPlanFile reads and decodes a recovery plan JSON file.
func loadPlanFile(path string) (*recovery.RecoveryPlan, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read plan %s: %w", path, err)
	}
	var p recovery.RecoveryPlan
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("parse plan %s: %w", path, err)
	}
	if len(p.MessageIDs) == 0 {
		return nil, fmt.Errorf("plan %s selects no messages", path)
	}
	return &p, nil
}
