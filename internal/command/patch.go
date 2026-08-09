package command

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/patch"
	"github.com/HalxDocs/dlq_inspector/internal/safety"
)

func newPatchCmd(opts *GlobalOptions) *cobra.Command {
	var (
		id          string
		destination string
		confirm     bool
		yes         bool
		reason      string
		sets        []string
	)

	cmd := &cobra.Command{
		Use:   "patch [queue]",
		Short: "Patch a message payload and replay it",
		Long: `Apply controlled edits to a failed message's JSON payload and replay it to
its original destination — the fix-then-replay path for messages classified
REQUIRES_FIX.

--set accepts dotted paths (customer_id, billing.address.city, items.0.sku)
and JSON values (443, true, ["a","b"]); anything that is not valid JSON is
treated as a string.

Without --confirm this is a dry run: it renders the old->new payload diff,
runs the same safety checks as dlq replay, and changes nothing. With
--confirm the patched payload is republished first and only then is the
DLQ copy removed, and the diff is written to the audit trail.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := commandContext(cmd.Context())
			defer cancel()

			ops, err := parseSetOps(sets)
			if err != nil {
				return err
			}

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

			store, err := openAuditStore(opts)
			if err != nil {
				return err
			}
			defer store.Close()

			// Compute the patched payload and its diff from the live message;
			// the safety gate then re-inspects and applies the same checks as
			// an unpatched replay.
			m, err := b.Inspect(ctx, queue, id)
			if err != nil {
				return err
			}
			patched, err := patch.ApplySet(m.Payload, ops)
			if err != nil {
				return err
			}
			diff, err := patch.Diff(m.Payload, patched)
			if err != nil {
				return err
			}
			if diff == "" {
				return fmt.Errorf("--set produced no change — refusing to replay an unchanged payload")
			}

			req := safety.Request{
				Broker:      b,
				Audit:       store,
				Queue:       queue,
				MessageID:   id,
				Destination: destination,
				BrokerName:  profile.Broker,
				Profile:     effectiveProfileName(opts),
				Reason:      reason,
				Action:      audit.ActionPatch,
				Payload:     patched,
				Diff:        diff,
			}

			if confirm {
				if !yes && stdinIsTerminal() {
					ok, err := promptConfirm(cmd, fmt.Sprintf("Patch and replay message %s from %s? [y/N] ", id, queue))
					if err != nil {
						return err
					}
					if !ok {
						fmt.Fprintln(cmd.OutOrStdout(), "Patch cancelled.")
						return nil
					}
				}
				req.Confirm = true
				res, err := safety.Execute(ctx, req)
				if err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Patched and replayed %s -> %s\n", res.MessageID, res.Destination)
				fmt.Fprintln(cmd.OutOrStdout(), "Audit entry written (with diff).")
				return nil
			}

			preview, err := safety.DryRun(ctx, req)
			if err != nil {
				return err
			}
			return renderReplayPreview(cmd, preview, diff)
		},
	}

	cmd.Flags().StringVar(&id, "id", "", "message ID to patch and replay")
	cmd.Flags().StringArrayVar(&sets, "set", nil, "set a dotted path to a value, e.g. customer_id=443 (repeatable)")
	cmd.Flags().StringVar(&destination, "destination", "", "override the replay destination (defaults to the message's original queue)")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "execute the patch and replay (without it, this is a dry run)")
	cmd.Flags().BoolVar(&yes, "yes", false, "skip the interactive confirmation prompt")
	cmd.Flags().StringVar(&reason, "reason", "", "operator-provided reason, recorded in the audit trail")
	return cmd
}

// parseSetOps parses --set path=value flags into patch operations. The value
// may be empty (an empty string), but the path must not be.
func parseSetOps(sets []string) ([]patch.SetOp, error) {
	ops := make([]patch.SetOp, 0, len(sets))
	for _, s := range sets {
		i := strings.Index(s, "=")
		if i <= 0 {
			return nil, fmt.Errorf("invalid --set %q (want path=value)", s)
		}
		ops = append(ops, patch.SetOp{Path: s[:i], Value: s[i+1:]})
	}
	if len(ops) == 0 {
		return nil, fmt.Errorf("at least one --set is required")
	}
	return ops, nil
}
