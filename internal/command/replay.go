package command

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/config"
	"github.com/HalxDocs/dlq_inspector/internal/safety"
)

func newReplayCmd(opts *GlobalOptions) *cobra.Command {
	var (
		id          string
		destination string
		confirm     bool
		yes         bool
		dryRun      bool
		reason      string
	)

	cmd := &cobra.Command{
		Use:   "replay [queue]",
		Short: "Replay a failed message to its original destination",
		Long: `Replay a failed message from a dead-letter queue back to its original
destination.

The queue defaults to the profile's default_queue. Replay is always gated:
without --confirm this is a dry run that previews the operation and changes
nothing. With --confirm, the message is republished first and only then
removed from the DLQ, and every outcome is written to the local audit trail.`,
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

			store, err := openAuditStore(opts)
			if err != nil {
				return err
			}
			defer store.Close()

			req := safety.Request{
				Broker:      b,
				Audit:       store,
				Queue:       queue,
				MessageID:   id,
				Destination: destination,
				BrokerName:  profile.Broker,
				Profile:     effectiveProfileName(opts),
				Reason:      reason,
			}

			if confirm {
				if !yes && stdinIsTerminal() {
					ok, err := promptConfirm(cmd, fmt.Sprintf("Replay message %s from %s? [y/N] ", id, queue))
					if err != nil {
						return err
					}
					if !ok {
						fmt.Fprintln(cmd.OutOrStdout(), "Replay cancelled.")
						return nil
					}
				}
				req.Confirm = true
				res, err := safety.Execute(ctx, req)
				if err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Replayed %s -> %s\n", res.MessageID, res.Destination)
				fmt.Fprintln(cmd.OutOrStdout(), "Audit entry written.")
				return nil
			}

			if dryRun {
				fmt.Fprintln(cmd.OutOrStdout(), "Dry run requested explicitly.")
			}
			preview, err := safety.DryRun(ctx, req)
			if err != nil {
				return err
			}
			return renderReplayPreview(cmd, preview, "")
		},
	}

	cmd.Flags().StringVar(&id, "id", "", "message ID to replay")
	cmd.Flags().StringVar(&destination, "destination", "", "override the replay destination (defaults to the message's original queue)")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "execute the replay (without it, this is a dry run)")
	cmd.Flags().BoolVar(&yes, "yes", false, "skip the interactive confirmation prompt")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "explicitly preview the replay (the default without --confirm)")
	cmd.Flags().StringVar(&reason, "reason", "", "operator-provided reason, recorded in the audit trail")
	return cmd
}

// openAuditStore opens the audit store configured in the config file.
func openAuditStore(opts *GlobalOptions) (*audit.Store, error) {
	cfg, err := config.Load(opts.ConfigPath)
	if err != nil {
		return nil, err
	}
	path, err := config.ExpandPath(cfg.Audit.Path)
	if err != nil {
		return nil, err
	}
	return audit.Open(path)
}

// effectiveProfileName returns the profile name used for audit attribution.
func effectiveProfileName(opts *GlobalOptions) string {
	if opts.Profile != "" {
		return opts.Profile
	}
	cfg, err := config.Load(opts.ConfigPath)
	if err != nil {
		return ""
	}
	return cfg.DefaultProfile
}

// stdinIsTerminal reports whether stdin is an interactive terminal, in which
// case a confirmation prompt is shown. A plain char-device check is not
// enough — /dev/null and NUL are char devices too — so this uses the
// terminal ioctl probe from x/term.
func stdinIsTerminal() bool {
	return term.IsTerminal(int(os.Stdin.Fd()))
}

// promptConfirm asks the operator to confirm an operation, returning false
// when they decline.
func promptConfirm(cmd *cobra.Command, prompt string) (bool, error) {
	fmt.Fprint(cmd.OutOrStdout(), prompt)
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return false, err
	}
	return strings.EqualFold(strings.TrimSpace(line), "y"), nil
}

// renderReplayPreview prints the dry-run preview: what would happen, the
// safety checks that passed, the payload diff for patched replays, any
// warnings, and the changes-made: NONE line.
func renderReplayPreview(cmd *cobra.Command, p *safety.Preview, diff string) error {
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Message:\t%s\n", p.Message.ID)
	fmt.Fprintf(tw, "Queue:\t%s\n", p.Message.Queue)
	fmt.Fprintf(tw, "Destination:\t%s\n", p.Destination)
	fmt.Fprintf(tw, "Retries:\t%d\n", p.Message.RetryCount)
	if p.Message.FailureReason != "" {
		fmt.Fprintf(tw, "Failure reason:\t%s\n", p.Message.FailureReason)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	fmt.Fprintln(cmd.OutOrStdout(), "\nSafety checks:")
	for _, c := range p.SafetyChecks {
		fmt.Fprintf(cmd.OutOrStdout(), "  [ok] %s\n", c)
	}

	if diff != "" {
		fmt.Fprintln(cmd.OutOrStdout(), "\nPayload diff:")
		fmt.Fprintln(cmd.OutOrStdout(), diff)
	}

	if p.Duplicate.MatchFound {
		fmt.Fprintln(cmd.OutOrStdout(), "\nPOSSIBLE DUPLICATE")
		for _, w := range p.Warnings {
			fmt.Fprintf(cmd.OutOrStdout(), "  [!] %s\n", w)
		}
		fmt.Fprintln(cmd.OutOrStdout(), "Review before proceeding.")
	} else if len(p.Warnings) > 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "\nWarnings:")
		for _, w := range p.Warnings {
			fmt.Fprintf(cmd.OutOrStdout(), "  [!] %s\n", w)
		}
	}

	fmt.Fprintln(cmd.OutOrStdout(), "\nChanges made: NONE")
	fmt.Fprintln(cmd.OutOrStdout(), "Dry run — re-run with --confirm to replay.")
	return nil
}
