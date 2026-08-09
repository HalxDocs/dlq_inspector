// Package command implements the dlq CLI commands. Commands parse and
// validate flags, resolve profiles, and delegate to the broker layer or
// recovery engine — they never talk to a broker SDK directly.
package command

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/config"
)

// GlobalOptions carries the values of root-level flags shared by all commands.
type GlobalOptions struct {
	// ConfigPath is the path to the config file (~/.dlq/config.yaml).
	ConfigPath string
	// Profile is the connection profile to use.
	Profile string
	// Output is the output format: "text" or "json".
	Output string
}

// NewRoot builds the root dlq command with all subcommands attached.
func NewRoot(version, commit, date string) *cobra.Command {
	opts := &GlobalOptions{}

	defaultConfig, _ := config.DefaultPath()

	root := &cobra.Command{
		Use:   "dlq",
		Short: "Inspect, analyze, and safely recover failed messages from dead-letter queues",
		Long: `DLQ Inspector is a local-first CLI for debugging and safely recovering failed
background jobs and messages sitting in dead-letter queues.

Workflow: Inspect -> Analyze -> Classify -> Plan -> Validate -> Dry-run -> Recover -> Audit`,
		Version: version,
		// Do not dump the full usage text on runtime errors.
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			switch opts.Output {
			case "text", "json", "jsonl":
				return nil
			default:
				return fmt.Errorf("invalid --output %q (want text, json, or jsonl)", opts.Output)
			}
		},
	}

	root.PersistentFlags().StringVar(&opts.ConfigPath, "config", defaultConfig, "path to the config file (default ~/.dlq/config.yaml)")
	root.PersistentFlags().StringVar(&opts.Profile, "profile", "", "connection profile to use")
	root.PersistentFlags().StringVar(&opts.Output, "output", "text", "output format: text or json")

	root.AddCommand(
		newVersionCmd(version, commit, date),
		newConnectCmd(opts),
		newProfilesCmd(opts),
		newQueuesCmd(opts),
		newStatsCmd(opts),
		newInspectCmd(opts),
		newSearchCmd(opts),
		newReplayCmd(opts),
		newPatchCmd(opts),
		newHistoryCmd(opts),
		newAnalyzeCmd(opts),
		newPlanCmd(opts),
		newRecoverCmd(opts),
		newRollbackCmd(opts),
		newPolicyCmd(opts),
	)

	return root
}

// writeJSON writes v as indented JSON to the command's output stream.
func writeJSON(cmd *cobra.Command, v any) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
