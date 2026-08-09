package command

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/config"
	"github.com/HalxDocs/dlq_inspector/internal/policy"
)

func newPolicyCmd(opts *GlobalOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "policy",
		Short: "Validate and apply recovery policies",
		Long: `Recovery policies are YAML files, committed alongside the service they
protect, that encode what is safe to replay. When a policy is loaded for a
profile, its rules override the classifier's inference for matching
messages: dlq analyze and dlq plan honor it.

Run dlq policy validate in CI to catch a broken rule before it reaches
production; dlq policy apply binds a policy to a profile.`,
	}
	cmd.AddCommand(newPolicyValidateCmd(opts), newPolicyApplyCmd(opts))
	return cmd
}

func newPolicyValidateCmd(opts *GlobalOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "validate <policy.yaml>",
		Short: "Parse and validate a recovery policy file",
		Long: `Parse and validate a policy file, reporting every problem with its rule
number. Exits non-zero on any breakage, so CI can gate on it.

With --output json the verdict is machine-readable: {"valid":true,"rules":N}
or {"valid":false,"errors":[...]}.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			pol, err := policy.Load(path)
			if err != nil {
				if opts.Output == "json" {
					_ = writeJSON(cmd, map[string]any{
						"valid":  false,
						"errors": strings.Split(err.Error(), "; "),
					})
				}
				return err // cobra prints it to stderr; exit code is non-zero
			}
			if opts.Output == "json" {
				return writeJSON(cmd, map[string]any{"valid": true, "rules": len(pol.Rules)})
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s: valid (%d rules)\n", path, len(pol.Rules))
			return nil
		},
	}
}

func newPolicyApplyCmd(opts *GlobalOptions) *cobra.Command {
	var profileName string

	cmd := &cobra.Command{
		Use:   "apply <policy.yaml>",
		Short: "Bind a recovery policy to a profile",
		Long: `Validate a policy file and bind it to a connection profile, so dlq analyze
and dlq plan honor its rules. The policy path is stored in the profile
(policy_file); the file itself stays where it lives — typically committed
next to the service it protects.

The profile defaults to the config's default_profile.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path, err := policyPath(args[0])
			if err != nil {
				return err
			}
			// Refuse to bind a policy that does not parse — a broken policy
			// must be caught here, not when an analysis quietly runs without
			// the rules the operator believes are in force.
			if _, err := policy.Load(path); err != nil {
				return fmt.Errorf("policy %s does not validate: %w", args[0], err)
			}

			cfg, err := config.Load(opts.ConfigPath)
			if err != nil {
				return err
			}
			name := profileName
			if name == "" {
				name = cfg.DefaultProfile
			}
			profile, err := cfg.Profile(name)
			if err != nil {
				return err
			}

			profile.PolicyFile = path
			if err := config.Save(opts.ConfigPath, cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Policy applied to profile %q: %s\n", name, path)
			fmt.Fprintln(cmd.OutOrStdout(), "dlq analyze and dlq plan will honor it for this profile.")
			return nil
		},
	}

	cmd.Flags().StringVar(&profileName, "profile", "", "profile to bind the policy to (defaults to default_profile)")
	return cmd
}

// policyPath normalizes a policy file path for storage: expand ~ and make it
// absolute so the profile reference works regardless of the working directory
// later.
func policyPath(p string) (string, error) {
	expanded, err := config.ExpandPath(p)
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(expanded)
	if err != nil {
		return "", fmt.Errorf("resolve policy path: %w", err)
	}
	return abs, nil
}

// loadPolicyForProfile loads the policy bound to a profile, if any. A missing
// or broken policy file is an error — the profile explicitly references it, so
// analyses must not silently run without the rules the operator believes are
// in force.
func loadPolicyForProfile(name string, profile *config.Profile) (*policy.Policy, error) {
	if profile == nil || profile.PolicyFile == "" {
		return nil, nil
	}
	path, err := config.ExpandPath(profile.PolicyFile)
	if err != nil {
		return nil, err
	}
	pol, err := policy.Load(path)
	if err != nil {
		return nil, fmt.Errorf("profile %q policy: %w", name, err)
	}
	return pol, nil
}
