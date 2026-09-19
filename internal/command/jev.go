package command

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/config"
	"github.com/HalxDocs/dlq_inspector/internal/jev"
)

func newJevCmd(opts *GlobalOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "jev",
		Short: "Opt in to or out of Jev AI assist",
		Long: `Jev assist is off by default: dlq analyze and dlq plan classify with the
built-in rules and never touch the network.

Opt in per profile with 'dlq jev enable' (your key stays in the environment —
BYOK, never stored), opt out anytime with 'dlq jev disable', and inspect the
current state with 'dlq jev status'. Per-run --with-jev / --without-jev flags
on analyze and plan always override the profile setting without changing it.`,
	}
	cmd.AddCommand(newJevEnableCmd(opts), newJevDisableCmd(opts), newJevStatusCmd(opts))
	return cmd
}

// jevProfile resolves the profile a jev subcommand acts on: the --profile
// flag when given, else the config's default_profile.
func jevProfile(opts *GlobalOptions, flagVal string) (*config.Config, string, *config.Profile, error) {
	cfg, err := config.Load(opts.ConfigPath)
	if err != nil {
		return nil, "", nil, err
	}
	name := flagVal
	if name == "" {
		name = cfg.DefaultProfile
	}
	profile, err := cfg.Profile(name)
	if err != nil {
		return nil, "", nil, err
	}
	return cfg, name, profile, nil
}

func newJevEnableCmd(opts *GlobalOptions) *cobra.Command {
	var (
		profileName string
		all         bool
		threshold   float64
		model       string
		endpoint    string
		maxCalls    int
		apiKeyEnv   string
	)
	cmd := &cobra.Command{
		Use:   "enable",
		Short: "Enable Jev assist for a profile",
		Long: `Enable Jev assist for a profile: dlq analyze and dlq plan will resolve
ambiguous classifications with Jev (metadata only — never payload bytes).

Your API key is NOT stored: only the env var NAME is saved (default
TYPESAFE_API_KEY). Export it before running, e.g.
export TYPESAFE_API_KEY=ts_... — if it is unset at runtime, commands warn
and fall back to rules instead of failing.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if cmd.Flags().Changed("threshold") && (threshold <= 0 || threshold > 1) {
				return fmt.Errorf("invalid --threshold %.2f (want 0-1 exclusive of 0)", threshold)
			}
			if cmd.Flags().Changed("max-calls") && maxCalls < 0 {
				return fmt.Errorf("invalid --max-calls %d (want >= 0)", maxCalls)
			}
			cfg, name, profile, err := jevProfile(opts, profileName)
			if err != nil {
				return err
			}
			if profile.Jev == nil {
				profile.Jev = &config.JevSettings{}
			}
			profile.Jev.Enabled = true
			// Only overlay options the operator passed: re-enabling must not
			// clobber previously stored tuning with flag defaults.
			if cmd.Flags().Changed("all") {
				profile.Jev.All = all
			}
			if cmd.Flags().Changed("threshold") {
				profile.Jev.Threshold = threshold
			}
			if cmd.Flags().Changed("model") {
				profile.Jev.Model = model
			}
			if cmd.Flags().Changed("endpoint") {
				profile.Jev.Endpoint = endpoint
			}
			if cmd.Flags().Changed("max-calls") {
				profile.Jev.MaxCalls = maxCalls
			}
			if cmd.Flags().Changed("api-key-env") {
				profile.Jev.APIKeyEnv = apiKeyEnv
			}
			if err := config.Save(opts.ConfigPath, cfg); err != nil {
				return err
			}
			eff := profile.Jev.Effective()
			fmt.Fprintf(cmd.OutOrStdout(), "Jev assist enabled for profile %q.\n", name)
			fmt.Fprintf(cmd.OutOrStdout(), "Settings: threshold %.2f, model %s, max-calls %d, key from %s.\n",
				eff.Threshold, eff.Model, eff.MaxCalls, eff.APIKeyEnv)
			if os.Getenv(eff.APIKeyEnv) == "" {
				fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s is not set — export it before running analyze, or Jev will fall back to rules\n", eff.APIKeyEnv)
			}
			fmt.Fprintln(cmd.OutOrStdout(), "Next: dlq analyze <queue>  (assist is now automatic)")
			fmt.Fprintln(cmd.OutOrStdout(), "Opt out anytime: dlq jev disable / per-run: --without-jev")
			return nil
		},
	}
	cmd.Flags().StringVar(&profileName, "profile", "", "profile to enable Jev for (defaults to default_profile)")
	cmd.Flags().BoolVar(&all, "all", false, "consult Jev for every message, not just INVESTIGATE ones")
	cmd.Flags().Float64Var(&threshold, "threshold", jev.DefaultThreshold, "minimum Jev confidence to accept (0-1)")
	cmd.Flags().StringVar(&model, "model", jev.DefaultModel, "Jev model ID")
	cmd.Flags().StringVar(&endpoint, "endpoint", jev.DefaultEndpoint, "Jev API endpoint")
	cmd.Flags().IntVar(&maxCalls, "max-calls", jev.DefaultMaxCalls, "maximum Jev API calls per run")
	cmd.Flags().StringVar(&apiKeyEnv, "api-key-env", jev.APIKeyEnv, "env var holding the Jev API key (name only, never the value)")
	return cmd
}

func newJevDisableCmd(opts *GlobalOptions) *cobra.Command {
	var profileName string
	cmd := &cobra.Command{
		Use:   "disable",
		Short: "Disable Jev assist for a profile (back to rules)",
		Long: `Disable Jev assist for a profile: analyze and plan return to the
built-in rule-based classifier and make no network calls. Stored Jev tuning
is kept, so 'dlq jev enable' restores it. Per-run --without-jev does the
same for a single invocation without changing the profile.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, name, profile, err := jevProfile(opts, profileName)
			if err != nil {
				return err
			}
			if profile.Jev == nil || !profile.Jev.Enabled {
				fmt.Fprintf(cmd.OutOrStdout(), "Jev assist already off for profile %q (rule-based).\n", name)
				return nil
			}
			profile.Jev.Enabled = false
			if err := config.Save(opts.ConfigPath, cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Jev assist disabled for profile %q — back to rule-based.\n", name)
			fmt.Fprintln(cmd.OutOrStdout(), "Re-enable anytime: dlq jev enable / per-run: --with-jev")
			return nil
		},
	}
	cmd.Flags().StringVar(&profileName, "profile", "", "profile to disable Jev for (defaults to default_profile)")
	return cmd
}

func newJevStatusCmd(opts *GlobalOptions) *cobra.Command {
	var profileName string
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show Jev assist state for a profile",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, name, profile, err := jevProfile(opts, profileName)
			if err != nil {
				return err
			}
			eff := profile.Jev.Effective()
			keySet := os.Getenv(eff.APIKeyEnv) != ""
			if opts.Output == "json" {
				return writeJSON(cmd, map[string]any{
					"profile":        name,
					"enabled":        eff.Enabled,
					"all":            eff.All,
					"threshold":      eff.Threshold,
					"model":          eff.Model,
					"endpoint":       eff.Endpoint,
					"max_calls":      eff.MaxCalls,
					"api_key_env":    eff.APIKeyEnv,
					"api_key_is_set": keySet,
				})
			}
			state := "off (rule-based)"
			if eff.Enabled {
				state = "on (Jev assist)"
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Jev assist for profile %q: %s\n", name, state)
			fmt.Fprintf(cmd.OutOrStdout(), "Threshold: %.2f  Model: %s  Max-calls: %d  All-messages: %v\n",
				eff.Threshold, eff.Model, eff.MaxCalls, eff.All)
			fmt.Fprintf(cmd.OutOrStdout(), "Key: %s %s\n", eff.APIKeyEnv, setLabel(keySet))
			if eff.Enabled && !keySet {
				fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s is not set — commands will fall back to rules\n", eff.APIKeyEnv)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&profileName, "profile", "", "profile to inspect (defaults to default_profile)")
	return cmd
}

func setLabel(set bool) string {
	if set {
		return "(set)"
	}
	return "(not set)"
}
