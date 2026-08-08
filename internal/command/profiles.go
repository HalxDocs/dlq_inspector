package command

import (
	"fmt"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/config"
)

func newProfilesCmd(opts *GlobalOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profiles",
		Short: "Manage saved connection profiles",
	}
	cmd.AddCommand(newProfilesListCmd(opts))
	return cmd
}

func newProfilesListCmd(opts *GlobalOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List saved connection profiles",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(opts.ConfigPath)
			if err != nil {
				return err
			}
			if len(cfg.Profiles) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No profiles configured. Use 'dlq connect' to add one.")
				return nil
			}

			names := make([]string, 0, len(cfg.Profiles))
			for name := range cfg.Profiles {
				names = append(names, name)
			}
			sort.Strings(names)

			if opts.Output == "json" {
				entries := make([]map[string]any, 0, len(names))
				for _, name := range names {
					entries = append(entries, profileJSON(name, name == cfg.DefaultProfile, cfg.Profiles[name]))
				}
				return writeJSON(cmd, entries)
			}

			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "NAME\tBROKER\tDEFAULT QUEUE\tURL SOURCE")
			for _, name := range names {
				p := cfg.Profiles[name]
				marker := ""
				if name == cfg.DefaultProfile {
					marker = " (default)"
				}
				fmt.Fprintf(tw, "%s%s\t%s\t%s\t%s\n", name, marker, p.Broker, p.DefaultQueue, urlSource(p))
			}
			return tw.Flush()
		},
	}
}

// urlSource describes where a profile's URL comes from without printing the
// value itself, so connection secrets never leak into terminal output.
func urlSource(p *config.Profile) string {
	if p.URLEnv != "" {
		return "env:" + p.URLEnv
	}
	return "url"
}

func profileJSON(name string, isDefault bool, p *config.Profile) map[string]any {
	return map[string]any{
		"name":          name,
		"default":       isDefault,
		"broker":        p.Broker,
		"default_queue": p.DefaultQueue,
		"url_source":    urlSource(p),
	}
}
