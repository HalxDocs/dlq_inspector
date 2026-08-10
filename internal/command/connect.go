package command

import (
	"fmt"
	"net/url"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/config"
)

type connectOptions struct {
	url           string
	urlEnv        string
	defaultQueue  string
	managementURL string
}

func newConnectCmd(opts *GlobalOptions) *cobra.Command {
	co := &connectOptions{}

	cmd := &cobra.Command{
		Use:   "connect <broker>",
		Short: "Save a broker connection profile (validation only, no connection)",
		Long: `Save a named connection profile for a broker. This phase validates and
persists the profile only — it does not open a connection.

Prefer --url-env (the name of an environment variable holding the URL) over
--url so connection secrets never land in the config file.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			brokerName := args[0]

			if _, err := broker.New(brokerName); err != nil {
				return err
			}
			if co.url != "" && co.urlEnv != "" {
				return fmt.Errorf("set only one of --url or --url-env")
			}
			if co.url == "" && co.urlEnv == "" {
				return fmt.Errorf("one of --url or --url-env is required")
			}
			if co.url != "" {
				if err := validateURL(brokerName, co.url); err != nil {
					return err
				}
			}

			profile := &config.Profile{
				Broker:        brokerName,
				URL:           co.url,
				URLEnv:        co.urlEnv,
				DefaultQueue:  co.defaultQueue,
				ManagementURL: co.managementURL,
			}

			name := opts.Profile
			if name == "" {
				name = "default"
			}

			cfg, err := config.Load(opts.ConfigPath)
			if err != nil {
				return err
			}
			if cfg.Profiles == nil {
				cfg.Profiles = map[string]*config.Profile{}
			}
			cfg.Profiles[name] = profile
			if cfg.DefaultProfile == "" {
				cfg.DefaultProfile = name
			}
			if err := config.Save(opts.ConfigPath, cfg); err != nil {
				return err
			}

			if opts.Output == "json" {
				return writeJSON(cmd, map[string]any{
					"profile": name,
					"broker":  brokerName,
					"config":  opts.ConfigPath,
				})
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Saved profile %q (%s) to %s\n", name, brokerName, opts.ConfigPath)
			return nil
		},
	}

	cmd.Flags().StringVar(&co.url, "url", "", "connection URL (amqp:// or amqps:// for rabbitmq; redis:// or rediss:// for redisstream)")
	cmd.Flags().StringVar(&co.urlEnv, "url-env", "", "environment variable holding the connection URL (preferred; keeps secrets out of the config file)")
	cmd.Flags().StringVar(&co.defaultQueue, "default-queue", "", "default queue to operate on")
	cmd.Flags().StringVar(&co.managementURL, "management-url", "", "management API base URL (defaults to the AMQP host on port 15672)")

	return cmd
}

// validateURL checks the URL scheme against the broker's expected scheme,
// so `dlq connect redisstream --url redis://...` works and a rabbitmq URL is
// not silently accepted for Redis (or vice versa).
func validateURL(brokerName, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid connection URL: %w", err)
	}
	switch brokerName {
	case "rabbitmq":
		if u.Scheme != "amqp" && u.Scheme != "amqps" {
			return fmt.Errorf("unsupported URL scheme %q for rabbitmq (want amqp or amqps)", u.Scheme)
		}
	case "redisstream":
		if u.Scheme != "redis" && u.Scheme != "rediss" {
			return fmt.Errorf("unsupported URL scheme %q for redisstream (want redis or rediss)", u.Scheme)
		}
	}
	return nil
}
