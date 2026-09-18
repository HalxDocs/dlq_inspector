package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/HalxDocs/dlq_inspector/internal/jev"
)

// Profile describes a named broker connection in the config file. Secrets are
// never stored in plaintext: prefer URLEnv, which names an environment
// variable holding the connection URL.
type Profile struct {
	Broker           string `yaml:"broker"`
	URL              string `yaml:"url,omitempty"`
	URLEnv           string `yaml:"url_env,omitempty"`
	DefaultQueue     string `yaml:"default_queue,omitempty"`
	ManagementURL    string `yaml:"management_url,omitempty"`
	RequireConfirm   bool   `yaml:"require_confirm,omitempty"`
	RequireCoConfirm bool   `yaml:"require_co_confirm,omitempty"`
	PolicyFile       string `yaml:"policy_file,omitempty"`
	// SensitiveFields are dotted payload paths masked by default in
	// inspect/search output (e.g. "customer.email"). Revealed only with
	// --show-sensitive.
	SensitiveFields []string `yaml:"sensitive_fields,omitempty"`
	// Jev, when non-nil with Enabled true, turns on Jev assist by default
	// for dlq analyze/plan on this profile. Nil means classic rule-based
	// behavior. Managed with `dlq jev enable|disable`; per-run
	// --with-jev/--without-jev flags always override it.
	Jev *JevSettings `yaml:"jev,omitempty"`
}

// JevSettings persists one profile's Jev assist preference. Only the key's
// env var NAME is stored here — the key value itself always stays in the
// environment (BYOK).
type JevSettings struct {
	Enabled   bool    `yaml:"enabled"`
	All       bool    `yaml:"all,omitempty"`
	Threshold float64 `yaml:"threshold,omitempty"`
	Model     string  `yaml:"model,omitempty"`
	Endpoint  string  `yaml:"endpoint,omitempty"`
	MaxCalls  int     `yaml:"max_calls,omitempty"`
	APIKeyEnv string  `yaml:"api_key_env,omitempty"`
}

// Effective returns the settings with defaults applied for every zero value.
// Nil-safe: a nil receiver yields all defaults with Enabled false.
func (s *JevSettings) Effective() JevSettings {
	out := JevSettings{
		Threshold: jev.DefaultThreshold,
		Model:     jev.DefaultModel,
		Endpoint:  jev.DefaultEndpoint,
		MaxCalls:  jev.DefaultMaxCalls,
		APIKeyEnv: jev.APIKeyEnv,
	}
	if s == nil {
		return out
	}
	out.Enabled = s.Enabled
	out.All = s.All
	if s.Threshold > 0 {
		out.Threshold = s.Threshold
	}
	if s.Model != "" {
		out.Model = s.Model
	}
	if s.Endpoint != "" {
		out.Endpoint = s.Endpoint
	}
	if s.MaxCalls > 0 {
		out.MaxCalls = s.MaxCalls
	}
	if s.APIKeyEnv != "" {
		out.APIKeyEnv = s.APIKeyEnv
	}
	return out
}

// ResolveURL returns the effective connection URL for the profile, preferring
// the environment variable named by URLEnv and falling back to the inline URL.
func (p *Profile) ResolveURL() (string, error) {
	if p.URLEnv != "" {
		v, ok := os.LookupEnv(p.URLEnv)
		if !ok || v == "" {
			return "", fmt.Errorf("environment variable %s referenced by the profile is not set", p.URLEnv)
		}
		return v, nil
	}
	if p.URL != "" {
		return p.URL, nil
	}
	return "", errors.New("profile has neither url nor url_env set")
}
