package config

import (
	"errors"
	"fmt"
	"os"
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
