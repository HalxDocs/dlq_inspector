// Package config loads and persists the local CLI configuration
// (~/.dlq/config.yaml): named broker profiles, the default profile, and audit
// settings. Secrets are never stored in plaintext — profiles reference an
// environment variable (url_env) instead of an inline URL.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the top-level document of the local config file.
type Config struct {
	DefaultProfile string              `yaml:"default_profile"`
	Profiles       map[string]*Profile `yaml:"profiles"`
	Audit          AuditConfig         `yaml:"audit"`
}

// AuditConfig configures the local audit store.
type AuditConfig struct {
	Path          string `yaml:"path"`
	RetentionDays int    `yaml:"retention_days"`
}

// DefaultAuditPath is where the audit store lives by default.
const DefaultAuditPath = "~/.dlq/audit.db"

// DefaultAuditRetentionDays is how long audit entries are kept by default.
const DefaultAuditRetentionDays = 90

// DefaultPath returns the default config file location: ~/.dlq/config.yaml.
func DefaultPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".dlq", "config.yaml"), nil
}

// Defaults returns a Config with sensible defaults for a first run.
func Defaults() *Config {
	return &Config{
		Audit: AuditConfig{
			Path:          DefaultAuditPath,
			RetentionDays: DefaultAuditRetentionDays,
		},
	}
}

// Load reads the config file at path. A missing file is not an error — it
// yields an empty Config with defaults (first run, nothing configured yet).
func Load(path string) (*Config, error) {
	cfg := Defaults()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	return cfg, nil
}

// Save writes the config to path, creating parent directories as needed.
// The file is written with owner-only permissions since it may contain
// connection details.
func Save(path string, cfg *Config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write config %s: %w", path, err)
	}
	return nil
}

// Profile returns the named profile, or the default profile when name is empty.
func (c *Config) Profile(name string) (*Profile, error) {
	if name == "" {
		name = c.DefaultProfile
	}
	if name == "" {
		return nil, fmt.Errorf("no profile specified and no default_profile set")
	}
	p, ok := c.Profiles[name]
	if !ok {
		return nil, fmt.Errorf("profile %q not found", name)
	}
	return p, nil
}

// ExpandPath expands a leading "~/" to the user's home directory.
func ExpandPath(path string) (string, error) {
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home directory: %w", err)
		}
		if path == "~" {
			return home, nil
		}
		return filepath.Join(home, path[2:]), nil
	}
	return path, nil
}
