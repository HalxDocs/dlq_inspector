package selfupdate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"
)

const (
	// DefaultRepo is the GitHub repository releases are fetched from.
	DefaultRepo = "HalxDocs/dlq_inspector"
	// DefaultProject is the goreleaser project_name used in release archive
	// names.
	DefaultProject = "dlq-inspector"

	defaultAPIRoot      = "https://api.github.com"
	defaultDownloadRoot = "https://github.com"
	defaultUserAgent    = "dlq-self-update"

	maxAttempts = 3
)

// Config configures an Updater.
type Config struct {
	// Repo is the GitHub repository in "owner/repo" form.
	Repo string
	// Project is the artifact project name used in release archive names
	// (the goreleaser project_name).
	Project string
	// Version is the currently installed version, as injected by ldflags.
	Version string
	// Goos and Goarch override the platform the archive is selected for.
	Goos   string
	Goarch string
	// APIRoot and DownloadRoot override the GitHub endpoints (used by tests).
	APIRoot      string
	DownloadRoot string
	// Token authenticates API requests and raises the rate limit.
	Token string
	// UserAgent is sent on every request.
	UserAgent string
	// Timeout bounds each HTTP request.
	Timeout time.Duration
	// Client overrides the HTTP client (defaults to one with Timeout).
	Client *http.Client
}

// New validates cfg and returns an Updater. Defaults (repo, project,
// platform, endpoints, client) are applied for empty fields.
func New(cfg Config) (*Updater, error) {
	if cfg.Repo == "" {
		cfg.Repo = DefaultRepo
	}
	if cfg.Project == "" {
		cfg.Project = DefaultProject
	}
	if cfg.Goos == "" {
		cfg.Goos = runtime.GOOS
	}
	if cfg.Goarch == "" {
		cfg.Goarch = runtime.GOARCH
	}
	if cfg.APIRoot == "" {
		cfg.APIRoot = defaultAPIRoot
	}
	if cfg.DownloadRoot == "" {
		cfg.DownloadRoot = defaultDownloadRoot
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = defaultUserAgent
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Minute
	}
	cli := cfg.Client
	if cli == nil {
		cli = &http.Client{Timeout: cfg.Timeout}
	}

	parts := strings.Split(cfg.Repo, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("invalid repo %q: want owner/repo", cfg.Repo)
	}
	return &Updater{cfg: cfg, cli: cli}, nil
}

// Resolve returns the latest release, or the release for a specific tag when
// tag is non-empty.
func (u *Updater) Resolve(ctx context.Context, tag string) (*Release, error) {
	if tag != "" {
		return u.release(ctx, "releases/tags/"+url.PathEscape(tag))
	}
	return u.release(ctx, "releases/latest")
}

func (u *Updater) apiURL(path string) string {
	return u.cfg.APIRoot + "/repos/" + u.cfg.Repo + "/" + path
}

// assetURL builds a download URL from the download root, used as a fallback
// when a release asset has no browser_download_url.
func (u *Updater) assetURL(tag, name string) string {
	return u.cfg.DownloadRoot + "/" + u.cfg.Repo + "/releases/download/" +
		url.PathEscape(tag) + "/" + url.PathEscape(name)
}

// release fetches release metadata from the GitHub API.
func (u *Updater) release(ctx context.Context, path string) (*Release, error) {
	data, err := u.getBody(ctx, u.apiURL(path), "application/vnd.github+json")
	if err != nil {
		return nil, err
	}
	var rel Release
	if err := json.Unmarshal(data, &rel); err != nil {
		return nil, fmt.Errorf("parse release metadata: %w", err)
	}
	if rel.TagName == "" {
		return nil, fmt.Errorf("release metadata has no tag_name")
	}
	return &rel, nil
}

// get performs one GET with retries for transient failures. Callers must
// close the response body.
func (u *Updater) get(ctx context.Context, url, accept string) (*http.Response, error) {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", u.cfg.UserAgent)
		req.Header.Set("Accept", accept)
		if u.cfg.Token != "" {
			req.Header.Set("Authorization", "Bearer "+u.cfg.Token)
		}

		resp, err := u.cli.Do(req)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if attempt < maxAttempts-1 {
				time.Sleep(retryDelay(attempt))
			}
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("GET %s: %s", url, resp.Status)
			resp.Body.Close()
			if attempt < maxAttempts-1 {
				time.Sleep(retryDelay(attempt))
			}
			continue
		}
		return resp, nil
	}
	return nil, lastErr
}

// getBody fetches a URL and returns the (size-capped) response body.
func (u *Updater) getBody(ctx context.Context, url, accept string) ([]byte, error) {
	resp, err := u.get(ctx, url, accept)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, u.statusError(url, resp)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxMetadataBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxMetadataBytes {
		return nil, fmt.Errorf("response from %s exceeds %d bytes", url, maxMetadataBytes)
	}
	return data, nil
}

// statusError turns a non-200 response into an actionable error, detecting
// GitHub's anonymous rate limit so the operator knows to set a token.
func (u *Updater) statusError(url string, resp *http.Response) error {
	switch {
	case resp.StatusCode == http.StatusNotFound:
		if strings.Contains(url, "/releases/latest") {
			return fmt.Errorf("no GitHub releases found for %s", u.cfg.Repo)
		}
		return fmt.Errorf("GitHub resource not found: %s", resp.Status)
	case resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized:
		if resp.Header.Get("X-RateLimit-Remaining") == "0" {
			return fmt.Errorf("GitHub API rate limit exceeded (%s); set --github-token or $GITHUB_TOKEN to raise it", resp.Status)
		}
		return fmt.Errorf("GitHub rejected the request (%s); check --github-token", resp.Status)
	default:
		return fmt.Errorf("GitHub request failed: %s", resp.Status)
	}
}

// retryDelay is an exponential backoff with jitter for transient failures.
func retryDelay(attempt int) time.Duration {
	d := time.Duration(250*(1<<attempt)) * time.Millisecond
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	return d + time.Duration(rand.Intn(100))*time.Millisecond
}
