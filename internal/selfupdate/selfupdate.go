// Package selfupdate implements `dlq self-update`: resolving the latest (or a
// pinned) GitHub release for this project, downloading the archive that
// matches the running platform, verifying its sha256 against the release's
// checksums.txt before anything is unpacked or replaced, and swapping the
// running binary into place.
package selfupdate

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// Release is the subset of a GitHub release the updater needs.
type Release struct {
	TagName string  `json:"tag_name"`
	Assets  []Asset `json:"assets"`
}

// Asset is one downloadable file attached to a release.
type Asset struct {
	Name string `json:"name"`
	URL  string `json:"browser_download_url"`
}

// Result describes the outcome of a check or an update.
type Result struct {
	CurrentVersion  string `json:"current_version"`
	LatestVersion   string `json:"latest_version"`
	UpdateAvailable bool   `json:"update_available"`
	AssetName       string `json:"asset,omitempty"`
	InstallPath     string `json:"install_path,omitempty"`
	Installed       bool   `json:"installed"`
	Pending         bool   `json:"pending,omitempty"`
	SkippedReason   string `json:"skipped_reason,omitempty"`
}

// UpdateOptions control a single Update call.
type UpdateOptions struct {
	// Force installs even when already at the requested version.
	Force bool
	// InstallPath overrides the binary that gets replaced. Empty means the
	// currently executing binary.
	InstallPath string
}

// Updater resolves releases, verifies, and installs updates.
type Updater struct {
	cfg Config
	cli *http.Client
}

// Safety caps: real release archives are a few MB, but bound every read so a
// malicious or corrupted release cannot exhaust memory or disk.
const (
	maxMetadataBytes = 4 << 20
	maxArchiveBytes  = 1 << 30
	maxBinaryBytes   = 512 << 20
	maxEntries       = 16
)

// AssetFor returns the archive asset matching the configured platform, or an
// error naming the available assets when none matches.
func (u *Updater) AssetFor(release *Release) (Asset, error) {
	ext := ".tar.gz"
	if u.cfg.Goos == "windows" {
		ext = ".zip"
	}
	suffix := "_" + u.cfg.Goos + "_" + u.cfg.Goarch + ext
	for _, a := range release.Assets {
		if !strings.HasPrefix(a.Name, u.cfg.Project+"_") || !strings.HasSuffix(a.Name, suffix) {
			continue
		}
		return a, nil
	}
	return Asset{}, fmt.Errorf("no %s release archive for %s/%s in release %s (assets: %s)",
		u.cfg.Project, u.cfg.Goos, u.cfg.Goarch, release.TagName, assetNames(release.Assets))
}

// UpdateAvailable reports whether current is older than latest. A current
// version that is not a released semantic version (e.g. the "dev" default of
// an unreleased build) is treated as older than any release.
func UpdateAvailable(current, latest string) (bool, error) {
	lat, err := parseSemver(latest)
	if err != nil {
		return false, fmt.Errorf("latest version %q is not a semantic version: %w", latest, err)
	}
	cur, err := parseSemver(current)
	if err != nil {
		return current != latest, nil
	}
	return cur.Compare(lat) < 0, nil
}

// Plan reports whether an update is available for the release without
// downloading anything.
func (u *Updater) Plan(release *Release) (*Result, error) {
	asset, err := u.AssetFor(release)
	if err != nil {
		return nil, err
	}
	res := &Result{
		CurrentVersion: u.cfg.Version,
		LatestVersion:  release.TagName,
		AssetName:      asset.Name,
	}
	if exe, err := os.Executable(); err == nil {
		res.InstallPath = exe
	}
	available, err := UpdateAvailable(u.cfg.Version, release.TagName)
	if err != nil {
		return nil, err
	}
	res.UpdateAvailable = available
	if !available {
		res.SkippedReason = "already up to date"
	}
	return res, nil
}

// Update downloads, verifies, and installs the release. The archive's sha256
// is checked against the release checksums.txt before anything is unpacked;
// a mismatch or a missing checksum refuses the update.
func (u *Updater) Update(ctx context.Context, release *Release, opts UpdateOptions) (*Result, error) {
	installPath := opts.InstallPath
	if installPath == "" {
		exe, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("resolve current executable: %w", err)
		}
		installPath = exe
	}

	asset, err := u.AssetFor(release)
	if err != nil {
		return nil, err
	}

	res := &Result{
		CurrentVersion: u.cfg.Version,
		LatestVersion:  release.TagName,
		AssetName:      asset.Name,
		InstallPath:    installPath,
	}

	available, err := UpdateAvailable(u.cfg.Version, release.TagName)
	if err != nil {
		return nil, err
	}
	if !available && !opts.Force {
		res.SkippedReason = "already up to date"
		return res, nil
	}
	res.UpdateAvailable = true

	expected, err := u.checksumFor(ctx, release, asset.Name)
	if err != nil {
		return nil, err
	}

	url := asset.URL
	if url == "" {
		url = u.assetURL(release.TagName, asset.Name)
	}
	dl, err := u.downloadToTemp(ctx, url)
	if err != nil {
		return nil, err
	}
	defer os.Remove(dl.Path)

	if !strings.EqualFold(dl.SHA256, expected) {
		return nil, fmt.Errorf("checksum mismatch for %s: checksums.txt says %s, downloaded %s — refusing to install",
			asset.Name, expected, dl.SHA256)
	}

	bin, err := u.extractBinary(asset.Name, dl.Path)
	if err != nil {
		return nil, err
	}

	if err := installBinary(bin, installPath); err != nil {
		if errors.Is(err, errPendingUpdate) {
			res.Pending = true
			return res, nil
		}
		return nil, err
	}
	res.Installed = true
	return res, nil
}

func assetNames(assets []Asset) string {
	names := make([]string, 0, len(assets))
	for _, a := range assets {
		names = append(names, a.Name)
	}
	return strings.Join(names, ", ")
}
