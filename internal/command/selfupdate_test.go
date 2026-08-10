package command

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/selfupdate"
)

// selfUpdateRecorder captures what the stubbed seams were asked to do.
type selfUpdateRecorder struct {
	resolveTag string
	performed  bool
}

// stubSelfUpdate replaces the self-update seams with deterministic stubs and
// returns a recorder so tests can assert on the command's behavior.
func stubSelfUpdate(t *testing.T, plan *selfupdate.Result) *selfUpdateRecorder {
	t.Helper()
	rec := &selfUpdateRecorder{}
	origNew, origResolve, origPlan, origPerform := selfUpdateNew, selfUpdateResolve, selfUpdatePlan, selfUpdatePerform
	t.Cleanup(func() {
		selfUpdateNew, selfUpdateResolve, selfUpdatePlan, selfUpdatePerform = origNew, origResolve, origPlan, origPerform
	})
	selfUpdateNew = func(version, token string) (*selfupdate.Updater, error) {
		return &selfupdate.Updater{}, nil
	}
	selfUpdateResolve = func(ctx context.Context, u *selfupdate.Updater, tag string) (*selfupdate.Release, error) {
		rec.resolveTag = tag
		return &selfupdate.Release{TagName: plan.LatestVersion}, nil
	}
	selfUpdatePlan = func(u *selfupdate.Updater, rel *selfupdate.Release) (*selfupdate.Result, error) {
		return plan, nil
	}
	selfUpdatePerform = func(ctx context.Context, u *selfupdate.Updater, rel *selfupdate.Release, opts selfupdate.UpdateOptions) (*selfupdate.Result, error) {
		rec.performed = true
		return plan, nil
	}
	return rec
}

func TestSelfUpdateCheckUpToDate(t *testing.T) {
	stubSelfUpdate(t, &selfupdate.Result{
		CurrentVersion:  "v1.0.0",
		LatestVersion:   "v1.0.0",
		UpdateAvailable: false,
		SkippedReason:   "already up to date",
	})
	out, err := runCommand(t, "self-update", "--check")
	if err != nil {
		t.Fatalf("self-update --check (up to date) should exit 0: %v", err)
	}
	if !strings.Contains(out, "Already up to date.") {
		t.Errorf("output = %q", out)
	}
}

func TestSelfUpdateCheckUpdateAvailable(t *testing.T) {
	stubSelfUpdate(t, &selfupdate.Result{
		CurrentVersion:  "v0.9.0",
		LatestVersion:   "v1.0.0",
		UpdateAvailable: true,
		AssetName:       "dlq-inspector_1.0.0_linux_amd64.tar.gz",
		InstallPath:     "/usr/local/bin/dlq",
	})
	out, err := runCommand(t, "self-update", "--check")
	var ec *ExitCodeError
	if !errors.As(err, &ec) || ec.Code != 1 {
		t.Fatalf("want exit code 1 (update available), got err=%v", err)
	}
	if !strings.Contains(out, "Update available.") {
		t.Errorf("output = %q", out)
	}
}

func TestSelfUpdateCheckFailure(t *testing.T) {
	rec := stubSelfUpdate(t, &selfupdate.Result{LatestVersion: "v1.0.0"})
	origPlan := selfUpdatePlan
	defer func() { selfUpdatePlan = origPlan }()
	selfUpdatePlan = func(u *selfupdate.Updater, rel *selfupdate.Release) (*selfupdate.Result, error) {
		return nil, errors.New("boom")
	}
	_, err := runCommand(t, "self-update", "--check")
	var ec *ExitCodeError
	if !errors.As(err, &ec) || ec.Code != 2 {
		t.Fatalf("want exit code 2 (check failed), got err=%v", err)
	}
	_ = rec
}

func TestSelfUpdateDryRun(t *testing.T) {
	rec := stubSelfUpdate(t, &selfupdate.Result{
		CurrentVersion:  "v0.9.0",
		LatestVersion:   "v1.0.0",
		UpdateAvailable: true,
		AssetName:       "dlq-inspector_1.0.0_linux_amd64.tar.gz",
		InstallPath:     "/usr/local/bin/dlq",
	})
	out, err := runCommand(t, "self-update")
	if err != nil {
		t.Fatalf("self-update: %v", err)
	}
	if !strings.Contains(out, "Re-run with --confirm") {
		t.Errorf("output = %q", out)
	}
	if rec.performed {
		t.Error("dry run must not install")
	}
}

func TestSelfUpdateConfirmInstalls(t *testing.T) {
	rec := stubSelfUpdate(t, &selfupdate.Result{
		CurrentVersion:  "v0.9.0",
		LatestVersion:   "v1.0.0",
		UpdateAvailable: true,
		AssetName:       "dlq-inspector_1.0.0_linux_amd64.tar.gz",
		InstallPath:     "/usr/local/bin/dlq",
		Installed:       true,
	})
	out, err := runCommand(t, "self-update", "--confirm", "--yes")
	if err != nil {
		t.Fatalf("self-update --confirm: %v", err)
	}
	if !strings.Contains(out, "Updated:") {
		t.Errorf("output = %q", out)
	}
	if !rec.performed {
		t.Error("confirmed update must call the installer")
	}
}

func TestSelfUpdateConfirmUpToDateDoesNothing(t *testing.T) {
	rec := stubSelfUpdate(t, &selfupdate.Result{
		CurrentVersion:  "v1.0.0",
		LatestVersion:   "v1.0.0",
		UpdateAvailable: false,
		SkippedReason:   "already up to date",
	})
	out, err := runCommand(t, "self-update", "--confirm", "--yes")
	if err != nil {
		t.Fatalf("self-update --confirm: %v", err)
	}
	if rec.performed {
		t.Error("up-to-date confirm must not install")
	}
	if !strings.Contains(out, "Already up to date.") {
		t.Errorf("output = %q", out)
	}
}

func TestSelfUpdateSpecificVersion(t *testing.T) {
	rec := stubSelfUpdate(t, &selfupdate.Result{
		CurrentVersion:  "v0.9.0",
		LatestVersion:   "v1.0.0",
		UpdateAvailable: true,
	})
	_, err := runCommand(t, "self-update", "--version", "v1.0.0", "--check")
	var ec *ExitCodeError
	if !errors.As(err, &ec) || ec.Code != 1 {
		t.Fatalf("want exit code 1, got err=%v", err)
	}
	if rec.resolveTag != "v1.0.0" {
		t.Errorf("resolve tag = %q, want v1.0.0", rec.resolveTag)
	}
}

func TestSelfUpdateJSON(t *testing.T) {
	stubSelfUpdate(t, &selfupdate.Result{
		CurrentVersion:  "v0.9.0",
		LatestVersion:   "v1.0.0",
		UpdateAvailable: true,
		AssetName:       "dlq-inspector_1.0.0_linux_amd64.tar.gz",
		InstallPath:     "/usr/local/bin/dlq",
	})
	out, err := runCommand(t, "self-update", "--output", "json")
	if err != nil {
		t.Fatalf("self-update --output json: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("output is not valid JSON %q: %v", out, err)
	}
	if m["update_available"] != true || m["latest_version"] != "v1.0.0" {
		t.Errorf("json = %v", m)
	}
	if strings.Contains(out, "Re-run with --confirm") {
		t.Errorf("JSON output must not be polluted by text hints: %q", out)
	}
}
