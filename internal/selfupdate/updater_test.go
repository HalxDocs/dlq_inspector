package selfupdate

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// newReleaseServer serves a fake GitHub API (latest release metadata) and the
// release's download assets. tag and assetName describe the release; archive
// and checksums are served verbatim under /download/<tag>/.
func newReleaseServer(t *testing.T, tag, assetName string, archive, checksums []byte) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	dlURL := func(name string) string {
		return srv.URL + "/download/" + tag + "/" + name
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/HalxDocs/dlq_inspector/releases/latest", func(w http.ResponseWriter, r *http.Request) {
		assets := []map[string]string{
			{"name": "checksums.txt", "browser_download_url": dlURL("checksums.txt")},
			{"name": assetName, "browser_download_url": dlURL(assetName)},
		}
		body, _ := json.Marshal(map[string]any{"tag_name": tag, "assets": assets})
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	})
	mux.HandleFunc("/download/", func(w http.ResponseWriter, r *http.Request) {
		name := path.Base(r.URL.Path)
		switch name {
		case "checksums.txt":
			w.Write(checksums)
		default:
			w.Write(archive)
		}
	})
	srv = httptest.NewServer(mux)
	return srv
}

func newTestUpdater(t *testing.T, cfg Config) *Updater {
	t.Helper()
	cfg.Repo = "HalxDocs/dlq_inspector"
	cfg.Project = "dlq-inspector"
	u, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestUpdateEndToEnd(t *testing.T) {
	bin := []byte("#!/bin/sh\nfake dlq binary\n")
	archive := makeTarGz(t, "dlq", bin)
	sum := sha256.Sum256(archive)
	checksums := []byte(fmt.Sprintf("%x  dlq-inspector_1.2.3_linux_amd64.tar.gz\n", sum))

	srv := newReleaseServer(t, "v1.2.3", "dlq-inspector_1.2.3_linux_amd64.tar.gz", archive, checksums)
	defer srv.Close()

	installPath := filepath.Join(t.TempDir(), "dlq")
	u := newTestUpdater(t, Config{
		Version:      "v1.2.2",
		Goos:         "linux",
		Goarch:       "amd64",
		APIRoot:      srv.URL,
		DownloadRoot: srv.URL,
	})

	rel, err := u.Resolve(context.Background(), "")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if rel.TagName != "v1.2.3" {
		t.Fatalf("tag = %q, want v1.2.3", rel.TagName)
	}

	res, err := u.Update(context.Background(), rel, UpdateOptions{InstallPath: installPath})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !res.Installed || !res.UpdateAvailable {
		t.Fatalf("unexpected result: %+v", res)
	}
	got, err := os.ReadFile(installPath)
	if err != nil {
		t.Fatalf("read installed binary: %v", err)
	}
	if !stringBytesEqual(got, bin) {
		t.Errorf("installed binary mismatch")
	}
	if runtime.GOOS != "windows" {
		st, err := os.Stat(installPath)
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm()&0o111 == 0 {
			t.Errorf("installed binary is not executable: %v", st.Mode())
		}
	}
}

func TestUpdateRejectsChecksumMismatch(t *testing.T) {
	bin := []byte("fake")
	archive := makeTarGz(t, "dlq", bin)
	bad := []byte("0000000000000000000000000000000000000000000000000000000000000000  dlq-inspector_1.2.3_linux_amd64.tar.gz\n")
	srv := newReleaseServer(t, "v1.2.3", "dlq-inspector_1.2.3_linux_amd64.tar.gz", archive, bad)
	defer srv.Close()

	installPath := filepath.Join(t.TempDir(), "dlq")
	u := newTestUpdater(t, Config{
		Version:      "v1.2.2",
		Goos:         "linux",
		Goarch:       "amd64",
		APIRoot:      srv.URL,
		DownloadRoot: srv.URL,
	})
	rel, err := u.Resolve(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = u.Update(context.Background(), rel, UpdateOptions{InstallPath: installPath})
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("want checksum mismatch error, got %v", err)
	}
	if _, err := os.Stat(installPath); !os.IsNotExist(err) {
		t.Error("install path must not exist after a refused update")
	}
}

func TestUpdateRefusesWithoutChecksums(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := json.Marshal(map[string]any{
			"tag_name": "v1.2.3",
			"assets": []map[string]string{
				{"name": "dlq-inspector_1.2.3_linux_amd64.tar.gz", "browser_download_url": "http://127.0.0.1:1/archive"},
			},
		})
		w.Write(body)
	}))
	defer srv.Close()

	u := newTestUpdater(t, Config{
		Version: "v1.2.2", Goos: "linux", Goarch: "amd64",
		APIRoot: srv.URL, DownloadRoot: srv.URL,
	})
	rel, err := u.Resolve(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = u.Update(context.Background(), rel, UpdateOptions{InstallPath: filepath.Join(t.TempDir(), "dlq")})
	if err == nil || !strings.Contains(err.Error(), "no checksums.txt") {
		t.Fatalf("want missing-checksums error, got %v", err)
	}
}

func TestUpdateUpToDateDoesNothing(t *testing.T) {
	installPath := filepath.Join(t.TempDir(), "dlq")
	if err := os.WriteFile(installPath, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	u := newTestUpdater(t, Config{
		Version: "v1.2.3", Goos: "linux", Goarch: "amd64",
		APIRoot: "http://127.0.0.1:1", DownloadRoot: "http://127.0.0.1:1",
	})
	rel := &Release{TagName: "v1.2.3", Assets: []Asset{{Name: "dlq-inspector_1.2.3_linux_amd64.tar.gz", URL: "http://127.0.0.1:1/x"}}}
	res, err := u.Update(context.Background(), rel, UpdateOptions{InstallPath: installPath})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if res.UpdateAvailable || res.SkippedReason != "already up to date" {
		t.Errorf("result = %+v", res)
	}
	got, _ := os.ReadFile(installPath)
	if !stringBytesEqual(got, []byte("old")) {
		t.Error("up-to-date update must not touch the binary")
	}
}

func TestUpdateForceInstallsSameVersion(t *testing.T) {
	bin := []byte("force")
	archive := makeTarGz(t, "dlq", bin)
	sum := sha256.Sum256(archive)
	checksums := []byte(fmt.Sprintf("%x  dlq-inspector_1.2.3_linux_amd64.tar.gz\n", sum))
	srv := newReleaseServer(t, "v1.2.3", "dlq-inspector_1.2.3_linux_amd64.tar.gz", archive, checksums)
	defer srv.Close()

	installPath := filepath.Join(t.TempDir(), "dlq")
	u := newTestUpdater(t, Config{
		Version: "v1.2.3", Goos: "linux", Goarch: "amd64",
		APIRoot: srv.URL, DownloadRoot: srv.URL,
	})
	rel, err := u.Resolve(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	res, err := u.Update(context.Background(), rel, UpdateOptions{InstallPath: installPath, Force: true})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !res.Installed {
		t.Fatalf("forced install did not run: %+v", res)
	}
}

func TestPlanUpToDate(t *testing.T) {
	u := newTestUpdater(t, Config{Version: "v1.2.3", Goos: "linux", Goarch: "amd64"})
	rel := &Release{
		TagName: "v1.2.3",
		Assets:  []Asset{{Name: "dlq-inspector_1.2.3_linux_amd64.tar.gz"}},
	}
	res, err := u.Plan(rel)
	if err != nil {
		t.Fatal(err)
	}
	if res.UpdateAvailable {
		t.Errorf("expected no update: %+v", res)
	}
	if res.SkippedReason != "already up to date" {
		t.Errorf("skipped_reason = %q", res.SkippedReason)
	}
}

func TestPlanMissingPlatformAsset(t *testing.T) {
	u := newTestUpdater(t, Config{Version: "v1.2.2", Goos: "solaris", Goarch: "sparc"})
	rel := &Release{
		TagName: "v1.2.3",
		Assets:  []Asset{{Name: "dlq-inspector_1.2.3_linux_amd64.tar.gz"}},
	}
	if _, err := u.Plan(rel); err == nil {
		t.Fatal("expected an error for a platform with no asset")
	}
}

func TestAssetForSelectsZipOnWindows(t *testing.T) {
	u := newTestUpdater(t, Config{Version: "v1.2.2", Goos: "windows", Goarch: "amd64"})
	rel := &Release{
		TagName: "v1.2.3",
		Assets: []Asset{
			{Name: "dlq-inspector_1.2.3_windows_amd64.zip"},
			{Name: "dlq-inspector_1.2.3_linux_amd64.tar.gz"},
		},
	}
	asset, err := u.AssetFor(rel)
	if err != nil {
		t.Fatal(err)
	}
	if asset.Name != "dlq-inspector_1.2.3_windows_amd64.zip" {
		t.Errorf("asset = %q", asset.Name)
	}
}

func TestResolveByTag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/releases/tags/v9.9.9") {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		body, _ := json.Marshal(map[string]any{"tag_name": "v9.9.9", "assets": []any{}})
		w.Write(body)
	}))
	defer srv.Close()

	u := newTestUpdater(t, Config{APIRoot: srv.URL, DownloadRoot: srv.URL})
	rel, err := u.Resolve(context.Background(), "v9.9.9")
	if err != nil {
		t.Fatal(err)
	}
	if rel.TagName != "v9.9.9" {
		t.Errorf("tag = %q", rel.TagName)
	}
}

func stringBytesEqual(a, b []byte) bool {
	return string(a) == string(b)
}
