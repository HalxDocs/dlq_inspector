package selfupdate

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestRateLimitDetected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "0")
		http.Error(w, "rate limited", http.StatusForbidden)
	}))
	defer srv.Close()

	u, err := New(Config{Version: "0.1.0", APIRoot: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = u.Resolve(context.Background(), "")
	if err == nil || !strings.Contains(err.Error(), "rate limit exceeded") {
		t.Fatalf("Resolve err = %v, want rate-limit error mentioning a token", err)
	}
}

func TestNewValidatesRepo(t *testing.T) {
	if _, err := New(Config{Repo: "not-a-slash"}); err == nil {
		t.Error("New with an invalid repo must fail")
	}
	if _, err := New(Config{}); err != nil {
		t.Errorf("New with defaults: %v", err)
	}
}

func TestUpdateMissingPlatformAsset(t *testing.T) {
	bin := []byte("fake")
	archive := makeTarGz(t, "dlq", bin)
	srv := newReleaseServer(t, "v1.2.3", "dlq-inspector_1.2.3_linux_amd64.tar.gz", archive, nil)
	defer srv.Close()

	// The release only carries a linux_amd64 archive; ask for windows_amd64.
	u := newTestUpdater(t, Config{
		Version: "v1.2.2", Goos: "windows", Goarch: "amd64",
		APIRoot: srv.URL, DownloadRoot: srv.URL,
	})
	rel, err := u.Resolve(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = u.Update(context.Background(), rel, UpdateOptions{InstallPath: filepath.Join(t.TempDir(), "dlq.exe")})
	if err == nil || !strings.Contains(err.Error(), "no dlq-inspector release archive") {
		t.Fatalf("Update err = %v, want missing-asset error", err)
	}
}

func TestAssetForListsAvailable(t *testing.T) {
	u := newTestUpdater(t, Config{Version: "v1.2.2", Goos: "linux", Goarch: "amd64"})
	rel := &Release{
		TagName: "v1.2.3",
		Assets:  []Asset{{Name: "dlq-inspector_1.2.3_solaris_sparc.tar.gz"}, {Name: "checksums.txt"}},
	}
	_, err := u.AssetFor(rel)
	if err == nil || !strings.Contains(err.Error(), "assets:") {
		t.Fatalf("AssetFor err = %v, want error naming available assets", err)
	}
}
