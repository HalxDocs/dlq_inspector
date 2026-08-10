package selfupdate

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func makeTarGz(t *testing.T, name string, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func makeZip(t *testing.T, name string, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func writeTemp(t *testing.T, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "archive")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestExtractTarGz(t *testing.T) {
	bin := []byte("tarbinary")
	arch := makeTarGz(t, "dlq", bin)
	got, err := (&Updater{}).extractBinary("dlq-inspector_1.0.0_linux_amd64.tar.gz", writeTemp(t, arch))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if !bytes.Equal(got, bin) {
		t.Errorf("got %q, want %q", got, bin)
	}
}

func TestExtractTarGzNested(t *testing.T) {
	bin := []byte("nestedbinary")
	arch := makeTarGz(t, "dlq-inspector_1.0.0_linux_amd64/dlq", bin)
	got, err := (&Updater{}).extractBinary("dlq-inspector_1.0.0_linux_amd64.tar.gz", writeTemp(t, arch))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if !bytes.Equal(got, bin) {
		t.Errorf("got %q, want %q", got, bin)
	}
}

func TestExtractZip(t *testing.T) {
	bin := []byte("zipbinary")
	arch := makeZip(t, "dlq.exe", bin)
	got, err := (&Updater{}).extractBinary("dlq-inspector_1.0.0_windows_amd64.zip", writeTemp(t, arch))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if !bytes.Equal(got, bin) {
		t.Errorf("got %q, want %q", got, bin)
	}
}

func TestExtractRejectsArchiveWithoutBinary(t *testing.T) {
	arch := makeTarGz(t, "README.md", []byte("nothing useful"))
	_, err := (&Updater{}).extractBinary("dlq-inspector_1.0.0_linux_amd64.tar.gz", writeTemp(t, arch))
	if err == nil || !strings.Contains(err.Error(), "no dlq binary") {
		t.Fatalf("err = %v, want 'no dlq binary'", err)
	}
}

func TestIsBinaryName(t *testing.T) {
	for _, name := range []string{"dlq", "dlq.exe", "./dlq", "some/dir/dlq.exe"} {
		if !isBinaryName(name) {
			t.Errorf("isBinaryName(%q) = false", name)
		}
	}
	for _, name := range []string{"dlq2", "README.md", "dlq-inspector", "sub/dlq.bak"} {
		if isBinaryName(name) {
			t.Errorf("isBinaryName(%q) = true", name)
		}
	}
}
