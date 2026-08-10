package selfupdate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
)

// downloadedFile is a fully written temp file plus its sha256.
type downloadedFile struct {
	Path   string
	SHA256 string
	Size   int64
}

// downloadToTemp streams url to a temp file, computing its sha256 along the
// way and enforcing the archive size cap. The caller owns the temp file.
func (u *Updater) downloadToTemp(ctx context.Context, url string) (*downloadedFile, error) {
	resp, err := u.get(ctx, url, "application/octet-stream")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, u.statusError(url, resp)
	}

	f, err := os.CreateTemp("", "dlq-download-*")
	if err != nil {
		return nil, err
	}
	name := f.Name()
	cleanup := func() {
		f.Close()
		os.Remove(name)
	}

	h := sha256.New()
	written, err := copyVerified(io.MultiWriter(f, h), resp.Body, maxArchiveBytes)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("download %s: %w", url, err)
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return nil, fmt.Errorf("sync download: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(name)
		return nil, err
	}
	return &downloadedFile{Path: name, SHA256: hex.EncodeToString(h.Sum(nil)), Size: written}, nil
}

// copyVerified copies src to dst, failing when more than max bytes arrive.
func copyVerified(dst io.Writer, src io.Reader, max int64) (int64, error) {
	written, err := io.Copy(dst, io.LimitReader(src, max+1))
	if err != nil {
		return written, err
	}
	if written > max {
		return written, fmt.Errorf("response exceeds %d bytes", max)
	}
	return written, nil
}
