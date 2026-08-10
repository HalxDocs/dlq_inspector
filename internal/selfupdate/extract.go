package selfupdate

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

// extractBinary unpacks the archive at path and returns the dlq binary.
// Format is chosen by the asset name suffix (.zip vs .tar.gz).
func (u *Updater) extractBinary(assetName, path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if strings.HasSuffix(assetName, ".zip") {
		st, err := f.Stat()
		if err != nil {
			return nil, err
		}
		return extractZip(f, st.Size())
	}
	return extractTarGz(f)
}

// extractTarGz streams a tar.gz and returns the dlq binary entry, capped at
// maxBinaryBytes and refusing archives with too many entries.
func extractTarGz(r io.Reader) ([]byte, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("open gzip stream: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	count := 0
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read tar archive: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA {
			continue
		}
		count++
		if count > maxEntries {
			return nil, fmt.Errorf("archive contains more than %d files; refusing", maxEntries)
		}
		if isBinaryName(hdr.Name) {
			return readLimited(tr, maxBinaryBytes)
		}
	}
	return nil, fmt.Errorf("archive contains no dlq binary")
}

// extractZip reads the dlq binary entry from a zip archive.
func extractZip(ra io.ReaderAt, size int64) ([]byte, error) {
	zr, err := zip.NewReader(ra, size)
	if err != nil {
		return nil, fmt.Errorf("open zip archive: %w", err)
	}
	if len(zr.File) == 0 {
		return nil, fmt.Errorf("zip archive is empty")
	}
	if len(zr.File) > maxEntries {
		return nil, fmt.Errorf("archive contains more than %d files; refusing", maxEntries)
	}
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		if isBinaryName(f.Name) {
			if f.UncompressedSize64 > maxBinaryBytes {
				return nil, fmt.Errorf("binary in archive exceeds %d bytes", maxBinaryBytes)
			}
			rc, err := f.Open()
			if err != nil {
				return nil, err
			}
			defer rc.Close()
			return readLimited(rc, maxBinaryBytes)
		}
	}
	return nil, fmt.Errorf("archive contains no dlq binary")
}

// readLimited reads at most max+1 bytes and fails when the content exceeds
// max, so a zip bomb or corrupt entry cannot allocate unboundedly.
func readLimited(r io.Reader, max int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, fmt.Errorf("binary exceeds %d bytes", max)
	}
	return data, nil
}

// isBinaryName reports whether an archive entry is the dlq executable,
// whether it sits at the archive root or under a wrapper directory.
func isBinaryName(name string) bool {
	base := path.Base(strings.TrimPrefix(name, "./"))
	return base == "dlq" || base == "dlq.exe"
}
