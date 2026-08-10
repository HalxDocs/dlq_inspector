package selfupdate

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// checksumFor returns the expected sha256 of archive name from the release's
// checksums.txt. The update refuses to proceed when checksums.txt is missing
// or has no entry for the archive.
func (u *Updater) checksumFor(ctx context.Context, release *Release, name string) (string, error) {
	asset, ok := findAsset(release.Assets, "checksums.txt")
	if !ok {
		return "", fmt.Errorf("release %s has no checksums.txt asset; refusing to update without a checksum to verify against", release.TagName)
	}
	url := asset.URL
	if url == "" {
		url = u.assetURL(release.TagName, asset.Name)
	}
	data, err := u.getBody(ctx, url, "application/octet-stream")
	if err != nil {
		return "", err
	}
	sums, err := ParseChecksums(data)
	if err != nil {
		return "", fmt.Errorf("checksums.txt: %w", err)
	}
	expected, ok := sums[name]
	if !ok {
		return "", fmt.Errorf("checksums.txt has no entry for %s", name)
	}
	return expected, nil
}

func findAsset(assets []Asset, name string) (Asset, bool) {
	for _, a := range assets {
		if a.Name == name {
			return a, true
		}
	}
	return Asset{}, false
}

// ParseChecksums parses a goreleaser checksums.txt: one "<sha256>  <file>"
// per line. Lines with fewer than two fields are skipped. A leading "*" on
// the hash (the bsdtar form) is accepted.
func ParseChecksums(data []byte) (map[string]string, error) {
	sums := make(map[string]string)
	sc := bufio.NewScanner(bytes.NewReader(data))
	lineNo := 0
	for sc.Scan() {
		lineNo++
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		// bsdtar writes the star on the filename ("hash *file"); accept it on
		// either field so both the goreleaser and bsdtar forms parse.
		sum := strings.TrimPrefix(fields[0], "*")
		name := strings.TrimPrefix(fields[1], "*")
		if len(sum) != sha256.Size*2 {
			return nil, fmt.Errorf("line %d: invalid sha256 %q", lineNo, sum)
		}
		if _, err := hex.DecodeString(sum); err != nil {
			return nil, fmt.Errorf("line %d: invalid sha256 %q: %w", lineNo, sum, err)
		}
		sums[name] = strings.ToLower(sum)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return sums, nil
}
