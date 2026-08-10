package selfupdate

import (
	"fmt"
	"strconv"
	"strings"
)

// semver is the subset of semantic versioning needed to compare release tags.
type semver struct {
	Major int
	Minor int
	Patch int
	Pre   []string
}

// parseSemver parses "1.2.3", "v1.2.3", prereleases ("-rc.1"), and build
// metadata ("+meta", which is ignored for ordering). Non-release strings such
// as "dev" are rejected.
func parseSemver(s string) (semver, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "v")
	s = strings.TrimPrefix(s, "V")
	if i := strings.IndexByte(s, '+'); i >= 0 {
		s = s[:i]
	}
	core, preStr := s, ""
	if i := strings.IndexByte(s, '-'); i >= 0 {
		core, preStr = s[:i], s[i+1:]
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return semver{}, fmt.Errorf("%q is not a semantic version", s)
	}
	nums := make([]int, 3)
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return semver{}, fmt.Errorf("invalid version component %q in %q", p, s)
		}
		nums[i] = n
	}
	v := semver{Major: nums[0], Minor: nums[1], Patch: nums[2]}
	if preStr != "" {
		v.Pre = strings.Split(preStr, ".")
	}
	return v, nil
}

// Compare orders two versions: -1, 0, +1. A prerelease sorts before the same
// version without a prerelease (semver spec).
func (s semver) Compare(o semver) int {
	if c := cmpInt(s.Major, o.Major); c != 0 {
		return c
	}
	if c := cmpInt(s.Minor, o.Minor); c != 0 {
		return c
	}
	if c := cmpInt(s.Patch, o.Patch); c != 0 {
		return c
	}
	return cmpPrerelease(s.Pre, o.Pre)
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpPrerelease(a, b []string) int {
	if len(a) == 0 && len(b) == 0 {
		return 0
	}
	if len(a) == 0 {
		return 1
	}
	if len(b) == 0 {
		return -1
	}
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := cmpPreIdentifier(a[i], b[i]); c != 0 {
			return c
		}
	}
	return cmpInt(len(a), len(b))
}

func cmpPreIdentifier(a, b string) int {
	an, aNum := strconv.Atoi(a)
	bn, bNum := strconv.Atoi(b)
	switch {
	case aNum == nil && bNum == nil:
		return cmpInt(an, bn)
	case aNum == nil:
		return -1 // numeric identifiers sort before alphanumeric
	case bNum == nil:
		return 1
	default:
		return strings.Compare(a, b)
	}
}
