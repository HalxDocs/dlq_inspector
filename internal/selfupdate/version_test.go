package selfupdate

import "testing"

func TestParseSemver(t *testing.T) {
	cases := []struct {
		in string
		ok bool
	}{
		{"1.2.3", true},
		{"v1.2.3", true},
		{"V1.2.3", true},
		{"1.2.3-rc.1", true},
		{"v1.2.3-alpha.1+build5", true},
		{"1.2.3+build", true},
		{"  v1.2.3  ", true},
		{"1.2", false},
		{"abc", false},
		{"1.2.x", false},
		{"1.2.3.4", false},
		{"dev", false},
		{"", false},
	}
	for _, c := range cases {
		_, err := parseSemver(c.in)
		if (err == nil) != c.ok {
			t.Errorf("parseSemver(%q) ok=%v, want ok=%v (err=%v)", c.in, err == nil, c.ok, err)
		}
	}
}

func TestVersionCompare(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"1.0.0", "1.0.0", 0},
		{"v1.0.0", "1.0.0", 0},
		{"1.0.0", "1.0.1", -1},
		{"1.0.1", "1.0.0", 1},
		{"1.2.0", "1.3.0", -1},
		{"2.0.0", "10.0.0", -1},
		{"1.0.0-rc.1", "1.0.0", -1},
		{"1.0.0", "1.0.0-rc.1", 1},
		{"1.0.0-alpha", "1.0.0-beta", -1},
		{"1.0.0-alpha.1", "1.0.0-alpha.2", -1},
		{"1.0.0-alpha.10", "1.0.0-alpha.2", 1},
		{"1.0.0-rc.1", "1.0.0-rc.2", -1},
	}
	for _, c := range cases {
		va, err := parseSemver(c.a)
		if err != nil {
			t.Fatalf("parse %q: %v", c.a, err)
		}
		vb, err := parseSemver(c.b)
		if err != nil {
			t.Fatalf("parse %q: %v", c.b, err)
		}
		if got := va.Compare(vb); got != c.want {
			t.Errorf("Compare(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestUpdateAvailable(t *testing.T) {
	cases := []struct {
		current, latest string
		want            bool
	}{
		{"v1.0.0", "v1.0.1", true},
		{"v1.0.1", "v1.0.1", false},
		{"v1.1.0", "v1.0.0", false},
		{"v1.0.0-rc.1", "v1.0.0", true},
		{"dev", "v1.0.0", true},
		{"", "v1.0.0", true},
	}
	for _, c := range cases {
		got, err := UpdateAvailable(c.current, c.latest)
		if err != nil {
			t.Fatalf("UpdateAvailable(%q, %q): %v", c.current, c.latest, err)
		}
		if got != c.want {
			t.Errorf("UpdateAvailable(%q, %q) = %v, want %v", c.current, c.latest, got, c.want)
		}
	}
}

func TestUpdateAvailableRejectsBadLatest(t *testing.T) {
	if _, err := UpdateAvailable("v1.0.0", "not-a-version"); err == nil {
		t.Error("expected error for unparsable latest version")
	}
}
