package selfupdate

import (
	"strings"
	"testing"
)

func checksumHex() string {
	// 64 hex chars: sha256("test")
	return "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
}

func TestParseChecksums(t *testing.T) {
	data := []byte(
		checksumHex() + "  dlq-inspector_1.2.3_linux_amd64.tar.gz\n" +
			checksumHex() + " *dlq-inspector_1.2.3_windows_amd64.zip\n" +
			"\n" +
			"#comment\n" +
			"not-enough-fields\n",
	)
	sums, err := ParseChecksums(data)
	if err != nil {
		t.Fatalf("ParseChecksums: %v", err)
	}
	if len(sums) != 2 {
		t.Fatalf("sums = %d entries, want 2", len(sums))
	}
	if got := sums["dlq-inspector_1.2.3_linux_amd64.tar.gz"]; got != checksumHex() {
		t.Errorf("linux sum = %q", got)
	}
	if got := sums["dlq-inspector_1.2.3_windows_amd64.zip"]; got != checksumHex() {
		t.Errorf("windows sum = %q", got)
	}
}

func TestParseChecksumsRejectsBadLine(t *testing.T) {
	data := []byte("short dlq-inspector_1.2.3_linux_amd64.tar.gz\n")
	if _, err := ParseChecksums(data); err == nil {
		t.Fatal("expected error for a short checksum")
	}
}

func TestParseChecksumsRejectsNonHex(t *testing.T) {
	data := []byte("zzzz" + strings.Repeat("0", 60) + "  file.tar.gz\n")
	if _, err := ParseChecksums(data); err == nil {
		t.Fatal("expected error for a non-hex checksum")
	}
}
