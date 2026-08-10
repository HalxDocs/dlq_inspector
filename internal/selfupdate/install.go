package selfupdate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// errPendingUpdate signals that the new binary was staged and the swap is
// scheduled to complete after the current process exits (Windows).
var errPendingUpdate = errors.New("update staged; replacement scheduled after process exit")

// installBinary writes data to target via a temp file in the same directory
// and swaps it into place. The swap is atomic on POSIX; on Windows, when the
// running binary is locked, the replacement is deferred to a companion script
// (see replace_windows.go).
func installBinary(data []byte, target string) error {
	dir := filepath.Dir(target)
	tmp, err := os.CreateTemp(dir, ".dlq-update-*")
	if err != nil {
		return fmt.Errorf("create temp file in %s: %w", dir, err)
	}
	name := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(name)
		return fmt.Errorf("write new binary: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(name)
		return fmt.Errorf("sync new binary: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(name)
		return fmt.Errorf("close new binary: %w", err)
	}
	if err := os.Chmod(name, 0o755); err != nil {
		os.Remove(name)
		return fmt.Errorf("chmod new binary: %w", err)
	}

	err = replaceBinary(name, target)
	if errors.Is(err, errPendingUpdate) {
		// The companion script owns the temp file now.
		return errPendingUpdate
	}
	os.Remove(name)
	if err != nil {
		return fmt.Errorf("replace %s: %w", target, err)
	}
	return nil
}
