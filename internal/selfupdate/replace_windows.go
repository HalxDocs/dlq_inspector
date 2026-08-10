//go:build windows

package selfupdate

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

// replaceBinary swaps newPath into target. Windows locks a running
// executable, so a direct rename only succeeds when the target is not the
// binary executing this process. When it fails, a companion .cmd script is
// launched detached: it waits until dlq exits, renames the old binary away,
// moves the new one into place, and deletes itself.
func replaceBinary(newPath, target string) error {
	if err := os.Rename(newPath, target); err == nil {
		return nil
	}

	dir := filepath.Dir(target)
	oldPath := filepath.Join(dir, filepath.Base(target)+".old")
	scriptPath := filepath.Join(dir, ".dlq-update.cmd")

	script := "@echo off\r\n" +
		"setlocal\r\n" +
		"rem Awaiting dlq exit before swapping the updated binary.\r\n" +
		fmt.Sprintf("del \"%s\" >nul 2>&1\r\n", oldPath) +
		":loop\r\n" +
		fmt.Sprintf("ren \"%s\" \"%s\" >nul 2>&1\r\n", target, filepath.Base(oldPath)) +
		"if errorlevel 1 (\r\n" +
		"  timeout /t 1 /nobreak >nul\r\n" +
		"  goto loop\r\n" +
		")\r\n" +
		fmt.Sprintf("del \"%s\" >nul 2>&1\r\n", oldPath) +
		fmt.Sprintf("move /Y \"%s\" \"%s\" >nul 2>&1\r\n", newPath, target) +
		"del \"%~f0\" >nul 2>&1\r\n"

	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		return fmt.Errorf("write update script: %w", err)
	}

	cmd := exec.Command("cmd", "/c", "start", "", "cmd", "/c", scriptPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if err := cmd.Start(); err != nil {
		os.Remove(scriptPath)
		return fmt.Errorf("launch update script: %w", err)
	}
	_ = cmd.Process.Release()
	return errPendingUpdate
}
