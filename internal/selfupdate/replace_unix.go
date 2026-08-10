//go:build !windows

package selfupdate

import "os"

// replaceBinary swaps newPath into target atomically. On POSIX, renaming over
// a running executable is safe — the running process keeps its inode.
func replaceBinary(newPath, target string) error {
	return os.Rename(newPath, target)
}
