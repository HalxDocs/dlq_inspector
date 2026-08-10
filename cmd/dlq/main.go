// Command dlq is the entrypoint for the DLQ Inspector CLI.
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/HalxDocs/dlq_inspector/internal/command"
)

// Build-time version metadata:
//
//	go build -ldflags "-X main.version=v1.0.0 -X main.commit=$(git rev-parse --short HEAD) -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	root := command.NewRoot(version, commit, date)
	if err := root.Execute(); err != nil {
		// self-update --check uses a specific exit code so scripts can
		// branch on whether an update is available; the underlying error is
		// still printed so exit 2 carries the reason.
		var ec *command.ExitCodeError
		if errors.As(err, &ec) {
			if ec.Err != nil {
				fmt.Fprintln(os.Stderr, "Error:", ec.Err)
			}
			os.Exit(ec.Code)
		}
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}
