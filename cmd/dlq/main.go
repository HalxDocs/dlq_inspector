// Command dlq is the entrypoint for the DLQ Inspector CLI.
package main

import (
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
		os.Exit(1)
	}
}
