package command

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/selfupdate"
)

// Exit codes for `dlq self-update --check`.
const (
	exitUpToDate        = 0
	exitUpdateAvailable = 1
	exitCheckFailed     = 2
)

// ExitCodeError lets a command request a specific process exit code;
// cmd/dlq's main translates it into os.Exit. self-update --check uses it so
// scripts can branch on whether an update is available.
type ExitCodeError struct {
	Code int
	Err  error
}

func (e *ExitCodeError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return fmt.Sprintf("exit code %d", e.Code)
}

// Unwrap exposes the wrapped check error.
func (e *ExitCodeError) Unwrap() error { return e.Err }

// self-update seams, overridden in tests so the command layer runs without
// network access.
var (
	selfUpdateNew = func(version, token string) (*selfupdate.Updater, error) {
		return selfupdate.New(selfupdate.Config{Version: version, Token: token})
	}
	selfUpdateResolve = func(ctx context.Context, u *selfupdate.Updater, tag string) (*selfupdate.Release, error) {
		return u.Resolve(ctx, tag)
	}
	selfUpdatePlan = func(u *selfupdate.Updater, rel *selfupdate.Release) (*selfupdate.Result, error) {
		return u.Plan(rel)
	}
	selfUpdatePerform = func(ctx context.Context, u *selfupdate.Updater, rel *selfupdate.Release, opts selfupdate.UpdateOptions) (*selfupdate.Result, error) {
		return u.Update(ctx, rel, opts)
	}
)

func newSelfUpdateCmd(version string, opts *GlobalOptions) *cobra.Command {
	var (
		check     bool
		confirm   bool
		yes       bool
		force     bool
		targetTag string
		token     string
	)

	cmd := &cobra.Command{
		Use:   "self-update",
		Short: "Update dlq to the latest GitHub release",
		Long: `Update dlq to the latest GitHub release for your platform.

The release archive and its checksums.txt are fetched from GitHub. The
archive's sha256 is verified against checksums.txt BEFORE anything is unpacked
or replaced; a mismatch (or a missing checksum) refuses the update. On Linux
and macOS the running binary is swapped atomically; on Windows the replacement
completes after dlq exits.

Without --confirm this only checks: it reports the latest version and what
would be installed, and changes nothing. --check is the scriptable form — it
exits 0 when already up to date, 1 when an update is available, and 2 when the
check could not be completed.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			if token == "" {
				token = os.Getenv("GITHUB_TOKEN")
			}

			u, err := selfUpdateNew(version, token)
			if err != nil {
				return checkError(check, err)
			}

			rel, err := selfUpdateResolve(ctx, u, targetTag)
			if err != nil {
				return checkError(check, err)
			}

			res, err := selfUpdatePlan(u, rel)
			if err != nil {
				return checkError(check, err)
			}

			if opts.Output == "json" {
				if err := writeJSON(cmd, res); err != nil {
					return err
				}
			} else {
				renderSelfUpdateStatus(cmd, res)
			}

			if check {
				if res.UpdateAvailable {
					return &ExitCodeError{Code: exitUpdateAvailable}
				}
				return nil
			}

			if !res.UpdateAvailable && !force {
				return nil
			}

			if !confirm {
				if opts.Output != "json" {
					fmt.Fprintln(cmd.OutOrStdout(), "\nRe-run with --confirm to download and install.")
				}
				return nil
			}

			if !yes && stdinIsTerminal() {
				ok, err := promptConfirm(cmd, fmt.Sprintf("Download and install dlq %s over %s? [y/N] ", res.LatestVersion, res.InstallPath))
				if err != nil {
					return err
				}
				if !ok {
					fmt.Fprintln(cmd.OutOrStdout(), "Update cancelled.")
					return nil
				}
			}

			res, err = selfUpdatePerform(ctx, u, rel, selfupdate.UpdateOptions{Force: force})
			if err != nil {
				return err
			}

			if opts.Output == "json" {
				return writeJSON(cmd, res)
			}
			return renderSelfUpdateResult(cmd, res)
		},
	}

	cmd.Flags().BoolVar(&check, "check", false, "check for an update and exit (0 = up to date, 1 = update available, 2 = check failed)")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "download, verify, and install the update")
	cmd.Flags().BoolVar(&yes, "yes", false, "skip the interactive confirmation prompt")
	cmd.Flags().BoolVar(&force, "force", false, "install even when already at the requested version")
	cmd.Flags().StringVar(&targetTag, "version", "", "update to a specific release tag (e.g. v1.2.3) instead of the latest")
	cmd.Flags().StringVar(&token, "github-token", "", "GitHub token for higher API rate limits (default $GITHUB_TOKEN)")
	return cmd
}

// checkError wraps err as an ExitCodeError when running in --check mode so the
// failure surfaces as exit code 2 instead of the generic 1.
func checkError(check bool, err error) error {
	if check {
		return &ExitCodeError{Code: exitCheckFailed, Err: err}
	}
	return err
}

func renderSelfUpdateStatus(cmd *cobra.Command, res *selfupdate.Result) {
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Current version:\t%s\n", res.CurrentVersion)
	fmt.Fprintf(tw, "Latest version:\t%s\n", res.LatestVersion)
	if res.UpdateAvailable {
		fmt.Fprintf(tw, "Archive:\t%s\n", res.AssetName)
		fmt.Fprintf(tw, "Install path:\t%s\n", res.InstallPath)
	}
	_ = tw.Flush()
	if res.UpdateAvailable {
		fmt.Fprintln(cmd.OutOrStdout(), "\nUpdate available.")
	} else {
		fmt.Fprintln(cmd.OutOrStdout(), "\nAlready up to date.")
	}
}

func renderSelfUpdateResult(cmd *cobra.Command, res *selfupdate.Result) error {
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Updated:\t%s -> %s\n", res.CurrentVersion, res.LatestVersion)
	fmt.Fprintf(tw, "Installed to:\t%s\n", res.InstallPath)
	fmt.Fprintf(tw, "Verified:\tsha256 from the release checksums.txt\n")
	if err := tw.Flush(); err != nil {
		return err
	}
	if res.Pending {
		fmt.Fprintln(cmd.OutOrStdout(), "\nThe new binary will replace this one after dlq exits.")
	}
	return nil
}
