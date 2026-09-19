package command

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/config"
	"github.com/HalxDocs/dlq_inspector/internal/jev"
	"github.com/HalxDocs/dlq_inspector/internal/recovery"
)

// analyzeResult is the structured JSON payload of dlq analyze --output json.
type analyzeResult struct {
	Queue       string                  `json:"queue"`
	Total       int                     `json:"total"`
	GeneratedAt time.Time               `json:"generated_at"`
	Groups      []recovery.FailureGroup `json:"groups"`
	Jev         *jevStats               `json:"jev,omitempty"`
}

// jevStats summarizes one Jev-assisted run for JSON consumers.
type jevStats struct {
	Calls   int `json:"calls"`
	Cached  int `json:"cached"`
	Decided int `json:"decided"`
}

// jevFlags carries the dlq analyze/plan Jev assist options.
type jevFlags struct {
	withJev    bool
	withoutJev bool
	all        bool
	threshold  float64
	model      string
	endpoint   string
	maxCalls   int
	apiKeyEnv  string
}

func (f *jevFlags) register(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&f.withJev, "with-jev", false, "use Jev assist for this run (overrides profile)")
	cmd.Flags().BoolVar(&f.withoutJev, "without-jev", false, "force rule-based classification for this run (overrides profile)")
	cmd.Flags().BoolVar(&f.all, "jev-all", false, "consult Jev for every message, not just INVESTIGATE ones")
	cmd.Flags().Float64Var(&f.threshold, "jev-threshold", jev.DefaultThreshold, "minimum Jev confidence to accept (0-1)")
	cmd.Flags().StringVar(&f.model, "jev-model", jev.DefaultModel, "Jev model ID (pin a version for tuned thresholds)")
	cmd.Flags().StringVar(&f.endpoint, "jev-endpoint", jev.DefaultEndpoint, "Jev API endpoint (for gateways/mocks)")
	cmd.Flags().IntVar(&f.maxCalls, "jev-max-calls", jev.DefaultMaxCalls, "maximum Jev API calls per run (spend guard)")
	cmd.Flags().StringVar(&f.apiKeyEnv, "jev-api-key-env", jev.APIKeyEnv, "env var holding the Jev API key (BYOK: key value never stored or flagged)")
}

// resolveJev merges the profile default with per-run flags and returns the
// assessor to use, or nil for rule-based behavior. Precedence:
//
//	--without-jev (explicit opt-out) > --with-jev (explicit opt-in) >
//	profile jev.enabled (via `dlq jev enable`).
//
// Explicitly passed tuning flags (--jev-threshold, --jev-model, --jev-all,
// --jev-endpoint, --jev-max-calls, --jev-api-key-env) overlay the stored
// profile settings for this run only. The second return is quiet: true means
// the enable hint must stay hidden (explicit opt-out, Jev active, or the
// missing-key warning already said enough).
func (f *jevFlags) resolveJev(cmd *cobra.Command, profile *config.Profile) (*recovery.JevAssessor, bool) {
	changed := cmd.Flags().Changed
	if changed("without-jev") && f.withoutJev {
		return nil, true
	}
	var eff = (&config.JevSettings{}).Effective()
	on := false
	if profile != nil {
		eff = profile.Jev.Effective()
		on = eff.Enabled
	}
	if changed("with-jev") && f.withJev {
		on = true
	}
	if !on {
		return nil, false
	}
	if changed("jev-all") {
		eff.All = f.all
	}
	if changed("jev-threshold") {
		eff.Threshold = f.threshold
	}
	if changed("jev-model") {
		eff.Model = f.model
	}
	if changed("jev-endpoint") {
		eff.Endpoint = f.endpoint
	}
	if changed("jev-max-calls") {
		eff.MaxCalls = f.maxCalls
	}
	if changed("jev-api-key-env") {
		eff.APIKeyEnv = f.apiKeyEnv
	}
	if eff.Threshold <= 0 || eff.Threshold > 1 {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: jev threshold %.2f out of range, using %.2f\n", eff.Threshold, jev.DefaultThreshold)
		eff.Threshold = jev.DefaultThreshold
	}
	if eff.APIKeyEnv == "" {
		eff.APIKeyEnv = jev.APIKeyEnv
	}
	if os.Getenv(eff.APIKeyEnv) == "" {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s is not set: falling back to rules (export it or run 'dlq jev status')\n", eff.APIKeyEnv)
		return nil, true
	}
	return &recovery.JevAssessor{
		Client:          jev.NewClient(os.Getenv(eff.APIKeyEnv), eff.Model, eff.Endpoint),
		Threshold:       eff.Threshold,
		Cache:           jev.NewCache(),
		MaxCalls:        eff.MaxCalls,
		OnlyInvestigate: !eff.All,
	}, true
}

// buildJevAssessor is resolveJev without a profile: flags only, kept for
// callers with no connection profile in scope.
func (f *jevFlags) buildJevAssessor(cmd *cobra.Command) *recovery.JevAssessor {
	a, _ := f.resolveJev(cmd, nil)
	return a
}

func newAnalyzeCmd(opts *GlobalOptions) *cobra.Command {
	var (
		limit int
		jf    jevFlags
	)

	cmd := &cobra.Command{
		Use:   "analyze [queue]",
		Short: "Group failed messages by failure pattern and classify each group",
		Long: `Analyze a dead-letter queue and turn a pile of messages into grouped
failure patterns: each group shares a normalized error signature, destination,
event type, and retry range, and carries a recovery recommendation
(REPLAYABLE / REQUIRES_FIX / DO_NOT_REPLAY / INVESTIGATE).

The queue defaults to the profile's default_queue. Analysis is read-only —
no message is published, acked, or moved.

With --with-jev, messages the rule-based classifier leaves as INVESTIGATE
are resolved by Jev (metadata only: error text, retry counts, destinations —
never payload bytes). 'dlq jev enable' makes assist automatic per profile;
--without-jev forces rule-based for one run. Header duplicates and policy
matches always win over Jev; low-confidence or failed Jev calls fall back
to rules silently.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := commandContext(cmd.Context())
			defer cancel()

			b, profile, err := openBroker(ctx, opts)
			if err != nil {
				return err
			}
			defer b.Close()

			queue, err := resolveQueue(args, profile.DefaultQueue)
			if err != nil {
				return err
			}

			pol, err := loadPolicyForProfile(effectiveProfileName(opts), profile)
			if err != nil {
				return err
			}

			msgs, err := b.Search(ctx, queue, broker.SearchFilter{Limit: limit})
			if err != nil {
				return err
			}
			if len(msgs) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No messages to analyze.")
				return nil
			}

			assessor, quiet := jf.resolveJev(cmd, profile)
			groups := (recovery.Analyzer{Policy: pol, Jev: assessor}).AnalyzeWithContext(ctx, msgs)
			stats := summarizeJev(assessor, groups)

			if opts.Output == "json" {
				return writeJSON(cmd, analyzeResult{
					Queue:       queue,
					Total:       len(msgs),
					GeneratedAt: time.Now().UTC(),
					Groups:      groups,
					Jev:         stats,
				})
			}
			return renderAnalyze(cmd, queue, len(msgs), groups, stats, quiet)
		},
	}

	cmd.Flags().IntVar(&limit, "limit", 1000, "maximum number of messages to analyze")
	jf.register(cmd)
	return cmd
}

func summarizeJev(a *recovery.JevAssessor, groups []recovery.FailureGroup) *jevStats {
	if a == nil || !a.Enabled() {
		return nil
	}
	decided := 0
	for _, g := range groups {
		decided += g.JevDecided
	}
	cached := 0
	if a.Cache != nil {
		cached = a.Cache.Hits()
	}
	return &jevStats{Calls: a.Calls(), Cached: cached, Decided: decided}
}

// renderAnalyze prints the summary line and one block per failure group.
// quiet suppresses the Jev enable hint (explicit opt-out or Jev already ran).
func renderAnalyze(cmd *cobra.Command, queue string, total int, groups []recovery.FailureGroup, stats *jevStats, quiet bool) error {
	fmt.Fprintf(cmd.OutOrStdout(), "%d messages analyzed in %s\n", total, queue)
	for i, g := range groups {
		fmt.Fprintf(cmd.OutOrStdout(), "\nGROUP %d -- %s [%s]\n", i+1, g.Label, g.ID)
		fmt.Fprintf(cmd.OutOrStdout(), "%d messages - %.1f%%\n", g.Count, g.Percentage)
		if g.JevDecided > 0 {
			fmt.Fprintf(cmd.OutOrStdout(), "Recommendation: %s (confidence %.2f, jev %d/%d)\n", g.Recommendation, g.Confidence, g.JevDecided, g.Count)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Recommendation: %s (confidence %.2f)\n", g.Recommendation, g.Confidence)
		}

		tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		fmt.Fprintf(tw, "Signature:\t%s\n", g.Signature)
		if g.Destination != "" {
			fmt.Fprintf(tw, "Destination:\t%s\n", g.Destination)
		}
		if g.EventType != "" {
			fmt.Fprintf(tw, "Event type:\t%s\n", g.EventType)
		}
		fmt.Fprintf(tw, "Retries:\t%s\n", g.RetryBucket)
		if g.PayloadShape != "" {
			fmt.Fprintf(tw, "Payload shape:\t%s\n", g.PayloadShape)
		}
		if !g.FirstSeen.IsZero() {
			fmt.Fprintf(tw, "Time window:\t%s to %s\n",
				g.FirstSeen.UTC().Format(time.RFC3339), g.LastSeen.UTC().Format(time.RFC3339))
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	if stats != nil {
		fmt.Fprintf(cmd.OutOrStdout(), "\nJev: %d calls (%d cached), %d messages decided by jev\n", stats.Calls, stats.Cached, stats.Decided)
	} else if !quiet {
		if n := investigateCount(groups); n > 0 {
			fmt.Fprintf(cmd.OutOrStdout(), "\nTip: %d message(s) still need investigation — enable AI assist: export TYPESAFE_API_KEY=... then 'dlq jev enable' (or per-run --with-jev)\n", n)
		}
	}
	return nil
}

// investigateCount sums messages in INVESTIGATE-majority groups.
func investigateCount(groups []recovery.FailureGroup) int {
	n := 0
	for _, g := range groups {
		if g.Recommendation == recovery.Investigate {
			n += g.Count
		}
	}
	return n
}
