package redisstream_test

// The full recovery loop over Redis Streams: seed a DLQ stream with failing
// entries (payload + dead-letter metadata as fields), then run the real CLI
// workflow — analyze, plan, dry-run recover, confirmed recover — and assert
// the audit trail. This is the Phase 9 gate: the same analyzer, classifier,
// planner, validator, and executor work here with zero changes, because they
// only ever talk to broker.Broker.
//
// The fixture writes DLQ entries directly (the equivalent of messages already
// dead-lettered by an application) and creates the destination stream
// explicitly — the executor refuses to publish into a stream that does not
// exist, and that invariant holds for every broker.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/HalxDocs/dlq_inspector/internal/command"
	"github.com/HalxDocs/dlq_inspector/internal/config"
	"github.com/HalxDocs/dlq_inspector/internal/recovery"
)

func TestRecoveryLoopOverRedis(t *testing.T) {
	url := os.Getenv("DLQ_TEST_REDIS_URL")
	if url == "" {
		t.Skip("DLQ_TEST_REDIS_URL not set")
	}

	ctx := context.Background()
	client := dialRedis(t, ctx, url)
	defer client.Close()

	// ---- Fixture: an empty destination stream plus a DLQ with four failing
	// entries; the fourth carries x-duplicate-of so the classifier must say
	// DO_NOT_REPLAY and the plan must leave it in the DLQ. ----
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	source := "orders-" + suffix
	dlq := source + "-dlq"

	emptyStream(t, ctx, client, source)
	t.Cleanup(func() {
		_ = client.Del(ctx, source).Err()
		_ = client.Del(ctx, dlq).Err()
	})

	for i := 1; i <= 3; i++ {
		if err := client.XAdd(ctx, &redis.XAddArgs{Stream: dlq, Values: map[string]any{
			"payload":      fmt.Sprintf(`{"order_id":%d,"customer_id":%d}`, i, 1000+i),
			"destination":  source,
			"error":        "rejected",
			"retries":      "1",
			"content_type": "application/json",
		}}).Err(); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	if err := client.XAdd(ctx, &redis.XAddArgs{Stream: dlq, Values: map[string]any{
		"payload":      `{"order_id":4,"customer_id":1004}`,
		"destination":  source,
		"error":        "rejected",
		"retries":      "1",
		"content_type": "application/json",
		"headers":      `{"x-duplicate-of":"evt_original_order_4","x-event-type":"order.duplicate"}`,
	}}).Err(); err != nil {
		t.Fatalf("seed duplicate: %v", err)
	}
	waitStreamDepth(t, ctx, client, dlq, 4)

	// ---- Config: profile pointing at the live broker, temp audit store. ----
	cli := newRedisCLI(t, url, dlq)
	runCLI := func(t *testing.T, args ...string) string { return cli.run(t, args...) }

	// ---- Analyze: four entries form two groups; the duplicate-marked one
	// must be its own group with a DO_NOT_REPLAY recommendation. ----
	out := runCLI(t, "analyze", "--config", cli.cfgPath)
	for _, want := range []string{"4 messages analyzed", "Rejected", "REQUIRES_FIX", "DO_NOT_REPLAY", "order.duplicate"} {
		if !strings.Contains(out, want) {
			t.Errorf("analyze output missing %q:\n%s", want, out)
		}
	}

	// ---- Plan: the three replayable messages are selected; the duplicate is
	// excluded and recorded on the plan. ----
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	out = runCLI(t, "plan", "--config", cli.cfgPath, "--output-file", planPath)
	if !strings.Contains(out, "3 messages selected") || !strings.Contains(out, "1 excluded (left in DLQ)") {
		t.Errorf("plan output = %q", out)
	}
	planBytes, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatalf("read plan: %v", err)
	}
	var plan recovery.RecoveryPlan
	if err := json.Unmarshal(planBytes, &plan); err != nil {
		t.Fatalf("parse plan: %v\n%s", err, planBytes)
	}
	if len(plan.MessageIDs) != 3 || plan.Destination != source {
		t.Errorf("plan = %+v, want destination %q", plan, source)
	}
	if len(plan.Excluded) != 1 || plan.Excluded[0].Classification != "DO_NOT_REPLAY" {
		t.Errorf("plan exclusions = %+v", plan.Excluded)
	}

	// ---- Dry-run: validates the selected three, reports the exclusion,
	// changes nothing. ----
	out = runCLI(t, "recover", "--plan", planPath, "--config", cli.cfgPath)
	for _, want := range []string{"3/3 passed", "Messages to replay:", "3", "Excluded:", "Changes made: NONE"} {
		if !strings.Contains(out, want) {
			t.Errorf("dry-run output missing %q:\n%s", want, out)
		}
	}
	waitStreamDepth(t, ctx, client, dlq, 4)

	// ---- Confirm: replays the three, leaves the duplicate in the DLQ. ----
	out = runCLI(t, "recover", "--plan", planPath, "--confirm", "--batch-size", "2", "--rate-limit", "0", "--output", "json", "--config", cli.cfgPath)
	var execRes recovery.ExecutionResult
	if err := json.Unmarshal([]byte(out), &execRes); err != nil {
		t.Fatalf("confirm output is not JSON: %v\n%s", err, out)
	}
	if execRes.Replayed != 3 || execRes.Excluded != 1 || execRes.Failed != 0 || execRes.NewDLQEntries != 0 || execRes.Skipped != 0 || execRes.Tripped {
		t.Errorf("execution result = %+v, want 3 replayed, 1 excluded, 0 failed", execRes)
	}
	waitStreamDepth(t, ctx, client, dlq, 1)
	waitStreamDepth(t, ctx, client, source, 3)

	// ---- History: the plan's full trail records every outcome. ----
	out = runCLI(t, "history", "--plan", plan.ID, "--config", cli.cfgPath)
	if got := strings.Count(out, "success"); got != 3 {
		t.Errorf("history has %d success entries, want 3:\n%s", got, out)
	}
	for _, want := range []string{"dry_run", "completed", "(plan)", "skipped"} {
		if !strings.Contains(out, want) {
			t.Errorf("history output missing %q:\n%s", want, out)
		}
	}
}

// TestRecoveryLoopMissingDestinationOverRedis covers the dangerous case where
// the replay destination stream does not exist: a DLQ entry names a
// destination that was never created. Redis would silently create a stream on
// publish, so the executor's existence probe must refuse a confirmed run
// before any publish or ack — with the DLQ copy and the audit trail proving
// nothing was lost. The dry-run must surface the missing destination.
func TestRecoveryLoopMissingDestinationOverRedis(t *testing.T) {
	url := os.Getenv("DLQ_TEST_REDIS_URL")
	if url == "" {
		t.Skip("DLQ_TEST_REDIS_URL not set")
	}

	ctx := context.Background()
	client := dialRedis(t, ctx, url)
	defer client.Close()

	// One failing entry dead-lettered into the DLQ, naming a destination
	// stream that does not exist (it is deliberately never created).
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	source := "orders-" + suffix
	dlq := source + "-dlq"
	if err := client.XAdd(ctx, &redis.XAddArgs{Stream: dlq, Values: map[string]any{
		"payload":      `{"order_id":1,"customer_id":1001}`,
		"destination":  source,
		"error":        "rejected",
		"retries":      "1",
		"content_type": "application/json",
	}}).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Cleanup(func() { _ = client.Del(ctx, dlq).Err() })
	waitStreamDepth(t, ctx, client, dlq, 1)

	cli := newRedisCLI(t, url, dlq)
	runCLI := func(t *testing.T, args ...string) string { return cli.run(t, args...) }
	runCLIErr := func(t *testing.T, args ...string) (string, error) { return cli.runErr(args...) }

	// ---- Plan: the message is selected, with the missing stream recorded as
	// its destination. ----
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	out := runCLI(t, "plan", "--config", cli.cfgPath, "--output-file", planPath)
	if !strings.Contains(out, "1 messages selected") {
		t.Errorf("plan output = %q", out)
	}
	planBytes, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatalf("read plan: %v", err)
	}
	var plan recovery.RecoveryPlan
	if err := json.Unmarshal(planBytes, &plan); err != nil {
		t.Fatalf("parse plan: %v\n%s", err, planBytes)
	}
	if len(plan.MessageIDs) != 1 || plan.Destination != source {
		t.Errorf("plan = %+v, want destination %q", plan, source)
	}

	// ---- Dry-run: validation passes and the missing destination is surfaced
	// as a warning — nothing moves. ----
	out = runCLI(t, "recover", "--plan", planPath, "--config", cli.cfgPath)
	for _, want := range []string{
		"Destination warning:", source, "does not exist",
		"Payload validation:", "1/1 passed",
		"Messages to replay:", "1",
		"Changes made: NONE",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("dry-run output missing %q:\n%s", want, out)
		}
	}
	waitStreamDepth(t, ctx, client, dlq, 1)

	// ---- Confirm: refused before any publish or ack; the DLQ copy stays. ----
	out, err = runCLIErr(t, "recover", "--plan", planPath, "--confirm", "--rate-limit", "0", "--config", cli.cfgPath)
	if err == nil || !strings.Contains(err.Error(), "refusing to run") {
		t.Fatalf("expected refusal, got err=%v\n%s", err, out)
	}
	if !strings.Contains(err.Error(), source) {
		t.Errorf("refusal error = %q, want the destination name", err)
	}
	waitStreamDepth(t, ctx, client, dlq, 1)

	// ---- Audit: the trail shows the plan, the dry-run, and the refusal — no
	// success entry, because nothing was replayed. ----
	out = runCLI(t, "history", "--plan", plan.ID, "--config", cli.cfgPath)
	for _, want := range []string{"dry_run", "refused", "(plan)"} {
		if !strings.Contains(out, want) {
			t.Errorf("history output missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "success") {
		t.Errorf("history shows a success for a refused run:\n%s", out)
	}
}

// TestRecoveryLoopPolicyOverRedis proves the policy gate end to end on the
// second broker: a message the classifier would mark REQUIRES_FIX is flipped
// to DO_NOT_REPLAY by a profile-bound policy, excluded from the plan with the
// rule as its reason, and left untouched in the DLQ by the confirmed run —
// while the policy-independent message replays normally.
func TestRecoveryLoopPolicyOverRedis(t *testing.T) {
	url := os.Getenv("DLQ_TEST_REDIS_URL")
	if url == "" {
		t.Skip("DLQ_TEST_REDIS_URL not set")
	}

	ctx := context.Background()
	client := dialRedis(t, ctx, url)
	defer client.Close()

	// Two failing entries: one plain (REQUIRES_FIX by the classifier), one
	// carrying an event type the policy marks DO_NOT_REPLAY.
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	source := "orders-" + suffix
	dlq := source + "-dlq"

	emptyStream(t, ctx, client, source)
	t.Cleanup(func() {
		_ = client.Del(ctx, source).Err()
		_ = client.Del(ctx, dlq).Err()
	})

	seed := func(payload, headers string) {
		t.Helper()
		vals := map[string]any{
			"payload":      payload,
			"destination":  source,
			"error":        "rejected",
			"retries":      "1",
			"content_type": "application/json",
		}
		if headers != "" {
			vals["headers"] = headers
		}
		if err := client.XAdd(ctx, &redis.XAddArgs{Stream: dlq, Values: vals}).Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	seed(`{"order_id":1,"customer_id":1001}`, "")
	seed(`{"order_id":2,"customer_id":1002}`, `{"x-event-type":"order.cancelled"}`)
	waitStreamDepth(t, ctx, client, dlq, 2)

	// The policy: order.cancelled events are never replayed. Validate it the
	// way CI would, then bind it to the profile.
	policyPath := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policyPath, []byte("rules:\n  - when: event_type == order.cancelled\n    action: do_not_replay\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cli := newRedisCLI(t, url, dlq)
	runCLI := func(t *testing.T, args ...string) string { return cli.run(t, args...) }

	out := runCLI(t, "policy", "validate", policyPath, "--config", cli.cfgPath)
	if !strings.Contains(out, "valid (1 rule)") {
		t.Errorf("validate output = %q", out)
	}
	out = runCLI(t, "policy", "apply", policyPath, "--profile", "ci", "--config", cli.cfgPath)
	if !strings.Contains(out, `Policy applied to profile "ci"`) {
		t.Errorf("apply output = %q", out)
	}

	// ---- Analyze: the policy flips order.cancelled to DO_NOT_REPLAY while
	// the plain message stays REQUIRES_FIX. ----
	out = runCLI(t, "analyze", "--config", cli.cfgPath)
	for _, want := range []string{"2 messages analyzed", "order.cancelled", "DO_NOT_REPLAY", "REQUIRES_FIX"} {
		if !strings.Contains(out, want) {
			t.Errorf("analyze output missing %q:\n%s", want, out)
		}
	}

	// ---- Plan: only the plain message is selected; the policy-excluded one
	// is recorded with the rule as its reason. ----
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	out = runCLI(t, "plan", "--config", cli.cfgPath, "--output-file", planPath)
	if !strings.Contains(out, "1 messages selected") || !strings.Contains(out, "1 excluded (left in DLQ)") {
		t.Errorf("plan output = %q", out)
	}
	planBytes, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatalf("read plan: %v", err)
	}
	var plan recovery.RecoveryPlan
	if err := json.Unmarshal(planBytes, &plan); err != nil {
		t.Fatalf("parse plan: %v\n%s", err, planBytes)
	}
	if len(plan.MessageIDs) != 1 || len(plan.Excluded) != 1 {
		t.Fatalf("plan = %+v, want 1 selected and 1 excluded", plan)
	}
	if !strings.Contains(plan.Excluded[0].Reason, "policy rule") {
		t.Errorf("exclusion reason = %q, want the policy rule named", plan.Excluded[0].Reason)
	}

	// ---- Dry-run: validates the selected one, reports the exclusion, moves
	// nothing. ----
	out = runCLI(t, "recover", "--plan", planPath, "--config", cli.cfgPath)
	for _, want := range []string{"1/1 passed", "Excluded:", "Messages to replay:", "1", "Changes made: NONE"} {
		if !strings.Contains(out, want) {
			t.Errorf("dry-run output missing %q:\n%s", want, out)
		}
	}
	waitStreamDepth(t, ctx, client, dlq, 2)

	// ---- Confirm: replays exactly one; the policy-excluded message stays in
	// the DLQ. ----
	out = runCLI(t, "recover", "--plan", planPath, "--confirm", "--rate-limit", "0", "--output", "json", "--config", cli.cfgPath)
	var execRes recovery.ExecutionResult
	if err := json.Unmarshal([]byte(out), &execRes); err != nil {
		t.Fatalf("confirm output is not JSON: %v\n%s", err, out)
	}
	if execRes.Replayed != 1 || execRes.Excluded != 1 || execRes.Failed != 0 || execRes.Tripped {
		t.Errorf("execution result = %+v, want 1 replayed and 1 excluded", execRes)
	}
	waitStreamDepth(t, ctx, client, dlq, 1)
	waitStreamDepth(t, ctx, client, source, 1)

	// ---- History: one success plus the policy-driven skip. ----
	out = runCLI(t, "history", "--plan", plan.ID, "--config", cli.cfgPath)
	if got := strings.Count(out, "success"); got != 1 {
		t.Errorf("history has %d success entries, want 1:\n%s", got, out)
	}
	if !strings.Contains(out, "skipped") || !strings.Contains(out, "policy") {
		t.Errorf("history missing the policy skip:\n%s", out)
	}
}

// dialRedis opens a go-redis client against the test instance.
func dialRedis(t *testing.T, ctx context.Context, url string) *redis.Client {
	t.Helper()
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("ping redis: %v", err)
	}
	return client
}

// emptyStream creates a stream that persists even with zero entries. A plain
// XADD+XDEL removes the key entirely, but a stream carrying a consumer group
// is never auto-deleted — so XGROUP CREATE with MKSTREAM gives the fixture a
// stable, empty destination the executor's existence probe accepts.
func emptyStream(t *testing.T, ctx context.Context, client *redis.Client, name string) {
	t.Helper()
	if err := client.XGroupCreateMkStream(ctx, name, "dlq-inspector-fixture", "0").Err(); err != nil {
		t.Fatalf("create stream %s: %v", name, err)
	}
}

// redisCLI runs the public CLI against a temp config pointing at the live
// Redis instance, with a temp audit store.
type redisCLI struct {
	cfgPath string
}

func newRedisCLI(t *testing.T, url, dlq string) *redisCLI {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfg := config.Defaults()
	cfg.DefaultProfile = "ci"
	cfg.Audit.Path = filepath.Join(t.TempDir(), "audit.db")
	cfg.Profiles = map[string]*config.Profile{
		"ci": {Broker: "redisstream", URL: url, DefaultQueue: dlq},
	}
	if err := config.Save(cfgPath, cfg); err != nil {
		t.Fatal(err)
	}
	return &redisCLI{cfgPath: cfgPath}
}

// run executes the CLI, failing the test on error.
func (c *redisCLI) run(t *testing.T, args ...string) string {
	t.Helper()
	out, err := c.runErr(args...)
	if err != nil {
		t.Fatalf("dlq %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// runErr executes the CLI and returns the error, for paths that must refuse.
func (c *redisCLI) runErr(args ...string) (string, error) {
	root := command.NewRoot("test", "abc123", "2026-01-01")
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		return buf.String(), err
	}
	return buf.String(), nil
}

// waitStreamDepth polls XLEN until the stream holds exactly want entries.
func waitStreamDepth(t *testing.T, ctx context.Context, client *redis.Client, stream string, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		n, err := client.XLen(ctx, stream).Result()
		if err == nil && int(n) == want {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	n, err := client.XLen(ctx, stream).Result()
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	t.Fatalf("stream %s depth = %d, want %d", stream, n, want)
}
