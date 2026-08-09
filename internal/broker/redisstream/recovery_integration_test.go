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
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	client := redis.NewClient(opts)
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// ---- Fixture: an empty destination stream plus a DLQ with four failing
	// entries; the fourth carries x-duplicate-of so the classifier must say
	// DO_NOT_REPLAY and the plan must leave it in the DLQ. ----
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	source := "orders-" + suffix
	dlq := source + "-dlq"

	// Create the source stream so it exists as the replay destination, then
	// delete the seed entry (Redis keeps the empty stream key).
	seedID, err := client.XAdd(ctx, &redis.XAddArgs{Stream: source, Values: map[string]any{"payload": "init"}}).Result()
	if err != nil {
		t.Fatalf("create source stream: %v", err)
	}
	if err := client.XDel(ctx, source, seedID).Err(); err != nil {
		t.Fatalf("empty source stream: %v", err)
	}
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
	runCLI := func(t *testing.T, args ...string) string {
		t.Helper()
		root := command.NewRoot("test", "abc123", "2026-01-01")
		var buf bytes.Buffer
		root.SetOut(&buf)
		root.SetErr(&buf)
		root.SetArgs(args)
		if err := root.Execute(); err != nil {
			t.Fatalf("dlq %s: %v\n%s", strings.Join(args, " "), err, buf.String())
		}
		return buf.String()
	}

	// ---- Analyze: four entries form two groups; the duplicate-marked one
	// must be its own group with a DO_NOT_REPLAY recommendation. ----
	out := runCLI(t, "analyze", "--config", cfgPath)
	for _, want := range []string{"4 messages analyzed", "Rejected", "REQUIRES_FIX", "DO_NOT_REPLAY", "order.duplicate"} {
		if !strings.Contains(out, want) {
			t.Errorf("analyze output missing %q:\n%s", want, out)
		}
	}

	// ---- Plan: the three replayable messages are selected; the duplicate is
	// excluded and recorded on the plan. ----
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	out = runCLI(t, "plan", "--config", cfgPath, "--output-file", planPath)
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
	out = runCLI(t, "recover", "--plan", planPath, "--config", cfgPath)
	for _, want := range []string{"3/3 passed", "Messages to replay:", "3", "Excluded:", "Changes made: NONE"} {
		if !strings.Contains(out, want) {
			t.Errorf("dry-run output missing %q:\n%s", want, out)
		}
	}
	waitStreamDepth(t, ctx, client, dlq, 4)

	// ---- Confirm: replays the three, leaves the duplicate in the DLQ. ----
	out = runCLI(t, "recover", "--plan", planPath, "--confirm", "--batch-size", "2", "--rate-limit", "0", "--output", "json", "--config", cfgPath)
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
	out = runCLI(t, "history", "--plan", plan.ID, "--config", cfgPath)
	if got := strings.Count(out, "success"); got != 3 {
		t.Errorf("history has %d success entries, want 3:\n%s", got, out)
	}
	for _, want := range []string{"dry_run", "completed", "(plan)", "skipped"} {
		if !strings.Contains(out, want) {
			t.Errorf("history output missing %q:\n%s", want, out)
		}
	}
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
