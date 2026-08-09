package audit

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "audit.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestAppendAndRecent(t *testing.T) {
	s := openTestStore(t)

	first := Entry{Action: ActionReplay, MessageID: "m1", SourceQueue: "orders-dlq", Destination: "orders", Confirmed: true, Result: "success", Broker: "rabbitmq", Profile: "dev"}
	second := Entry{Action: ActionInspect, MessageID: "m2", SourceQueue: "orders-dlq", Broker: "rabbitmq", Profile: "dev"}
	if err := s.Append(first); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Append(second); err != nil {
		t.Fatalf("Append: %v", err)
	}

	recent, err := s.Recent(10)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(recent) != 2 {
		t.Fatalf("Recent len = %d, want 2", len(recent))
	}
	// Newest first.
	if recent[0].MessageID != "m2" || recent[1].MessageID != "m1" {
		t.Errorf("order = %s, %s; want m2, m1", recent[0].MessageID, recent[1].MessageID)
	}
	if !recent[1].Confirmed || recent[1].Result != "success" {
		t.Errorf("first entry = %+v", recent[1])
	}
	if recent[0].Timestamp.IsZero() {
		t.Error("timestamp not set")
	}
}

func TestRecentLimit(t *testing.T) {
	s := openTestStore(t)
	for i := 0; i < 5; i++ {
		if err := s.Append(Entry{Action: ActionInspect, MessageID: string(rune('a' + i)), Broker: "rabbitmq"}); err != nil {
			t.Fatal(err)
		}
	}
	recent, err := s.Recent(2)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(recent) != 2 || recent[0].MessageID != "e" {
		t.Fatalf("Recent(2) = %+v", recent)
	}
}

func TestReplayedOnlyCountsConfirmedSuccess(t *testing.T) {
	s := openTestStore(t)

	// Dry-run must not count as duplicate evidence.
	if err := s.Append(Entry{Action: ActionReplay, MessageID: "m1", DryRun: true, Confirmed: false, Result: "dry_run"}); err != nil {
		t.Fatal(err)
	}
	// A failed publish must not count either.
	if err := s.Append(Entry{Action: ActionReplay, MessageID: "m1", Confirmed: true, Result: "publish_failed"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(Entry{Action: ActionReplay, MessageID: "m1", Confirmed: true, Result: "success"}); err != nil {
		t.Fatal(err)
	}

	replayed, err := s.Replayed("m1")
	if err != nil {
		t.Fatalf("Replayed: %v", err)
	}
	if len(replayed) != 1 || replayed[0].Result != "success" {
		t.Fatalf("Replayed = %+v, want only the confirmed success", replayed)
	}

	if other, err := s.Replayed("nope"); err != nil || len(other) != 0 {
		t.Errorf("Replayed(nope) = %+v, %v; want empty", other, err)
	}
}

func TestByPlan(t *testing.T) {
	s := openTestStore(t)
	for _, e := range []Entry{
		{Action: ActionRecover, PlanID: "plan_a", MessageID: "m1", SourceQueue: "q", Destination: "d", Confirmed: true, Result: "success"},
		{Action: ActionRecover, PlanID: "plan_a", MessageID: "m2", SourceQueue: "q", Destination: "d", Confirmed: true, Result: "publish_failed"},
		{Action: ActionRecover, PlanID: "plan_a", SourceQueue: "q", Destination: "d", Confirmed: true, Result: "completed"},
		{Action: ActionRecover, PlanID: "plan_b", MessageID: "x", SourceQueue: "q", Destination: "d", Confirmed: true, Result: "success"},
	} {
		if err := s.Append(e); err != nil {
			t.Fatal(err)
		}
	}

	entries, err := s.ByPlan("plan_a")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("ByPlan len = %d, want 3", len(entries))
	}
	// Oldest first (execution order): m1, m2, then the plan summary.
	if entries[0].MessageID != "m1" || entries[1].MessageID != "m2" || entries[2].MessageID != "" {
		t.Errorf("order = %+v", entries)
	}
	if entries[2].Result != "completed" {
		t.Errorf("summary result = %s", entries[2].Result)
	}

	if other, err := s.ByPlan("plan_nope"); err != nil || len(other) != 0 {
		t.Errorf("ByPlan(unknown) = %+v, %v; want empty", other, err)
	}
}

func TestReplayedCountsRecoverSuccess(t *testing.T) {
	s := openTestStore(t)
	if err := s.Append(Entry{Action: ActionRecover, PlanID: "plan_a", MessageID: "m1", SourceQueue: "q", Destination: "d", Confirmed: true, Result: "success"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(Entry{Action: ActionRecover, PlanID: "plan_a", MessageID: "m2", SourceQueue: "q", Destination: "d", Confirmed: true, Result: "publish_failed"}); err != nil {
		t.Fatal(err)
	}

	replayed, err := s.Replayed("m1")
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 {
		t.Fatalf("Replayed(m1) = %+v, want the recover success", replayed)
	}
	if other, err := s.Replayed("m2"); err != nil || len(other) != 0 {
		t.Errorf("Replayed(m2) = %+v, %v; want empty (publish_failed is not evidence)", other, err)
	}
}

func TestExportJSONL(t *testing.T) {
	s := openTestStore(t)
	if err := s.Append(Entry{Action: ActionReplay, MessageID: "m1", SourceQueue: "q", Confirmed: true, Result: "success", Timestamp: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)}); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "export.jsonl")
	if err := ExportJSONL(path, mustRecent(t, s)); err != nil {
		t.Fatalf("ExportJSONL: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("export lines = %d, want 1: %s", len(lines), data)
	}
	var e Entry
	if err := json.Unmarshal([]byte(lines[0]), &e); err != nil {
		t.Fatalf("export line not JSON: %v", err)
	}
	if e.Action != ActionReplay || e.MessageID != "m1" || e.Result != "success" {
		t.Errorf("export entry = %+v", e)
	}
}

func mustRecent(t *testing.T, s *Store) []Entry {
	t.Helper()
	entries, err := s.Recent(0)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func TestPayloadDiffRoundTrip(t *testing.T) {
	s := openTestStore(t)
	diff := "- customer_id: 1000\n+ customer_id: 443\n"
	if err := s.Append(Entry{
		Action: ActionPatch, MessageID: "m1", SourceQueue: "orders-dlq", Destination: "orders",
		Confirmed: true, Result: "success", PayloadDiff: diff,
	}); err != nil {
		t.Fatal(err)
	}
	recent := mustRecent(t, s)
	if len(recent) != 1 || recent[0].PayloadDiff != diff {
		t.Fatalf("entries = %+v, want the diff round-tripped", recent)
	}
}

// oldSchema is the audit table as created before the payload_diff column
// existed. Opening a store built from it must migrate the column in so old
// stores keep working and new entries with diffs can be written.
const oldSchema = `
CREATE TABLE IF NOT EXISTS audit (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp     TEXT NOT NULL,
    action        TEXT NOT NULL,
    message_id    TEXT NOT NULL DEFAULT '',
    plan_id       TEXT NOT NULL DEFAULT '',
    source_queue  TEXT NOT NULL DEFAULT '',
    destination   TEXT NOT NULL DEFAULT '',
    dry_run       INTEGER NOT NULL DEFAULT 0,
    confirmed     INTEGER NOT NULL DEFAULT 0,
    result        TEXT NOT NULL DEFAULT '',
    broker        TEXT NOT NULL DEFAULT '',
    profile       TEXT NOT NULL DEFAULT '',
    reason        TEXT NOT NULL DEFAULT ''
);
`

func TestMigrateAddsPayloadDiff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(oldSchema); err != nil {
		t.Fatal(err)
	}
	// An entry written by the old version, before the column existed.
	if _, err := db.Exec(
		`INSERT INTO audit (timestamp, action, message_id, plan_id, source_queue, destination,
		   dry_run, confirmed, result, broker, profile, reason)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		time.Now().UTC().Format(time.RFC3339Nano), "replay", "m1", "", "orders-dlq", "orders",
		0, 1, "success", "rabbitmq", "dev", ""); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(path) // Open runs the migration
	if err != nil {
		t.Fatalf("Open after migration: %v", err)
	}
	defer s.Close()

	// Writing an entry with a diff works on the migrated store.
	diff := "- customer_id: 1000\n+ customer_id: 443\n"
	if err := s.Append(Entry{Action: ActionPatch, MessageID: "m2", SourceQueue: "orders-dlq", Result: "success", PayloadDiff: diff}); err != nil {
		t.Fatalf("Append after migration: %v", err)
	}

	recent := mustRecent(t, s)
	if len(recent) != 2 {
		t.Fatalf("entries = %d, want the pre-migration entry plus the new one", len(recent))
	}
	// Newest first: the new entry round-trips its diff; the old entry (from
	// before the column existed) reads back with an empty diff.
	if recent[0].MessageID != "m2" || recent[0].PayloadDiff != diff {
		t.Errorf("new entry = %+v", recent[0])
	}
	if recent[1].MessageID != "m1" || recent[1].PayloadDiff != "" {
		t.Errorf("old entry = %+v", recent[1])
	}
}
