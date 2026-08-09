package audit

import (
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
