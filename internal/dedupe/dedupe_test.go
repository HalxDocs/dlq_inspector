package dedupe

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

func openStore(t *testing.T) *audit.Store {
	t.Helper()
	s, err := audit.Open(filepath.Join(t.TempDir(), "audit.db"))
	if err != nil {
		t.Fatalf("audit.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCheckAuditNoMatch(t *testing.T) {
	ev, err := CheckAudit(openStore(t), &message.Message{ID: "m1", EventID: "evt_1"})
	if err != nil {
		t.Fatalf("CheckAudit: %v", err)
	}
	if ev.MatchFound {
		t.Errorf("unexpected match: %+v", ev)
	}
	if ev.EventID != "evt_1" {
		t.Errorf("EventID not echoed: %+v", ev)
	}
}

func TestCheckAuditFindsPriorReplay(t *testing.T) {
	s := openStore(t)
	when := time.Now().UTC().Add(-time.Hour)
	if err := s.Append(audit.Entry{
		Timestamp:   when,
		Action:      audit.ActionReplay,
		MessageID:   "m1",
		Confirmed:   true,
		Result:      "success",
		SourceQueue: "orders-dlq",
		Destination: "orders",
	}); err != nil {
		t.Fatal(err)
	}

	ev, err := CheckAudit(s, &message.Message{ID: "m1"})
	if err != nil {
		t.Fatalf("CheckAudit: %v", err)
	}
	if !ev.MatchFound {
		t.Fatal("expected a duplicate match")
	}
	if ev.MatchSource != "audit" {
		t.Errorf("MatchSource = %q, want audit", ev.MatchSource)
	}
	if ev.PriorReplayAt == nil || !ev.PriorReplayAt.Equal(when) {
		t.Errorf("PriorReplayAt = %v, want %v", ev.PriorReplayAt, when)
	}
}

func TestCheckAuditIgnoresDryRunAndFailures(t *testing.T) {
	s := openStore(t)
	if err := s.Append(audit.Entry{Action: audit.ActionReplay, MessageID: "m1", DryRun: true, Result: "dry_run"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(audit.Entry{Action: audit.ActionReplay, MessageID: "m1", Confirmed: true, Result: "publish_failed"}); err != nil {
		t.Fatal(err)
	}
	ev, err := CheckAudit(s, &message.Message{ID: "m1"})
	if err != nil {
		t.Fatal(err)
	}
	if ev.MatchFound {
		t.Errorf("dry-run/failed replays must not count as duplicates: %+v", ev)
	}
}

func TestCheckAuditNilStore(t *testing.T) {
	ev, err := CheckAudit(nil, &message.Message{ID: "m1"})
	if err != nil {
		t.Fatalf("CheckAudit(nil): %v", err)
	}
	if ev.MatchFound {
		t.Error("nil store must not match")
	}
}
