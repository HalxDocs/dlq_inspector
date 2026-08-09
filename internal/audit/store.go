// Package audit implements the append-only local audit trail: what the tool
// did, when, and with what result. It is the only source of truth for "what
// did this tool actually change, and when" — essential for incident
// postmortems. Entries are stored in SQLite (cgo-free modernc.org/sqlite) and
// can be exported to JSONL.
package audit

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// Action is the kind of operation an entry records.
type Action string

// Supported actions.
const (
	ActionInspect  Action = "inspect"
	ActionPatch    Action = "patch"
	ActionReplay   Action = "replay"
	ActionAnalyze  Action = "analyze"
	ActionPlan     Action = "plan"
	ActionRecover  Action = "recover"
	ActionRollback Action = "rollback"
)

// Entry is one immutable audit record. Result carries the outcome
// ("success", "dry_run", "publish_failed", ...) so the trail shows failures
// as well as successes.
type Entry struct {
	Timestamp   time.Time `json:"timestamp"`
	Action      Action    `json:"action"`
	MessageID   string    `json:"message_id"`
	PlanID      string    `json:"plan_id,omitempty"`
	SourceQueue string    `json:"source_queue"`
	Destination string    `json:"destination,omitempty"`
	DryRun      bool      `json:"dry_run"`
	Confirmed   bool      `json:"confirmed"`
	Result      string    `json:"result"`
	Broker      string    `json:"broker"`
	Profile     string    `json:"profile"`
	Reason      string    `json:"reason,omitempty"`
	// PayloadDiff is the old->new payload diff for patched replays, so the
	// trail shows exactly what was changed. Empty for unpatched operations.
	PayloadDiff string `json:"payload_diff,omitempty"`
}

// Snapshot is a point-in-time copy of a message taken just before a confirmed
// recovery replayed it, so a bad recovery can be reversed: rollback
// republishes the snapshot to the DLQ it came from.
type Snapshot struct {
	PlanID      string            `json:"plan_id"`
	MessageID   string            `json:"message_id"`
	SourceQueue string            `json:"source_queue"`
	Destination string            `json:"destination"`
	Payload     []byte            `json:"payload,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Timestamp   time.Time         `json:"timestamp"`
}

// Store is an append-only audit log backed by SQLite.
type Store struct {
	path string
	db   *sql.DB
}

// Open opens (creating if needed) the audit store at path. Parent directories
// are created as needed. Open fails early so the safety gate never runs
// without a place to write its record.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("audit: empty store path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("audit: create store directory: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("audit: open store %s: %w", path, err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("audit: init store %s: %w", path, err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("audit: migrate store %s: %w", path, err)
	}
	return &Store{path: path, db: db}, nil
}

const schema = `
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
    reason        TEXT NOT NULL DEFAULT '',
    payload_diff  TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_audit_message ON audit(message_id, action);

CREATE TABLE IF NOT EXISTS snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id      TEXT NOT NULL DEFAULT '',
    message_id   TEXT NOT NULL DEFAULT '',
    source_queue TEXT NOT NULL DEFAULT '',
    destination  TEXT NOT NULL DEFAULT '',
    payload      BLOB NOT NULL,
    content_type TEXT NOT NULL DEFAULT '',
    headers      TEXT NOT NULL DEFAULT '',
    timestamp    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_snapshots_plan ON snapshots(plan_id);
`

// migrate adds columns introduced after the initial schema, so stores created
// by older versions keep working instead of failing on INSERT.
func migrate(db *sql.DB) error {
	cols, err := tableColumns(db, "audit")
	if err != nil {
		return err
	}
	if !cols["payload_diff"] {
		if _, err := db.Exec(`ALTER TABLE audit ADD COLUMN payload_diff TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("add payload_diff column: %w", err)
		}
	}
	return nil
}

// tableColumns returns the set of column names of a table.
func tableColumns(db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols := map[string]bool{}
	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notnull int
			dflt    any
			pk      int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols[name] = true
	}
	return cols, rows.Err()
}

// SaveSnapshot records one message snapshot, append-only like the audit log.
// Snapshots are what make a bad recovery reversible: rollback republishes
// them to the DLQ they came from.
func (s *Store) SaveSnapshot(sn Snapshot) error {
	if sn.Timestamp.IsZero() {
		sn.Timestamp = time.Now().UTC()
	}
	hdr, err := json.Marshal(sn.Headers)
	if err != nil {
		return fmt.Errorf("audit: marshal snapshot headers: %w", err)
	}
	_, err = s.db.Exec(
		`INSERT INTO snapshots (plan_id, message_id, source_queue, destination, payload, content_type, headers, timestamp)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		sn.PlanID,
		sn.MessageID,
		sn.SourceQueue,
		sn.Destination,
		sn.Payload,
		sn.ContentType,
		string(hdr),
		sn.Timestamp.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("audit: save snapshot: %w", err)
	}
	return nil
}

// Snapshots returns every snapshot recorded for one recovery plan, in the
// order they were replayed — the exact set rollback would restore.
func (s *Store) Snapshots(planID string) ([]Snapshot, error) {
	rows, err := s.db.Query(`SELECT plan_id, message_id, source_queue, destination, payload, content_type, headers, timestamp
	          FROM snapshots WHERE plan_id = ? ORDER BY id ASC`, planID)
	if err != nil {
		return nil, fmt.Errorf("audit: query snapshots: %w", err)
	}
	defer rows.Close()

	var out []Snapshot
	for rows.Next() {
		var (
			sn          Snapshot
			hdr, ts     string
			payload     []byte
			contentType string
		)
		if err := rows.Scan(&sn.PlanID, &sn.MessageID, &sn.SourceQueue, &sn.Destination,
			&payload, &contentType, &hdr, &ts); err != nil {
			return nil, fmt.Errorf("audit: scan snapshot: %w", err)
		}
		sn.Payload = payload
		sn.ContentType = contentType
		if hdr != "" {
			if err := json.Unmarshal([]byte(hdr), &sn.Headers); err != nil {
				return nil, fmt.Errorf("audit: parse snapshot headers: %w", err)
			}
		}
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			return nil, fmt.Errorf("audit: parse snapshot timestamp %q: %w", ts, err)
		}
		sn.Timestamp = t
		out = append(out, sn)
	}
	return out, rows.Err()
}

// Close closes the underlying database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Append records one entry. Entries are never updated or deleted.
func (s *Store) Append(e Entry) error {
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	_, err := s.db.Exec(
		`INSERT INTO audit (timestamp, action, message_id, plan_id, source_queue, destination,
		   dry_run, confirmed, result, broker, profile, reason, payload_diff)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.Timestamp.Format(time.RFC3339Nano),
		string(e.Action),
		e.MessageID,
		e.PlanID,
		e.SourceQueue,
		e.Destination,
		boolInt(e.DryRun),
		boolInt(e.Confirmed),
		e.Result,
		e.Broker,
		e.Profile,
		e.Reason,
		e.PayloadDiff,
	)
	if err != nil {
		return fmt.Errorf("audit: append entry: %w", err)
	}
	return nil
}

// Recent returns the most recent entries, newest first, up to limit (0 or
// negative means no limit).
func (s *Store) Recent(limit int) ([]Entry, error) {
	query := `SELECT timestamp, action, message_id, plan_id, source_queue, destination,
	          dry_run, confirmed, result, broker, profile, reason, payload_diff
	          FROM audit ORDER BY id DESC`
	args := []any{}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	return s.query(query, args...)
}

// ByPlan returns every entry belonging to one recovery plan, oldest first,
// so the full trail (per-message outcomes plus the plan-level summary) can
// be reviewed in execution order.
func (s *Store) ByPlan(planID string) ([]Entry, error) {
	return s.query(`SELECT timestamp, action, message_id, plan_id, source_queue, destination,
	          dry_run, confirmed, result, broker, profile, reason, payload_diff
	          FROM audit
	          WHERE plan_id = ?
	          ORDER BY id ASC`,
		planID)
}

// Replayed returns confirmed, successful replay entries for a message ID —
// the duplicate evidence signal: a message that was already replayed or
// recovered successfully should not be replayed again without thought. Both
// single replay and batch recovery actions count.
func (s *Store) Replayed(messageID string) ([]Entry, error) {
	return s.query(`SELECT timestamp, action, message_id, plan_id, source_queue, destination,
	          dry_run, confirmed, result, broker, profile, reason, payload_diff
	          FROM audit
	          WHERE message_id = ? AND action IN ('replay', 'recover') AND confirmed = 1 AND result = 'success'
	          ORDER BY id DESC`,
		messageID)
}

func (s *Store) query(query string, args ...any) ([]Entry, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("audit: query: %w", err)
	}
	defer rows.Close()

	var out []Entry
	for rows.Next() {
		var (
			e                    Entry
			ts, action, result   string
			planID, src, dest    string
			dryRun, confirmed    int
			broker, profile, rsn string
			diff                 string
		)
		if err := rows.Scan(&ts, &action, &e.MessageID, &planID, &src, &dest,
			&dryRun, &confirmed, &result, &broker, &profile, &rsn, &diff); err != nil {
			return nil, fmt.Errorf("audit: scan entry: %w", err)
		}
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			return nil, fmt.Errorf("audit: parse timestamp %q: %w", ts, err)
		}
		out = append(out, Entry{
			Timestamp:   t,
			Action:      Action(action),
			MessageID:   e.MessageID,
			PlanID:      planID,
			SourceQueue: src,
			Destination: dest,
			DryRun:      dryRun == 1,
			Confirmed:   confirmed == 1,
			Result:      result,
			Broker:      broker,
			Profile:     profile,
			Reason:      rsn,
			PayloadDiff: diff,
		})
	}
	return out, rows.Err()
}

// ExportJSONL writes entries to path as newline-delimited JSON.
func ExportJSONL(path string, entries []Entry) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("audit: create export %s: %w", path, err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			return fmt.Errorf("audit: write export: %w", err)
		}
	}
	return w.Flush()
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
