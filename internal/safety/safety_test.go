package safety

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// recordingBroker is a minimal Broker that records Publish/Ack calls and can
// fail them on demand. Used to prove the gate's ordering invariants.
type recordingBroker struct {
	mu         sync.Mutex
	msgs       map[string][]message.Message
	published  []string
	acked      []string
	publishErr error
	ackErr     error
	inspectErr error
	// missingQueues are queue names Stats reports as not existing.
	missingQueues map[string]bool
	// statsErr, when set, is returned by Stats for every queue.
	statsErr error
}

func (b *recordingBroker) Connect(ctx context.Context, cfg broker.ConnectionConfig) error { return nil }
func (b *recordingBroker) Close() error                                                   { return nil }
func (b *recordingBroker) ListQueues(ctx context.Context) ([]broker.QueueSummary, error) {
	return nil, nil
}
func (b *recordingBroker) Stats(ctx context.Context, queue string) (broker.QueueStats, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.statsErr != nil {
		return broker.QueueStats{}, b.statsErr
	}
	if b.missingQueues[queue] {
		return broker.QueueStats{}, fmt.Errorf("queue %q not found: %w", queue, broker.ErrQueueNotFound)
	}
	return broker.QueueStats{}, nil
}
func (b *recordingBroker) Search(ctx context.Context, queue string, f broker.SearchFilter) ([]message.Message, error) {
	return nil, nil
}

func (b *recordingBroker) Inspect(ctx context.Context, queue, id string) (*message.Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inspectErr != nil {
		return nil, b.inspectErr
	}
	for _, m := range b.msgs[queue] {
		if m.ID == id {
			c := m
			return &c, nil
		}
	}
	return nil, fmt.Errorf("message %q not found in queue %q", id, queue)
}

func (b *recordingBroker) Publish(ctx context.Context, destination string, m *message.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published = append(b.published, m.ID)
	return b.publishErr
}

func (b *recordingBroker) Ack(ctx context.Context, queue, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.acked = append(b.acked, id)
	return b.ackErr
}

func dlqMessage() message.Message {
	return message.Message{
		ID:          "m1",
		Queue:       "orders-dlq",
		Destination: "orders",
		Payload:     []byte(`{"order_id":123}`),
	}
}

func testRequest(b *recordingBroker, s *audit.Store) Request {
	return Request{
		Broker:     b,
		Audit:      s,
		Queue:      "orders-dlq",
		MessageID:  "m1",
		BrokerName: "rabbitmq",
		Profile:    "dev",
	}
}

// confirmedRequest is testRequest with an explicit confirm, as a real
// operator must provide.
func confirmedRequest(b *recordingBroker, s *audit.Store) Request {
	r := testRequest(b, s)
	r.Confirm = true
	return r
}

func openAudit(t *testing.T) *audit.Store {
	t.Helper()
	s, err := audit.Open(filepath.Join(t.TempDir(), "audit.db"))
	if err != nil {
		t.Fatalf("audit.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestExecuteRequiresConfirm(t *testing.T) {
	b := &recordingBroker{msgs: map[string][]message.Message{"orders-dlq": {dlqMessage()}}}
	_, err := Execute(context.Background(), testRequest(b, openAudit(t)))
	if !errors.Is(err, ErrConfirmRequired) {
		t.Fatalf("Execute without confirm = %v, want ErrConfirmRequired", err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.published) != 0 || len(b.acked) != 0 {
		t.Fatalf("mutating I/O without confirm: published=%v acked=%v", b.published, b.acked)
	}
}

func TestPreviewPerformsNoMutatingIO(t *testing.T) {
	s := openAudit(t)
	b := &recordingBroker{msgs: map[string][]message.Message{"orders-dlq": {dlqMessage()}}}
	p, err := DryRun(context.Background(), testRequest(b, s))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if p.Destination != "orders" {
		t.Errorf("Destination = %q, want orders", p.Destination)
	}
	if len(p.SafetyChecks) != 4 {
		t.Errorf("SafetyChecks = %v", p.SafetyChecks)
	}
	found := false
	for _, c := range p.SafetyChecks {
		if c == "destination_exists" {
			found = true
		}
	}
	if !found {
		t.Errorf("SafetyChecks = %v, missing destination_exists", p.SafetyChecks)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.published) != 0 || len(b.acked) != 0 {
		t.Fatalf("Preview performed mutating I/O: published=%v acked=%v", b.published, b.acked)
	}

	// The dry-run itself is audited.
	entries, err := s.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !entries[0].DryRun || entries[0].Result != "dry_run" || entries[0].MessageID != "m1" {
		t.Fatalf("dry-run audit entry = %+v", entries)
	}
}

func TestExecutePublishFailureLeavesMessage(t *testing.T) {
	s := openAudit(t)
	b := &recordingBroker{
		msgs:       map[string][]message.Message{"orders-dlq": {dlqMessage()}},
		publishErr: errors.New("broker down"),
	}
	_, err := Execute(context.Background(), confirmedRequest(b, s))
	if err == nil {
		t.Fatal("Execute expected publish error")
	}

	b.mu.Lock()
	acked := append([]string(nil), b.acked...)
	b.mu.Unlock()
	if len(acked) != 0 {
		t.Fatalf("Ack called after failed publish: %v", acked)
	}

	entries, err := s.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Result != "publish_failed" {
		t.Fatalf("audit entries = %+v, want publish_failed", entries)
	}
}

func TestExecutePublishBeforeAck(t *testing.T) {
	s := openAudit(t)
	b := &recordingBroker{msgs: map[string][]message.Message{"orders-dlq": {dlqMessage()}}}
	res, err := Execute(context.Background(), confirmedRequest(b, s))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.Published || !res.Acked {
		t.Errorf("Result = %+v, want published+acked", res)
	}
	if res.Audit.Result != "success" || !res.Audit.Confirmed || res.Audit.DryRun {
		t.Errorf("audit entry = %+v", res.Audit)
	}

	b.mu.Lock()
	published := append([]string(nil), b.published...)
	acked := append([]string(nil), b.acked...)
	b.mu.Unlock()
	if len(published) != 1 || len(acked) != 1 || published[0] != "m1" || acked[0] != "m1" {
		t.Fatalf("published=%v acked=%v", published, acked)
	}
}

func TestExecuteAckFailureReported(t *testing.T) {
	s := openAudit(t)
	b := &recordingBroker{
		msgs:   map[string][]message.Message{"orders-dlq": {dlqMessage()}},
		ackErr: errors.New("ack failed"),
	}
	_, err := Execute(context.Background(), confirmedRequest(b, s))
	if err == nil {
		t.Fatal("expected ack failure error")
	}
	entries, err := s.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Result != "ack_failed" {
		t.Fatalf("audit entries = %+v, want ack_failed", entries)
	}
}

func TestPreviewSurfacesDuplicateEvidence(t *testing.T) {
	s := openAudit(t)
	when := time.Now().UTC().Add(-24 * time.Hour)
	if err := s.Append(audit.Entry{
		Timestamp: when, Action: audit.ActionReplay, MessageID: "m1",
		Confirmed: true, Result: "success", SourceQueue: "orders-dlq",
	}); err != nil {
		t.Fatal(err)
	}

	b := &recordingBroker{msgs: map[string][]message.Message{"orders-dlq": {dlqMessage()}}}
	p, err := DryRun(context.Background(), testRequest(b, s))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !p.Duplicate.MatchFound || p.Duplicate.MatchSource != "audit" {
		t.Errorf("Duplicate = %+v, want audit match", p.Duplicate)
	}
	if len(p.Warnings) == 0 {
		t.Error("expected a duplicate warning")
	}
}

func TestExecuteMessageNotFound(t *testing.T) {
	b := &recordingBroker{msgs: map[string][]message.Message{}}
	_, err := Execute(context.Background(), testRequest(b, openAudit(t)))
	if err == nil {
		t.Fatal("expected not-found error")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.published) != 0 || len(b.acked) != 0 {
		t.Fatalf("I/O performed for missing message: %v %v", b.published, b.acked)
	}
}

func TestExecuteNoDestination(t *testing.T) {
	m := dlqMessage()
	m.Destination = ""
	b := &recordingBroker{msgs: map[string][]message.Message{"orders-dlq": {m}}}
	_, err := Execute(context.Background(), testRequest(b, openAudit(t)))
	if err == nil {
		t.Fatal("expected no-destination error")
	}
}

func TestExecuteDestinationOverride(t *testing.T) {
	m := dlqMessage()
	m.Destination = "" // normally an error — but the override saves it
	b := &recordingBroker{msgs: map[string][]message.Message{"orders-dlq": {m}}}
	r := testRequest(b, openAudit(t))
	r.Destination = "orders"
	r.Confirm = true
	res, err := Execute(context.Background(), r)
	if err != nil {
		t.Fatalf("Execute with override: %v", err)
	}
	if res.Destination != "orders" {
		t.Errorf("Destination = %q", res.Destination)
	}
}

func TestPreviewSurfacesMissingDestination(t *testing.T) {
	s := openAudit(t)
	b := &recordingBroker{
		msgs:          map[string][]message.Message{"orders-dlq": {dlqMessage()}},
		missingQueues: map[string]bool{"orders": true},
	}
	p, err := DryRun(context.Background(), testRequest(b, s))
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	// The destination check is NOT reported as passed, and a warning names
	// the missing queue.
	for _, c := range p.SafetyChecks {
		if c == "destination_exists" {
			t.Error("destination_exists reported as a passed check for a missing queue")
		}
	}
	found := false
	for _, w := range p.Warnings {
		if strings.Contains(w, "orders") && strings.Contains(w, "refused") {
			found = true
		}
	}
	if !found {
		t.Errorf("warnings = %v, want the missing-destination warning", p.Warnings)
	}

	b.mu.Lock()
	published := len(b.published)
	acked := len(b.acked)
	b.mu.Unlock()
	if published != 0 || acked != 0 {
		t.Fatalf("dry-run performed mutating I/O: published=%d acked=%d", published, acked)
	}

	// The dry-run itself is still audited.
	entries, err := s.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Result != "dry_run" {
		t.Fatalf("audit entries = %+v, want one dry_run", entries)
	}
}

func TestExecuteRefusesMissingDestination(t *testing.T) {
	s := openAudit(t)
	b := &recordingBroker{
		msgs:          map[string][]message.Message{"orders-dlq": {dlqMessage()}},
		missingQueues: map[string]bool{"orders": true},
	}
	_, err := Execute(context.Background(), confirmedRequest(b, s))
	if !errors.Is(err, ErrDestinationMissing) {
		t.Fatalf("got %v, want ErrDestinationMissing", err)
	}

	b.mu.Lock()
	published := len(b.published)
	acked := len(b.acked)
	b.mu.Unlock()
	if published != 0 || acked != 0 {
		t.Fatalf("refused run performed I/O: published=%d acked=%d", published, acked)
	}

	// The refusal is audited with the destination named in the reason.
	entries, err := s.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Result != "refused" || entries[0].MessageID != "m1" {
		t.Fatalf("audit entries = %+v, want one refused entry", entries)
	}
	if !strings.Contains(entries[0].Reason, "orders") {
		t.Errorf("refusal reason = %q, want the queue name", entries[0].Reason)
	}
}

func TestExecuteRefusesDestinationCheckError(t *testing.T) {
	// A Stats failure that is not "queue not found" (broker down, permission
	// denied) also refuses — we cannot verify the destination, so nothing is
	// published or acked.
	s := openAudit(t)
	b := &recordingBroker{
		msgs:     map[string][]message.Message{"orders-dlq": {dlqMessage()}},
		statsErr: errors.New("broker down"),
	}
	_, err := Execute(context.Background(), confirmedRequest(b, s))
	if err == nil || !strings.Contains(err.Error(), "destination check") {
		t.Fatalf("got %v, want a destination-check error", err)
	}
	b.mu.Lock()
	published := len(b.published)
	acked := len(b.acked)
	b.mu.Unlock()
	if published != 0 || acked != 0 {
		t.Fatalf("refused run performed I/O: published=%d acked=%d", published, acked)
	}
	entries, err := s.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Result != "refused" {
		t.Fatalf("audit entries = %+v, want one refused entry", entries)
	}
}
