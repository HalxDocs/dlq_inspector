package recovery

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// valBroker is a minimal in-memory Broker for validator tests. It records
// every Publish/Ack so tests can prove the dry-run performed no mutating I/O.
type valBroker struct {
	mu        sync.Mutex
	msgs      map[string][]message.Message
	searches  int
	publishes int
	acks      int
	searchErr error
}

func (b *valBroker) Connect(ctx context.Context, cfg broker.ConnectionConfig) error { return nil }
func (b *valBroker) Close() error                                                   { return nil }
func (b *valBroker) ListQueues(ctx context.Context) ([]broker.QueueSummary, error)  { return nil, nil }
func (b *valBroker) Inspect(ctx context.Context, queue, id string) (*message.Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, m := range b.msgs[queue] {
		if m.ID == id {
			c := m
			return &c, nil
		}
	}
	return nil, fmt.Errorf("not found")
}
func (b *valBroker) Search(ctx context.Context, queue string, f broker.SearchFilter) ([]message.Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.searches++
	if b.searchErr != nil {
		return nil, b.searchErr
	}
	return append([]message.Message(nil), b.msgs[queue]...), nil
}
func (b *valBroker) Publish(ctx context.Context, dest string, m *message.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.publishes++
	return nil
}
func (b *valBroker) Ack(ctx context.Context, queue, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.acks++
	return nil
}
func (b *valBroker) Stats(ctx context.Context, queue string) (broker.QueueStats, error) {
	return broker.QueueStats{}, nil
}

func plan(t *testing.T, ids []string) *RecoveryPlan {
	t.Helper()
	return &RecoveryPlan{
		ID:           "plan_test",
		Queue:        "orders-dlq",
		MessageIDs:   ids,
		Destination:  "orders",
		Action:       "replay",
		Limits:       DefaultLimits(),
		SafetyChecks: []string{CheckSchema, CheckDuplicate},
	}
}

func TestValidateAllPass(t *testing.T) {
	b := &valBroker{msgs: map[string][]message.Message{
		"orders-dlq": {
			{ID: "m1", Destination: "orders", Payload: []byte(`{"order_id":1}`)},
			{ID: "m2", Destination: "orders", Payload: []byte(`{"order_id":2}`)},
		},
	}}
	res, err := (PlanValidator{Broker: b}).Validate(context.Background(), plan(t, []string{"m1", "m2"}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Selected != 2 || res.Validated != 2 || res.Skipped != 0 || res.ToReplay != 2 {
		t.Errorf("result = %+v", res)
	}
	if len(res.ChecksRun) != 2 {
		t.Errorf("checks_run = %v", res.ChecksRun)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.publishes != 0 || b.acks != 0 {
		t.Fatalf("dry-run performed mutating I/O: publishes=%d acks=%d", b.publishes, b.acks)
	}
}

func TestValidateRejectsEmptySafetyChecks(t *testing.T) {
	p := plan(t, []string{"m1"})
	p.SafetyChecks = nil
	b := &valBroker{msgs: map[string][]message.Message{"orders-dlq": {{ID: "m1", Payload: []byte(`{}`)}}}}
	_, err := (PlanValidator{Broker: b}).Validate(context.Background(), p)
	if !errors.Is(err, ErrNoSafetyChecks) {
		t.Fatalf("got %v, want ErrNoSafetyChecks", err)
	}
}

func TestValidateSkipsNotFound(t *testing.T) {
	b := &valBroker{msgs: map[string][]message.Message{
		"orders-dlq": {{ID: "m1", Payload: []byte(`{}`)}},
	}}
	res, err := (PlanValidator{Broker: b}).Validate(context.Background(), plan(t, []string{"m1", "gone"}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Selected != 2 || res.Skipped != 1 || res.ToReplay != 1 {
		t.Errorf("result = %+v", res)
	}
	if len(res.SkippedMessages) != 1 || res.SkippedMessages[0].MessageID != "gone" || res.SkippedMessages[0].Reason != SkipNotFound {
		t.Errorf("skipped = %+v", res.SkippedMessages)
	}
}

func TestValidateSkipsInvalidPayload(t *testing.T) {
	b := &valBroker{msgs: map[string][]message.Message{
		"orders-dlq": {
			{ID: "good", Payload: []byte(`{"ok":true}`)},
			{ID: "bad", Payload: []byte(`not json at all`)},
		},
	}}
	res, err := (PlanValidator{Broker: b}).Validate(context.Background(), plan(t, []string{"good", "bad"}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Validated != 1 || res.Skipped != 1 {
		t.Errorf("result = %+v", res)
	}
	if res.SkippedMessages[0].MessageID != "bad" || res.SkippedMessages[0].Reason != SkipInvalid {
		t.Errorf("skipped = %+v", res.SkippedMessages)
	}
}

func TestValidateFlagsDuplicateEvidence(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "audit.db")
	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Append(audit.Entry{
		Action: audit.ActionReplay, MessageID: "m1", SourceQueue: "orders-dlq",
		Destination: "orders", Confirmed: true, Result: "success", Timestamp: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	b := &valBroker{msgs: map[string][]message.Message{
		"orders-dlq": {
			{ID: "m1", Destination: "orders", Payload: []byte(`{"order_id":1}`)},
			{ID: "m2", Destination: "orders", Payload: []byte(`{"order_id":2}`)},
		},
	}}
	res, err := (PlanValidator{Broker: b, Audit: store}).Validate(context.Background(), plan(t, []string{"m1", "m2"}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Duplicates != 1 || res.Skipped != 1 || res.ToReplay != 1 {
		t.Errorf("result = %+v", res)
	}
	if res.SkippedMessages[0].Reason != SkipDuplicate {
		t.Errorf("skipped = %+v", res.SkippedMessages)
	}
}

func TestValidateWithoutDuplicateCheck(t *testing.T) {
	// When the plan omits the duplicate check, prior replays are not a skip
	// reason — the plan author chose not to gate on them.
	auditPath := filepath.Join(t.TempDir(), "audit.db")
	store, err := audit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Append(audit.Entry{
		Action: audit.ActionReplay, MessageID: "m1", SourceQueue: "orders-dlq",
		Destination: "orders", Confirmed: true, Result: "success",
	}); err != nil {
		t.Fatal(err)
	}

	p := plan(t, []string{"m1"})
	p.SafetyChecks = []string{CheckSchema}
	b := &valBroker{msgs: map[string][]message.Message{
		"orders-dlq": {{ID: "m1", Destination: "orders", Payload: []byte(`{}`)}},
	}}
	res, err := (PlanValidator{Broker: b, Audit: store}).Validate(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if res.Duplicates != 0 || res.Skipped != 0 {
		t.Errorf("result = %+v, want no duplicate gate", res)
	}
}

func TestValidateBrokerError(t *testing.T) {
	b := &valBroker{searchErr: errors.New("broker down")}
	_, err := (PlanValidator{Broker: b}).Validate(context.Background(), plan(t, []string{"m1"}))
	if err == nil {
		t.Fatal("expected broker error to surface")
	}
}
