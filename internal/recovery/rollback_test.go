package recovery

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/audit"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

func rollbackSnaps() []audit.Snapshot {
	return []audit.Snapshot{
		{PlanID: "plan_rb", MessageID: "m1", SourceQueue: "orders-dlq", Destination: "orders", Payload: []byte(`{"order_id":1}`), ContentType: "application/json", Headers: map[string]string{"x-event-type": "order.placed"}},
		{PlanID: "plan_rb", MessageID: "m2", SourceQueue: "orders-dlq", Destination: "orders", Payload: []byte(`{"order_id":2}`)},
		{PlanID: "plan_rb", MessageID: "m3", SourceQueue: "orders-dlq", Destination: "orders", Payload: []byte(`{"order_id":3}`)},
	}
}

func TestRollbackDryRunPublishesNothing(t *testing.T) {
	store := execStore(t)
	b := &valBroker{}
	res, err := Rollback(context.Background(), b, store, rollbackSnaps(), RollbackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Snapshots != 3 || res.Restored != 0 || res.Failed != 0 || res.DLQ != "orders-dlq" {
		t.Errorf("result = %+v", res)
	}
	if res.MissingDLQ != "" {
		t.Errorf("missing = %q, want empty", res.MissingDLQ)
	}
	b.mu.Lock()
	publishes := b.publishes
	b.mu.Unlock()
	if publishes != 0 {
		t.Fatalf("dry-run published %d messages", publishes)
	}
}

func TestRollbackDryRunSurfacesMissingDLQ(t *testing.T) {
	store := execStore(t)
	b := &valBroker{missingQueues: map[string]bool{"orders-dlq": true}}
	res, err := Rollback(context.Background(), b, store, rollbackSnaps(), RollbackOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.MissingDLQ != "orders-dlq" || res.Restored != 0 {
		t.Errorf("result = %+v, want the missing DLQ surfaced and nothing restored", res)
	}
	b.mu.Lock()
	publishes := b.publishes
	b.mu.Unlock()
	if publishes != 0 {
		t.Fatalf("dry-run published %d messages", publishes)
	}
}

func TestRollbackRestoresAndAudits(t *testing.T) {
	store := execStore(t)
	b := &valBroker{}
	res, err := Rollback(context.Background(), b, store, rollbackSnaps(), RollbackOptions{
		Confirm: true, Reason: "bad replay", BrokerName: "rabbitmq", Profile: "dev",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Restored != 3 || res.Failed != 0 {
		t.Errorf("result = %+v, want 3 restored and 0 failed", res)
	}

	b.mu.Lock()
	dests := append([]string(nil), b.publishedDests...)
	msgs := append([]message.Message(nil), b.publishedMsgs...)
	b.mu.Unlock()
	if len(dests) != 3 {
		t.Fatalf("publishes = %d, want 3", len(dests))
	}
	for i, d := range dests {
		if d != "orders-dlq" {
			t.Errorf("publish %d dest = %q, want orders-dlq (the DLQ it came from)", i, d)
		}
	}
	// The restored message carries the original destination (on the message
	// and as the x-destination header) so a future plan can replay it again.
	if msgs[0].Destination != "orders" || msgs[0].Headers["x-destination"] != "orders" {
		t.Errorf("restored message = %+v, want the original destination preserved", msgs[0])
	}
	if string(msgs[0].Payload) != `{"order_id":1}` || msgs[0].ContentType != "application/json" {
		t.Errorf("restored message = %+v, want the original payload/content type", msgs[0])
	}
	if msgs[0].ID != "m1" || msgs[0].Headers["x-event-type"] != "order.placed" {
		t.Errorf("restored message = %+v, want the original identity and headers", msgs[0])
	}

	entries, err := store.Recent(100)
	if err != nil {
		t.Fatal(err)
	}
	// 3 per-message rollback successes + 1 plan-level completed.
	if len(entries) != 4 {
		t.Fatalf("audit entries = %d, want 4", len(entries))
	}
	success := 0
	for _, e := range entries {
		if e.Action != audit.ActionRollback || !e.Confirmed {
			t.Errorf("entry = %+v, want a confirmed rollback action", e)
		}
		if e.Result == "success" {
			success++
		}
		if !strings.Contains(e.Reason, "bad replay") {
			t.Errorf("reason = %q, want the operator reason recorded", e.Reason)
		}
	}
	if success != 3 {
		t.Errorf("rollback success entries = %d, want 3", success)
	}
}

func TestRollbackRefusesMissingDLQ(t *testing.T) {
	store := execStore(t)
	b := &valBroker{missingQueues: map[string]bool{"orders-dlq": true}}
	_, err := Rollback(context.Background(), b, store, rollbackSnaps(), RollbackOptions{
		Confirm: true, BrokerName: "rabbitmq", Profile: "dev",
	})
	if !errors.Is(err, ErrDestinationMissing) {
		t.Fatalf("got %v, want ErrDestinationMissing", err)
	}
	b.mu.Lock()
	publishes := b.publishes
	b.mu.Unlock()
	if publishes != 0 {
		t.Fatalf("refused rollback published %d messages", publishes)
	}
	entries, err := store.Recent(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Result != "refused" || entries[0].Action != audit.ActionRollback {
		t.Errorf("audit entries = %+v, want one refused rollback", entries)
	}
}

func TestRollbackPublishFailureCountsAndAudits(t *testing.T) {
	store := execStore(t)
	b := &valBroker{failPublish: map[string]int{"m2": 5}}
	res, err := Rollback(context.Background(), b, store, rollbackSnaps(), RollbackOptions{Confirm: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Restored != 2 || res.Failed != 1 {
		t.Errorf("result = %+v, want 2 restored and 1 failed", res)
	}
	entries, err := store.Recent(100)
	if err != nil {
		t.Fatal(err)
	}
	failed := false
	for _, e := range entries {
		if e.MessageID == "m2" && e.Result == "failed" {
			failed = true
			if !strings.Contains(e.Reason, "not restored") {
				t.Errorf("failure reason = %q, want the message named as unrestored", e.Reason)
			}
		}
	}
	if !failed {
		t.Errorf("no failed audit entry for m2; entries = %+v", entries)
	}
}

func TestRollbackEmptySnapshots(t *testing.T) {
	_, err := Rollback(context.Background(), &valBroker{}, execStore(t), nil, RollbackOptions{Confirm: true})
	if err == nil || !strings.Contains(err.Error(), "no snapshots") {
		t.Fatalf("got %v, want a no-snapshots error", err)
	}
}
