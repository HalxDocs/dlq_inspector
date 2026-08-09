package recovery

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

func planFixture() []message.Message {
	return []message.Message{
		{ID: "m1", Destination: "orders", FailureReason: "timeout connecting to 10.0.4.5:6432", RetryCount: 1},
		{ID: "m2", Destination: "orders", FailureReason: "timeout connecting to 10.0.4.9:6432", RetryCount: 1},
		{ID: "m3", Destination: "orders", FailureReason: "validation failed: customer_id is required"},
		{ID: "m4", Destination: "orders", FailureReason: "duplicate event already processed"},
		{ID: "m5", Destination: "orders", FailureReason: "timeout connecting to 10.0.4.11:6432", RetryCount: 2},
	}
}

func TestBuildPlanAllGroupsExcludesDoNotReplay(t *testing.T) {
	plan, err := BuildPlan(planFixture(), PlanOptions{Queue: "orders-dlq"})
	if err != nil {
		t.Fatal(err)
	}
	// m4 is DO_NOT_REPLAY and must be excluded by default; the four others
	// (three timeout, one validation) are selected.
	if len(plan.MessageIDs) != 4 {
		t.Fatalf("message_ids = %v, want 4 (DO_NOT_REPLAY excluded)", plan.MessageIDs)
	}
	for _, id := range plan.MessageIDs {
		if id == "m4" {
			t.Error("DO_NOT_REPLAY message selected")
		}
	}
	// The exclusion is recorded on the plan with its reason.
	if len(plan.Excluded) != 1 || plan.Excluded[0].MessageID != "m4" ||
		plan.Excluded[0].Classification != DoNotReplay || plan.Excluded[0].Reason == "" {
		t.Errorf("excluded = %+v, want m4 recorded as DO_NOT_REPLAY", plan.Excluded)
	}
	if plan.Destination != "orders" {
		t.Errorf("destination = %q", plan.Destination)
	}
	if plan.Action != "replay" {
		t.Errorf("action = %q", plan.Action)
	}
	if len(plan.SafetyChecks) != 3 {
		t.Errorf("safety checks = %v", plan.SafetyChecks)
	}
	if !contains(plan.SafetyChecks, CheckDestination) {
		t.Errorf("safety checks %v missing destination_checked", plan.SafetyChecks)
	}
}

func TestBuildPlanGroupFilter(t *testing.T) {
	timeoutID := groupID("timeout connecting to {ip}:{port}")
	plan, err := BuildPlan(planFixture(), PlanOptions{Queue: "orders-dlq", GroupID: timeoutID})
	if err != nil {
		t.Fatal(err)
	}
	// The timeout group has m1, m2, m5 — the validation and duplicate
	// messages belong to other groups.
	if len(plan.MessageIDs) != 3 {
		t.Fatalf("message_ids = %v, want the 3 timeout messages", plan.MessageIDs)
	}
	if plan.GroupID != timeoutID || plan.GroupLabel != "Timeout connecting to" {
		t.Errorf("group = %q (%q)", plan.GroupID, plan.GroupLabel)
	}
}

func TestBuildPlanIncludeDoNotReplay(t *testing.T) {
	plan, err := BuildPlan(planFixture(), PlanOptions{Queue: "orders-dlq", IncludeDoNotReplay: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.MessageIDs) != 5 {
		t.Fatalf("message_ids = %v, want all 5 with IncludeDoNotReplay", plan.MessageIDs)
	}
	if len(plan.Excluded) != 0 {
		t.Errorf("excluded = %+v, want none when including DO_NOT_REPLAY", plan.Excluded)
	}
}

func TestBuildPlanRecordsHeaderDuplicateExclusion(t *testing.T) {
	// A message the application itself marks as a duplicate (x-duplicate-of
	// header) is excluded and recorded, even though its broker-set failure
	// text would normally classify it REQUIRES_FIX.
	msgs := []message.Message{
		{ID: "good", Destination: "orders", FailureReason: "rejected"},
		{ID: "dup", Destination: "orders", FailureReason: "rejected", Headers: map[string]string{"x-duplicate-of": "evt_42"}},
	}
	plan, err := BuildPlan(msgs, PlanOptions{Queue: "orders-dlq"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.MessageIDs) != 1 || plan.MessageIDs[0] != "good" {
		t.Errorf("message_ids = %v, want only good", plan.MessageIDs)
	}
	if len(plan.Excluded) != 1 || plan.Excluded[0].MessageID != "dup" || plan.Excluded[0].Classification != DoNotReplay {
		t.Errorf("excluded = %+v, want dup recorded", plan.Excluded)
	}
}

func TestBuildPlanUnknownGroup(t *testing.T) {
	_, err := BuildPlan(planFixture(), PlanOptions{Queue: "orders-dlq", GroupID: "deadbeef"})
	if err == nil || !strings.Contains(err.Error(), "no messages selected") {
		t.Fatalf("expected no-messages error, got %v", err)
	}
}

func TestBuildPlanEmpty(t *testing.T) {
	if _, err := BuildPlan(nil, PlanOptions{Queue: "q"}); err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestBuildPlanMixedDestinations(t *testing.T) {
	msgs := []message.Message{
		{ID: "a", Destination: "orders", FailureReason: "timeout"},
		{ID: "b", Destination: "payments", FailureReason: "timeout"},
	}
	if _, err := BuildPlan(msgs, PlanOptions{Queue: "q"}); err == nil {
		t.Fatal("expected mixed-destination error without --destination")
	}
	plan, err := BuildPlan(msgs, PlanOptions{Queue: "q", Destination: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Destination != "orders" {
		t.Errorf("destination = %q, want the override", plan.Destination)
	}
}

func TestBuildPlanNoDestinationMetadata(t *testing.T) {
	msgs := []message.Message{{ID: "a", FailureReason: "timeout"}}
	if _, err := BuildPlan(msgs, PlanOptions{Queue: "q"}); err == nil {
		t.Fatal("expected error when no destination is known")
	}
}

func TestBuildPlanDefaultsLimits(t *testing.T) {
	plan, err := BuildPlan(planFixture(), PlanOptions{Queue: "orders-dlq"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Limits.BatchSize != 25 || plan.Limits.RateLimit != "10/s" || plan.Limits.Concurrency != 1 {
		t.Errorf("limits = %+v, want defaults", plan.Limits)
	}
}

func TestPlanJSONGolden(t *testing.T) {
	plan := RecoveryPlan{
		ID:           "plan_1234567890",
		CreatedAt:    testTime(),
		Queue:        "orders-dlq",
		GroupID:      "aabbccdd",
		GroupLabel:   "Timeout connecting to",
		MessageIDs:   []string{"m1", "m2"},
		Destination:  "orders",
		Action:       "replay",
		Limits:       PlanLimits{BatchSize: 25, RateLimit: "10/s", Concurrency: 1},
		SafetyChecks: []string{CheckSchema, CheckDuplicate},
	}
	b, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	for _, want := range []string{
		`"id": "plan_1234567890"`,
		`"queue": "orders-dlq"`,
		`"group_id": "aabbccdd"`,
		`"group_label": "Timeout connecting to"`,
		`"message_ids": [` + "\n    \"m1\",\n    \"m2\"",
		`"destination": "orders"`,
		`"action": "replay"`,
		`"batch_size": 25`,
		`"rate_limit": "10/s"`,
		`"concurrency": 1`,
		`"safety_checks": [` + "\n    \"schema_validated\",\n    \"duplicate_checked\"",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("plan JSON missing %q:\n%s", want, got)
		}
	}
}

func testTime() time.Time {
	return time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
}
