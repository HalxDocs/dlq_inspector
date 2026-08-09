package rabbitmq_test

// The full recovery loop against a real RabbitMQ: publish failing messages,
// dead-letter them into a DLQ, then run the real CLI workflow — analyze,
// plan, dry-run recover, confirmed recover — and assert the audit trail
// records every outcome. This is what the CI workflow (which provides
// DLQ_TEST_AMQP_URL / DLQ_TEST_MANAGEMENT_URL) exercises end to end.
//
// The fixture uses raw AMQP (declare a source queue wired to a DLQ through a
// dead-letter exchange, publish, nack) because the tool deliberately has no
// "create infrastructure" command; the workflow itself runs entirely through
// the public CLI.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/HalxDocs/dlq_inspector/internal/command"
	"github.com/HalxDocs/dlq_inspector/internal/config"
	"github.com/HalxDocs/dlq_inspector/internal/recovery"
)

func TestRecoveryLoopEndToEnd(t *testing.T) {
	url := os.Getenv("DLQ_TEST_AMQP_URL")
	mgmtURL := os.Getenv("DLQ_TEST_MANAGEMENT_URL")
	if url == "" || mgmtURL == "" {
		t.Skip("DLQ_TEST_AMQP_URL / DLQ_TEST_MANAGEMENT_URL not set")
	}

	// ---- Fixture: source queue dead-lettering into a DLQ. ----
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	defer ch.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	source := "orders-" + suffix
	dlq := source + "-dlq"
	dlx := "dlx-" + suffix

	if err := ch.ExchangeDeclare(dlx, "direct", true, false, false, false, nil); err != nil {
		t.Fatalf("declare dlx: %v", err)
	}
	t.Cleanup(func() { _ = ch.ExchangeDelete(dlx, false, false) })

	if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		t.Fatalf("declare dlq: %v", err)
	}
	if err := ch.QueueBind(dlq, source, dlx, false, nil); err != nil {
		t.Fatalf("bind dlq: %v", err)
	}
	t.Cleanup(func() { _, _ = ch.QueueDelete(dlq, false, false, false) })

	if _, err := ch.QueueDeclare(source, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    dlx,
		"x-dead-letter-routing-key": source,
	}); err != nil {
		t.Fatalf("declare source: %v", err)
	}
	t.Cleanup(func() { _, _ = ch.QueueDelete(source, false, false, false) })

	// Publish four failing messages with publisher confirms so we know they
	// arrived before consuming. The fourth carries x-duplicate-of: the
	// application itself marks it as a duplicate, so the classifier must say
	// DO_NOT_REPLAY and the plan must leave it in the DLQ.
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("enable confirms: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 4))
	for i := 1; i <= 3; i++ {
		body := fmt.Sprintf(`{"order_id":%d,"customer_id":%d}`, i, 1000+i)
		if err := ch.Publish("", source, false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if err := ch.Publish("", source, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         []byte(`{"order_id":4,"customer_id":1004}`),
		Headers: amqp.Table{
			"x-duplicate-of": "evt_original_order_4",
			"x-event-type":   "order.duplicate",
		},
	}); err != nil {
		t.Fatalf("publish duplicate: %v", err)
	}
	for i := 0; i < 4; i++ {
		if c := <-confirms; !c.Ack {
			t.Fatalf("publish %d not confirmed", i+1)
		}
	}

	// Drive them to the DLQ: consume without auto-ack and nack with
	// requeue=false, which is what dead-letters a message. The consumer is
	// cancelled immediately afterwards so it cannot swallow the replayed
	// messages later (a live consumer receives them and holds them unacked,
	// which would make the source queue look empty).
	deliveries, err := ch.Consume(source, "fixture-consumer", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	for i := 0; i < 4; i++ {
		select {
		case d := <-deliveries:
			if err := d.Nack(false, false); err != nil {
				t.Fatalf("nack %d: %v", i+1, err)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("timed out waiting for delivery %d", i+1)
		}
	}
	if err := ch.Cancel("fixture-consumer", false); err != nil {
		t.Fatalf("cancel consumer: %v", err)
	}
	waitQueueDepth(t, ch, dlq, 4)

	// ---- Config: profile pointing at the live broker, temp audit store. ----
	auditPath := filepath.Join(t.TempDir(), "audit.db")
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfg := config.Defaults()
	cfg.DefaultProfile = "ci"
	cfg.Audit.Path = auditPath
	cfg.Profiles = map[string]*config.Profile{
		"ci": {Broker: "rabbitmq", URL: url, ManagementURL: mgmtURL, DefaultQueue: dlq},
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

	// ---- Analyze: four messages form two groups; the duplicate-marked one
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
	if !strings.Contains(out, "Plan written: "+planPath) || !strings.Contains(out, "3 messages selected") ||
		!strings.Contains(out, "1 excluded (left in DLQ)") {
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
		t.Errorf("plan = %+v", plan)
	}
	if len(plan.Excluded) != 1 || plan.Excluded[0].Classification != "DO_NOT_REPLAY" {
		t.Errorf("plan exclusions = %+v, want the duplicate recorded as DO_NOT_REPLAY", plan.Excluded)
	}

	// ---- Dry-run: validates the selected three, reports the exclusion,
	// changes nothing. ----
	out = runCLI(t, "recover", "--plan", planPath, "--config", cfgPath)
	for _, want := range []string{"Payload validation:", "3/3 passed", "Messages to replay:", "3", "Excluded:", "Changes made: NONE"} {
		if !strings.Contains(out, want) {
			t.Errorf("dry-run output missing %q:\n%s", want, out)
		}
	}
	// The dry-run must not have moved anything.
	waitQueueDepth(t, ch, dlq, 4)

	// ---- Confirm: replays the three, leaves the duplicate in the DLQ. ----
	out = runCLI(t, "recover", "--plan", planPath, "--confirm", "--batch-size", "2", "--rate-limit", "0", "--output", "json", "--config", cfgPath)
	var execRes recovery.ExecutionResult
	if err := json.Unmarshal([]byte(out), &execRes); err != nil {
		t.Fatalf("confirm output is not JSON: %v\n%s", err, out)
	}
	if execRes.Replayed != 3 || execRes.Excluded != 1 || execRes.Failed != 0 || execRes.NewDLQEntries != 0 || execRes.Skipped != 0 || execRes.Tripped {
		t.Errorf("execution result = %+v, want 3 replayed, 1 excluded, 0 failed", execRes)
	}

	// The duplicate stays in the DLQ untouched; the source queue holds the
	// three replayed messages.
	waitQueueDepth(t, ch, dlq, 1)
	waitQueueDepth(t, ch, source, 3)

	// ---- History: the plan's full trail records every outcome, including
	// the skipped duplicate. ----
	out = runCLI(t, "history", "--plan", plan.ID, "--config", cfgPath)
	if !strings.Contains(out, "Plan "+plan.ID) {
		t.Errorf("history missing plan header:\n%s", out)
	}
	if got := strings.Count(out, "success"); got != 3 {
		t.Errorf("history has %d success entries, want 3:\n%s", got, out)
	}
	for _, want := range []string{"dry_run", "completed", "(plan)", "skipped"} {
		if !strings.Contains(out, want) {
			t.Errorf("history output missing %q:\n%s", want, out)
		}
	}
}

// waitQueueDepth polls an AMQP passive declare until the queue holds exactly
// want messages (or the test fails after a deadline).
func waitQueueDepth(t *testing.T, ch *amqp.Channel, queue string, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
		if err == nil && q.Messages == want {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("queue %s: %v", queue, err)
	}
	t.Fatalf("queue %s depth = %d, want %d", queue, q.Messages, want)
}
