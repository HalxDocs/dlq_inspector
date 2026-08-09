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

	f := newRecoveryFixture(t, ch)
	f.deadLetter(t, ch, []amqp.Publishing{
		{ContentType: "application/json", DeliveryMode: amqp.Persistent, Body: []byte(`{"order_id":1,"customer_id":1001}`)},
		{ContentType: "application/json", DeliveryMode: amqp.Persistent, Body: []byte(`{"order_id":2,"customer_id":1002}`)},
		{ContentType: "application/json", DeliveryMode: amqp.Persistent, Body: []byte(`{"order_id":3,"customer_id":1003}`)},
		// The fourth carries x-duplicate-of: the application itself marks it
		// as a duplicate, so the classifier must say DO_NOT_REPLAY and the
		// plan must leave it in the DLQ.
		{ContentType: "application/json", DeliveryMode: amqp.Persistent, Body: []byte(`{"order_id":4,"customer_id":1004}`), Headers: amqp.Table{
			"x-duplicate-of": "evt_original_order_4",
			"x-event-type":   "order.duplicate",
		}},
	}, 4)

	// ---- Config: profile pointing at the live broker, temp audit store. ----
	cli := newRecoveryCLI(t, url, mgmtURL, f.dlq)
	runCLI := func(t *testing.T, args ...string) string { return cli.run(t, args...) }

	// ---- Analyze: four messages form two groups; the duplicate-marked one
	// must be its own group with a DO_NOT_REPLAY recommendation. ----
	out := runCLI(t, "analyze", "--config", cli.cfgPath)
	for _, want := range []string{"4 messages analyzed", "Rejected", "REQUIRES_FIX", "DO_NOT_REPLAY", "order.duplicate"} {
		if !strings.Contains(out, want) {
			t.Errorf("analyze output missing %q:\n%s", want, out)
		}
	}

	// ---- Plan: the three replayable messages are selected; the duplicate is
	// excluded and recorded on the plan. ----
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	out = runCLI(t, "plan", "--config", cli.cfgPath, "--output-file", planPath)
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
	if len(plan.MessageIDs) != 3 || plan.Destination != f.source {
		t.Errorf("plan = %+v", plan)
	}
	if len(plan.Excluded) != 1 || plan.Excluded[0].Classification != "DO_NOT_REPLAY" {
		t.Errorf("plan exclusions = %+v, want the duplicate recorded as DO_NOT_REPLAY", plan.Excluded)
	}

	// ---- Dry-run: validates the selected three, reports the exclusion,
	// changes nothing. ----
	out = runCLI(t, "recover", "--plan", planPath, "--config", cli.cfgPath)
	for _, want := range []string{"Payload validation:", "3/3 passed", "Messages to replay:", "3", "Excluded:", "Changes made: NONE"} {
		if !strings.Contains(out, want) {
			t.Errorf("dry-run output missing %q:\n%s", want, out)
		}
	}
	// The dry-run must not have moved anything.
	waitQueueDepth(t, ch, f.dlq, 4)

	// ---- Confirm: replays the three, leaves the duplicate in the DLQ. ----
	out = runCLI(t, "recover", "--plan", planPath, "--confirm", "--batch-size", "2", "--rate-limit", "0", "--output", "json", "--config", cli.cfgPath)
	var execRes recovery.ExecutionResult
	if err := json.Unmarshal([]byte(out), &execRes); err != nil {
		t.Fatalf("confirm output is not JSON: %v\n%s", err, out)
	}
	if execRes.Replayed != 3 || execRes.Excluded != 1 || execRes.Failed != 0 || execRes.NewDLQEntries != 0 || execRes.Skipped != 0 || execRes.Tripped {
		t.Errorf("execution result = %+v, want 3 replayed, 1 excluded, 0 failed", execRes)
	}

	// The duplicate stays in the DLQ untouched; the source queue holds the
	// three replayed messages.
	waitQueueDepth(t, ch, f.dlq, 1)
	waitQueueDepth(t, ch, f.source, 3)

	// ---- History: the plan's full trail records every outcome, including
	// the skipped duplicate. ----
	out = runCLI(t, "history", "--plan", plan.ID, "--config", cli.cfgPath)
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

// TestRecoveryLoopMissingDestination covers the dangerous case where the
// replay destination no longer exists: a message was dead-lettered, then its
// source queue was deleted, so x-death metadata points at a vanished queue.
// A publish into a nonexistent queue is confirmed and silently dropped, so
// acking the DLQ copy would lose the message. The dry-run must surface the
// missing destination, and a confirmed run must refuse before any publish or
// ack — with the DLQ copy and the audit trail proving nothing was lost.
func TestRecoveryLoopMissingDestination(t *testing.T) {
	url := os.Getenv("DLQ_TEST_AMQP_URL")
	mgmtURL := os.Getenv("DLQ_TEST_MANAGEMENT_URL")
	if url == "" || mgmtURL == "" {
		t.Skip("DLQ_TEST_AMQP_URL / DLQ_TEST_MANAGEMENT_URL not set")
	}

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

	// One failing message dead-lettered into the DLQ.
	f := newRecoveryFixture(t, ch)
	f.deadLetter(t, ch, []amqp.Publishing{
		{ContentType: "application/json", DeliveryMode: amqp.Persistent, Body: []byte(`{"order_id":1,"customer_id":1001}`)},
	}, 1)

	// Delete the source queue: the message's replay destination (derived from
	// its x-death metadata) no longer exists.
	if _, err := ch.QueueDelete(f.source, false, false, false); err != nil {
		t.Fatalf("delete source queue: %v", err)
	}

	cli := newRecoveryCLI(t, url, mgmtURL, f.dlq)
	runCLI := func(t *testing.T, args ...string) string { return cli.run(t, args...) }
	runCLIErr := func(t *testing.T, args ...string) (string, error) { return cli.runErr(args...) }

	// ---- Plan: the message is selected, with the vanished queue recorded as
	// its destination. ----
	planPath := filepath.Join(t.TempDir(), "recovery.json")
	out := runCLI(t, "plan", "--config", cli.cfgPath, "--output-file", planPath)
	if !strings.Contains(out, "Plan written: "+planPath) || !strings.Contains(out, "1 messages selected") {
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
	if len(plan.MessageIDs) != 1 || plan.Destination != f.source {
		t.Errorf("plan = %+v, want destination %q", plan, f.source)
	}

	// ---- Dry-run: validation passes and the missing destination is surfaced
	// as a warning — nothing moves. ----
	out = runCLI(t, "recover", "--plan", planPath, "--config", cli.cfgPath)
	for _, want := range []string{
		"Destination warning:", f.source, "does not exist",
		"Payload validation:", "1/1 passed",
		"Messages to replay:", "1",
		"Changes made: NONE",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("dry-run output missing %q:\n%s", want, out)
		}
	}
	waitQueueDepth(t, ch, f.dlq, 1)

	// ---- Confirm: refused before any publish or ack; the DLQ copy stays. ----
	out, err = runCLIErr(t, "recover", "--plan", planPath, "--confirm", "--rate-limit", "0", "--config", cli.cfgPath)
	if err == nil || !strings.Contains(err.Error(), "refusing to run") {
		t.Fatalf("expected refusal, got err=%v\n%s", err, out)
	}
	if !strings.Contains(err.Error(), f.source) {
		t.Errorf("refusal error = %q, want the destination name", err)
	}
	waitQueueDepth(t, ch, f.dlq, 1)

	// ---- Audit: the trail shows the plan, the dry-run, and the refusal —
	// no success entry, because nothing was replayed. ----
	out = runCLI(t, "history", "--plan", plan.ID, "--config", cli.cfgPath)
	for _, want := range []string{"dry_run", "refused", "(plan)"} {
		if !strings.Contains(out, want) {
			t.Errorf("history output missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "success") {
		t.Errorf("history shows a success for a refused run:\n%s", out)
	}
}

// recoveryFixture owns the queues/exchange of one recovery-loop scenario: a
// source queue whose rejected messages dead-letter into a DLQ.
type recoveryFixture struct {
	source string
	dlq    string
	dlx    string
}

// newRecoveryFixture declares the fixture topology (dead-letter exchange, DLQ
// bound to the source routing key, source queue dead-lettering into the DLQ)
// and registers cleanups for every resource.
func newRecoveryFixture(t *testing.T, ch *amqp.Channel) *recoveryFixture {
	t.Helper()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	f := &recoveryFixture{
		source: "orders-" + suffix,
		dlq:    "orders-" + suffix + "-dlq",
		dlx:    "dlx-" + suffix,
	}

	if err := ch.ExchangeDeclare(f.dlx, "direct", true, false, false, false, nil); err != nil {
		t.Fatalf("declare dlx: %v", err)
	}
	t.Cleanup(func() { _ = ch.ExchangeDelete(f.dlx, false, false) })

	if _, err := ch.QueueDeclare(f.dlq, true, false, false, false, nil); err != nil {
		t.Fatalf("declare dlq: %v", err)
	}
	if err := ch.QueueBind(f.dlq, f.source, f.dlx, false, nil); err != nil {
		t.Fatalf("bind dlq: %v", err)
	}
	t.Cleanup(func() { _, _ = ch.QueueDelete(f.dlq, false, false, false) })

	if _, err := ch.QueueDeclare(f.source, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    f.dlx,
		"x-dead-letter-routing-key": f.source,
	}); err != nil {
		t.Fatalf("declare source: %v", err)
	}
	t.Cleanup(func() { _, _ = ch.QueueDelete(f.source, false, false, false) })
	return f
}

// deadLetter publishes each message to the source queue with publisher
// confirms, consumes without auto-ack, and nacks with requeue=false — which
// is what dead-letters a message into the DLQ. The consumer is named and
// cancelled immediately afterwards so it cannot swallow the replayed messages
// later (a live consumer would receive them and hold them unacked, making the
// source queue look empty).
func (f *recoveryFixture) deadLetter(t *testing.T, ch *amqp.Channel, pubs []amqp.Publishing, want int) {
	t.Helper()
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("enable confirms: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, len(pubs)))
	for i := range pubs {
		if err := ch.Publish("", f.source, false, false, pubs[i]); err != nil {
			t.Fatalf("publish %d: %v", i+1, err)
		}
	}
	for i := 0; i < len(pubs); i++ {
		if c := <-confirms; !c.Ack {
			t.Fatalf("publish %d not confirmed", i+1)
		}
	}

	deliveries, err := ch.Consume(f.source, "fixture-consumer", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	for i := 0; i < len(pubs); i++ {
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
	waitQueueDepth(t, ch, f.dlq, want)
}

// recoveryCLI runs the public CLI against a temp config pointing at the live
// broker, with a temp audit store.
type recoveryCLI struct {
	cfgPath string
}

func newRecoveryCLI(t *testing.T, url, mgmtURL, dlq string) *recoveryCLI {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfg := config.Defaults()
	cfg.DefaultProfile = "ci"
	cfg.Audit.Path = filepath.Join(t.TempDir(), "audit.db")
	cfg.Profiles = map[string]*config.Profile{
		"ci": {Broker: "rabbitmq", URL: url, ManagementURL: mgmtURL, DefaultQueue: dlq},
	}
	if err := config.Save(cfgPath, cfg); err != nil {
		t.Fatal(err)
	}
	return &recoveryCLI{cfgPath: cfgPath}
}

// run executes the CLI, failing the test on error.
func (c *recoveryCLI) run(t *testing.T, args ...string) string {
	t.Helper()
	out, err := c.runErr(args...)
	if err != nil {
		t.Fatalf("dlq %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// runErr executes the CLI and returns the error, for paths that must refuse.
func (c *recoveryCLI) runErr(args ...string) (string, error) {
	root := command.NewRoot("test", "abc123", "2026-01-01")
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		return buf.String(), err
	}
	return buf.String(), nil
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
