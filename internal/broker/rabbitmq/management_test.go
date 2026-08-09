package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
)

func TestManagementURLDerived(t *testing.T) {
	got, err := managementURL(broker.ConnectionConfig{URL: "amqp://guest:guest@localhost:5672/"})
	if err != nil {
		t.Fatal(err)
	}
	if got != "http://localhost:15672" {
		t.Errorf("managementURL = %q, want http://localhost:15672", got)
	}
}

func TestManagementURLDerivedAmqps(t *testing.T) {
	got, err := managementURL(broker.ConnectionConfig{URL: "amqps://user:pass@broker.example.com:5671/"})
	if err != nil {
		t.Fatal(err)
	}
	if got != "https://broker.example.com:15672" {
		t.Errorf("managementURL = %q, want https://broker.example.com:15672", got)
	}
}

func TestManagementURLOverride(t *testing.T) {
	cfg := broker.ConnectionConfig{
		URL:           "amqp://localhost:5672/",
		ManagementURL: "http://mgmt.example.com:15672/",
	}
	got, err := managementURL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got != "http://mgmt.example.com:15672" {
		t.Errorf("managementURL = %q, want trimmed override", got)
	}
}

func TestManagementURLEmptyHost(t *testing.T) {
	if _, err := managementURL(broker.ConnectionConfig{URL: "amqp://"}); err == nil {
		t.Error("managementURL expected error for hostless URL")
	}
}

func TestManagementClientVhostEncoding(t *testing.T) {
	c, err := newManagementClient(broker.ConnectionConfig{
		URL:           "amqp://guest:guest@localhost:5672/",
		ManagementURL: "http://localhost:15672/",
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.encodedVhost != "%2F" {
		t.Errorf("default vhost encoded = %q, want %%2F", c.encodedVhost)
	}
	if c.baseURL != "http://localhost:15672" || c.user != "guest" || c.pass != "guest" {
		t.Errorf("client = %+v", c)
	}
}

func TestManagementClientCustomVhost(t *testing.T) {
	c, err := newManagementClient(broker.ConnectionConfig{URL: "amqp://user:secret@host:5672/tenant"})
	if err != nil {
		t.Fatal(err)
	}
	if c.encodedVhost != "tenant" {
		t.Errorf("vhost = %q, want tenant", c.encodedVhost)
	}
	if c.user != "user" || c.pass != "secret" {
		t.Errorf("creds = (%q, %q)", c.user, c.pass)
	}
}

func TestFriendlyQueueErrorWrapsNotFound(t *testing.T) {
	// A 404 from a passive declare must surface as broker.ErrQueueNotFound so
	// the recovery engine can tell "destination does not exist" (refuse to
	// run) from a transient broker error (report it).
	err := friendlyQueueError("vanished", &amqp.Error{Code: amqp.NotFound, Reason: "NOT_FOUND - no queue 'vanished'"})
	if !errors.Is(err, broker.ErrQueueNotFound) {
		t.Fatalf("err = %v, want it to wrap broker.ErrQueueNotFound", err)
	}
	if !strings.Contains(err.Error(), "vanished") {
		t.Errorf("err = %q, want the queue name", err)
	}
}

func TestFriendlyQueueErrorOtherCodes(t *testing.T) {
	// Access-refused and transport errors are NOT not-found: they must not
	// match the sentinel, so the recovery engine reports them rather than
	// refusing on a false premise.
	for _, code := range []int{amqp.AccessRefused, amqp.InternalError} {
		err := friendlyQueueError("orders", &amqp.Error{Code: code, Reason: "nope"})
		if errors.Is(err, broker.ErrQueueNotFound) {
			t.Errorf("code %d matched ErrQueueNotFound", code)
		}
	}
	if errors.Is(friendlyQueueError("orders", errors.New("connection reset")), broker.ErrQueueNotFound) {
		t.Error("non-AMQP error matched ErrQueueNotFound")
	}
}

func TestManagementClientEscapesSlashVhost(t *testing.T) {
	c, err := newManagementClient(broker.ConnectionConfig{URL: "amqp://guest:guest@host:5672/my%2Fvhost"})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.encodedVhost; got != url.PathEscape("my/vhost") {
		t.Errorf("vhost = %q", got)
	}
}

// TestListQueuesMapsPending pins the RabbitMQ side of the pending surface:
// the management API's messages_unacknowledged (deliveries in flight) must
// land in QueueSummary.Pending, the same signal the Redis adapter reports
// from consumer-group PELs.
func TestListQueuesMapsPending(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[{"name":"orders","durable":true,"auto_delete":false,"messages":42,"consumers":1,"messages_unacknowledged":7}]`)
	}))
	defer srv.Close()

	c := &managementClient{
		baseURL:      srv.URL,
		user:         "guest",
		pass:         "guest",
		encodedVhost: "%2F",
		http:         srv.Client(),
	}
	qs, err := c.listQueues(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(qs) != 1 {
		t.Fatalf("queues = %d, want 1", len(qs))
	}
	if qs[0].Pending != 7 || qs[0].Messages != 42 || qs[0].Consumers != 1 {
		t.Errorf("summary = %+v, want messages 42, consumers 1, pending 7", qs[0])
	}
}
