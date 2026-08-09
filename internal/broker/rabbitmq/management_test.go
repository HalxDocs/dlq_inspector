package rabbitmq

import (
	"net/url"
	"testing"

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

func TestManagementClientEscapesSlashVhost(t *testing.T) {
	c, err := newManagementClient(broker.ConnectionConfig{URL: "amqp://guest:guest@host:5672/my%2Fvhost"})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.encodedVhost; got != url.PathEscape("my/vhost") {
		t.Errorf("vhost = %q", got)
	}
}
