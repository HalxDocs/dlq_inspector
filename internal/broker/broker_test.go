package broker_test

import (
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/broker/rabbitmq"
)

// Compile-time assertion that the adapter satisfies the contract.
var _ broker.Broker = (*rabbitmq.Adapter)(nil)

func TestNewRegisteredBroker(t *testing.T) {
	b, err := broker.New("rabbitmq")
	if err != nil {
		t.Fatalf("New(rabbitmq) unexpected error: %v", err)
	}
	if b == nil {
		t.Fatal("New(rabbitmq) returned nil broker")
	}
}

func TestNewUnknownBroker(t *testing.T) {
	if _, err := broker.New("kafka"); err == nil {
		t.Fatal("New(kafka) expected error for unregistered broker")
	}
}

func TestRegistered(t *testing.T) {
	names := broker.Registered()
	if len(names) != 1 || names[0] != "rabbitmq" {
		t.Fatalf("Registered() = %v, want [rabbitmq]", names)
	}
}

func TestRabbitMQConnectRejectsEmptyURL(t *testing.T) {
	b, err := broker.New("rabbitmq")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Connect(t.Context(), broker.ConnectionConfig{}); err == nil {
		t.Fatal("Connect() with empty URL expected error")
	}
}

func TestRabbitMQRemainingStubs(t *testing.T) {
	b, err := broker.New("rabbitmq")
	if err != nil {
		t.Fatal(err)
	}
	// Publish is the last unimplemented surface until phase 3 lands it behind
	// the shared safety gate.
	if err := b.Publish(t.Context(), "q", nil); err != rabbitmq.ErrNotImplemented {
		t.Errorf("Publish() = %v, want ErrNotImplemented", err)
	}
}
