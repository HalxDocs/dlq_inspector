package broker_test

import (
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	"github.com/HalxDocs/dlq_inspector/internal/broker/rabbitmq"
	"github.com/HalxDocs/dlq_inspector/internal/message"
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

func TestRabbitMQWriteRequiresConnect(t *testing.T) {
	b, err := broker.New("rabbitmq")
	if err != nil {
		t.Fatal(err)
	}
	// Publish and Ack must refuse to run before Connect — the safety gate
	// depends on these never silently no-op'ing.
	if err := b.Publish(t.Context(), "q", &message.Message{ID: "m"}); err != rabbitmq.ErrNotConnected {
		t.Errorf("Publish() = %v, want ErrNotConnected", err)
	}
	if err := b.Ack(t.Context(), "q", "m"); err != rabbitmq.ErrNotConnected {
		t.Errorf("Ack() = %v, want ErrNotConnected", err)
	}
}
