package rabbitmq

import (
	"context"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// Publish sends a message to a destination queue via the default exchange,
// routing by queue name. Dead-letter bookkeeping headers (x-death,
// x-first-death-*) are stripped so the replayed message starts fresh; all
// other headers — including event IDs and idempotency keys — are preserved.
// The message is published persistently and carries the stable message ID so
// downstream consumers and future DLQ hops see the same identifier.
//
// Publisher confirms are used: Publish only returns nil once the broker has
// acknowledged the message, so the safety gate can safely ack the DLQ copy
// afterwards.
func (a *Adapter) Publish(ctx context.Context, destination string, m *message.Message) error {
	if a.conn == nil {
		return ErrNotConnected
	}
	if destination == "" {
		return fmt.Errorf("rabbitmq: empty destination")
	}
	if m == nil {
		return fmt.Errorf("rabbitmq: nil message")
	}

	ch, err := a.conn.conn.Channel()
	if err != nil {
		return fmt.Errorf("rabbitmq: open publish channel: %w", err)
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("rabbitmq: enable publisher confirms: %w", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	headers := publishHeaders(m.Headers)

	if err := ch.PublishWithContext(ctx, "", destination, false, false, amqp.Publishing{
		ContentType:  m.ContentType,
		MessageId:    m.ID,
		Timestamp:    m.Timestamp,
		DeliveryMode: amqp.Persistent,
		Body:         m.Payload,
		Headers:      headers,
	}); err != nil {
		return fmt.Errorf("rabbitmq: publish to %q: %w", destination, err)
	}

	select {
	case c := <-confirms:
		if !c.Ack {
			return fmt.Errorf("rabbitmq: broker rejected publish to %q", destination)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("rabbitmq: publish to %q: %w", destination, ctx.Err())
	case <-time.After(10 * time.Second):
		return fmt.Errorf("rabbitmq: timed out waiting for publish confirm to %q", destination)
	}
}

// Ack removes the message with the given ID from a queue. RabbitMQ has no
// random-access delete, so this consumes the queue one message at a time
// without acknowledging, acknowledges the target, and requeues every other
// message in its original order. On any error the channel is closed, which
// makes RabbitMQ requeue all unacked deliveries automatically — the DLQ copy
// is never lost.
func (a *Adapter) Ack(ctx context.Context, queue, id string) error {
	if a.conn == nil {
		return ErrNotConnected
	}
	if queue == "" {
		return fmt.Errorf("rabbitmq: empty queue name")
	}
	if id == "" {
		return fmt.Errorf("rabbitmq: empty message id")
	}

	ch, err := a.conn.conn.Channel()
	if err != nil {
		return fmt.Errorf("rabbitmq: open ack channel: %w", err)
	}
	defer ch.Close() // closing with unacked deliveries requeues them

	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("rabbitmq: set prefetch: %w", err)
	}

	var held []amqp.Delivery
	var target *amqp.Delivery
	for {
		d, ok, err := ch.Get(queue, false) // no auto-ack: we decide per message
		if err != nil {
			return fmt.Errorf("rabbitmq: ack scan queue %q: %w", queue, err)
		}
		if !ok {
			break // queue drained without finding the target
		}
		if messageID(d, normalizeHeaders(d.Headers)) == id {
			t := d
			target = &t
			break
		}
		held = append(held, d)
	}

	if target == nil {
		requeueAll(held)
		return fmt.Errorf("message %q not found in queue %q", id, queue)
	}
	if err := target.Ack(false); err != nil {
		return fmt.Errorf("rabbitmq: ack message %q: %w", id, err)
	}
	requeueAll(held)
	return nil
}

// publishHeaders rebuilds the outgoing header table, dropping RabbitMQ's
// dead-letter bookkeeping headers (x-death, x-first-death-*) so the replayed
// message starts fresh.
func publishHeaders(in map[string]string) amqp.Table {
	out := amqp.Table{}
	for k, v := range in {
		if strings.HasPrefix(k, "x-death") || strings.HasPrefix(k, "x-first-death") {
			continue
		}
		out[k] = v
	}
	return out
}

// requeueAll returns unacked deliveries to the queue in their original order.
// Nack with requeue puts each delivery at the head, so nacking in reverse
// order restores the original FIFO order.
func requeueAll(held []amqp.Delivery) {
	for i := len(held) - 1; i >= 0; i-- {
		_ = held[i].Nack(false, true)
	}
}
