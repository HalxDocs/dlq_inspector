// Command seed is a development helper that fills a DLQ with realistic
// failing messages so the dlq workflow can be exercised end to end. It is
// NOT part of the shipped binary (goreleaser builds only ./cmd/dlq) and has
// no recovery behavior of its own — it only creates test data.
//
// The fixture mirrors what a real application's dead-lettering produces:
//
//   - three "timeout connecting to the payment gateway" messages  -> REPLAYABLE
//   - two "invalid customer_id" messages                          -> REQUIRES_FIX
//   - one message the application itself marked as a duplicate    -> DO_NOT_REPLAY
//
// On RabbitMQ the messages are published straight into the DLQ carrying a
// crafted x-death header (queue = original queue, reason = failure text,
// count = retries), exactly the shape RabbitMQ writes when it dead-letters
// a rejected message — so the mapper resolves the replay destination and
// failure reason the same way. On Redis the same fixture is written as
// stream fields (payload/destination/error/retries/headers).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type fixture struct {
	reason  string
	retries int64
	headers map[string]string
	payload func(n int) string
}

// fixtures is the mix of failures the seed produces: three timeouts, two
// validation errors, one duplicate. count > 6 cycles back to timeouts.
var fixtures = []fixture{
	{reason: "timeout connecting to upstream payment gateway after 30s", retries: 1,
		headers: map[string]string{"x-event-type": "order.paid"},
		payload: func(n int) string {
			return fmt.Sprintf(`{"order_id":%d,"customer_id":%d,"amount":129.99,"currency":"USD"}`, n, 1000+n)
		}},
	{reason: "timeout connecting to upstream payment gateway after 30s", retries: 1,
		headers: map[string]string{"x-event-type": "order.paid"},
		payload: func(n int) string {
			return fmt.Sprintf(`{"order_id":%d,"customer_id":%d,"amount":59.50,"currency":"USD"}`, n, 1000+n)
		}},
	{reason: "timeout connecting to upstream payment gateway after 30s", retries: 1,
		headers: map[string]string{"x-event-type": "order.paid"},
		payload: func(n int) string {
			return fmt.Sprintf(`{"order_id":%d,"customer_id":%d,"amount":250.00,"currency":"USD"}`, n, 1000+n)
		}},
	{reason: "invalid customer_id: must be a number", retries: 0,
		headers: map[string]string{"x-event-type": "order.created"},
		payload: func(n int) string {
			return fmt.Sprintf(`{"order_id":%d,"customer_id":"not-a-number","amount":19.99}`, n)
		}},
	{reason: "invalid customer_id: must be a number", retries: 0,
		headers: map[string]string{"x-event-type": "order.created"},
		payload: func(n int) string { return fmt.Sprintf(`{"order_id":%d,"customer_id":"n/a","amount":45.00}`, n) }},
	{reason: "rejected", retries: 1,
		headers: map[string]string{"x-event-type": "order.duplicate", "x-duplicate-of": "evt_original_order"},
		payload: func(n int) string { return fmt.Sprintf(`{"order_id":%d,"customer_id":%d,"amount":10.00}`, n, 1000+n) }},
}

func fixtureAt(i int) fixture { return fixtures[i%len(fixtures)] }

func main() {
	broker := flag.String("broker", "rabbitmq", "broker to seed: rabbitmq or redis")
	amqpURL := flag.String("amqp-url", "amqp://guest:guest@localhost:5672/", "RabbitMQ AMQP URL")
	redisURL := flag.String("redis-url", "redis://localhost:6379/0", "Redis URL")
	source := flag.String("source", "orders", "name of the source queue / destination stream")
	dlq := flag.String("dlq", "", "name of the DLQ (default <source>-dlq)")
	count := flag.Int("count", 6, "number of messages to seed")
	pending := flag.Int("pending", 2, "Redis only: entries to leave unacknowledged in a consumer group (pending visibility demo)")
	flag.Parse()

	if *dlq == "" {
		*dlq = *source + "-dlq"
	}

	var err error
	switch *broker {
	case "rabbitmq":
		err = seedRabbitMQ(*amqpURL, *source, *dlq, *count)
	case "redis":
		err = seedRedis(*redisURL, *source, *dlq, *count, *pending)
	default:
		log.Fatalf("unknown broker %q (want rabbitmq or redis)", *broker)
	}
	if err != nil {
		log.Fatalf("seed failed: %v", err)
	}
	fmt.Printf("Seeded %d messages into %s (%s).\n", *count, *dlq, *broker)
	fmt.Printf("Try: dlq analyze %s  →  dlq plan %s  →  dlq recover --plan recovery.json --dry-run\n", *dlq, *dlq)
}

// seedRabbitMQ declares the source queue (the replay destination) and the
// DLQ, then publishes the fixture directly into the DLQ with a crafted
// x-death header — the shape RabbitMQ writes when it dead-letters.
func seedRabbitMQ(url, source, dlq string, count int) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("dial %s: %w", url, err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	for _, name := range []string{source, dlq} {
		if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare %s: %w", name, err)
		}
	}

	for i := 0; i < count; i++ {
		f := fixtureAt(i)
		n := i + 1
		pub := amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(f.payload(n)),
			Headers: amqp.Table{
				"x-death": []interface{}{amqp.Table{
					"queue":  source,
					"reason": f.reason,
					"count":  f.retries,
					"time":   time.Now().UTC(),
				}},
			},
		}
		for k, v := range f.headers {
			pub.Headers[k] = v
		}
		if err := ch.Publish("", dlq, false, false, pub); err != nil {
			return fmt.Errorf("publish %d: %w", n, err)
		}
	}
	return nil
}

// seedRedis writes the fixture into the DLQ stream as fields, creates the
// destination stream (the executor refuses to publish into a stream that
// does not exist), and leaves pending entries in a consumer group on the DLQ
// so the pending visibility (dlq stats / dlq queues) has something to show.
func seedRedis(url, source, dlq string, count, pending int) error {
	ctx := context.Background()
	opts, err := redis.ParseURL(url)
	if err != nil {
		return fmt.Errorf("parse %s: %w", url, err)
	}
	client := redis.NewClient(opts)
	defer client.Close()

	// An empty stream cannot persist without a consumer group, so create the
	// destination with MKSTREAM; it stays alive once the group exists.
	if err := client.XGroupCreateMkStream(ctx, source, "workers", "$").Err(); err != nil &&
		!isBusyGroup(err) {
		return fmt.Errorf("create destination stream %s: %w", source, err)
	}

	for i := 0; i < count; i++ {
		f := fixtureAt(i)
		n := i + 1
		vals := map[string]any{
			"payload":      f.payload(n),
			"destination":  source,
			"error":        f.reason,
			"retries":      fmt.Sprintf("%d", f.retries),
			"content_type": "application/json",
		}
		if len(f.headers) > 0 {
			h, err := json.Marshal(f.headers)
			if err != nil {
				return err
			}
			vals["headers"] = string(h)
		}
		if err := client.XAdd(ctx, &redis.XAddArgs{Stream: dlq, Values: vals}).Err(); err != nil {
			return fmt.Errorf("seed %d: %w", n, err)
		}
	}

	if pending > 0 {
		// A consumer group on the DLQ itself: reading without acknowledging
		// leaves those entries in the group's PEL, which Stats reports as
		// pending — the demo for the pending-visibility surface.
		if err := client.XGroupCreateMkStream(ctx, dlq, "inspection", "0").Err(); err != nil &&
			!isBusyGroup(err) {
			return fmt.Errorf("create group on %s: %w", dlq, err)
		}
		if _, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: "inspection", Consumer: "seed",
			Streams: []string{dlq, ">"}, Count: int64(pending),
		}).Result(); err != nil {
			return fmt.Errorf("read pending: %w", err)
		}
	}
	return nil
}

func isBusyGroup(err error) bool {
	return err != nil && (err.Error() == "BUSYGROUP Consumer Group name already exists")
}
