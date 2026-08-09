package rabbitmq

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
)

// connection wraps an AMQP connection and channel with safe close semantics.
// The channel is long-lived; AMQP channels survive consumer/queue operations
// and are cheap to recover on reconnect later phases.
type connection struct {
	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
	cfg  broker.ConnectionConfig
}

// dial opens an AMQP connection and channel from the connection config.
func dial(ctx context.Context, cfg broker.ConnectionConfig) (*connection, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("rabbitmq: empty connection URL")
	}

	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: dial %s: %w", redactURL(cfg.URL), err)
	}

	// Drain connection-close notifications so a dropped connection surfaces as
	// an error on later calls instead of a silently closed channel.
	closeCh := make(chan *amqp.Error, 1)
	conn.NotifyClose(closeCh)
	go func() {
		select {
		case <-ctx.Done():
		case <-closeCh:
		}
	}()

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("rabbitmq: open channel: %w", err)
	}

	return &connection{conn: conn, ch: ch, cfg: cfg}, nil
}

// redactURL removes any password from a connection URL for safe error output.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid url>"
	}
	if u.User != nil {
		if _, has := u.User.Password(); has {
			u.User = url.User(u.User.Username())
		}
	}
	return u.String()
}

// close closes the channel and connection. It is idempotent and safe to call
// multiple times.
func (c *connection) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ch != nil {
		_ = c.ch.Close()
		c.ch = nil
	}
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}
