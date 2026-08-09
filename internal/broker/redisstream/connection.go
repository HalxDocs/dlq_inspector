package redisstream

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
)

// connection wraps the go-redis client with the config it was dialed from.
type connection struct {
	client *redis.Client
	cfg    broker.ConnectionConfig
}

// dial opens a Redis client from the connection config and verifies it with a
// ping so Connect fails fast on a bad address or credentials.
func dial(ctx context.Context, cfg broker.ConnectionConfig) (*connection, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("redisstream: empty connection URL")
	}
	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("redisstream: parse url: %w", err)
	}
	client := redis.NewClient(opts)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redisstream: connect: %w", err)
	}
	return &connection{client: client, cfg: cfg}, nil
}

func (c *connection) close() error {
	if c == nil || c.client == nil {
		return nil
	}
	err := c.client.Close()
	c.client = nil
	return err
}
