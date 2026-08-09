package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
)

// managementClient talks to the RabbitMQ management HTTP API. The AMQP
// protocol has no queue-listing operation, so the management plugin (default
// port 15672) is the only way to enumerate queues.
type managementClient struct {
	baseURL      string
	user         string
	pass         string
	encodedVhost string
	http         *http.Client
}

// mgmtQueue mirrors the subset of the /api/queues payload we need.
type mgmtQueue struct {
	Name       string `json:"name"`
	Durable    bool   `json:"durable"`
	AutoDelete bool   `json:"auto_delete"`
	Messages   int    `json:"messages"`
	Consumers  int    `json:"consumers"`
}

// newManagementClient derives the management base URL, credentials, and vhost
// from the connection config. A management_url in the profile overrides the
// derived default (same host, port 15672).
func newManagementClient(cfg broker.ConnectionConfig) (*managementClient, error) {
	base, err := managementURL(cfg)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: parse connection URL: %w", err)
	}
	user, pass := "guest", "guest"
	if u.User != nil {
		user = u.User.Username()
		if p, ok := u.User.Password(); ok {
			pass = p
		}
	}
	// The management API expects the vhost URL-escaped in the path (default
	// vhost "/" becomes %2F). EscapedPath preserves any escapes already
	// present in the connection URL, so we must not re-escape.
	encodedVhost := "%2F"
	if p := strings.TrimPrefix(u.EscapedPath(), "/"); p != "" {
		encodedVhost = p
	}

	return &managementClient{
		baseURL:      base,
		user:         user,
		pass:         pass,
		encodedVhost: encodedVhost,
		http:         &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// managementURL returns the management API base URL, preferring an explicit
// override and otherwise deriving it from the AMQP URL (same host, port
// 15672, https when the AMQP URL is amqps).
func managementURL(cfg broker.ConnectionConfig) (string, error) {
	if cfg.ManagementURL != "" {
		return strings.TrimSuffix(cfg.ManagementURL, "/"), nil
	}
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return "", fmt.Errorf("rabbitmq: parse connection URL: %w", err)
	}
	if u.Hostname() == "" {
		return "", fmt.Errorf("rabbitmq: connection URL has no host")
	}
	scheme := "http"
	if u.Scheme == "amqps" {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s:15672", scheme, u.Hostname()), nil
}

// listQueues fetches and maps every queue visible to the management API.
func (m *managementClient) listQueues(ctx context.Context) ([]broker.QueueSummary, error) {
	var raw []mgmtQueue
	if err := m.get(ctx, "/api/queues/"+m.encodedVhost, &raw); err != nil {
		return nil, fmt.Errorf("rabbitmq: list queues: %w", err)
	}
	queues := make([]broker.QueueSummary, 0, len(raw))
	for _, q := range raw {
		queues = append(queues, broker.QueueSummary{
			Name:       q.Name,
			Durable:    q.Durable,
			AutoDelete: q.AutoDelete,
			Messages:   q.Messages,
			Consumers:  q.Consumers,
		})
	}
	return queues, nil
}

// get performs an authenticated GET and decodes the JSON response.
func (m *managementClient) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(m.user, m.pass)

	resp, err := m.http.Do(req)
	if err != nil {
		return fmt.Errorf("management API unreachable at %s (is the management plugin enabled?): %w", m.baseURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("management API %s returned %s: %s", path, resp.Status, strings.TrimSpace(string(body)))
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode management API response: %w", err)
	}
	return nil
}
