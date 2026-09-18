package jev

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// ErrNoAPIKey is returned when no TypeSafe API key is available. Callers
// treat it as "Jev disabled" and fall back to the rule-based classifier.
var ErrNoAPIKey = errors.New("jev: no API key (set TYPESAFE_API_KEY)")

// Question is one typed Jev question. Type is "choice", "noul", or "score".
// Criteria carries the option map (choice) or rubric list (score); it is
// omitted for noul questions.
type Question struct {
	Type         string `json:"type"`
	Instructions string `json:"instructions"`
	Criteria     any    `json:"criteria,omitempty"`
}

// Usage reports token consumption for one evaluation call.
type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// Answer is one typed Jev answer. Only the fields matching Type are set:
// Choice+Probabilities+Confidence for choice, Score+Legend+Confidence for
// score, Noul for noul.
type Answer struct {
	Type          string             `json:"type"`
	Choice        string             `json:"choice,omitempty"`
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	Confidence    float64            `json:"confidence,omitempty"`
	Score         float64            `json:"score,omitempty"`
	Legend        map[string]string  `json:"legend,omitempty"`
	Noul          float64            `json:"noul,omitempty"`
}

// Response is one Jev evaluation response. Model is the versioned ID that
// answered (e.g. jev-1.13.0) — log it for threshold-tuned decisions.
type Response struct {
	Model   string            `json:"model"`
	Answers map[string]Answer `json:"answers"`
	Usage   Usage             `json:"usage"`
}

type evaluateRequest struct {
	State     string              `json:"state"`
	Model     string              `json:"model"`
	Questions map[string]Question `json:"questions"`
}

// Client evaluates Jev questions over HTTP using only the standard library.
type Client struct {
	// Endpoint is the System One URL (override for gateways/mocks).
	Endpoint string
	// Model is the model ID to request (alias or pinned version).
	Model string
	// APIKey is the bearer token. Never logged.
	APIKey string
	// HTTP is the transport; zero value uses a 10s-timeout client.
	HTTP *http.Client
}

func (c *Client) endpoint() string {
	if c.Endpoint != "" {
		return c.Endpoint
	}
	return DefaultEndpoint
}

func (c *Client) model() string {
	if c.Model != "" {
		return c.Model
	}
	return DefaultModel
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: DefaultTimeoutSeconds * time.Second}
}

// NewClient builds a Client with explicit credentials.
func NewClient(apiKey, model, endpoint string) *Client {
	return &Client{APIKey: apiKey, Model: model, Endpoint: endpoint}
}

// NewFromEnv builds a Client from TYPESAFE_API_KEY. It returns ErrNoAPIKey
// when the variable is empty or unset.
func NewFromEnv() (*Client, error) {
	key := strings.TrimSpace(os.Getenv(APIKeyEnv))
	if key == "" {
		return nil, ErrNoAPIKey
	}
	return &Client{APIKey: key}, nil
}

// Evaluate sends one state with all questions in a single call. Questions
// are evaluated in parallel and independently server-side, so callers should
// batch every independent question they need. A nil map is rejected.
func (c *Client) Evaluate(ctx context.Context, state string, questions map[string]Question) (*Response, error) {
	if strings.TrimSpace(c.APIKey) == "" {
		return nil, ErrNoAPIKey
	}
	if len(questions) == 0 {
		return nil, errors.New("jev: no questions to evaluate")
	}
	body, err := json.Marshal(evaluateRequest{
		State:     state,
		Model:     c.model(),
		Questions: questions,
	})
	if err != nil {
		return nil, fmt.Errorf("jev: encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("jev: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.APIKey)

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("jev: call: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("jev: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("jev: endpoint returned %s: %s", resp.Status, snippet(raw))
	}
	var out Response
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("jev: decode response: %w", err)
	}
	if len(out.Answers) == 0 {
		return nil, errors.New("jev: response contained no answers")
	}
	return &out, nil
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		return s[:300] + "..."
	}
	if s == "" {
		return "(empty body)"
	}
	return s
}
