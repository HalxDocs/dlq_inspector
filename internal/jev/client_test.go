package jev

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEvaluateRoundTrip(t *testing.T) {
	var gotAuth, gotModel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		var req evaluateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		gotModel = req.Model
		if req.State == "" || len(req.Questions) == 0 {
			t.Errorf("empty state/questions: %+v", req)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"jev-1.13.0","answers":{"classification":{"type":"choice","choice":"replayable","probabilities":{"replayable":0.9},"confidence":0.8}},"usage":{"input_tokens":10,"output_tokens":0}}`))
	}))
	defer srv.Close()

	c := NewClient("test-key", "", srv.URL)
	resp, err := c.Evaluate(context.Background(), "error=timeout", map[string]Question{
		"classification": {Type: "choice", Instructions: "classify"},
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if gotAuth != "Bearer test-key" {
		t.Errorf("auth = %q, want bearer", gotAuth)
	}
	if gotModel != DefaultModel {
		t.Errorf("model = %q, want %q", gotModel, DefaultModel)
	}
	if resp.Model != "jev-1.13.0" {
		t.Errorf("response model = %q", resp.Model)
	}
	if resp.Answers["classification"].Choice != "replayable" {
		t.Errorf("choice = %q", resp.Answers["classification"].Choice)
	}
}

func TestEvaluateNoAPIKey(t *testing.T) {
	c := NewClient("", "", "http://localhost")
	_, err := c.Evaluate(context.Background(), "s", map[string]Question{"q": {Type: "noul"}})
	if err == nil || !strings.Contains(err.Error(), "API key") {
		t.Fatalf("want no-key error, got %v", err)
	}
}

func TestEvaluateServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("bad key"))
	}))
	defer srv.Close()

	c := NewClient("k", "", srv.URL)
	_, err := c.Evaluate(context.Background(), "s", map[string]Question{"q": {Type: "noul"}})
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("want status error, got %v", err)
	}
}

func TestNewFromEnvMissing(t *testing.T) {
	t.Setenv(APIKeyEnv, "")
	if _, err := NewFromEnv(); err != ErrNoAPIKey {
		t.Fatalf("want ErrNoAPIKey, got %v", err)
	}
}
