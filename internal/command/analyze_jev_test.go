package command

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/jev"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

func jevMockServer(t *testing.T, choice string, conf float64, hits *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*hits++
		if r.Header.Get("Authorization") == "" {
			t.Error("missing Authorization header")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"jev-1.13.0","answers":{` +
			`"classification":{"type":"choice","choice":"` + choice + `","probabilities":{"` + choice + `":` +
			floatStr(conf) + `},"confidence":` + floatStr(conf) + `},` +
			`"is_duplicate":{"type":"noul","noul":0.01},` +
			`"replay_risk":{"type":"score","score":0.2}},` +
			`"usage":{"input_tokens":10,"output_tokens":0}}`))
	}))
}

func floatStr(f float64) string {
	if f == 0.93 {
		return "0.93"
	}
	if f == 0.9 {
		return "0.9"
	}
	return "0.5"
}

func TestAnalyzeWithJevResolvesInvestigate(t *testing.T) {
	hits := 0
	srv := jevMockServer(t, "replayable", 0.93, &hits)
	defer srv.Close()
	t.Setenv(jev.APIKeyEnv, "test-key")

	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": analyzeFixture()}})

	out, err := runCommand(t, "analyze", "--config", cfgPath,
		"--with-jev", "--jev-endpoint", srv.URL)
	if err != nil {
		t.Fatalf("analyze --with-jev: %v\n%s", err, out)
	}
	// Only the single INVESTIGATE message (m6) consults Jev.
	if hits != 1 {
		t.Errorf("jev calls = %d, want 1", hits)
	}
	if !strings.Contains(out, "jev 1/1") {
		t.Errorf("output missing jev marker:\n%s", out)
	}
	if !strings.Contains(out, "Jev: 1 calls") {
		t.Errorf("output missing Jev summary:\n%s", out)
	}
	// The resolved group is now REPLAYABLE instead of INVESTIGATE.
	if strings.Contains(out, "Recommendation: INVESTIGATE") {
		t.Errorf("INVESTIGATE should be resolved by Jev:\n%s", out)
	}
}

func TestAnalyzeWithJevWithoutKeyFallsBack(t *testing.T) {
	t.Setenv(jev.APIKeyEnv, "")
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": analyzeFixture()}})

	out, err := runCommand(t, "analyze", "--config", cfgPath, "--with-jev")
	if err != nil {
		t.Fatalf("analyze --with-jev without key: %v\n%s", err, out)
	}
	if !strings.Contains(out, "falling back to rules") {
		t.Errorf("want fallback warning:\n%s", out)
	}
	if !strings.Contains(out, "Recommendation: INVESTIGATE") {
		t.Errorf("rules fallback should keep INVESTIGATE:\n%s", out)
	}
}
