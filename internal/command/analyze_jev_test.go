package command

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/config"
	"github.com/HalxDocs/dlq_inspector/internal/jev"
	"github.com/HalxDocs/dlq_inspector/internal/message"
)

// enableJevInConfig flips Jev on for the dev profile of the test config.
func enableJevInConfig(t *testing.T, cfgPath string) {
	t.Helper()
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Profiles["dev"].Jev = &config.JevSettings{Enabled: true}
	if err := config.Save(cfgPath, cfg); err != nil {
		t.Fatal(err)
	}
}

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

func TestAnalyzeProfileEnabledAutoAssists(t *testing.T) {
	hits := 0
	srv := jevMockServer(t, "replayable", 0.93, &hits)
	defer srv.Close()
	t.Setenv(jev.APIKeyEnv, "test-key")

	cfgPath, _ := replayTestConfig(t)
	enableJevInConfig(t, cfgPath)
	// Point the enabled profile at the mock server.
	cfg, _ := config.Load(cfgPath)
	cfg.Profiles["dev"].Jev.Endpoint = srv.URL
	if err := config.Save(cfgPath, cfg); err != nil {
		t.Fatal(err)
	}
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": analyzeFixture()}})

	// No flags: the profile default drives Jev.
	out, err := runCommand(t, "analyze", "--config", cfgPath)
	if err != nil {
		t.Fatalf("analyze with enabled profile: %v\n%s", err, out)
	}
	if hits != 1 || !strings.Contains(out, "jev 1/1") {
		t.Errorf("profile Jev not honored (hits=%d):\n%s", hits, out)
	}
}

func TestAnalyzeWithoutJevOverridesProfile(t *testing.T) {
	t.Setenv(jev.APIKeyEnv, "test-key")
	cfgPath, _ := replayTestConfig(t)
	enableJevInConfig(t, cfgPath)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": analyzeFixture()}})

	out, err := runCommand(t, "analyze", "--config", cfgPath, "--without-jev")
	if err != nil {
		t.Fatalf("analyze --without-jev: %v\n%s", err, out)
	}
	if strings.Contains(out, "jev 1/1") || strings.Contains(out, "Jev:") {
		t.Errorf("--without-jev did not opt out:\n%s", out)
	}
	if strings.Contains(out, "Tip:") {
		t.Errorf("explicit opt-out must not nag:\n%s", out)
	}
	if !strings.Contains(out, "Recommendation: INVESTIGATE") {
		t.Errorf("rule-based fallback lost:\n%s", out)
	}
}

func TestAnalyzeHintWhenInvestigateAndJevOff(t *testing.T) {
	t.Setenv(jev.APIKeyEnv, "")
	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": analyzeFixture()}})

	out, err := runCommand(t, "analyze", "--config", cfgPath)
	if err != nil {
		t.Fatalf("analyze: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Tip:") || !strings.Contains(out, "dlq jev enable") {
		t.Errorf("want enable hint:\n%s", out)
	}

	// JSON mode stays machine-clean: no hint.
	out, err = runCommand(t, "analyze", "--config", cfgPath, "--output", "json")
	if err != nil {
		t.Fatalf("analyze json: %v\n%s", err, out)
	}
	if strings.Contains(out, "Tip:") {
		t.Errorf("hint leaked into JSON:\n%s", out)
	}
}

func TestProfilesListShowsJev(t *testing.T) {
	cfgPath, _ := replayTestConfig(t)
	out, err := runCommand(t, "profiles", "list", "--config", cfgPath)
	if err != nil {
		t.Fatalf("profiles list: %v\n%s", err, out)
	}
	if !strings.Contains(out, "JEV") || !strings.Contains(out, "off") {
		t.Errorf("list missing JEV column:\n%s", out)
	}
	enableJevInConfig(t, cfgPath)
	out, err = runCommand(t, "profiles", "list", "--config", cfgPath)
	if err != nil {
		t.Fatalf("profiles list: %v\n%s", err, out)
	}
	if !strings.Contains(out, "on") {
		t.Errorf("list missing enabled state:\n%s", out)
	}
}

func TestAnalyzeWithJevCustomKeyEnv(t *testing.T) {
	hits := 0
	srv := jevMockServer(t, "replayable", 0.93, &hits)
	defer srv.Close()
	// Default key absent, per-env key present: BYOK per environment.
	t.Setenv(jev.APIKeyEnv, "")
	t.Setenv("TYPESAFE_PROD_KEY", "prod-key")

	cfgPath, _ := replayTestConfig(t)
	withFakeBroker(t, &fakeBroker{msgs: map[string][]message.Message{"orders-dlq": analyzeFixture()}})

	out, err := runCommand(t, "analyze", "--config", cfgPath,
		"--with-jev", "--jev-endpoint", srv.URL, "--jev-api-key-env", "TYPESAFE_PROD_KEY")
	if err != nil {
		t.Fatalf("analyze --with-jev custom key env: %v\n%s", err, out)
	}
	if hits != 1 {
		t.Errorf("jev calls = %d, want 1", hits)
	}
	if !strings.Contains(out, "jev 1/1") {
		t.Errorf("output missing jev marker:\n%s", out)
	}
}
