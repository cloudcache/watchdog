package watchdog

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type staticRuntimeMetricsProvider string

func (p staticRuntimeMetricsProvider) PrometheusText() []byte { return []byte(p) }

func TestRuntimeMetricsRegistryRejectsDuplicateAndRendersDeterministically(t *testing.T) {
	registry := NewRuntimeMetricsRegistry()
	if err := registry.Register("z", staticRuntimeMetricsProvider("metric_z 1")); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("a", staticRuntimeMetricsProvider("metric_a 1\n")); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("a", staticRuntimeMetricsProvider("duplicate 1\n")); err == nil {
		t.Fatal("expected duplicate provider rejection")
	}
	if got, want := string(registry.PrometheusText()), "metric_a 1\nmetric_z 1\n"; got != want {
		t.Fatalf("metrics = %q, want %q", got, want)
	}
}

func TestMetricsScrapeHandlerEnforcesDirectPeerAndBearerToken(t *testing.T) {
	token := strings.Repeat("s", 48)
	tokenFile := filepath.Join(t.TempDir(), "metrics.token")
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	handler, err := NewMetricsScrapeHandler(MetricsScrapeConfig{
		Enabled: true, TokenFile: tokenFile, AllowedCIDRs: []string{"127.0.0.0/8", "2001:db8::/32"},
	}, func() []byte { return []byte("watchdog_test_metric 1\n") })
	if err != nil {
		t.Fatal(err)
	}

	request := func(method, remote, authorization string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/metrics", nil)
		req.RemoteAddr = remote
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	if rec := request(http.MethodGet, "192.0.2.10:1234", "Bearer "+token); rec.Code != http.StatusForbidden {
		t.Fatalf("outside CIDR status=%d", rec.Code)
	}
	spoofed := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	spoofed.RemoteAddr = "192.0.2.10:1234"
	spoofed.Header.Set("X-Forwarded-For", "127.0.0.1")
	spoofed.Header.Set("Authorization", "Bearer "+token)
	spoofedResponse := httptest.NewRecorder()
	handler.ServeHTTP(spoofedResponse, spoofed)
	if spoofedResponse.Code != http.StatusForbidden {
		t.Fatalf("forwarded peer bypass status=%d", spoofedResponse.Code)
	}
	if rec := request(http.MethodGet, "127.0.0.1:1234", "Bearer wrong"); rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("bad token status=%d headers=%v", rec.Code, rec.Header())
	}
	if rec := request(http.MethodGet, "127.0.0.1:1234", "Bearer "+token); rec.Code != http.StatusOK || rec.Body.String() != "watchdog_test_metric 1\n" {
		t.Fatalf("valid scrape status=%d body=%q", rec.Code, rec.Body.String())
	}
	if rec := request(http.MethodHead, "[2001:db8::1]:1234", "Bearer "+token); rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("HEAD status=%d body=%q", rec.Code, rec.Body.String())
	}
	if rec := request(http.MethodPost, "127.0.0.1:1234", "Bearer "+token); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status=%d", rec.Code)
	}
}

func TestMetricsScrapeHandlerStartupValidation(t *testing.T) {
	if handler, err := NewMetricsScrapeHandler(MetricsScrapeConfig{}, nil); err != nil || handler != nil {
		t.Fatalf("disabled handler=%v err=%v", handler, err)
	}
	for _, cfg := range []MetricsScrapeConfig{
		{Enabled: true, AllowedCIDRs: []string{"127.0.0.0/8"}},
		{Enabled: true, TokenFile: "missing", AllowedCIDRs: []string{"bad"}},
	} {
		if _, err := NewMetricsScrapeHandler(cfg, func() []byte { return nil }); err == nil {
			t.Fatalf("invalid config was accepted: %+v", cfg)
		}
	}
}
