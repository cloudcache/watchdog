package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	platform "github.com/cloudcache/watchdog/internal/watchdog"
	"github.com/pocketbase/pocketbase/core"
	pbrouter "github.com/pocketbase/pocketbase/tools/router"
)

type reloadableMetricsRoute struct {
	mu      sync.RWMutex
	handler http.Handler
}

func (r *reloadableMetricsRoute) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	r.mu.RLock()
	handler := r.handler
	r.mu.RUnlock()
	handler.ServeHTTP(w, request)
}

func (r *reloadableMetricsRoute) replace(handler http.Handler) {
	r.mu.Lock()
	r.handler = handler
	r.mu.Unlock()
}

type integrationMetricsProvider string

func (p integrationMetricsProvider) PrometheusText() []byte { return []byte(p) }

// TestPlatformMetricsVictoriaMetricsPullLifecycle verifies the production
// route boundary with an actual TLS reverse proxy and VictoriaMetrics
// promscrape process. It is opt-in because it requires a local Docker daemon
// and the VictoriaMetrics image.
func TestPlatformMetricsVictoriaMetricsPullLifecycle(t *testing.T) {
	if os.Getenv("WATCHDOG_PLATFORM_METRICS_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_PLATFORM_METRICS_INTEGRATION=1 to run")
	}

	tokenA := strings.Repeat("a", 48)
	tokenB := strings.Repeat("b", 48)
	secretDir := t.TempDir()
	hubTokenFile := writeMetricsIntegrationFile(t, secretDir, "hub.token", tokenA+"\n")
	writeMetricsIntegrationFile(t, secretDir, "old.token", tokenA+"\n")
	writeMetricsIntegrationFile(t, secretDir, "new.token", tokenB+"\n")

	registry := platform.NewRuntimeMetricsRegistry()
	if err := registry.Register("integration", integrationMetricsProvider("watchdog_platform_scrape_contract 1\n")); err != nil {
		t.Fatal(err)
	}
	newRoute := func(allowedCIDRs []string) http.Handler {
		handler, err := platform.NewMetricsScrapeHandler(platform.MetricsScrapeConfig{
			Enabled:      true,
			TokenFile:    hubTokenFile,
			AllowedCIDRs: allowedCIDRs,
		}, registry.PrometheusText)
		if err != nil {
			t.Fatal(err)
		}
		return productionMetricsRoute(t, handler)
	}

	reloadable := &reloadableMetricsRoute{handler: newRoute([]string{"127.0.0.0/8"})}
	privateHub := httptest.NewServer(reloadable)
	t.Cleanup(privateHub.Close)
	upstream, err := url.Parse(privateHub.URL)
	if err != nil {
		t.Fatal(err)
	}
	reverseProxy := httputil.NewSingleHostReverseProxy(upstream)
	tlsProxy := newExternallyReachableTLSServer(t, reverseProxy)

	runID := "platform-metrics-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	vmURL := startPlatformScrapingVictoriaMetrics(t, secretDir, runID, tlsProxy.Listener.Addr().(*net.TCPAddr).Port)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	waitForPlatformVMValue(t, ctx, vmURL, runID, "old", "up", 1)
	waitForPlatformVMValue(t, ctx, vmURL, runID, "new", "up", 0)
	waitForPlatformVMValue(t, ctx, vmURL, runID, "old", "watchdog_platform_scrape_contract", 1)

	// The hub deliberately keeps the startup snapshot of its token. Merely
	// replacing the file does not mutate the live handler.
	if err := os.WriteFile(hubTokenFile, []byte(tokenB+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	assertMetricsProxyStatus(t, tlsProxy.Client(), tlsProxy.URL, tokenA, http.StatusOK)
	assertMetricsProxyStatus(t, tlsProxy.Client(), tlsProxy.URL, tokenB, http.StatusUnauthorized)

	// A controlled runtime restart rebuilds the same production route and
	// loads the rotated token. The reverse-proxy address remains stable.
	reloadable.replace(newRoute([]string{"127.0.0.0/8"}))
	waitForPlatformVMValue(t, ctx, vmURL, runID, "old", "up", 0)
	waitForPlatformVMValue(t, ctx, vmURL, runID, "new", "up", 1)

	// The ACL is evaluated against the proxy's direct connection, not its
	// forwarding headers. A denied proxy source makes both scrapes unhealthy.
	reloadable.replace(newRoute([]string{"192.0.2.0/24"}))
	assertMetricsProxyStatus(t, tlsProxy.Client(), tlsProxy.URL, tokenB, http.StatusForbidden)
	waitForPlatformVMValue(t, ctx, vmURL, runID, "new", "up", 0)

	// Restore the proxy CIDR through the same restart path.
	reloadable.replace(newRoute([]string{"127.0.0.0/8"}))
	waitForPlatformVMValue(t, ctx, vmURL, runID, "new", "up", 1)
}

func productionMetricsRoute(t testing.TB, handler http.Handler) http.Handler {
	t.Helper()
	router := pbrouter.NewRouter(func(w http.ResponseWriter, r *http.Request) (*core.RequestEvent, pbrouter.EventCleanupFunc) {
		return &core.RequestEvent{Event: pbrouter.Event{Response: w, Request: r}}, nil
	})
	mountPlatformMetricsHandler(router, handler)
	router.GET("/{path...}", func(e *core.RequestEvent) error {
		return e.HTML(http.StatusOK, "spa")
	})
	mux, err := router.BuildMux()
	if err != nil {
		t.Fatal(err)
	}
	return mux
}

func newExternallyReachableTLSServer(t testing.TB, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	if err := server.Listener.Close(); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	server.Listener = listener
	server.StartTLS()
	server.URL = "https://127.0.0.1:" + strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	t.Cleanup(server.Close)
	return server
}

func startPlatformScrapingVictoriaMetrics(t testing.TB, secretDir, runID string, proxyPort int) string {
	t.Helper()
	config := fmt.Sprintf(`global:
  scrape_interval: 250ms
  scrape_timeout: 200ms
scrape_configs:
  - job_name: watchdog-hub-old-token
    scheme: https
    tls_config:
      insecure_skip_verify: true
    bearer_token_file: /etc/watchdog/old.token
    static_configs:
      - targets: ["host.docker.internal:%d"]
        labels:
          integration_run: %q
          credential: old
  - job_name: watchdog-hub-new-token
    scheme: https
    tls_config:
      insecure_skip_verify: true
    bearer_token_file: /etc/watchdog/new.token
    static_configs:
      - targets: ["host.docker.internal:%d"]
        labels:
          integration_run: %q
          credential: new
`, proxyPort, runID, proxyPort, runID)
	writeMetricsIntegrationFile(t, secretDir, "promscrape.yml", config)
	container := "watchdog-platform-vm-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	image := strings.TrimSpace(os.Getenv("WATCHDOG_VICTORIAMETRICS_IMAGE"))
	if image == "" {
		image = "victoriametrics/victoria-metrics:latest"
	}
	command := exec.Command(
		"docker", "run", "--detach", "--rm", "--name", container,
		"--add-host", "host.docker.internal:host-gateway",
		"-p", "127.0.0.1::8428",
		"-v", secretDir+":/etc/watchdog:ro",
		image, "-promscrape.config=/etc/watchdog/promscrape.yml",
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("start scraping VictoriaMetrics: %v\n%s", err, output)
	}
	t.Cleanup(func() {
		cleanup := exec.Command("docker", "rm", "--force", container)
		if output, err := cleanup.CombinedOutput(); err != nil && !strings.Contains(string(output), "No such container") {
			t.Errorf("remove scraping VictoriaMetrics: %v\n%s", err, output)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		output, err := exec.CommandContext(ctx, "docker", "port", container, "8428/tcp").CombinedOutput()
		if err == nil {
			address := strings.TrimSpace(string(output))
			if strings.HasPrefix(address, "127.0.0.1:") {
				baseURL := "http://" + address
				request, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", nil)
				response, requestErr := http.DefaultClient.Do(request)
				if requestErr == nil {
					_, _ = io.Copy(io.Discard, response.Body)
					closeErr := response.Body.Close()
					if response.StatusCode == http.StatusOK && closeErr == nil {
						return baseURL
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			logs, _ := exec.Command("docker", "logs", container).CombinedOutput()
			t.Fatalf("wait for scraping VictoriaMetrics: %v (docker port output=%q)\n%s", ctx.Err(), output, logs)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func waitForPlatformVMValue(t testing.TB, ctx context.Context, baseURL, runID, credential, metric string, want float64) {
	t.Helper()
	query := fmt.Sprintf(`%s{integration_run=%q,credential=%q}`, metric, runID, credential)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		endpoint := baseURL + "/api/v1/query?" + url.Values{"query": []string{query}, "nocache": []string{"1"}}.Encode()
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			var result struct {
				Data struct {
					Result []struct {
						Value []json.RawMessage `json:"value"`
					} `json:"result"`
				} `json:"data"`
			}
			decodeErr := json.NewDecoder(response.Body).Decode(&result)
			_ = response.Body.Close()
			if decodeErr == nil && len(result.Data.Result) == 1 && len(result.Data.Result[0].Value) == 2 {
				var value string
				if json.Unmarshal(result.Data.Result[0].Value[1], &value) == nil {
					got, parseErr := strconv.ParseFloat(value, 64)
					if parseErr == nil && got == want {
						return
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for VictoriaMetrics query %q=%v: %v", query, want, ctx.Err())
		case <-ticker.C:
		}
	}
}

func assertMetricsProxyStatus(t testing.TB, client *http.Client, baseURL, token string, want int) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, baseURL+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != want {
		t.Fatalf("metrics status=%d, want %d", response.StatusCode, want)
	}
}

func writeMetricsIntegrationFile(t testing.TB, directory, name, contents string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
