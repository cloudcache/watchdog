package watchdog

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
)

type RuntimeMetricsProvider interface {
	PrometheusText() []byte
}

type runtimeMetricsProviderFunc func() []byte

func (fn runtimeMetricsProviderFunc) PrometheusText() []byte { return fn() }

// RuntimeMetricsRegistry is the single composition point for metrics emitted
// by embedded platform modules. Registration is startup-safe and rendering is
// deterministic, which makes duplicate ownership and scrape regressions easy
// to detect without introducing another HTTP server.
type RuntimeMetricsRegistry struct {
	mu        sync.RWMutex
	providers map[string]RuntimeMetricsProvider
}

func NewRuntimeMetricsRegistry() *RuntimeMetricsRegistry {
	return &RuntimeMetricsRegistry{providers: make(map[string]RuntimeMetricsProvider)}
}

func (r *RuntimeMetricsRegistry) Register(name string, provider RuntimeMetricsProvider) error {
	if r == nil {
		return errors.New("runtime metrics registry is required")
	}
	name = strings.TrimSpace(name)
	if name == "" || provider == nil {
		return errors.New("runtime metrics provider name and implementation are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.providers[name]; exists {
		return fmt.Errorf("runtime metrics provider %q is already registered", name)
	}
	r.providers[name] = provider
	return nil
}

func (r *RuntimeMetricsRegistry) PrometheusText() []byte {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	sort.Strings(names)
	providers := make([]RuntimeMetricsProvider, 0, len(names))
	for _, name := range names {
		providers = append(providers, r.providers[name])
	}
	r.mu.RUnlock()

	result := make([]byte, 0, 2048)
	for _, provider := range providers {
		part := provider.PrometheusText()
		result = append(result, part...)
		if len(part) > 0 && part[len(part)-1] != '\n' {
			result = append(result, '\n')
		}
	}
	return result
}

type metricsScrapeHandler struct {
	token      []byte
	allowedIPs []*net.IPNet
	render     func() []byte
}

// NewMetricsScrapeHandler loads the bearer secret once at startup and binds a
// direct-peer CIDR ACL. TLS remains the responsibility of the existing hub
// listener or its trusted reverse proxy; forwarding headers are deliberately
// ignored so an untrusted caller cannot spoof the ACL.
func NewMetricsScrapeHandler(cfg MetricsScrapeConfig, render func() []byte) (http.Handler, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if err := validateMetricsScrapeConfig(cfg); err != nil {
		return nil, err
	}
	secret, err := os.ReadFile(cfg.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("read metrics scrape token file: %w", err)
	}
	secret = []byte(strings.TrimSpace(string(secret)))
	if len(secret) < 32 || len(secret) > 4096 {
		return nil, errors.New("metrics scrape token must contain between 32 and 4096 non-whitespace bytes")
	}
	if render == nil {
		return nil, errors.New("metrics scrape renderer is required")
	}
	allowed := make([]*net.IPNet, 0, len(cfg.AllowedCIDRs))
	for _, value := range cfg.AllowedCIDRs {
		_, network, err := net.ParseCIDR(value)
		if err != nil {
			return nil, fmt.Errorf("parse metrics scrape CIDR %q: %w", value, err)
		}
		allowed = append(allowed, network)
	}
	return metricsScrapeHandler{token: secret, allowedIPs: allowed, render: render}, nil
}

func (h metricsScrapeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || !h.allows(net.ParseIP(peer)) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	presented := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(presented) < 8 || !strings.EqualFold(presented[:7], "Bearer ") ||
		subtle.ConstantTimeCompare([]byte(strings.TrimSpace(presented[7:])), h.token) != 1 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="watchdog-metrics"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = w.Write(h.render())
}

func (h metricsScrapeHandler) allows(ip net.IP) bool {
	if ip == nil {
		return false
	}
	for _, network := range h.allowedIPs {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}
