package hub

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pocketbase/pocketbase/core"
	pbrouter "github.com/pocketbase/pocketbase/tools/router"
)

func TestPlatformAPIRouteWinsOverSPAFallback(t *testing.T) {
	router := pbrouter.NewRouter(func(w http.ResponseWriter, r *http.Request) (*core.RequestEvent, pbrouter.EventCleanupFunc) {
		return &core.RequestEvent{Event: pbrouter.Event{Response: w, Request: r}}, nil
	})
	mountPlatformHandler(router, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	router.GET("/{path...}", func(e *core.RequestEvent) error {
		return e.HTML(http.StatusOK, "spa")
	})
	mux, err := router.BuildMux()
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") || strings.Contains(rec.Body.String(), "spa") {
		t.Fatalf("status=%d content-type=%q body=%q", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
}

func TestPlatformMetricsRouteWinsOverSPAFallback(t *testing.T) {
	router := pbrouter.NewRouter(func(w http.ResponseWriter, r *http.Request) (*core.RequestEvent, pbrouter.EventCleanupFunc) {
		return &core.RequestEvent{Event: pbrouter.Event{Response: w, Request: r}}, nil
	})
	mountPlatformMetricsHandler(router, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("watchdog_up 1\n"))
	}))
	router.GET("/{path...}", func(e *core.RequestEvent) error {
		return e.HTML(http.StatusOK, "spa")
	})
	mux, err := router.BuildMux()
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") || rec.Body.String() != "watchdog_up 1\n" {
		t.Fatalf("status=%d content-type=%q body=%q", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
}
