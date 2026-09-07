package hub

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pocketbase/pocketbase/apis"
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
	}), 0)
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

// TestAddressImportUploadRouteRaisesBodyLimit proves the MMDB/IPDB upload route
// escapes PocketBase's global body limit while every other /api/v1 route keeps
// it: a body larger than the global guard passes on /api/v1/address-imports but
// is rejected with 413 elsewhere. Regression cover for MMDB uploads (60–100 MB)
// failing against PocketBase's 32 MB default.
func TestAddressImportUploadRouteRaisesBodyLimit(t *testing.T) {
	router := pbrouter.NewRouter(func(w http.ResponseWriter, r *http.Request) (*core.RequestEvent, pbrouter.EventCleanupFunc) {
		return &core.RequestEvent{Event: pbrouter.Event{Response: w, Request: r}}, nil
	})
	router.Bind(apis.BodyLimit(1 << 20)) // stand-in for PocketBase's global default guard
	mountPlatformHandler(router, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), 8<<20) // configured upload ceiling
	mux, err := router.BuildMux()
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Repeat("x", 4<<20) // 4 MiB: over the 1 MiB global guard, under the upload ceiling

	uploadRec := httptest.NewRecorder()
	mux.ServeHTTP(uploadRec, httptest.NewRequest(http.MethodPost, "/api/v1/address-imports", strings.NewReader(body)))
	if uploadRec.Code == http.StatusRequestEntityTooLarge {
		t.Fatalf("upload route rejected a 4 MiB body with 413 despite the raised limit")
	}

	otherRec := httptest.NewRecorder()
	mux.ServeHTTP(otherRec, httptest.NewRequest(http.MethodPost, "/api/v1/address-sets", strings.NewReader(body)))
	if otherRec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("catch-all route accepted a 4 MiB body (code=%d); global guard not applied", otherRec.Code)
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
