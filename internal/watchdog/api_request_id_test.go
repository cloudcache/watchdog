package watchdog

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestIDMiddlewareEchoesValidID(t *testing.T) {
	handler := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if RequestIDFromContext(r.Context()) != "request-123" {
			t.Fatalf("context request id = %q", RequestIDFromContext(r.Context()))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	req.Header.Set(RequestIDHeader, "request-123")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Header().Get(RequestIDHeader) != "request-123" {
		t.Fatalf("response request id = %q", rec.Header().Get(RequestIDHeader))
	}
}

func TestRequestIDMiddlewareReplacesUnsafeID(t *testing.T) {
	handler := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	req.Header.Set(RequestIDHeader, "unsafe request\nvalue")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if got := rec.Header().Get(RequestIDHeader); got == "" || got == "unsafe request value" {
		t.Fatalf("response request id = %q", got)
	}
}
