package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAuthMiddlewareInjectsAuthContext(t *testing.T) {
	middleware := AuthMiddleware(func(*http.Request) (AuthContext, error) {
		return AuthContext{TenantID: "tenant-a", UserID: "user-a"}, nil
	})
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth, ok := AuthFromContext(r.Context())
		if !ok {
			t.Fatal("missing auth context")
		}
		WriteAPIJSON(w, http.StatusOK, map[string]ID{"user_id": auth.UserID})
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/me", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestAuthMiddlewareRejectsMissingAuth(t *testing.T) {
	middleware := AuthMiddleware(func(*http.Request) (AuthContext, error) {
		return AuthContext{}, errors.New("missing")
	})
	rec := httptest.NewRecorder()
	middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not run")
	})).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/me", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestAuthMiddlewarePreservesTypedFailure(t *testing.T) {
	middleware := AuthMiddleware(func(*http.Request) (AuthContext, error) {
		return AuthContext{}, &AuthAdapterError{
			Status:  http.StatusForbidden,
			Code:    APIErrorPermissionDenied,
			Message: "Identity is not provisioned",
		}
	})
	rec := httptest.NewRecorder()
	middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not run")
	})).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/me", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"code":"permission_denied"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestRequirePermissionAllowsGrant(t *testing.T) {
	auth := AuthContext{
		TenantID: "tenant-a",
		UserID:   "user-a",
		Grants: []Permission{{
			TenantID:     "tenant-a",
			SubjectType:  SubjectUser,
			SubjectID:    "user-a",
			ResourceType: ResourceTenant,
			ResourceID:   "tenant-a",
			Actions:      []Action{ActionView},
		}},
	}
	handler := RequirePermission(ActionView, TenantResource)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants", nil)
	handler.ServeHTTP(rec, req.WithContext(ContextWithAuth(req.Context(), auth)))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
}

func TestRequirePermissionRejectsMissingGrant(t *testing.T) {
	handler := RequirePermission(ActionView, TenantResource)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not run")
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants", nil)
	req = req.WithContext(ContextWithAuth(req.Context(), AuthContext{TenantID: "tenant-a", UserID: "user-a"}))
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestNewAPIV1RouterServesMe(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: true}, nil
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/me", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body AuthContext
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.UserID != "user-a" {
		t.Fatalf("user_id = %s", body.UserID)
	}
}

func TestNewAPIV1RouterUsesTenantDiscoveryAuth(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{}, authAdapterError(http.StatusBadRequest, APIErrorInvalidRequest, TenantHeader+" is required")
		},
		TenantDiscovery: func(*http.Request) (AuthContext, error) {
			return AuthContext{AvailableTenants: []Tenant{{ID: "tenant-a", Name: "Tenant A", Status: "active"}}}, nil
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/me/tenants", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"ID":"tenant-a"`) {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestNewAPIV1RouterReturnsJSONForUnknownAPIPath(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/not-a-route", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content type = %q, want application/json", rec.Header().Get("Content-Type"))
	}
}

func TestNewAPIV1RouterReadiness(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		router := NewAPIV1Router(APIV1RouterConfig{Readiness: func(context.Context) error { return nil }})
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/ready", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
		}
	})
	t.Run("not ready", func(t *testing.T) {
		router := NewAPIV1Router(APIV1RouterConfig{Readiness: func(context.Context) error { return errors.New("mysql down") }})
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/ready", nil))
		if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"code":"service_unavailable"`) {
			t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
		}
	})
}

func TestNewAPIV1RouterRuntimeHealthRequiresAuthentication(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{RuntimeHealth: func() PlatformRuntimeHealth {
		return PlatformRuntimeHealth{FlowStateCleanup: FlowStateCleanupRuntimeStatus{
			Enabled: true,
			Health:  FlowStateCleanupRuntimeHealth{Started: true, Running: true, Ready: true, ReconcileIdleTotal: 4},
		}}
	}})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/runtime", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unauthenticated status = %d, body = %s", rec.Code, rec.Body.String())
	}

	router = NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a"}, nil
		},
		RuntimeHealth: func() PlatformRuntimeHealth {
			return PlatformRuntimeHealth{FlowStateCleanup: FlowStateCleanupRuntimeStatus{
				Enabled: true,
				Health:  FlowStateCleanupRuntimeHealth{Started: true, Running: true, Ready: true, ReconcileIdleTotal: 4},
			}}
		},
	})
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/runtime", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"reconcile_idle_total":4`) {
		t.Fatalf("authenticated status = %d, body = %s", rec.Code, rec.Body.String())
	}

	router = NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a"}, nil
		},
		RuntimeMetrics: func() []byte {
			return []byte("watchdog_flow_state_cleanup_ready 1\n")
		},
	})
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/runtime/metrics", nil))
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" || !strings.Contains(rec.Body.String(), "cleanup_ready 1") {
		t.Fatalf("metrics status = %d, content type = %q, body = %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
}
