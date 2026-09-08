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

func TestAddressLibraryAuthUsesOneOwnerAndAdminOnlyWrites(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/address-prefixes", nil)
	auth := func(*http.Request) (AuthContext, error) {
		return AuthContext{TenantID: "tenant-consumer", UserID: "user-consumer", IsAdmin: true}, nil
	}
	view, admin := addressLibraryAuthAdapters(auth, "tenant-owner")
	selected, err := view(request)
	if err != nil || selected.TenantID != "tenant-owner" || selected.IsAdmin {
		t.Fatalf("shared address view = %+v err=%v", selected, err)
	}
	if !HasPermission(AccessRequest{
		TenantID: selected.TenantID, UserID: selected.UserID, Action: ActionView,
		Resource: ResourceRef{Type: ResourceTenant, ID: selected.TenantID},
	}, selected.Grants) {
		t.Fatal("shared address view did not receive read-only owner scope")
	}
	if _, err := admin(request); err == nil {
		t.Fatal("admin of a consumer tenant could mutate the shared address library")
	}

	ownerAuth := func(*http.Request) (AuthContext, error) {
		return AuthContext{TenantID: "tenant-owner", UserID: "user-owner", IsAdmin: true}, nil
	}
	_, ownerAdmin := addressLibraryAuthAdapters(ownerAuth, "tenant-owner")
	if selected, err := ownerAdmin(request); err != nil || selected.TenantID != "tenant-owner" || !selected.IsAdmin {
		t.Fatalf("owner admin = %+v err=%v", selected, err)
	}
}

func TestAddressLibraryOwnerAdminKeepsCapabilityWhenAnotherTenantIsSelected(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/address-prefixes", nil)
	auth := func(r *http.Request) (AuthContext, error) {
		if r.Header.Get(TenantHeader) == "tenant-owner" {
			return AuthContext{TenantID: "tenant-owner", UserID: "user-shared", IsAdmin: true}, nil
		}
		return AuthContext{TenantID: "tenant-consumer", UserID: "user-shared", IsAdmin: true}, nil
	}
	_, admin := addressLibraryAuthAdapters(auth, "tenant-owner")
	selected, err := admin(request)
	if err != nil || selected.TenantID != "tenant-owner" || !selected.IsAdmin {
		t.Fatalf("owner capability from another selected tenant = %+v err=%v", selected, err)
	}

	router := NewAPIV1Router(APIV1RouterConfig{Auth: auth, AddressLibraryOwner: "tenant-owner"})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/me", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	var identity AuthContext
	if err := json.Unmarshal(response.Body.Bytes(), &identity); err != nil {
		t.Fatal(err)
	}
	if !identity.CanManageAddressLibrary {
		t.Fatalf("identity = %+v, want address-library management capability", identity)
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
	for _, test := range []struct {
		name      string
		auth      AuthContext
		canManage bool
	}{
		{name: "owner administrator", auth: AuthContext{TenantID: "tenant-owner", UserID: "user-owner", IsAdmin: true}, canManage: true},
		{name: "owner member", auth: AuthContext{TenantID: "tenant-owner", UserID: "user-member"}},
		{name: "other tenant administrator", auth: AuthContext{TenantID: "tenant-consumer", UserID: "user-consumer", IsAdmin: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			router := NewAPIV1Router(APIV1RouterConfig{
				Auth:                func(*http.Request) (AuthContext, error) { return test.auth, nil },
				AddressLibraryOwner: "tenant-owner",
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
			if body.UserID != test.auth.UserID || body.CanManageAddressLibrary != test.canManage {
				t.Fatalf("body = %+v, want user %q can_manage_address_library=%v", body, test.auth.UserID, test.canManage)
			}
		})
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
		return PlatformRuntimeHealth{}
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
			return PlatformRuntimeHealth{CollectorPrincipalProvider: CollectorPrincipalProviderRuntimeStatus{
				Enabled: true,
				Health: CollectorPrincipalProviderRuntimeHealth{
					AcceptingRequests: true, RequestSuccessTotal: 3,
				},
			}}
		},
	})
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/runtime", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"request_success_total":3`) {
		t.Fatalf("authenticated status = %d, body = %s", rec.Code, rec.Body.String())
	}

	router = NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a"}, nil
		},
		RuntimeMetrics: func() []byte {
			return []byte("watchdog_collector_principal_provider_enabled 1\n")
		},
	})
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/runtime/metrics", nil))
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" || !strings.Contains(rec.Body.String(), "principal_provider_enabled 1") {
		t.Fatalf("metrics status = %d, content type = %q, body = %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
}
