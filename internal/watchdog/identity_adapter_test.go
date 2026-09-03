package watchdog

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type identityAdapterTestRepository struct {
	projections []IdentityProjection
	roles       []ID
	grants      []Permission
	admin       bool
	err         error
}

func (r *identityAdapterTestRepository) ListIdentityProjections(context.Context, string, string) ([]IdentityProjection, error) {
	return r.projections, r.err
}

func (r *identityAdapterTestRepository) ListUserRoleIDs(context.Context, ID, ID) ([]ID, error) {
	return r.roles, r.err
}

func (r *identityAdapterTestRepository) ListPermissionsForUser(context.Context, ID, ID) ([]Permission, error) {
	return r.grants, r.err
}

func (r *identityAdapterTestRepository) IsUserTenantAdmin(context.Context, ID, ID) (bool, error) {
	return r.admin, r.err
}

func activeProjection(tenantID, userID ID) IdentityProjection {
	return IdentityProjection{
		User:   User{ID: userID, TenantID: tenantID, Status: "active"},
		Tenant: Tenant{ID: tenantID, Name: string(tenantID), Status: "active"},
	}
}

func TestIdentityAuthContextAdapterSelectsAuthorizedTenant(t *testing.T) {
	repo := &identityAdapterTestRepository{
		projections: []IdentityProjection{activeProjection("tenant-a", "user-a"), activeProjection("tenant-b", "user-b")},
		roles:       []ID{"role-admin"},
		grants:      []Permission{{TenantID: "tenant-b", SubjectID: "user-b"}},
		admin:       true,
	}
	adapter := NewIdentityAuthContextAdapter(func(*http.Request) (ExternalIdentity, error) {
		return ExternalIdentity{Provider: "pocketbase", Subject: "pb-user"}, nil
	}, repo)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/me", nil)
	req.Header.Set(TenantHeader, "tenant-b")
	auth, err := adapter(req)
	if err != nil {
		t.Fatal(err)
	}
	if auth.TenantID != "tenant-b" || auth.UserID != "user-b" || auth.ExternalSubject != "pb-user" || !auth.IsAdmin {
		t.Fatalf("auth = %#v", auth)
	}
	if len(auth.AvailableTenants) != 2 || len(auth.RoleIDs) != 1 || len(auth.Grants) != 1 {
		t.Fatalf("incomplete authorization context = %#v", auth)
	}
}

func TestIdentityAuthContextAdapterFailsClosed(t *testing.T) {
	tests := []struct {
		name         string
		authenticate ExternalIdentityAuthenticator
		repo         *identityAdapterTestRepository
		tenant       string
		status       int
		code         APIErrorCode
	}{
		{name: "invalid token", authenticate: func(*http.Request) (ExternalIdentity, error) { return ExternalIdentity{}, errors.New("invalid") }, repo: &identityAdapterTestRepository{}, status: http.StatusUnauthorized, code: APIErrorUnauthorized},
		{name: "not provisioned", authenticate: func(*http.Request) (ExternalIdentity, error) {
			return ExternalIdentity{Provider: "pocketbase", Subject: "missing"}, nil
		}, repo: &identityAdapterTestRepository{}, status: http.StatusForbidden, code: APIErrorPermissionDenied},
		{name: "projection store down", authenticate: func(*http.Request) (ExternalIdentity, error) {
			return ExternalIdentity{Provider: "pocketbase", Subject: "pb-user"}, nil
		}, repo: &identityAdapterTestRepository{err: errors.New("down")}, status: http.StatusServiceUnavailable, code: APIErrorServiceUnavailable},
		{name: "tenant required", authenticate: func(*http.Request) (ExternalIdentity, error) {
			return ExternalIdentity{Provider: "pocketbase", Subject: "pb-user"}, nil
		}, repo: &identityAdapterTestRepository{projections: []IdentityProjection{activeProjection("tenant-a", "user-a"), activeProjection("tenant-b", "user-b")}}, status: http.StatusBadRequest, code: APIErrorInvalidRequest},
		{name: "cross tenant denied", authenticate: func(*http.Request) (ExternalIdentity, error) {
			return ExternalIdentity{Provider: "pocketbase", Subject: "pb-user"}, nil
		}, repo: &identityAdapterTestRepository{projections: []IdentityProjection{activeProjection("tenant-a", "user-a")}}, tenant: "tenant-b", status: http.StatusForbidden, code: APIErrorPermissionDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := NewIdentityAuthContextAdapter(tt.authenticate, tt.repo)
			req := httptest.NewRequest(http.MethodGet, "/api/v1/me", nil)
			if tt.tenant != "" {
				req.Header.Set(TenantHeader, tt.tenant)
			}
			_, err := adapter(req)
			if !IsAuthAdapterError(err, tt.status, tt.code) {
				t.Fatalf("error = %v, want status=%d code=%s", err, tt.status, tt.code)
			}
		})
	}
}

func TestIdentityAuthContextAdapterRejectsDisabledProjection(t *testing.T) {
	projection := activeProjection("tenant-a", "user-a")
	projection.Tenant.Status = "disabled"
	adapter := NewIdentityAuthContextAdapter(func(*http.Request) (ExternalIdentity, error) {
		return ExternalIdentity{Provider: "pocketbase", Subject: "pb-user"}, nil
	}, &identityAdapterTestRepository{projections: []IdentityProjection{projection}})
	_, err := adapter(httptest.NewRequest(http.MethodGet, "/api/v1/me", nil))
	if !IsAuthAdapterError(err, http.StatusForbidden, APIErrorPermissionDenied) {
		t.Fatalf("error = %v", err)
	}
}

func TestIdentityTenantDiscoveryDoesNotRequireTenantSelection(t *testing.T) {
	repo := &identityAdapterTestRepository{
		projections: []IdentityProjection{activeProjection("tenant-a", "user-a"), activeProjection("tenant-b", "user-b")},
	}
	adapter := NewIdentityTenantDiscoveryAdapter(func(*http.Request) (ExternalIdentity, error) {
		return ExternalIdentity{Provider: "pocketbase", Subject: "pb-user"}, nil
	}, repo)
	auth, err := adapter(httptest.NewRequest(http.MethodGet, "/api/v1/me/tenants", nil))
	if err != nil {
		t.Fatal(err)
	}
	if auth.TenantID != "" || auth.UserID != "" || len(auth.RoleIDs) != 0 || len(auth.Grants) != 0 {
		t.Fatalf("discovery loaded tenant-scoped authorization: %#v", auth)
	}
	if auth.ExternalSubject != "pb-user" || len(auth.AvailableTenants) != 2 {
		t.Fatalf("discovery context = %#v", auth)
	}
}
