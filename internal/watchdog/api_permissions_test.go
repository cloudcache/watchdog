package watchdog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakePermissionRepository struct {
	grants  []Permission
	deleted Permission
}

func (r *fakePermissionRepository) ListPermissionsForUser(context.Context, ID, ID) ([]Permission, error) {
	return r.grants, nil
}

func (r *fakePermissionRepository) ListPermissions(context.Context, ID) ([]Permission, error) {
	return r.grants, nil
}

func (r *fakePermissionRepository) ReplacePermission(_ context.Context, grant Permission) error {
	r.grants = append(r.grants, grant)
	return nil
}

func (r *fakePermissionRepository) DeletePermission(_ context.Context, tenantID ID, subjectType SubjectType, subjectID ID, resourceType ResourceType, resourceID ID) error {
	r.deleted = Permission{TenantID: tenantID, SubjectType: subjectType, SubjectID: subjectID, ResourceType: resourceType, ResourceID: resourceID}
	return nil
}

func TestAPIPermissionsEffectiveDoesNotRequireAdmin(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: permissionTestAuth(false), Permissions: &fakePermissionRepository{}})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/permissions/effective", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestAPIPermissionsPutRequiresAdmin(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: permissionTestAuth(false), Permissions: &fakePermissionRepository{}})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/permissions", strings.NewReader(`{"ID":"perm-a","SubjectType":"user","SubjectID":"user-b","ResourceType":"target","ResourceID":"target-a","Actions":["view"]}`)))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestAPIPermissionsPutStoresTenantScopedGrant(t *testing.T) {
	repo := &fakePermissionRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: permissionTestAuth(true), Permissions: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/permissions", strings.NewReader(`{"ID":"perm-a","SubjectType":"user","SubjectID":"user-b","ResourceType":"target","ResourceID":"target-a","Actions":["view"]}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.grants) != 1 || repo.grants[0].TenantID != "tenant-a" {
		t.Fatalf("grants = %#v", repo.grants)
	}
}

func permissionTestAuth(admin bool) AuthContextAdapter {
	return func(*http.Request) (AuthContext, error) {
		actions := []Action{ActionView}
		if admin {
			actions = append(actions, ActionAdmin)
		}
		grants := []Permission{{
			TenantID:     "tenant-a",
			SubjectType:  SubjectUser,
			SubjectID:    "user-a",
			ResourceType: ResourceTenant,
			ResourceID:   "tenant-a",
			Actions:      actions,
		}}
		return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: admin, Grants: grants}, nil
	}
}
