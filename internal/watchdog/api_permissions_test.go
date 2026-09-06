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

func TestAPIPermissionsPutValidatesValueLayerActions(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		actions string
		status  int
	}{
		{name: "value layers", actions: `"view_raw","view_supplier","view_customer","export_raw","export_supplier","export_customer"`, status: http.StatusOK},
		{name: "unknown", actions: `"view_raw","generated_action"`, status: http.StatusBadRequest},
		{name: "empty", actions: ``, status: http.StatusBadRequest},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repo := &fakePermissionRepository{}
			router := NewAPIV1Router(APIV1RouterConfig{Auth: permissionTestAuth(true), Permissions: repo})
			body := `{"ID":"perm-value","SubjectType":"user","SubjectID":"user-b","ResourceType":"tenant","ResourceID":"tenant-a","Actions":[` + testCase.actions + `]}`
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/api/v1/permissions", strings.NewReader(body)))
			if response.Code != testCase.status {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
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
