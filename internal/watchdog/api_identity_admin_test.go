package watchdog

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeIdentityAdminRepository struct {
	users     map[ID]User
	roles     map[ID]Role
	userRoles map[ID][]ID
}

func newFakeIdentityAdminRepository() *fakeIdentityAdminRepository {
	return &fakeIdentityAdminRepository{
		users:     map[ID]User{},
		roles:     map[ID]Role{},
		userRoles: map[ID][]ID{},
	}
}

func (f *fakeIdentityAdminRepository) ListTenantUsers(_ context.Context, tenantID ID) ([]User, error) {
	var users []User
	for _, user := range f.users {
		if user.TenantID == tenantID {
			users = append(users, user)
		}
	}
	return users, nil
}

func (f *fakeIdentityAdminRepository) GetTenantUser(_ context.Context, tenantID, userID ID) (User, error) {
	user, ok := f.users[userID]
	if !ok || user.TenantID != tenantID {
		return User{}, sql.ErrNoRows
	}
	return user, nil
}

func (f *fakeIdentityAdminRepository) CreateUser(_ context.Context, user User) (User, error) {
	if user.ID == "" {
		user.ID = ID("user-" + user.Email)
	}
	f.users[user.ID] = user
	return user, nil
}

func (f *fakeIdentityAdminRepository) UpdateUser(_ context.Context, user User) (User, error) {
	current, ok := f.users[user.ID]
	if !ok || current.TenantID != user.TenantID {
		return User{}, sql.ErrNoRows
	}
	f.users[user.ID] = user
	return user, nil
}

func (f *fakeIdentityAdminRepository) DisableUser(_ context.Context, tenantID, userID ID) error {
	user, ok := f.users[userID]
	if !ok || user.TenantID != tenantID {
		return sql.ErrNoRows
	}
	user.Status = "disabled"
	f.users[userID] = user
	return nil
}

func (f *fakeIdentityAdminRepository) ListRoles(_ context.Context, tenantID ID) ([]Role, error) {
	var roles []Role
	for _, role := range f.roles {
		if role.TenantID == tenantID {
			roles = append(roles, role)
		}
	}
	return roles, nil
}

func (f *fakeIdentityAdminRepository) GetTenantRole(_ context.Context, tenantID, roleID ID) (Role, error) {
	role, ok := f.roles[roleID]
	if !ok || role.TenantID != tenantID {
		return Role{}, sql.ErrNoRows
	}
	return role, nil
}

func (f *fakeIdentityAdminRepository) CreateRole(_ context.Context, role Role) (Role, error) {
	if role.ID == "" {
		role.ID = ID("role-" + role.Name)
	}
	f.roles[role.ID] = role
	return role, nil
}

func (f *fakeIdentityAdminRepository) UpdateRole(_ context.Context, role Role) (Role, error) {
	current, ok := f.roles[role.ID]
	if !ok || current.TenantID != role.TenantID {
		return Role{}, sql.ErrNoRows
	}
	f.roles[role.ID] = role
	return role, nil
}

func (f *fakeIdentityAdminRepository) DeleteRole(_ context.Context, tenantID, roleID ID) error {
	role, ok := f.roles[roleID]
	if !ok || role.TenantID != tenantID {
		return sql.ErrNoRows
	}
	delete(f.roles, roleID)
	return nil
}

func (f *fakeIdentityAdminRepository) ReplaceUserRoles(_ context.Context, tenantID, userID ID, roleIDs []ID) error {
	user, ok := f.users[userID]
	if !ok || user.TenantID != tenantID {
		return sql.ErrNoRows
	}
	for _, roleID := range roleIDs {
		role, ok := f.roles[roleID]
		if !ok || role.TenantID != tenantID {
			return errRoleNotInTenant
		}
	}
	f.userRoles[userID] = append([]ID(nil), roleIDs...)
	return nil
}

func (f *fakeIdentityAdminRepository) ListUserRoleIDs(_ context.Context, _ ID, userID ID) ([]ID, error) {
	return f.userRoles[userID], nil
}

type recordingAuditRepository struct {
	logs []AuditLog
}

func (r *recordingAuditRepository) CreateAuditLog(_ context.Context, log AuditLog) error {
	r.logs = append(r.logs, log)
	return nil
}

func identityAdminTestAuth(*http.Request) (AuthContext, error) {
	return AuthContext{TenantID: "tenant-a", UserID: "admin-a", IsAdmin: true}, nil
}

func newIdentityAdminTestRouter(repo *fakeIdentityAdminRepository, audit *recordingAuditRepository) http.Handler {
	return NewAPIV1Router(APIV1RouterConfig{
		Auth:          identityAdminTestAuth,
		IdentityAdmin: repo,
		Audit:         audit,
	})
}

func TestIdentityAdminRequiresAdminAction(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:          networkTestAuth, // view-only grants, not tenant admin
		IdentityAdmin: newFakeIdentityAdminRepository(),
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/users", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestIdentityAdminUserLifecycle(t *testing.T) {
	repo := newFakeIdentityAdminRepository()
	audit := &recordingAuditRepository{}
	router := newIdentityAdminTestRouter(repo, audit)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/users",
		strings.NewReader(`{"email":"ops@example.com","name":"Ops","auth_provider":"pocketbase","external_subject_id":"pb_1"}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.users) != 1 {
		t.Fatalf("users = %d", len(repo.users))
	}
	var created User
	for _, user := range repo.users {
		created = user
	}
	if created.TenantID != "tenant-a" || created.Status != "active" {
		t.Fatalf("created = %+v", created)
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/api/v1/users/"+string(created.ID),
		strings.NewReader(`{"status":"disabled"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("patch status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.users[created.ID].Status != "disabled" {
		t.Fatalf("status = %s", repo.users[created.ID].Status)
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/users/"+string(created.ID), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", rec.Code, rec.Body.String())
	}

	if len(audit.logs) != 3 {
		t.Fatalf("audit logs = %d", len(audit.logs))
	}
	if audit.logs[0].Action != "user.create" || audit.logs[0].ActorID != "admin-a" {
		t.Fatalf("audit[0] = %+v", audit.logs[0])
	}
}

func TestIdentityAdminRejectsSelfDisable(t *testing.T) {
	repo := newFakeIdentityAdminRepository()
	repo.users["admin-a"] = User{ID: "admin-a", TenantID: "tenant-a", Email: "admin@example.com", Status: "active"}
	router := newIdentityAdminTestRouter(repo, &recordingAuditRepository{})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/users/admin-a", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.users["admin-a"].Status != "active" {
		t.Fatalf("self-disable went through")
	}
}

func TestIdentityAdminValidatesUserRequest(t *testing.T) {
	router := newIdentityAdminTestRouter(newFakeIdentityAdminRepository(), &recordingAuditRepository{})
	for _, body := range []string{
		`{"email":"not-an-email"}`,
		`{"email":"a@b.c","status":"weird"}`,
		`{"email":"a@b.c","auth_provider":"pocketbase"}`,
	} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/users", strings.NewReader(body)))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %s: status = %d", body, rec.Code)
		}
	}
}

func TestIdentityAdminRoleMembership(t *testing.T) {
	repo := newFakeIdentityAdminRepository()
	repo.users["user-1"] = User{ID: "user-1", TenantID: "tenant-a", Email: "u1@example.com", Status: "active"}
	repo.roles["role-1"] = Role{ID: "role-1", TenantID: "tenant-a", Name: "admin"}
	repo.roles["role-other"] = Role{ID: "role-other", TenantID: "tenant-b", Name: "outsider"}
	router := newIdentityAdminTestRouter(repo, &recordingAuditRepository{})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/users/user-1/roles",
		strings.NewReader(`{"role_ids":["role-1"]}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("replace status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.userRoles["user-1"]) != 1 || repo.userRoles["user-1"][0] != "role-1" {
		t.Fatalf("roles = %v", repo.userRoles["user-1"])
	}

	// A role from another tenant must be rejected wholesale.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/users/user-1/roles",
		strings.NewReader(`{"role_ids":["role-other"]}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("cross-tenant status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.userRoles["user-1"]) != 1 {
		t.Fatalf("membership changed on rejected update: %v", repo.userRoles["user-1"])
	}
}

func TestIdentityAdminRoleLifecycle(t *testing.T) {
	repo := newFakeIdentityAdminRepository()
	router := newIdentityAdminTestRouter(repo, &recordingAuditRepository{})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/roles", strings.NewReader(`{"name":"auditor"}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/api/v1/roles/role-auditor",
		strings.NewReader(`{"name":"auditors"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/roles/role-auditor", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.roles) != 0 {
		t.Fatalf("roles = %v", repo.roles)
	}
}
