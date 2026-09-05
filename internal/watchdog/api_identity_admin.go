package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// identityAdminAPI serves the tenant-scoped user and role management surface
// (PLAT-01). Every route requires the tenant admin action; credentials never
// pass through here — users are authorization projections of the external
// identity provider.
type identityAdminAPI struct {
	repo  IdentityAdminRepository
	audit AuditRepository
}

func registerIdentityAdminRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo IdentityAdminRepository, audit AuditRepository, idempotency IdempotencyRepository) {
	api := identityAdminAPI{repo: repo, audit: audit}
	admin := RequirePermission(ActionAdmin, TenantResource)
	mux.Handle("GET /api/v1/users", auth(admin(http.HandlerFunc(api.listUsers))))
	mux.Handle("POST /api/v1/users", auth(admin(WithIdempotency(idempotency, 0, api.createUser))))
	mux.Handle("GET /api/v1/users/{user_id}", auth(admin(http.HandlerFunc(api.getUser))))
	mux.Handle("PATCH /api/v1/users/{user_id}", auth(admin(http.HandlerFunc(api.updateUser))))
	mux.Handle("DELETE /api/v1/users/{user_id}", auth(admin(http.HandlerFunc(api.disableUser))))
	mux.Handle("GET /api/v1/users/{user_id}/roles", auth(admin(http.HandlerFunc(api.listUserRoles))))
	mux.Handle("PUT /api/v1/users/{user_id}/roles", auth(admin(http.HandlerFunc(api.replaceUserRoles))))
	mux.Handle("GET /api/v1/roles", auth(admin(http.HandlerFunc(api.listRoles))))
	mux.Handle("POST /api/v1/roles", auth(admin(WithIdempotency(idempotency, 0, api.createRole))))
	mux.Handle("PATCH /api/v1/roles/{role_id}", auth(admin(http.HandlerFunc(api.updateRole))))
	mux.Handle("DELETE /api/v1/roles/{role_id}", auth(admin(http.HandlerFunc(api.deleteRole))))
}

type userRequest struct {
	Email             string `json:"email"`
	Name              string `json:"name"`
	Status            string `json:"status"`
	AuthProvider      string `json:"auth_provider"`
	ExternalSubjectID string `json:"external_subject_id"`
}

func (api identityAdminAPI) listUsers(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	users, err := api.repo.ListTenantUsers(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": users})
}

func (api identityAdminAPI) getUser(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	user, err := api.repo.GetTenantUser(r.Context(), auth.TenantID, ID(r.PathValue("user_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "User not found", nil)
		return
	}
	SetEntityETag(w, user.UpdatedAt)
	WriteAPIJSON(w, http.StatusOK, user)
}

func (api identityAdminAPI) createUser(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	req, err := decodeUserRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if req.Status == "" {
		req.Status = "active"
	}
	user, err := api.repo.CreateUser(r.Context(), User{
		TenantID:          auth.TenantID,
		Email:             req.Email,
		Name:              req.Name,
		Status:            req.Status,
		AuthProvider:      req.AuthProvider,
		ExternalSubjectID: req.ExternalSubjectID,
	})
	if err != nil {
		writeIdentityWriteError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "user.create", "user", user.ID, map[string]any{"email": user.Email, "status": user.Status})
	WriteAPIJSON(w, http.StatusCreated, user)
}

func (api identityAdminAPI) updateUser(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	userID := ID(r.PathValue("user_id"))
	current, err := api.repo.GetTenantUser(r.Context(), auth.TenantID, userID)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "User not found", nil)
		return
	}
	if !CheckIfMatch(w, r, current.UpdatedAt) {
		return
	}
	var req userRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	next := current
	if req.Email != "" {
		next.Email = strings.TrimSpace(req.Email)
	}
	if req.Name != "" {
		next.Name = strings.TrimSpace(req.Name)
	}
	if req.Status != "" {
		next.Status = req.Status
	}
	if req.AuthProvider != "" || req.ExternalSubjectID != "" {
		next.AuthProvider = strings.TrimSpace(req.AuthProvider)
		next.ExternalSubjectID = strings.TrimSpace(req.ExternalSubjectID)
	}
	if err := validateUserFields(next.Email, next.Status, next.AuthProvider, next.ExternalSubjectID); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	updated, err := api.repo.UpdateUser(r.Context(), next)
	if err != nil {
		writeIdentityWriteError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "user.update", "user", updated.ID, map[string]any{
		"before": map[string]any{"email": current.Email, "status": current.Status},
		"after":  map[string]any{"email": updated.Email, "status": updated.Status},
	})
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api identityAdminAPI) disableUser(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	userID := ID(r.PathValue("user_id"))
	if userID == auth.UserID {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "You cannot disable your own account", nil)
		return
	}
	if current, err := api.repo.GetTenantUser(r.Context(), auth.TenantID, userID); err == nil {
		if !CheckIfMatch(w, r, current.UpdatedAt) {
			return
		}
	}
	if err := api.repo.DisableUser(r.Context(), auth.TenantID, userID); err != nil {
		writeIdentityWriteError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "user.disable", "user", userID, nil)
	WriteAPIJSON(w, http.StatusOK, map[string]any{"success": true, "status": "disabled"})
}

func (api identityAdminAPI) listUserRoles(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	userID := ID(r.PathValue("user_id"))
	if _, err := api.repo.GetTenantUser(r.Context(), auth.TenantID, userID); err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "User not found", nil)
		return
	}
	roleIDs, err := api.repo.ListUserRoleIDs(r.Context(), auth.TenantID, userID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": roleIDs})
}

func (api identityAdminAPI) replaceUserRoles(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	userID := ID(r.PathValue("user_id"))
	var req struct {
		RoleIDs []ID `json:"role_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := api.repo.ReplaceUserRoles(r.Context(), auth.TenantID, userID, req.RoleIDs); err != nil {
		if errors.Is(err, errRoleNotInTenant) {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		writeIdentityWriteError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "user.roles.replace", "user", userID, map[string]any{"role_ids": req.RoleIDs})
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": req.RoleIDs})
}

type roleRequest struct {
	Name  string `json:"name"`
	Scope string `json:"scope"`
}

func (api identityAdminAPI) listRoles(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	roles, err := api.repo.ListRoles(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": roles})
}

func (api identityAdminAPI) createRole(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	var req roleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "role name is required", nil)
		return
	}
	role, err := api.repo.CreateRole(r.Context(), Role{TenantID: auth.TenantID, Name: req.Name, Scope: req.Scope})
	if err != nil {
		writeIdentityWriteError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "role.create", "role", role.ID, map[string]any{"name": role.Name})
	WriteAPIJSON(w, http.StatusCreated, role)
}

func (api identityAdminAPI) updateRole(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	roleID := ID(r.PathValue("role_id"))
	var req roleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "role name is required", nil)
		return
	}
	if req.Scope == "" {
		req.Scope = "tenant"
	}
	if current, err := api.repo.GetTenantRole(r.Context(), auth.TenantID, roleID); err == nil {
		if !CheckIfMatch(w, r, current.UpdatedAt) {
			return
		}
	}
	role, err := api.repo.UpdateRole(r.Context(), Role{ID: roleID, TenantID: auth.TenantID, Name: req.Name, Scope: req.Scope})
	if err != nil {
		writeIdentityWriteError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "role.update", "role", role.ID, map[string]any{"name": role.Name})
	WriteAPIJSON(w, http.StatusOK, role)
}

func (api identityAdminAPI) deleteRole(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	roleID := ID(r.PathValue("role_id"))
	if current, err := api.repo.GetTenantRole(r.Context(), auth.TenantID, roleID); err == nil {
		if !CheckIfMatch(w, r, current.UpdatedAt) {
			return
		}
	}
	if err := api.repo.DeleteRole(r.Context(), auth.TenantID, roleID); err != nil {
		writeIdentityWriteError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "role.delete", "role", roleID, nil)
	WriteAPIJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (api identityAdminAPI) recordAudit(ctx context.Context, auth AuthContext, action string, resourceType ResourceType, resourceID ID, detail map[string]any) {
	if api.audit == nil {
		return
	}
	_ = api.audit.CreateAuditLog(ctx, AuditLog{
		TenantID:     auth.TenantID,
		ActorID:      auth.UserID,
		Action:       action,
		ResourceType: resourceType,
		ResourceID:   resourceID,
		Detail:       detail,
	})
}

func decodeUserRequest(r *http.Request) (userRequest, error) {
	defer r.Body.Close()
	var req userRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return userRequest{}, err
	}
	req.Email = strings.TrimSpace(req.Email)
	req.Name = strings.TrimSpace(req.Name)
	req.AuthProvider = strings.TrimSpace(req.AuthProvider)
	req.ExternalSubjectID = strings.TrimSpace(req.ExternalSubjectID)
	if err := validateUserFields(req.Email, req.Status, req.AuthProvider, req.ExternalSubjectID); err != nil {
		return userRequest{}, err
	}
	return req, nil
}

func validateUserFields(email, status, authProvider, externalSubject string) error {
	if email == "" || !strings.Contains(email, "@") {
		return errors.New("a valid email is required")
	}
	if status != "" && status != "active" && status != "disabled" {
		return errors.New("status must be active or disabled")
	}
	if (authProvider == "") != (externalSubject == "") {
		return errors.New("auth_provider and external_subject_id must be set together")
	}
	return nil
}

func writeIdentityWriteError(w http.ResponseWriter, err error) {
	if errors.Is(err, sql.ErrNoRows) {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Not found", nil)
		return
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "A record with this identity already exists", nil)
		return
	}
	WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
}
