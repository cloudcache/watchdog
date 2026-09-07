package watchdog

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

type permissionAPI struct {
	repo PermissionRepository
}

func registerPermissionRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo PermissionRepository) {
	api := permissionAPI{repo: repo}
	adminTenant := RequirePermission(ActionAdmin, TenantResource)
	mux.Handle("GET /api/v1/permissions", auth(adminTenant(http.HandlerFunc(api.list))))
	mux.Handle("PUT /api/v1/permissions", auth(adminTenant(http.HandlerFunc(api.put))))
	mux.Handle("DELETE /api/v1/permissions/{permission_id}", auth(adminTenant(http.HandlerFunc(api.delete))))
	mux.Handle("GET /api/v1/permissions/effective", auth(http.HandlerFunc(api.effective)))
}

func (api permissionAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	grants, err := api.repo.ListPermissions(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	resourceType := ResourceType(r.URL.Query().Get("resource_type"))
	resourceID := ID(r.URL.Query().Get("resource_id"))
	if resourceType != "" || resourceID != "" {
		grants = filterPermissionsByResource(grants, resourceType, resourceID)
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": grants})
}

func (api permissionAPI) put(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	grant, err := decodePermissionRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	grant.TenantID = auth.TenantID
	if err := api.repo.ReplacePermission(r.Context(), grant); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, grant)
}

func (api permissionAPI) delete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	var grant Permission
	if err := json.NewDecoder(r.Body).Decode(&grant); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	grant.ID = ID(r.PathValue("permission_id"))
	grant.TenantID = auth.TenantID
	if grant.SubjectType == "" || grant.SubjectID == "" || grant.ResourceType == "" || grant.ResourceID == "" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "permission subject and resource are required", nil)
		return
	}
	if err := api.repo.DeletePermission(r.Context(), grant.TenantID, grant.SubjectType, grant.SubjectID, grant.ResourceType, grant.ResourceID); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (api permissionAPI) effective(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": auth.Grants})
}

func decodePermissionRequest(r *http.Request) (Permission, error) {
	defer r.Body.Close()
	var grant Permission
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&grant); err != nil {
		return Permission{}, err
	}
	if err := ensureDashboardJSONEOF(decoder); err != nil {
		return Permission{}, err
	}
	if grant.ID == "" {
		return Permission{}, errors.New("permission id is required")
	}
	if grant.SubjectType == "" || grant.SubjectID == "" || grant.ResourceType == "" || grant.ResourceID == "" {
		return Permission{}, errors.New("permission subject and resource are required")
	}
	actions, err := normalizePermissionActions(grant.Actions)
	if err != nil {
		return Permission{}, err
	}
	grant.Actions = actions
	return grant, nil
}

func normalizePermissionActions(actions []Action) ([]Action, error) {
	if len(actions) == 0 {
		return nil, errors.New("at least one permission action is required")
	}
	allowed := map[Action]bool{
		ActionView: true, ActionConfigure: true, ActionOperate: true, ActionExport: true, ActionAdmin: true,
		ActionViewRaw: true, ActionViewSupplier: true, ActionViewCustomer: true,
		ActionExportRaw: true, ActionExportSupplier: true, ActionExportCustomer: true,
		ActionVPNView: true, ActionVPNExport: true, ActionVPNTriage: true, ActionVPNProbe: true,
		ActionConfigureAdjustment: true,
	}
	seen := make(map[Action]bool, len(actions))
	normalized := make([]Action, 0, len(actions))
	for _, action := range actions {
		if !allowed[action] {
			return nil, fmt.Errorf("unsupported permission action %q", action)
		}
		if !seen[action] {
			seen[action] = true
			normalized = append(normalized, action)
		}
	}
	return normalized, nil
}

func filterPermissionsByResource(grants []Permission, resourceType ResourceType, resourceID ID) []Permission {
	filtered := make([]Permission, 0, len(grants))
	for _, grant := range grants {
		if resourceType != "" && grant.ResourceType != resourceType {
			continue
		}
		if resourceID != "" && grant.ResourceID != resourceID {
			continue
		}
		filtered = append(filtered, grant)
	}
	return filtered
}
