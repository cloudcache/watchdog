package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
)

// Module center API (PLAT-02): descriptor listing with effective per-tenant
// enablement, and admin-gated enable/disable. Disabling a module hides entry
// points and stops workers; it never deletes module data.
type moduleAPI struct {
	registries *PlatformRegistries
	repo       TenantModuleRepository
	audit      AuditRepository
}

type moduleStateItem struct {
	Key            string
	Version        string
	DisplayName    string
	Dependencies   []string
	DefaultEnabled bool
	Enabled        bool
}

func registerModuleRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, registries *PlatformRegistries, repo TenantModuleRepository, audit AuditRepository) {
	api := moduleAPI{registries: registries, repo: repo, audit: audit}
	admin := RequirePermission(ActionAdmin, TenantResource)
	mux.Handle("GET /api/v1/modules", auth(http.HandlerFunc(api.list)))
	mux.Handle("GET /api/v1/modules/target-kinds", auth(http.HandlerFunc(api.listTargetKinds)))
	mux.Handle("GET /api/v1/tenants/{tenant_id}/modules", auth(admin(http.HandlerFunc(api.listForTenant))))
	mux.Handle("PUT /api/v1/tenants/{tenant_id}/modules", auth(admin(http.HandlerFunc(api.putForTenant))))
}

func (api moduleAPI) effectiveStates(ctx context.Context, tenantID ID) ([]moduleStateItem, error) {
	overrides := map[string]bool{}
	if api.repo != nil {
		stored, err := api.repo.ListTenantModuleStates(ctx, tenantID)
		if err != nil {
			return nil, err
		}
		overrides = stored
	}
	descriptors := api.registries.Modules.Descriptors()
	items := make([]moduleStateItem, 0, len(descriptors))
	for _, descriptor := range descriptors {
		enabled := descriptor.DefaultEnabled
		if value, ok := overrides[descriptor.Key]; ok {
			enabled = value
		}
		items = append(items, moduleStateItem{
			Key:            descriptor.Key,
			Version:        descriptor.Version,
			DisplayName:    descriptor.DisplayName,
			Dependencies:   descriptor.Dependencies,
			DefaultEnabled: descriptor.DefaultEnabled,
			Enabled:        enabled,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items, nil
}

func (api moduleAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	items, err := api.effectiveStates(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (api moduleAPI) listTargetKinds(w http.ResponseWriter, r *http.Request) {
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": api.registries.TargetKinds.List()})
}

func (api moduleAPI) listForTenant(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if ID(r.PathValue("tenant_id")) != auth.TenantID {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Tenant not found", nil)
		return
	}
	api.list(w, r)
}

func (api moduleAPI) putForTenant(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if ID(r.PathValue("tenant_id")) != auth.TenantID {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Tenant not found", nil)
		return
	}
	if api.repo == nil {
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Module state storage is unavailable", nil)
		return
	}
	var req struct {
		Modules map[string]bool `json:"modules"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if len(req.Modules) == 0 {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "modules map is required", nil)
		return
	}
	for key, enabled := range req.Modules {
		if _, ok := api.registries.Modules.Get(key); !ok {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "unknown module "+key, nil)
			return
		}
		if key == "core" && !enabled {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "the core module cannot be disabled", nil)
			return
		}
	}
	for key, enabled := range req.Modules {
		if err := api.repo.SetTenantModuleEnabled(r.Context(), auth.TenantID, key, enabled, auth.UserID); err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		if api.audit != nil {
			_ = api.audit.CreateAuditLog(r.Context(), AuditLog{
				TenantID:     auth.TenantID,
				ActorID:      auth.UserID,
				Action:       "module.set_enabled",
				ResourceType: "module",
				ResourceID:   ID(key),
				Detail:       map[string]any{"enabled": enabled},
			})
		}
	}
	items, err := api.effectiveStates(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items})
}
