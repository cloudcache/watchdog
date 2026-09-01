package watchdog

import (
	"encoding/json"
	"errors"
	"net/http"
)

type targetAPI struct {
	repo          TargetRepository
	seriesCleaner SeriesCleaner
	network       NetworkRepository
	discoveryJobs DiscoveryJobRepository
}

func registerTargetRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo TargetRepository, cleaner SeriesCleaner, network NetworkRepository, jobs DiscoveryJobRepository) {
	api := targetAPI{repo: repo, seriesCleaner: cleaner, network: network, discoveryJobs: jobs}
	mux.Handle("GET /api/v1/targets", auth(RequirePermission(ActionView, TenantResource)(http.HandlerFunc(api.list))))
	mux.Handle("POST /api/v1/targets", auth(RequirePermission(ActionConfigure, TenantResource)(http.HandlerFunc(api.create))))
	mux.Handle("GET /api/v1/targets/{target_id}", auth(RequirePermission(ActionView, targetResourceFromPath)(http.HandlerFunc(api.get))))
	mux.Handle("PATCH /api/v1/targets/{target_id}", auth(RequirePermission(ActionConfigure, targetResourceFromPath)(http.HandlerFunc(api.patch))))
	mux.Handle("DELETE /api/v1/targets/{target_id}", auth(RequirePermission(ActionConfigure, targetResourceFromPath)(http.HandlerFunc(api.delete))))
}

func (api targetAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	targets, err := api.repo.ListTargets(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	visible := make([]Target, 0, len(targets))
	for _, target := range targets {
		if canListTarget(auth, target.ID) {
			visible = append(visible, target)
		}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": visible})
}

func (api targetAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	target, err := api.repo.GetTarget(r.Context(), auth.TenantID, ID(r.PathValue("target_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Target not found", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, target)
}

func (api targetAPI) create(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	target, err := decodeTargetRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	target.TenantID = auth.TenantID
	created, err := api.repo.CreateTarget(r.Context(), target)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api targetAPI) patch(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	existing, err := api.repo.GetTarget(r.Context(), auth.TenantID, ID(r.PathValue("target_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Target not found", nil)
		return
	}
	target, err := decodeTargetRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	target.ID = existing.ID
	target.TenantID = auth.TenantID
	updated, err := api.repo.UpdateTarget(r.Context(), target)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if api.discoveryJobs != nil && api.network != nil && existing.Host != updated.Host {
		if device, derr := api.network.GetDeviceByTarget(r.Context(), auth.TenantID, updated.ID); derr == nil {
			_ = api.discoveryJobs.EnqueueDiscoveryJob(r.Context(), auth.TenantID, device.ID, "target_host_changed")
		}
	}
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api targetAPI) delete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	targetID := ID(r.PathValue("target_id"))
	if err := api.repo.DeleteTarget(r.Context(), auth.TenantID, targetID); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if api.seriesCleaner != nil {
		_ = api.seriesCleaner.DeleteSeries(r.Context(), []string{
			`{target_id="` + string(targetID) + `"}`,
		})
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeTargetRequest(r *http.Request) (Target, error) {
	defer r.Body.Close()
	var target Target
	if err := json.NewDecoder(r.Body).Decode(&target); err != nil {
		return Target{}, err
	}
	if target.Name == "" {
		return Target{}, errors.New("target name is required")
	}
	if target.Type == "" {
		return Target{}, errors.New("target type is required")
	}
	if target.Host == "" {
		return Target{}, errors.New("target host is required")
	}
	return target, nil
}

func targetResourceFromPath(r *http.Request, _ AuthContext) (ResourceRef, error) {
	targetID := ID(r.PathValue("target_id"))
	if targetID == "" {
		return ResourceRef{}, errors.New("target id is required")
	}
	return ResourceRef{Type: ResourceTarget, ID: targetID}, nil
}

func canListTarget(auth AuthContext, targetID ID) bool {
	if auth.IsAdmin {
		return true
	}
	for _, grant := range auth.Grants {
		if grant.TenantID != auth.TenantID || grant.ResourceType != ResourceTarget || grant.ResourceID != targetID {
			continue
		}
		if !subjectMatches(AccessRequest{UserID: auth.UserID, RoleIDs: auth.RoleIDs}, grant) {
			continue
		}
		if actionAllowed(ActionView, grant.Actions) {
			return true
		}
	}
	return false
}
