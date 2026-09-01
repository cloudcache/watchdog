package watchdog

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
)

type addressSetAPI struct {
	repo AddressSetRepository
}

func registerAddressSetRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo AddressSetRepository) {
	api := addressSetAPI{repo: repo}
	viewTenant := RequirePermission(ActionView, TenantResource)
	configureTenant := RequirePermission(ActionConfigure, TenantResource)
	mux.Handle("GET /api/v1/address-prefixes", auth(viewTenant(http.HandlerFunc(api.listPrefixes))))
	mux.Handle("POST /api/v1/address-prefixes", auth(configureTenant(http.HandlerFunc(api.upsertPrefix))))
	mux.Handle("DELETE /api/v1/address-prefixes/{prefix_id}", auth(configureTenant(http.HandlerFunc(api.deletePrefix))))
	mux.Handle("GET /api/v1/address-sets", auth(viewTenant(http.HandlerFunc(api.listSets))))
	mux.Handle("POST /api/v1/address-sets", auth(configureTenant(http.HandlerFunc(api.upsertSet))))
	mux.Handle("GET /api/v1/address-sets/{set_id}", auth(viewTenant(http.HandlerFunc(api.getSet))))
	mux.Handle("PATCH /api/v1/address-sets/{set_id}", auth(configureTenant(http.HandlerFunc(api.upsertSet))))
	mux.Handle("DELETE /api/v1/address-sets/{set_id}", auth(configureTenant(http.HandlerFunc(api.deleteSet))))
}

func (api addressSetAPI) listPrefixes(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	prefixes, err := api.repo.ListAddressPrefixes(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": prefixes})
}

func (api addressSetAPI) upsertPrefix(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	var prefix AddressPrefix
	if err := json.NewDecoder(r.Body).Decode(&prefix); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	prefix.TenantID = auth.TenantID
	if prefix.ID == "" {
		prefix.ID = uuid.New().String()
	}
	if prefix.CIDR == "" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "cidr is required", nil)
		return
	}
	if prefix.Labels == nil {
		prefix.Labels = map[string]string{}
	}
	if prefix.Source == "" {
		prefix.Source = "manual"
	}
	saved, err := api.repo.UpsertAddressPrefix(r.Context(), prefix)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusCreated, saved)
}

func (api addressSetAPI) deletePrefix(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if err := api.repo.DeleteAddressPrefix(r.Context(), auth.TenantID, r.PathValue("prefix_id")); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (api addressSetAPI) listSets(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	sets, err := api.repo.ListAddressSets(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": sets})
}

func (api addressSetAPI) getSet(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	set, err := api.repo.GetAddressSet(r.Context(), auth.TenantID, r.PathValue("set_id"))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Address set not found", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, set)
}

func (api addressSetAPI) upsertSet(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	var set AddressSet
	if err := json.NewDecoder(r.Body).Decode(&set); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	set.TenantID = auth.TenantID
	if id := r.PathValue("set_id"); id != "" {
		set.ID = id
	}
	if set.ID == "" {
		set.ID = uuid.New().String()
	}
	if set.Name == "" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "name is required", nil)
		return
	}
	if set.Selector == nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "selector is required", nil)
		return
	}
	if set.MatchDirection == "" {
		set.MatchDirection = "both"
	}
	set.Enabled = true
	saved, err := api.repo.UpsertAddressSet(r.Context(), set)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusCreated, saved)
}

func (api addressSetAPI) deleteSet(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if err := api.repo.DeleteAddressSet(r.Context(), auth.TenantID, r.PathValue("set_id")); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
