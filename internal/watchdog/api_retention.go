package watchdog

import (
	"encoding/json"
	"errors"
	"net/http"
)

type retentionAPI struct {
	repo RetentionRepository
}

func registerRetentionRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo RetentionRepository) {
	api := retentionAPI{repo: repo}
	configureTenant := RequirePermission(ActionConfigure, TenantResource)
	mux.Handle("GET /api/v1/retention/policies", auth(configureTenant(http.HandlerFunc(api.list))))
	mux.Handle("PUT /api/v1/retention/policies", auth(configureTenant(http.HandlerFunc(api.put))))
	mux.Handle("DELETE /api/v1/retention/policies/{policy_id}", auth(configureTenant(http.HandlerFunc(api.delete))))
}

func (api retentionAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	policies, err := api.repo.ListRetentionPolicies(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": policies})
}

func (api retentionAPI) put(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	policy, err := decodeRetentionPolicyRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	policy.TenantID = auth.TenantID
	saved, err := api.repo.UpsertRetentionPolicy(r.Context(), policy)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, saved)
}

func (api retentionAPI) delete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if err := api.repo.DeleteRetentionPolicy(r.Context(), auth.TenantID, ID(r.PathValue("policy_id"))); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeRetentionPolicyRequest(r *http.Request) (MetricRetentionPolicy, error) {
	defer r.Body.Close()
	var policy MetricRetentionPolicy
	if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
		return MetricRetentionPolicy{}, err
	}
	if policy.HighPrecisionDays == 0 {
		return MetricRetentionPolicy{}, errors.New("high precision days is required")
	}
	return policy, nil
}
