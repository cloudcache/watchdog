package watchdog

import (
	"database/sql"
	"errors"
	"net/http"
)

// Operation job status/cancel API (PLAT-00A). Jobs are tenant-scoped; reading
// needs the tenant view action, canceling the configure action.

type operationJobAPI struct {
	repo OperationJobRepository
}

func registerOperationJobRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo OperationJobRepository) {
	api := operationJobAPI{repo: repo}
	mux.Handle("GET /api/v1/operation-jobs/{job_id}", auth(RequirePermission(ActionView, TenantResource)(http.HandlerFunc(api.get))))
	mux.Handle("POST /api/v1/operation-jobs/{job_id}/actions/cancel", auth(RequirePermission(ActionConfigure, TenantResource)(http.HandlerFunc(api.cancel))))
}

func (api operationJobAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	job, err := api.repo.GetOperationJob(r.Context(), auth.TenantID, ID(r.PathValue("job_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Operation job not found", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, job)
}

func (api operationJobAPI) cancel(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	jobID := ID(r.PathValue("job_id"))
	if err := api.repo.RequestOperationJobCancel(r.Context(), auth.TenantID, jobID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Operation job not found", nil)
			return
		}
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	job, err := api.repo.GetOperationJob(r.Context(), auth.TenantID, jobID)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Operation job not found", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, job)
}
