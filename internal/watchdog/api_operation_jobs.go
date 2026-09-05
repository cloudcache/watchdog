package watchdog

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

// Operation job status/cancel API (PLAT-00A). Jobs are tenant-scoped; reading
// needs the tenant view action, canceling the configure action.

type operationJobAPI struct {
	repo OperationJobRepository
}

func registerOperationJobRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo OperationJobRepository) {
	api := operationJobAPI{repo: repo}
	mux.Handle("GET /api/v1/operation-jobs", auth(RequirePermission(ActionView, TenantResource)(http.HandlerFunc(api.list))))
	mux.Handle("GET /api/v1/operation-jobs/{job_id}", auth(RequirePermission(ActionView, TenantResource)(http.HandlerFunc(api.get))))
	mux.Handle("POST /api/v1/operation-jobs/{job_id}/actions/cancel", auth(RequirePermission(ActionConfigure, TenantResource)(http.HandlerFunc(api.cancel))))
}

func (api operationJobAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	query := r.URL.Query()
	filter := OperationJobFilter{
		JobType: strings.TrimSpace(query.Get("job_type")),
		Status:  strings.TrimSpace(query.Get("status")),
		Cursor:  strings.TrimSpace(query.Get("cursor")),
	}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be a positive integer", nil)
			return
		}
		filter.Limit = limit
	}
	jobs, nextCursor, err := api.repo.ListOperationJobs(r.Context(), auth.TenantID, filter)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	response := map[string]any{"items": jobs}
	if nextCursor != "" {
		response["next_cursor"] = nextCursor
	}
	WriteAPIJSON(w, http.StatusOK, response)
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
