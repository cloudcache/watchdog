package watchdog

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/http"
)

// PLAT-04 collector deletion: preview surfaces cascade vs blocking evidence;
// DELETE refuses a blocked collector (409) and otherwise enqueues an async
// delete job that writes a destruction receipt.

type collectorDeleteAPI struct {
	preview       CollectorDeletePreviewRepository
	operationJobs OperationJobRepository
}

func registerCollectorDeleteRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, preview CollectorDeletePreviewRepository, operationJobs OperationJobRepository) {
	api := collectorDeleteAPI{preview: preview, operationJobs: operationJobs}
	operateTenant := RequirePermission(ActionOperate, TenantResource)
	mux.Handle("GET /api/v1/collectors/{collector_id}/delete-preview", auth(operateTenant(http.HandlerFunc(api.preview_))))
	mux.Handle("DELETE /api/v1/collectors/{collector_id}", auth(operateTenant(http.HandlerFunc(api.delete))))
}

func (api collectorDeleteAPI) preview_(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	preview, err := api.preview.PreviewCollectorDelete(r.Context(), auth.TenantID, ID(r.PathValue("collector_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Collector not found", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, preview)
}

func (api collectorDeleteAPI) delete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	collectorID := ID(r.PathValue("collector_id"))
	preview, err := api.preview.PreviewCollectorDelete(r.Context(), auth.TenantID, collectorID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Collector not found", nil)
			return
		}
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if !preview.Deletable {
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest,
			"Collector has ownership evidence and cannot be deleted; revoke it first", map[string]any{"impacts": preview.Impacts})
		return
	}
	if api.operationJobs == nil {
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Async delete is not available", nil)
		return
	}
	impact := map[string]int{}
	for _, item := range preview.Impacts {
		if item.Behavior == "deleted" && item.Count > 0 {
			impact[item.ResourceType] = item.Count
		}
	}
	payload, err := EncodeCollectorDeletePayload(collectorID, impact)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	digest := sha256.Sum256([]byte("collector_delete:" + string(collectorID)))
	job, err := api.operationJobs.EnqueueOperationJob(r.Context(), OperationJob{
		TenantID:       auth.TenantID,
		JobType:        CollectorDeleteJobType,
		IdempotencyKey: "collector_delete:" + string(collectorID),
		RequestHash:    hex.EncodeToString(digest[:]),
		CheckpointJSON: payload,
		CreatedBy:      auth.UserID,
	})
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusAccepted, map[string]any{
		"job_id": job.ID, "status": job.Status,
		"status_url": "/api/v1/operation-jobs/" + string(job.ID),
	})
}
