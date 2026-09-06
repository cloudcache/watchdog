package watchdog

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type addressDimensionAPI struct {
	publisher AddressDimensionPublisher
	jobs      OperationJobRepository
}

func registerAddressDimensionRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, publisher AddressDimensionPublisher, jobs OperationJobRepository) {
	api := addressDimensionAPI{publisher: publisher, jobs: jobs}
	view := RequirePermission(ActionView, TenantResource)
	configure := RequirePermission(ActionConfigure, TenantResource)
	operate := RequirePermission(ActionOperate, TenantResource)
	mux.Handle("GET /api/v1/dimensions/address/versions", auth(view(http.HandlerFunc(api.list))))
	mux.Handle("GET /api/v1/dimensions/address/versions/{snapshot_id}", auth(view(http.HandlerFunc(api.get))))
	mux.Handle("POST /api/v1/dimensions/address/preview", auth(configure(http.HandlerFunc(api.preview))))
	mux.Handle("POST /api/v1/dimensions/address/publish", auth(operate(http.HandlerFunc(api.publish))))
}

func (api addressDimensionAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	filter := AddressDimensionListFilter{Status: strings.TrimSpace(r.URL.Query().Get("status")), Cursor: strings.TrimSpace(r.URL.Query().Get("cursor"))}
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be a positive integer", nil)
			return
		}
		filter.Limit = limit
	}
	items, cursor, err := api.publisher.ListAddressDimensionSnapshots(r.Context(), auth.TenantID, filter)
	if err != nil {
		writeAddressDimensionError(w, err)
		return
	}
	if items == nil {
		items = []AddressDimensionSnapshot{}
	}
	response := map[string]any{"items": items}
	if cursor != "" {
		response["next_cursor"] = cursor
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

func (api addressDimensionAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	item, err := api.publisher.GetAddressDimensionSnapshot(r.Context(), auth.TenantID, ID(r.PathValue("snapshot_id")))
	if err != nil {
		writeAddressDimensionError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(item.RowVersion))
	WriteAPIJSON(w, http.StatusOK, item)
}

func (api addressDimensionAPI) preview(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	var input struct {
		EffectiveFrom time.Time `json:"effective_from"`
	}
	if !decodeAddressTaxonomyInput(w, r, &input) {
		return
	}
	preview, err := api.publisher.PreviewAddressDimension(r.Context(), auth.TenantID, input.EffectiveFrom)
	if err != nil {
		writeAddressDimensionError(w, err)
		return
	}
	WriteAPIJSON(w, http.StatusOK, preview)
}

func (api addressDimensionAPI) publish(w http.ResponseWriter, r *http.Request) {
	if api.jobs == nil {
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Operation job service is not configured", nil)
		return
	}
	auth, _ := AuthFromContext(r.Context())
	var request AddressDimensionPublishRequest
	if !decodeAddressTaxonomyInput(w, r, &request) {
		return
	}
	payload, err := EncodeAddressDimensionPublishJobPayload(request)
	if err != nil {
		writeAddressDimensionError(w, err)
		return
	}
	hash := sha256.Sum256(payload)
	job, err := api.jobs.EnqueueOperationJob(r.Context(), OperationJob{
		TenantID: auth.TenantID, JobType: AddressDimensionPublishJob,
		// Include the semantic draft digest so a stale-preview terminal job does
		// not prevent a corrected draft from using the same effective minute.
		IdempotencyKey: "address-dimension:" + request.EffectiveFrom.UTC().Format(time.RFC3339) + ":" + strings.TrimPrefix(request.PreviewDigest, "sha256:"),
		RequestHash:    hex.EncodeToString(hash[:]), CheckpointJSON: payload, CreatedBy: auth.UserID,
	})
	if err != nil {
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusAccepted, map[string]any{"job": job})
}

func writeAddressDimensionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Address dimension snapshot not found", nil)
	case errors.Is(err, ErrAddressDimensionInvalid):
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
	case errors.Is(err, ErrAddressDimensionDraftChanged):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("draft_changed"), err.Error(), nil)
	case errors.Is(err, ErrAddressDimensionConflict):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, err.Error(), nil)
	default:
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
	}
}
