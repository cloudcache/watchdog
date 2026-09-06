package watchdog

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type addressDimensionGCAPI struct {
	repository AddressDimensionGCRepository
}

func registerAddressDimensionGCRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repository AddressDimensionGCRepository) {
	api := addressDimensionGCAPI{repository: repository}
	view := RequirePermission(ActionView, TenantResource)
	operate := RequirePermission(ActionOperate, TenantResource)
	mux.Handle("GET /api/v1/dimensions/address/gc/candidates", auth(view(http.HandlerFunc(api.listCandidates))))
	mux.Handle("POST /api/v1/dimensions/address/versions/{snapshot_id}/actions/schedule-gc", auth(operate(http.HandlerFunc(api.schedule))))
}

func (api addressDimensionGCAPI) listCandidates(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	filter := AddressDimensionGCFilter{Cursor: strings.TrimSpace(r.URL.Query().Get("cursor"))}
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be a positive integer", nil)
			return
		}
		filter.Limit = limit
	}
	items, cursor, err := api.repository.ListAddressDimensionGCCandidates(r.Context(), auth.TenantID, time.Now().UTC(), filter)
	if err != nil {
		writeAddressDimensionGCError(w, err)
		return
	}
	if items == nil {
		items = []AddressDimensionGCCandidate{}
	}
	response := map[string]any{"items": items}
	if cursor != "" {
		response["next_cursor"] = cursor
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

func (api addressDimensionGCAPI) schedule(w http.ResponseWriter, r *http.Request) {
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	var input struct {
		RetentionUntil time.Time `json:"retention_until"`
	}
	if !decodeAddressTaxonomyInput(w, r, &input) {
		return
	}
	auth, _ := AuthFromContext(r.Context())
	snapshot, err := api.repository.ScheduleAddressDimensionObjectGC(
		r.Context(), auth.TenantID, auth.UserID, ID(r.PathValue("snapshot_id")), expected, input.RetentionUntil,
	)
	if err != nil {
		writeAddressDimensionGCError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(snapshot.RowVersion))
	WriteAPIJSON(w, http.StatusOK, snapshot)
}

func writeAddressDimensionGCError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Address dimension snapshot not found", nil)
	case errors.Is(err, ErrAddressDimensionInvalid):
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
	case errors.Is(err, ErrAddressDimensionConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"), "Address dimension snapshot changed since it was read", nil)
	case errors.Is(err, ErrAddressDimensionInvalidTransition), errors.Is(err, ErrAddressDimensionGCNotEligible):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, err.Error(), nil)
	default:
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
	}
}
