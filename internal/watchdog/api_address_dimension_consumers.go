package watchdog

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type addressDimensionConsumerAPI struct {
	publisher AddressDimensionPublisher
	lifecycle AddressDimensionLifecycle
	reader    AddressDimensionConsumerStatusReader
}

func registerAddressDimensionConsumerRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, publisher AddressDimensionPublisher, lifecycle AddressDimensionLifecycle, reader AddressDimensionConsumerStatusReader) {
	api := addressDimensionConsumerAPI{publisher: publisher, lifecycle: lifecycle, reader: reader}
	view := RequirePermission(ActionView, TenantResource)
	mux.Handle("GET /api/v1/dimensions/address/status", auth(view(http.HandlerFunc(api.status))))
	mux.Handle("GET /api/v1/dimensions/address/versions/{snapshot_id}/consumers", auth(view(http.HandlerFunc(api.list))))
}

func (api addressDimensionConsumerAPI) status(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	at := time.Now().UTC()
	if raw := strings.TrimSpace(r.URL.Query().Get("at")); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "at must be RFC3339", nil)
			return
		}
		at = parsed.UTC()
	}
	activation, err := api.lifecycle.GetAddressDimensionActivationAt(r.Context(), auth.TenantID, at)
	if err != nil {
		writeAddressDimensionConsumerError(w, err)
		return
	}
	snapshot, err := api.publisher.GetAddressDimensionSnapshot(r.Context(), auth.TenantID, activation.SnapshotID)
	if err != nil {
		writeAddressDimensionConsumerError(w, err)
		return
	}
	summary, err := api.reader.GetAddressDimensionConsumerSummary(r.Context(), auth.TenantID, activation.SnapshotID)
	if err != nil {
		writeAddressDimensionConsumerError(w, err)
		return
	}
	WriteAPIJSON(w, http.StatusOK, AddressDimensionRuntimeStatus{At: at, Activation: activation, Snapshot: snapshot, Consumers: summary})
}

func (api addressDimensionConsumerAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	filter := AddressDimensionConsumerFilter{
		Query: strings.TrimSpace(r.URL.Query().Get("q")), State: strings.TrimSpace(r.URL.Query().Get("state")),
		Drift: strings.TrimSpace(r.URL.Query().Get("drift")), Cursor: strings.TrimSpace(r.URL.Query().Get("cursor")),
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be a positive integer", nil)
			return
		}
		filter.Limit = limit
	}
	snapshotID := ID(r.PathValue("snapshot_id"))
	items, cursor, err := api.reader.ListAddressDimensionConsumers(r.Context(), auth.TenantID, snapshotID, filter)
	if err != nil {
		writeAddressDimensionConsumerError(w, err)
		return
	}
	summary, err := api.reader.GetAddressDimensionConsumerSummary(r.Context(), auth.TenantID, snapshotID)
	if err != nil {
		writeAddressDimensionConsumerError(w, err)
		return
	}
	if items == nil {
		items = []AddressDimensionConsumerStatus{}
	}
	response := map[string]any{"items": items, "summary": summary}
	if cursor != "" {
		response["next_cursor"] = cursor
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

func writeAddressDimensionConsumerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Address dimension activation or snapshot not found", nil)
	case errors.Is(err, ErrAddressDimensionInvalid):
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
	default:
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
	}
}
