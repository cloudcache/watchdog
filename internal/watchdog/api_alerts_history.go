package watchdog

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

// PLAT P0 alert history API. A user lists and deletes only their own history
// (scoped by the authenticated tenant + user). Newest first, keyset paginated.

type alertsHistoryAPI struct {
	repo AlertHistoryRepository
}

func registerAlertsHistoryRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo AlertHistoryRepository) {
	api := alertsHistoryAPI{repo: repo}
	mux.Handle("GET /api/v1/me/alerts-history", auth(http.HandlerFunc(api.list)))
	mux.Handle("DELETE /api/v1/me/alerts-history/{id}", auth(http.HandlerFunc(api.delete)))
}

func (api alertsHistoryAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	query := r.URL.Query()
	filter := AlertHistoryPageFilter{Cursor: strings.TrimSpace(query.Get("cursor"))}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be a positive integer", nil)
			return
		}
		filter.Limit = limit
	}
	entries, nextCursor, err := api.repo.ListAlertHistory(r.Context(), auth.TenantID, auth.UserID, filter)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if entries == nil {
		entries = []AlertHistoryEntry{}
	}
	response := map[string]any{"items": entries}
	if nextCursor != "" {
		response["next_cursor"] = nextCursor
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

func (api alertsHistoryAPI) delete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if err := api.repo.DeleteAlertHistory(r.Context(), auth.TenantID, auth.UserID, ID(r.PathValue("id"))); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Alert history record not found", nil)
			return
		}
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
