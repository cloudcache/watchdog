package watchdog

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// PLAT P0 quiet hours API. A user CRUDs only their own windows (scoped by the
// authenticated user id).

type quietHoursAPI struct {
	repo QuietHoursRepository
}

func registerQuietHoursRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo QuietHoursRepository) {
	api := quietHoursAPI{repo: repo}
	mux.Handle("GET /api/v1/me/quiet-hours", auth(http.HandlerFunc(api.list)))
	mux.Handle("POST /api/v1/me/quiet-hours", auth(http.HandlerFunc(api.create)))
	mux.Handle("PATCH /api/v1/me/quiet-hours/{window_id}", auth(http.HandlerFunc(api.update)))
	mux.Handle("DELETE /api/v1/me/quiet-hours/{window_id}", auth(http.HandlerFunc(api.delete)))
}

type quietHourRequest struct {
	SystemID string    `json:"system_id"`
	Type     string    `json:"type"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
}

func (api quietHoursAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	windows, err := api.repo.ListQuietHours(r.Context(), auth.TenantID, auth.UserID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if windows == nil {
		windows = []QuietHourWindow{}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": windows})
}

func (api quietHoursAPI) create(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	window, ok := decodeQuietHour(w, r, "")
	if !ok {
		return
	}
	created, err := api.repo.CreateQuietHour(r.Context(), auth.TenantID, auth.UserID, window)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api quietHoursAPI) update(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	window, ok := decodeQuietHour(w, r, ID(r.PathValue("window_id")))
	if !ok {
		return
	}
	updated, err := api.repo.UpdateQuietHour(r.Context(), auth.TenantID, auth.UserID, window)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Quiet hour window not found", nil)
			return
		}
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api quietHoursAPI) delete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if err := api.repo.DeleteQuietHour(r.Context(), auth.TenantID, auth.UserID, ID(r.PathValue("window_id"))); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Quiet hour window not found", nil)
			return
		}
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeQuietHour(w http.ResponseWriter, r *http.Request, id ID) (QuietHourWindow, bool) {
	var req quietHourRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "invalid quiet hour body", nil)
		return QuietHourWindow{}, false
	}
	window := QuietHourWindow{ID: id, SystemID: req.SystemID, Type: req.Type, Start: req.Start, End: req.End}
	if err := ValidateQuietHourWindow(window); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return QuietHourWindow{}, false
	}
	return window, true
}
