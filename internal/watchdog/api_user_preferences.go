package watchdog

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
)

// PLAT P0 user preferences API. A user reads and writes only their own
// preferences (scoped by the authenticated user id, not a path param). The
// row_version is exposed as a quoted ETag; PUT requires If-Match once a row
// exists so concurrent tabs cannot silently clobber each other.

type userPreferencesAPI struct {
	repo UserPreferencesRepository
}

func registerUserPreferencesRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo UserPreferencesRepository) {
	api := userPreferencesAPI{repo: repo}
	mux.Handle("GET /api/v1/me/preferences", auth(http.HandlerFunc(api.get)))
	mux.Handle("PUT /api/v1/me/preferences", auth(http.HandlerFunc(api.put)))
}

func (api userPreferencesAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	prefs, err := api.repo.GetUserPreferences(r.Context(), auth.TenantID, auth.UserID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(prefs.RowVersion))
	writeUserPreferences(w, prefs)
}

func (api userPreferencesAPI) put(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "failed to read request body", nil)
		return
	}
	// The stored value must be a JSON object; a scalar or array would break the
	// merge/read contract the frontend relies on.
	var probe map[string]any
	if err := json.Unmarshal(body, &probe); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "preferences body must be a JSON object", nil)
		return
	}
	// Compact the body so stored JSON is canonical and small.
	var compact bytes.Buffer
	if err := json.Compact(&compact, body); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "invalid JSON body", nil)
		return
	}

	expectedVersion, ok := parseUserPreferencesIfMatch(w, r)
	if !ok {
		return
	}
	updated, err := api.repo.UpsertUserPreferences(r.Context(), UserPreferences{
		UserID: auth.UserID, TenantID: auth.TenantID, Settings: compact.Bytes(),
	}, expectedVersion)
	if err != nil {
		if errors.Is(err, ErrUserPreferencesConflict) {
			WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"),
				"Preferences changed since they were read; re-read and retry", nil)
			return
		}
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(updated.RowVersion))
	writeUserPreferences(w, updated)
}

// parseUserPreferencesIfMatch returns the expected row version. A missing
// If-Match maps to 0 (create-or-first-write); a present one must be a quoted
// unsigned integer.
func parseUserPreferencesIfMatch(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	raw := r.Header.Get("If-Match")
	if raw == "" || raw == "*" {
		return 0, true
	}
	if len(raw) < 3 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "If-Match must be a quoted row_version", nil)
		return 0, false
	}
	version, err := strconv.ParseUint(raw[1:len(raw)-1], 10, 64)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "If-Match must be a quoted row_version", nil)
		return 0, false
	}
	return version, true
}

func writeUserPreferences(w http.ResponseWriter, prefs UserPreferences) {
	settings := prefs.Settings
	if len(settings) == 0 {
		settings = json.RawMessage("{}")
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{
		"settings":    json.RawMessage(settings),
		"row_version": prefs.RowVersion,
	})
}
