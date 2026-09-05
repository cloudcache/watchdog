package watchdog

import (
	"encoding/json"
	"net/http"
)

// PLAT P0 notification channels API. A user reads and replaces only their own
// channels (scoped by the authenticated user id). PUT replaces the whole set
// to match the frontend's emails[]/webhooks[] editor.

type notificationChannelAPI struct {
	repo NotificationChannelRepository
}

func registerNotificationChannelRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo NotificationChannelRepository) {
	api := notificationChannelAPI{repo: repo}
	mux.Handle("GET /api/v1/me/notification-channels", auth(http.HandlerFunc(api.get)))
	mux.Handle("PUT /api/v1/me/notification-channels", auth(http.HandlerFunc(api.put)))
}

func (api notificationChannelAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	channels, err := api.repo.GetNotificationChannels(r.Context(), auth.TenantID, auth.UserID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, channels)
}

func (api notificationChannelAPI) put(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	var body NotificationChannels
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<18)).Decode(&body); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "body must be {emails:[],webhooks:[]}", nil)
		return
	}
	channels, err := ValidateNotificationChannels(body)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := api.repo.ReplaceNotificationChannels(r.Context(), auth.TenantID, auth.UserID, channels); err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, channels)
}
