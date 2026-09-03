package watchdog

import "net/http"

func registerBGPRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo NetworkRepository) {
	api := networkAPI{repo: repo}
	mux.Handle("GET /api/v1/network/devices/{device_id}/bgp", auth(http.HandlerFunc(api.listBGPSessions)))
	mux.Handle("GET /api/v1/network/bgp", auth(http.HandlerFunc(api.listAllBGPSessions)))
	mux.Handle("GET /api/v1/network/bgp/{session_id}", auth(http.HandlerFunc(api.getBGPSession)))
}

type bgpSessionListItem struct {
	BGPSession
	DeviceSysName string
	TargetID      ID
}

func (api networkAPI) listAllBGPSessions(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	devices, err := api.repo.ListDevices(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	accessible := make(map[ID]NetworkDevice, len(devices))
	for _, device := range devices {
		if canAccessTarget(auth, device.TargetID, ActionView) {
			accessible[device.ID] = device
		}
	}
	sessions, err := api.repo.ListAllBGPSessions(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	items := make([]bgpSessionListItem, 0, len(sessions))
	for _, session := range sessions {
		device, ok := accessible[session.DeviceID]
		if !ok {
			continue
		}
		items = append(items, bgpSessionListItem{BGPSession: session, DeviceSysName: device.SysName, TargetID: device.TargetID})
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (api networkAPI) listBGPSessions(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	device, err := api.repo.GetDevice(r.Context(), auth.TenantID, ID(r.PathValue("device_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Network device not found", nil)
		return
	}
	if !canAccessTarget(auth, device.TargetID, ActionView) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	sessions, err := api.repo.ListBGPSessions(r.Context(), auth.TenantID, device.ID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": sessions})
}

func (api networkAPI) getBGPSession(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	session, err := api.repo.GetBGPSession(r.Context(), auth.TenantID, ID(r.PathValue("session_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "BGP session not found", nil)
		return
	}
	device, err := api.repo.GetDevice(r.Context(), auth.TenantID, session.DeviceID)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Network device not found", nil)
		return
	}
	if !canAccessTarget(auth, device.TargetID, ActionView) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, session)
}
