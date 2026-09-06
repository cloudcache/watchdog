package watchdog

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

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
	query := r.URL.Query()
	// The server-driven table is opt-in: any of limit/offset/q/state/sort
	// switches to the paged path; without them the full-list behavior is kept.
	if query.Get("limit") != "" || query.Get("offset") != "" || query.Get("q") != "" ||
		query.Get("state") != "" || query.Get("sort") != "" || query.Get("order") != "" {
		api.listAllBGPSessionsPaged(w, r, auth, query)
		return
	}
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

// listAllBGPSessionsPaged is the server-driven variant: search/state/sort and
// offset paging are pushed into SQL over the session+device join (grant scoped),
// and the badge totals come from one grant-scoped aggregate.
func (api networkAPI) listAllBGPSessionsPaged(w http.ResponseWriter, r *http.Request, auth AuthContext, query url.Values) {
	q, err := parseBGPSessionQuery(query, "device")
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}

	all, allowedTargetIDs := visibleDeviceScope(auth)
	sessions, err := api.repo.ListAllBGPSessionsPage(r.Context(), auth.TenantID, all, allowedTargetIDs, q)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	// Devices give each session its display name / target; the grant scope is
	// already pushed into the page query, so this is just a name lookup.
	devices, err := api.repo.ListDevices(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	devicesByID := make(map[ID]NetworkDevice, len(devices))
	for _, device := range devices {
		devicesByID[device.ID] = device
	}
	items := make([]bgpSessionListItem, 0, len(sessions))
	for _, session := range sessions {
		device := devicesByID[session.DeviceID]
		items = append(items, bgpSessionListItem{BGPSession: session, DeviceSysName: device.SysName, TargetID: device.TargetID})
	}

	response := map[string]any{"items": items}
	if q.Offset == 0 {
		counts, err := api.repo.CountBGPSessions(r.Context(), auth.TenantID, all, allowedTargetIDs, q.Search)
		if err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		response["counts"] = counts
	}
	WriteAPIJSON(w, http.StatusOK, response)
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
	query := r.URL.Query()
	if query.Get("limit") != "" || query.Get("offset") != "" || query.Get("q") != "" ||
		query.Get("state") != "" || query.Get("sort") != "" || query.Get("order") != "" {
		q, err := parseBGPSessionQuery(query, "peer")
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		sessions, total, err := api.repo.ListDeviceBGPSessionsPage(r.Context(), auth.TenantID, device.ID, q)
		if err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		if sessions == nil {
			sessions = []BGPSession{}
		}
		response := map[string]any{"items": sessions, "total": total}
		if q.Offset == 0 {
			counts, err := api.repo.CountDeviceBGPSessions(r.Context(), auth.TenantID, device.ID)
			if err != nil {
				WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
				return
			}
			response["counts"] = counts
		}
		WriteAPIJSON(w, http.StatusOK, response)
		return
	}
	sessions, err := api.repo.ListBGPSessions(r.Context(), auth.TenantID, device.ID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": sessions})
}

func parseBGPSessionQuery(query url.Values, defaultSort string) (BGPSessionQuery, error) {
	q := BGPSessionQuery{
		Search: strings.TrimSpace(query.Get("q")),
		State:  strings.ToLower(strings.TrimSpace(query.Get("state"))),
		Sort:   strings.TrimSpace(query.Get("sort")),
	}
	if q.Sort == "" {
		q.Sort = defaultSort
	}
	if _, ok := bgpSortColumns[q.Sort]; !ok {
		return BGPSessionQuery{}, fmt.Errorf("invalid BGP sort")
	}
	if len(q.Search) > 256 {
		return BGPSessionQuery{}, fmt.Errorf("q must be at most 256 characters")
	}
	if q.State == "all" {
		q.State = ""
	}
	if len(q.State) > 32 {
		return BGPSessionQuery{}, fmt.Errorf("state must be at most 32 characters")
	}
	order := strings.ToLower(strings.TrimSpace(query.Get("order")))
	if order != "" && order != "asc" && order != "desc" {
		return BGPSessionQuery{}, fmt.Errorf("order must be asc or desc")
	}
	q.Desc = order == "desc"
	q.Limit = 100
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 500 {
			return BGPSessionQuery{}, fmt.Errorf("limit must be between 1 and 500")
		}
		q.Limit = limit
	}
	if raw := strings.TrimSpace(query.Get("offset")); raw != "" {
		offset, err := strconv.Atoi(raw)
		if err != nil || offset < 0 {
			return BGPSessionQuery{}, fmt.Errorf("offset must be a non-negative integer")
		}
		q.Offset = offset
	}
	return q, nil
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
