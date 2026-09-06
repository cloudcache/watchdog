package watchdog

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type networkPortListItem struct {
	NetworkPort
	Addresses []NetworkInterfaceAddress
}

func registerPortRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo NetworkRepository, portDeletePreview PortDeletePreviewRepository, operationJobs OperationJobRepository, cleaner SeriesCleaner) {
	api := networkAPI{repo: repo, portDeletePreview: portDeletePreview, operationJobs: operationJobs, seriesCleaner: cleaner}
	mux.Handle("GET /api/v1/network/devices/{device_id}/ports", auth(http.HandlerFunc(api.listPorts)))
	mux.Handle("GET /api/v1/network/ports/{port_id}", auth(http.HandlerFunc(api.getPort)))
	mux.Handle("PATCH /api/v1/network/ports/{port_id}", auth(http.HandlerFunc(api.patchPort)))
	mux.Handle("DELETE /api/v1/network/ports/{port_id}", auth(http.HandlerFunc(api.deletePort)))
	mux.Handle("GET /api/v1/network/ports/{port_id}/delete-preview", auth(http.HandlerFunc(api.previewPortDelete)))
	mux.Handle("GET /api/v1/network/ports/{port_id}/policy", auth(http.HandlerFunc(api.getPortPolicy)))
	mux.Handle("PATCH /api/v1/network/ports/{port_id}/policy", auth(http.HandlerFunc(api.patchPortPolicy)))
}

func (api networkAPI) listPorts(w http.ResponseWriter, r *http.Request) {
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
	values := r.URL.Query()
	if hasNetworkPortPageParams(values) {
		query, err := parseNetworkPortPage(values)
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		ports, total, err := api.repo.ListDevicePortsPage(r.Context(), auth.TenantID, device.ID, query)
		if err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		portIDs := make([]ID, 0, len(ports))
		for _, port := range ports {
			portIDs = append(portIDs, port.ID)
		}
		addresses, err := api.repo.ListInterfaceAddressesByPorts(r.Context(), auth.TenantID, portIDs)
		if err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		counts, err := api.repo.CountDevicePorts(r.Context(), auth.TenantID, device.ID)
		if err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		WriteAPIJSON(w, http.StatusOK, map[string]any{
			"items": joinNetworkPortAddresses(ports, addresses), "total": total, "counts": counts,
			"limit": query.Limit, "offset": query.Offset,
		})
		return
	}
	ports, err := api.repo.ListPorts(r.Context(), auth.TenantID, device.ID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	addresses, err := api.repo.ListInterfaceAddresses(r.Context(), auth.TenantID, device.ID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": joinNetworkPortAddresses(ports, addresses)})
}

func joinNetworkPortAddresses(ports []NetworkPort, addresses []NetworkInterfaceAddress) []networkPortListItem {
	byPort := make(map[ID][]NetworkInterfaceAddress, len(ports))
	for _, address := range addresses {
		byPort[address.PortID] = append(byPort[address.PortID], address)
	}
	items := make([]networkPortListItem, 0, len(ports))
	for _, port := range ports {
		items = append(items, networkPortListItem{NetworkPort: port, Addresses: byPort[port.ID]})
	}
	return items
}

func hasNetworkPortPageParams(values url.Values) bool {
	return values.Get("limit") != "" || values.Get("offset") != "" || values.Get("q") != "" ||
		values.Get("admin_status") != "" || values.Get("oper_status") != "" || values.Get("address_family") != "" ||
		values.Get("sort") != "" || values.Get("order") != ""
}

func parseNetworkPortPage(values url.Values) (NetworkPortQuery, error) {
	query := NetworkPortQuery{
		Search:        strings.TrimSpace(values.Get("q")),
		AdminStatus:   strings.ToLower(strings.TrimSpace(values.Get("admin_status"))),
		OperStatus:    strings.ToLower(strings.TrimSpace(values.Get("oper_status"))),
		AddressFamily: strings.ToLower(strings.TrimSpace(values.Get("address_family"))),
		Sort:          strings.TrimSpace(values.Get("sort")),
	}
	if len(query.Search) > 256 || len(query.AdminStatus) > 32 || len(query.OperStatus) > 32 {
		return NetworkPortQuery{}, fmt.Errorf("port filter is too long")
	}
	if query.AdminStatus == "all" {
		query.AdminStatus = ""
	}
	if query.OperStatus == "all" {
		query.OperStatus = ""
	}
	if query.AddressFamily == "all" {
		query.AddressFamily = ""
	}
	if query.AddressFamily != "" && query.AddressFamily != "ipv4" && query.AddressFamily != "ipv6" {
		return NetworkPortQuery{}, fmt.Errorf("address_family must be all, ipv4, or ipv6")
	}
	if query.Sort == "" {
		query.Sort = "if_index"
	}
	if _, ok := networkPortSortColumns[query.Sort]; !ok {
		return NetworkPortQuery{}, fmt.Errorf("invalid port sort")
	}
	order := strings.ToLower(strings.TrimSpace(values.Get("order")))
	if order != "" && order != "asc" && order != "desc" {
		return NetworkPortQuery{}, fmt.Errorf("order must be asc or desc")
	}
	query.Desc = order == "desc"
	var err error
	query.Limit, err = parseNetworkInventoryInteger(values.Get("limit"), 100, 1, 500)
	if err != nil {
		return NetworkPortQuery{}, fmt.Errorf("limit must be between 1 and 500")
	}
	query.Offset, err = parseNetworkInventoryInteger(values.Get("offset"), 0, 0, int(^uint(0)>>1))
	if err != nil {
		return NetworkPortQuery{}, fmt.Errorf("offset must be zero or greater")
	}
	return query, nil
}

func (api networkAPI) getPort(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	port, device, ok := api.authorizePort(w, r, ActionView)
	if !ok {
		return
	}
	var transceiver *NetworkPortTransceiver
	if value, err := api.repo.GetPortTransceiver(r.Context(), auth.TenantID, port.ID); err == nil {
		transceiver = &value
	} else if !errors.Is(err, sql.ErrNoRows) {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	SetEntityETag(w, port.UpdatedAt)
	WriteAPIJSON(w, http.StatusOK, map[string]any{"port": port, "device": device, "transceiver": transceiver})
}

func (api networkAPI) patchPort(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	port, _, ok := api.authorizePort(w, r, ActionConfigure)
	if !ok {
		return
	}
	if !CheckIfMatch(w, r, port.UpdatedAt) {
		return
	}
	updated, err := decodeNetworkPortRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	updated.ID = port.ID
	updated.TenantID = auth.TenantID
	updated.DeviceID = port.DeviceID
	if updated.IfIndex == 0 {
		updated.IfIndex = port.IfIndex
	}
	if err := api.repo.UpsertPorts(r.Context(), []NetworkPort{updated}); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	saved, err := api.repo.GetPort(r.Context(), auth.TenantID, port.ID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, saved)
}

func (api networkAPI) deletePort(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	port, _, ok := api.authorizePort(w, r, ActionConfigure)
	if !ok {
		return
	}
	if !CheckIfMatch(w, r, port.UpdatedAt) {
		return
	}
	if api.operationJobs != nil {
		impact := map[string]int{}
		if api.portDeletePreview != nil {
			if preview, previewErr := api.portDeletePreview.PreviewPortDelete(r.Context(), auth.TenantID, port.ID); previewErr == nil {
				for _, item := range preview.Impacts {
					if item.Behavior == "deleted" && item.Count > 0 {
						impact[item.ResourceType] = item.Count
					}
				}
			}
		}
		payload, err := EncodePortDeletePayload(port.ID, impact)
		if err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		digest := sha256.Sum256([]byte("port_delete:" + string(port.ID)))
		job, err := api.operationJobs.EnqueueOperationJob(r.Context(), OperationJob{
			TenantID:       auth.TenantID,
			JobType:        PortDeleteJobType,
			IdempotencyKey: "port_delete:" + string(port.ID),
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
		return
	}
	if err := api.repo.DeletePort(r.Context(), auth.TenantID, port.ID); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if api.seriesCleaner != nil {
		_ = api.seriesCleaner.DeleteSeries(r.Context(), []string{`{port_id="` + string(port.ID) + `"}`})
	}
	w.WriteHeader(http.StatusNoContent)
}

func (api networkAPI) previewPortDelete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	port, _, ok := api.authorizePort(w, r, ActionConfigure)
	if !ok {
		return
	}
	if api.portDeletePreview == nil {
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Delete preview is not available", nil)
		return
	}
	preview, err := api.portDeletePreview.PreviewPortDelete(r.Context(), auth.TenantID, port.ID)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Network port not found", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, preview)
}

func (api networkAPI) getPortPolicy(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	port, _, ok := api.authorizePort(w, r, ActionView)
	if !ok {
		return
	}
	policy, err := api.repo.GetPortPolicy(r.Context(), auth.TenantID, port.ID)
	if errors.Is(err, sql.ErrNoRows) {
		policy, err = api.defaultPolicyForPort(r, auth.TenantID, port)
	}
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, policy)
}

func (api networkAPI) patchPortPolicy(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	port, _, ok := api.authorizePort(w, r, ActionConfigure)
	if !ok {
		return
	}
	policy, err := decodePortPolicyRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	policy.TenantID = auth.TenantID
	policy.PortID = port.ID
	updated, err := api.repo.UpsertPortPolicy(r.Context(), policy)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api networkAPI) defaultPolicyForPort(r *http.Request, tenantID ID, port NetworkPort) (PortPolicy, error) {
	defaults, err := api.repo.GetTrafficPolicyDefaults(r.Context(), tenantID)
	if err != nil {
		return PortPolicy{}, err
	}
	policy := DefaultPortPolicyWithDefaults(tenantID, port.ID, portSideFromMetadata(port), defaults)
	policy.ID = stableID("policy", string(tenantID), string(port.ID))
	return policy, nil
}

func (api networkAPI) authorizePort(w http.ResponseWriter, r *http.Request, action Action) (NetworkPort, NetworkDevice, bool) {
	auth, _ := AuthFromContext(r.Context())
	port, err := api.repo.GetPort(r.Context(), auth.TenantID, ID(r.PathValue("port_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Network port not found", nil)
		return NetworkPort{}, NetworkDevice{}, false
	}
	device, err := api.repo.GetDevice(r.Context(), auth.TenantID, port.DeviceID)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Network device not found", nil)
		return NetworkPort{}, NetworkDevice{}, false
	}
	if auth.IsAdmin || HasPermission(AccessRequest{
		TenantID: auth.TenantID,
		UserID:   auth.UserID,
		RoleIDs:  auth.RoleIDs,
		Action:   action,
		Resource: ResourceRef{Type: ResourcePort, ID: port.ID, ParentID: device.TargetID},
	}, auth.Grants) {
		return port, device, true
	}
	WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
	return NetworkPort{}, NetworkDevice{}, false
}

func decodeNetworkPortRequest(r *http.Request) (NetworkPort, error) {
	defer r.Body.Close()
	var port NetworkPort
	if err := json.NewDecoder(r.Body).Decode(&port); err != nil {
		return NetworkPort{}, err
	}
	if port.IfName == "" && port.IfDescr == "" {
		return NetworkPort{}, errors.New("ifName or ifDescr is required")
	}
	if port.Metadata == nil {
		port.Metadata = map[string]string{}
	}
	return port, nil
}

func decodePortPolicyRequest(r *http.Request) (PortPolicy, error) {
	defer r.Body.Close()
	var policy PortPolicy
	if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
		return PortPolicy{}, err
	}
	return policy, nil
}
