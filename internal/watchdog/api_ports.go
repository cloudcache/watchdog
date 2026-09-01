package watchdog

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
)

func registerPortRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo NetworkRepository) {
	api := networkAPI{repo: repo}
	mux.Handle("GET /api/v1/network/devices/{device_id}/ports", auth(http.HandlerFunc(api.listPorts)))
	mux.Handle("GET /api/v1/network/ports/{port_id}", auth(http.HandlerFunc(api.getPort)))
	mux.Handle("PATCH /api/v1/network/ports/{port_id}", auth(http.HandlerFunc(api.patchPort)))
	mux.Handle("DELETE /api/v1/network/ports/{port_id}", auth(http.HandlerFunc(api.deletePort)))
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
	ports, err := api.repo.ListPorts(r.Context(), auth.TenantID, device.ID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": ports})
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
	WriteAPIJSON(w, http.StatusOK, map[string]any{"port": port, "device": device, "transceiver": transceiver})
}

func (api networkAPI) patchPort(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	port, _, ok := api.authorizePort(w, r, ActionConfigure)
	if !ok {
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
	if err := api.repo.DeletePort(r.Context(), auth.TenantID, port.ID); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
