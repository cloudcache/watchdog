package watchdog

import (
	"net/http"
)

type graphAPI struct {
	network NetworkRepository
}

func registerGraphRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, network NetworkRepository) {
	api := graphAPI{network: network}
	mux.Handle("GET /api/v1/graph/devices/{device_id}/overview", auth(http.HandlerFunc(api.deviceOverview)))
	mux.Handle("GET /api/v1/graph/ports/{port_id}/overview", auth(http.HandlerFunc(api.portOverview)))
}

func (api graphAPI) deviceOverview(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	device, err := api.network.GetDevice(r.Context(), auth.TenantID, ID(r.PathValue("device_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Network device not found", nil)
		return
	}
	if !canAccessTarget(auth, device.TargetID, ActionView) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	ports, err := api.network.ListPorts(r.Context(), auth.TenantID, device.ID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	bgpSessions, _ := api.network.ListBGPSessions(r.Context(), auth.TenantID, device.ID)
	sensors, _ := api.network.ListDeviceSensors(r.Context(), auth.TenantID, device.ID)
	WriteAPIJSON(w, http.StatusOK, NewNetworkDeviceOverviewDashboard(device, ports, bgpSessions, sensors))
}

func (api graphAPI) portOverview(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	port, err := api.network.GetPort(r.Context(), auth.TenantID, ID(r.PathValue("port_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Network port not found", nil)
		return
	}
	device, err := api.network.GetDevice(r.Context(), auth.TenantID, port.DeviceID)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Network device not found", nil)
		return
	}
	if !canAccessTarget(auth, device.TargetID, ActionView) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, NewNetworkPortOverviewDashboard(port))
}
