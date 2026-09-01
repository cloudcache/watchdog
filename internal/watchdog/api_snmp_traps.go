package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

type trapAPI struct {
	network   NetworkRepository
	collector SNMPCollectorRepository
	dispatcher func(ctx context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error)
	discoveryJobs DiscoveryJobRepository
}

func registerTrapRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, network NetworkRepository, collector SNMPCollectorRepository, dispatcher func(ctx context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error), jobs DiscoveryJobRepository) {
	api := trapAPI{network: network, collector: collector, dispatcher: dispatcher, discoveryJobs: jobs}
	mux.Handle("POST /api/v1/snmp/traps", auth(http.HandlerFunc(api.receiveTrap)))
}

func (api trapAPI) receiveTrap(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if api.dispatcher == nil {
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "SNMP trap dispatcher is not configured", nil)
		return
	}
	defer r.Body.Close()
	var req struct {
		SourceIP string             `json:"source_ip"`
		Hostname string             `json:"hostname"`
		TrapOID  string             `json:"trap_oid"`
		Uptime   uint64             `json:"uptime"`
		VarBinds []SNMPTrapVarBind  `json:"varbinds"`
		RawText  string             `json:"raw_text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if req.TrapOID == "" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "trap_oid is required", nil)
		return
	}
	device, err := api.findDeviceBySource(r.Context(), auth.TenantID, req.SourceIP, req.Hostname)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "device not found for trap source", nil)
		return
	}
	trap := SNMPTrap{
		SourceIP:   req.SourceIP,
		Hostname:   req.Hostname,
		TrapOID:    req.TrapOID,
		Uptime:     req.Uptime,
		VarBinds:   req.VarBinds,
		RawText:    req.RawText,
		ReceivedAt: time.Now().UTC(),
	}
	result, err := api.dispatcher(r.Context(), device, trap)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	api.persistTrapResult(r.Context(), device, result)
	WriteAPIJSON(w, http.StatusOK, map[string]any{
		"events": len(result.Events), "port_updates": len(result.PortUpdates),
		"bgp_updates": len(result.BGPUpdates), "immediate_poll": len(result.ImmediatePollRecipe),
		"rediscover": result.RediscoverDevice,
	})
}

func (api trapAPI) findDeviceBySource(ctx context.Context, tenantID ID, sourceIP, hostname string) (NetworkDevice, error) {
	devices, err := api.network.ListDevices(ctx, tenantID)
	if err != nil {
		return NetworkDevice{}, err
	}
	for _, device := range devices {
		if sourceIP != "" && device.SysName == hostname {
			return device, nil
		}
		if sourceIP != "" && (device.SysName == sourceIP || device.SysName == hostname) {
			return device, nil
		}
	}
	if len(devices) == 1 {
		return devices[0], nil
	}
	return NetworkDevice{}, errors.New("device not found")
}

func (api trapAPI) persistTrapResult(ctx context.Context, device NetworkDevice, result SNMPTrapHandleResult) {
	for _, event := range result.Events {
		_ = api.collector.CreateSNMPEvent(ctx, event)
	}
	if len(result.PortUpdates) > 0 {
		_ = api.network.UpsertPorts(ctx, result.PortUpdates)
	}
	if len(result.BGPUpdates) > 0 {
		_ = api.network.UpsertBGPSessions(ctx, result.BGPUpdates)
	}
	if result.RediscoverDevice && api.discoveryJobs != nil {
		_ = api.discoveryJobs.EnqueueDiscoveryJob(ctx, device.TenantID, device.ID, "trap_rediscover")
	}
}
