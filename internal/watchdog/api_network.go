package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type networkAPI struct {
	repo          NetworkRepository
	targets       TargetRepository
	agents        AgentRepository
	snmp          SNMPRepository
	discovery     SNMPDeviceDiscoverer
	collector     SNMPCollectorRepository
	seriesCleaner SeriesCleaner
	discoveryJobs DiscoveryJobRepository
	audit         AuditRepository
}

type networkDeviceSummary struct {
	Device         NetworkDevice
	Target         Target
	Agent          *SNMPAgentConfig `json:",omitempty"`
	PortCount      int
	UpPorts        int
	DownPorts      int
	BGPSessions    int
	EstablishedBGP int
	LastSeen       *time.Time `json:",omitempty"`
}

type networkDeviceInventoryUpdater interface {
	UpdateDeviceInventory(ctx context.Context, device NetworkDevice) (NetworkDevice, error)
}

func registerNetworkRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo NetworkRepository, targets TargetRepository, agents AgentRepository, snmp SNMPRepository, discovery SNMPDeviceDiscoverer, collector SNMPCollectorRepository, cleaner SeriesCleaner, jobs DiscoveryJobRepository, audit AuditRepository) {
	api := networkAPI{repo: repo, targets: targets, agents: agents, snmp: snmp, discovery: discovery, collector: collector, seriesCleaner: cleaner, discoveryJobs: jobs, audit: audit}
	configureTenant := RequirePermission(ActionConfigure, TenantResource)
	mux.Handle("GET /api/v1/network/devices", auth(http.HandlerFunc(api.listDevices)))
	mux.Handle("GET /api/v1/network/devices/summary", auth(http.HandlerFunc(api.listDeviceSummaries)))
	mux.Handle("POST /api/v1/network/devices", auth(http.HandlerFunc(api.createDevice)))
	mux.Handle("GET /api/v1/network/devices/{device_id}", auth(http.HandlerFunc(api.getDevice)))
	mux.Handle("GET /api/v1/network/devices/{device_id}/sensors", auth(http.HandlerFunc(api.listDeviceSensors)))
	mux.Handle("GET /api/v1/network/devices/{device_id}/inventory", auth(http.HandlerFunc(api.listDeviceInventory)))
	mux.Handle("GET /api/v1/network/devices/{device_id}/vlans", auth(http.HandlerFunc(api.listDeviceVLANs)))
	mux.Handle("GET /api/v1/network/devices/{device_id}/lags", auth(http.HandlerFunc(api.listDeviceLAGGroups)))
	mux.Handle("PATCH /api/v1/network/devices/{device_id}", auth(http.HandlerFunc(api.patchDevice)))
	mux.Handle("DELETE /api/v1/network/devices/{device_id}", auth(http.HandlerFunc(api.deleteDevice)))
	mux.Handle("PATCH /api/v1/network/devices/{device_id}/snmp", auth(http.HandlerFunc(api.patchDeviceSNMP)))
	mux.Handle("POST /api/v1/network/devices/{device_id}/snmp/discover", auth(http.HandlerFunc(api.discoverDeviceSNMP)))
	mux.Handle("GET /api/v1/network/devices/{device_id}/events", auth(http.HandlerFunc(api.listDeviceEvents)))
	mux.Handle("GET /api/v1/network/traffic-policy-defaults", auth(configureTenant(http.HandlerFunc(api.getTrafficPolicyDefaults))))
	mux.Handle("PUT /api/v1/network/traffic-policy-defaults", auth(configureTenant(http.HandlerFunc(api.putTrafficPolicyDefaults))))
}

func (api networkAPI) listDevices(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	devices, err := api.repo.ListDevices(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	visible := make([]NetworkDevice, 0, len(devices))
	for _, device := range devices {
		if canAccessTarget(auth, device.TargetID, ActionView) {
			visible = append(visible, device)
		}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": visible})
}

func (api networkAPI) listDeviceSummaries(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	devices, err := api.repo.ListDevices(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	targetsByID := map[ID]Target{}
	devicesByTargetID := map[ID]NetworkDevice{}
	agentsByTargetID := map[ID]SNMPAgentConfig{}
	for _, device := range devices {
		devicesByTargetID[device.TargetID] = device
	}
	if api.targets != nil {
		targets, err := api.targets.ListTargets(r.Context(), auth.TenantID)
		if err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		for _, target := range targets {
			targetsByID[target.ID] = target
		}
	}
	if api.agents != nil {
		agents, err := api.agents.ListAgents(r.Context(), auth.TenantID)
		if err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		for _, agent := range agents {
			if existing, ok := agentsByTargetID[agent.TargetID]; !ok || agent.UpdatedAt.After(existing.UpdatedAt) {
				agentsByTargetID[agent.TargetID] = agent
			}
		}
	}
	items := make([]networkDeviceSummary, 0, max(len(targetsByID), len(devices)))
	for _, target := range targetsByID {
		device, hasDevice := devicesByTargetID[target.ID]
		if target.Kind != TargetKindNetwork && !hasDevice {
			continue
		}
		if !canAccessTarget(auth, target.ID, ActionView) {
			continue
		}
		summary := networkDeviceSummary{Target: target}
		if agent, ok := agentsByTargetID[target.ID]; ok {
			summary.Agent = &agent
		}
		if hasDevice {
			if err := api.fillDeviceSummary(r.Context(), auth, &summary, device); err != nil {
				WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
				return
			}
		}
		items = append(items, summary)
	}
	if len(targetsByID) > 0 {
		WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items})
		return
	}
	for _, device := range devices {
		if !canAccessTarget(auth, device.TargetID, ActionView) {
			continue
		}
		summary := networkDeviceSummary{Target: targetsByID[device.TargetID]}
		if agent, ok := agentsByTargetID[device.TargetID]; ok {
			summary.Agent = &agent
		}
		if err := api.fillDeviceSummary(r.Context(), auth, &summary, device); err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		items = append(items, summary)
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (api networkAPI) fillDeviceSummary(ctx context.Context, auth AuthContext, summary *networkDeviceSummary, device NetworkDevice) error {
	ports, err := api.repo.ListPorts(ctx, auth.TenantID, device.ID)
	if err != nil {
		return err
	}
	bgp, err := api.repo.ListBGPSessions(ctx, auth.TenantID, device.ID)
	if err != nil {
		return err
	}
	summary.Device = device
	summary.PortCount = len(ports)
	summary.BGPSessions = len(bgp)
	for _, port := range ports {
		switch port.OperStatus {
		case "up":
			summary.UpPorts++
		case "down":
			summary.DownPorts++
		}
	}
	for _, session := range bgp {
		if session.State == "established" {
			summary.EstablishedBGP++
		}
	}
	if api.collector != nil {
		if lastSeen, err := api.collector.GetSNMPDeviceLastPolledAt(ctx, auth.TenantID, device.ID); err == nil && !lastSeen.IsZero() {
			summary.LastSeen = &lastSeen
		}
	}
	return nil
}

func (api networkAPI) getDevice(w http.ResponseWriter, r *http.Request) {
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
	SetEntityETag(w, device.UpdatedAt)
	WriteAPIJSON(w, http.StatusOK, device)
}

func (api networkAPI) createDevice(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	device, err := decodeNetworkDeviceRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	device.TenantID = auth.TenantID
	if !canAccessTarget(auth, device.TargetID, ActionConfigure) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	if api.targets != nil {
		if _, err := api.targets.GetTarget(r.Context(), auth.TenantID, device.TargetID); err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "target_id does not exist", nil)
			return
		}
	}
	if api.snmp != nil && device.SNMPProfileID != "" {
		if _, err := api.snmp.GetSNMPProfile(r.Context(), auth.TenantID, device.SNMPProfileID); err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "snmp_profile_id does not exist", nil)
			return
		}
	}
	existing, err := api.repo.ListDevices(r.Context(), auth.TenantID)
	if err == nil {
		for _, dev := range existing {
			if dev.ID == device.ID {
				WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "device already exists; use PATCH to update", nil)
				return
			}
		}
	}
	created, err := api.repo.UpsertDevice(r.Context(), device)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	api.auditLog(r.Context(), auth, "create", ResourceTarget, created.TargetID, "device_created", created.ID)
	if api.discoveryJobs != nil {
		_ = api.discoveryJobs.EnqueueDiscoveryJob(r.Context(), auth.TenantID, created.ID, "device_created")
	}
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api networkAPI) auditLog(ctx context.Context, auth AuthContext, action string, rt ResourceType, rid ID, detail string, entityID ID) {
	if api.audit == nil {
		return
	}
	_ = api.audit.CreateAuditLog(ctx, AuditLog{
		ID:           collectorStableID("audit", string(auth.TenantID), action, string(rt), string(rid), detail, time.Now().UTC().String()),
		TenantID:     auth.TenantID,
		ActorID:      auth.UserID,
		Action:       action,
		ResourceType: rt,
		ResourceID:   rid,
		Detail:       map[string]any{"detail": detail, "entity_id": entityID},
	})
}

func (api networkAPI) listDeviceSensors(w http.ResponseWriter, r *http.Request) {
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
	sensors, err := api.repo.ListDeviceSensors(r.Context(), auth.TenantID, device.ID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": sensors})
}

func (api networkAPI) listDeviceInventory(w http.ResponseWriter, r *http.Request) {
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
	entities, err := api.repo.ListDevicePhysicalEntities(r.Context(), auth.TenantID, device.ID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": entities})
}

func (api networkAPI) listDeviceVLANs(w http.ResponseWriter, r *http.Request) {
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
	vlans, err := api.repo.ListDeviceVLANs(r.Context(), auth.TenantID, device.ID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": vlans})
}

func (api networkAPI) listDeviceLAGGroups(w http.ResponseWriter, r *http.Request) {
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
	groups, err := api.repo.ListDeviceLAGGroups(r.Context(), auth.TenantID, device.ID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": groups})
}

func (api networkAPI) patchDevice(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	existing, err := api.repo.GetDevice(r.Context(), auth.TenantID, ID(r.PathValue("device_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Network device not found", nil)
		return
	}
	if !canAccessTarget(auth, existing.TargetID, ActionConfigure) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	if !CheckIfMatch(w, r, existing.UpdatedAt) {
		return
	}
	device, err := decodeNetworkDeviceRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	device.ID = existing.ID
	device.TenantID = auth.TenantID
	if device.TargetID != existing.TargetID && !canAccessTarget(auth, device.TargetID, ActionConfigure) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	device.SNMPProfileID = existing.SNMPProfileID
	device.SNMPPort = existing.SNMPPort
	device.SNMPSecurity = existing.SNMPSecurity
	var updated NetworkDevice
	if updater, ok := api.repo.(networkDeviceInventoryUpdater); ok {
		updated, err = updater.UpdateDeviceInventory(r.Context(), device)
	} else {
		updated, err = api.repo.UpsertDevice(r.Context(), device)
	}
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if api.discoveryJobs != nil && device.TargetID != existing.TargetID {
		_ = api.discoveryJobs.EnqueueDiscoveryJob(r.Context(), auth.TenantID, updated.ID, "target_changed")
	}
	api.auditLog(r.Context(), auth, "update", ResourceTarget, updated.TargetID, "device_patched", updated.ID)
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api networkAPI) deleteDevice(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	existing, err := api.repo.GetDevice(r.Context(), auth.TenantID, ID(r.PathValue("device_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Network device not found", nil)
		return
	}
	if !canAccessTarget(auth, existing.TargetID, ActionConfigure) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	if !CheckIfMatch(w, r, existing.UpdatedAt) {
		return
	}
	if err := api.repo.DeleteDevice(r.Context(), auth.TenantID, existing.ID); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	api.auditLog(r.Context(), auth, "delete", ResourceTarget, existing.TargetID, "device_deleted", existing.ID)
	if api.seriesCleaner != nil {
		_ = api.seriesCleaner.DeleteSeries(r.Context(), []string{
			`{device_id="` + string(existing.ID) + `"}`,
		})
	}
	w.WriteHeader(http.StatusNoContent)
}

func (api networkAPI) patchDeviceSNMP(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	device, err := api.repo.GetDevice(r.Context(), auth.TenantID, ID(r.PathValue("device_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Network device not found", nil)
		return
	}
	if !canAccessTarget(auth, device.TargetID, ActionConfigure) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	defer r.Body.Close()
	var req struct {
		SNMPProfileID ID
		SNMPPort      uint16
		SNMPSecurity  map[string]string
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if req.SNMPProfileID != "" {
		device.SNMPProfileID = req.SNMPProfileID
	}
	if req.SNMPPort != 0 {
		device.SNMPPort = req.SNMPPort
	}
	if req.SNMPSecurity != nil {
		device.SNMPSecurity = req.SNMPSecurity
	}
	updated, err := api.repo.UpsertDevice(r.Context(), device)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if api.discoveryJobs != nil {
		if err := api.discoveryJobs.EnqueueDiscoveryJob(r.Context(), auth.TenantID, updated.ID, "snmp_credentials_changed"); err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, "SNMP settings were saved but discovery could not be queued", nil)
			return
		}
		_ = updateNetworkTargetStatus(r.Context(), api.targets, auth.TenantID, updated.TargetID, "pending")
	}
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api networkAPI) listDeviceEvents(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if api.collector == nil {
		WriteAPIJSON(w, http.StatusOK, map[string]any{"items": []SNMPEvent{}})
		return
	}
	device, err := api.repo.GetDevice(r.Context(), auth.TenantID, ID(r.PathValue("device_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Network device not found", nil)
		return
	}
	if !canAccessTarget(auth, device.TargetID, ActionView) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	limit := parsePositiveInt(r.URL.Query().Get("limit"))
	events, err := api.collector.ListSNMPEvents(r.Context(), auth.TenantID, device.ID, limit)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if events == nil {
		events = []SNMPEvent{}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": events})
}

func (api networkAPI) discoverDeviceSNMP(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	if api.targets == nil || api.snmp == nil || api.discovery == nil || api.collector == nil {
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "SNMP discovery is not configured", nil)
		return
	}
	device, err := api.repo.GetDevice(r.Context(), auth.TenantID, ID(r.PathValue("device_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Network device not found", nil)
		return
	}
	if !canAccessTarget(auth, device.TargetID, ActionConfigure) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Permission denied", nil)
		return
	}
	target, err := api.targets.GetTarget(r.Context(), auth.TenantID, device.TargetID)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Target not found", nil)
		return
	}
	profile, err := api.snmp.GetSNMPProfile(r.Context(), auth.TenantID, device.SNMPProfileID)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "SNMP profile is required before discovery", nil)
		return
	}
	profile = ApplyDeviceSNMPOverrides(profile, device)
	_ = updateNetworkTargetStatus(r.Context(), api.targets, auth.TenantID, device.TargetID, "pending")
	result, err := api.discovery.Discover(r.Context(), SNMPDiscoveryEngineRequest{
		TenantID: auth.TenantID,
		TargetID: device.TargetID,
		Target:   SNMPCollectorTarget{Host: target.Host, Port: normalizeSNMPPort(device.SNMPPort)},
		Device:   device,
		Profile:  profile,
	})
	if err != nil {
		_ = updateNetworkTargetStatus(r.Context(), api.targets, auth.TenantID, device.TargetID, "down")
		_ = recordSNMPDiscoveryFailure(r.Context(), api.collector, auth.TenantID, device.ID, err.Error())
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	deleted := 0
	if len(result.Ports) > 0 {
		existingPorts, err := api.repo.ListPorts(r.Context(), auth.TenantID, device.ID)
		if err != nil {
			WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
			return
		}
		discoveredIndexes := make(map[uint64]struct{}, len(result.Ports))
		for _, port := range result.Ports {
			discoveredIndexes[port.IfIndex] = struct{}{}
		}
		for _, port := range existingPorts {
			if _, ok := discoveredIndexes[port.IfIndex]; ok {
				continue
			}
			if err := api.repo.DeletePort(r.Context(), auth.TenantID, port.ID); err != nil {
				WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
				return
			}
			deleted++
		}
	}
	report, err := ImportSNMPCollectorDiscoveryResult(r.Context(), api.repo, api.collector, auth.TenantID, device, result)
	if err != nil {
		_ = updateNetworkTargetStatus(r.Context(), api.targets, auth.TenantID, device.TargetID, "down")
		_ = recordSNMPDiscoveryFailure(r.Context(), api.collector, auth.TenantID, device.ID, err.Error())
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	_ = updateNetworkTargetStatus(r.Context(), api.targets, auth.TenantID, device.TargetID, "up")
	_ = promoteDiscoveredTargetName(r.Context(), api.targets, auth.TenantID, device.TargetID, report.Device.SysName)
	WriteAPIJSON(w, http.StatusOK, map[string]any{
		"ports": report.Ports, "sensors": report.Sensors, "count": report.Ports, "deleted": deleted,
		"vlans": report.VLANs, "entities": report.PhysicalEntities, "lags": report.LAGs,
		"interface_addresses": report.InterfaceAddresses,
		"bgp_sessions":        report.BGPSessions, "recipes": report.Recipes,
		"events": report.Events, "modules": report.DeviceModules,
	})
}

func prepareDeviceSensors(tenantID, deviceID ID, sensors []NetworkDeviceSensor) []NetworkDeviceSensor {
	prepared := make([]NetworkDeviceSensor, 0, len(sensors))
	for _, sensor := range sensors {
		sensor.TenantID = tenantID
		sensor.DeviceID = deviceID
		if sensor.Class == "" || sensor.Name == "" {
			continue
		}
		if sensor.Status == "" {
			sensor.Status = "unknown"
		}
		if sensor.Metadata == nil {
			sensor.Metadata = map[string]string{}
		}
		if sensor.ID == "" {
			sensor.ID = stableID("sensor", string(deviceID), sensor.Class, fmt.Sprint(sensor.SensorIndex), sensor.Name)
		}
		prepared = append(prepared, sensor)
	}
	return prepared
}

func decodeNetworkDeviceRequest(r *http.Request) (NetworkDevice, error) {
	defer r.Body.Close()
	var device NetworkDevice
	if err := json.NewDecoder(r.Body).Decode(&device); err != nil {
		return NetworkDevice{}, err
	}
	if device.ID == "" {
		return NetworkDevice{}, errors.New("network device id is required")
	}
	if device.TargetID == "" {
		return NetworkDevice{}, errors.New("target id is required")
	}
	return device, nil
}

func canAccessTarget(auth AuthContext, targetID ID, action Action) bool {
	if auth.IsAdmin {
		return true
	}
	return HasPermission(AccessRequest{
		TenantID: auth.TenantID,
		UserID:   auth.UserID,
		RoleIDs:  auth.RoleIDs,
		Action:   action,
		Resource: ResourceRef{Type: ResourceTarget, ID: targetID},
	}, auth.Grants)
}

func (api networkAPI) getTrafficPolicyDefaults(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	defaults, err := api.repo.GetTrafficPolicyDefaults(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, defaults)
}

func (api networkAPI) putTrafficPolicyDefaults(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	defer r.Body.Close()
	var defaults TrafficPolicyDefaults
	if err := json.NewDecoder(r.Body).Decode(&defaults); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	defaults.Provider = prepareTrafficPolicyDefault(auth.TenantID, PortSideProvider, defaults.Provider)
	defaults.Customer = prepareTrafficPolicyDefault(auth.TenantID, PortSideCustomer, defaults.Customer)
	provider, err := api.repo.UpsertTrafficPolicyDefault(r.Context(), defaults.Provider)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	customer, err := api.repo.UpsertTrafficPolicyDefault(r.Context(), defaults.Customer)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, TrafficPolicyDefaults{Provider: provider, Customer: customer})
}

func prepareTrafficPolicyDefault(tenantID ID, side PortSideType, policyDefault TrafficPolicyDefault) TrafficPolicyDefault {
	policyDefault.TenantID = tenantID
	policyDefault.SideType = side
	if policyDefault.ID == "" {
		policyDefault.ID = ID(string(tenantID) + "-" + string(side))
	}
	return policyDefault.Normalize(side)
}
