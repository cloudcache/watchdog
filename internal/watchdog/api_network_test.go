package watchdog

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeNetworkRepository struct {
	devices      []NetworkDevice
	ports        []NetworkPort
	transceiver  NetworkPortTransceiver
	sensors      []NetworkDeviceSensor
	bgp          []BGPSession
	physical     []PhysicalEntity
	vlans        []DeviceVLAN
	lags         []DeviceLAGGroup
	policy       PortPolicy
	defaults     TrafficPolicyDefaults
	deleted      ID
	deletedPort  ID
	deletedPorts []ID
}

func (r *fakeNetworkRepository) ListDevices(context.Context, ID) ([]NetworkDevice, error) {
	return r.devices, nil
}

func (r *fakeNetworkRepository) GetDevice(_ context.Context, _ ID, deviceID ID) (NetworkDevice, error) {
	for _, device := range r.devices {
		if device.ID == deviceID {
			return device, nil
		}
	}
	return NetworkDevice{}, errNotFoundForTest{}
}

func (r *fakeNetworkRepository) GetDeviceByTarget(_ context.Context, _ ID, targetID ID) (NetworkDevice, error) {
	for _, device := range r.devices {
		if device.TargetID == targetID {
			return device, nil
		}
	}
	return NetworkDevice{}, errNotFoundForTest{}
}

func (r *fakeNetworkRepository) UpsertDevice(_ context.Context, device NetworkDevice) (NetworkDevice, error) {
	for i := range r.devices {
		if r.devices[i].ID == device.ID {
			r.devices[i] = device
			return device, nil
		}
	}
	r.devices = append(r.devices, device)
	return device, nil
}

func (r *fakeNetworkRepository) UpdateDeviceInventory(_ context.Context, device NetworkDevice) (NetworkDevice, error) {
	for i := range r.devices {
		if r.devices[i].ID == device.ID {
			r.devices[i] = device
			return device, nil
		}
	}
	r.devices = append(r.devices, device)
	return device, nil
}

func (r *fakeNetworkRepository) DeleteDevice(_ context.Context, _ ID, deviceID ID) error {
	r.deleted = deviceID
	return nil
}

func (r *fakeNetworkRepository) ListPorts(_ context.Context, _ ID, deviceID ID) ([]NetworkPort, error) {
	ports := make([]NetworkPort, 0, len(r.ports))
	for _, port := range r.ports {
		if port.DeviceID == deviceID {
			ports = append(ports, port)
		}
	}
	return ports, nil
}

func (r *fakeNetworkRepository) GetPort(_ context.Context, _ ID, portID ID) (NetworkPort, error) {
	for _, port := range r.ports {
		if port.ID == portID {
			return port, nil
		}
	}
	return NetworkPort{}, errNotFoundForTest{}
}

func (r *fakeNetworkRepository) UpsertPorts(_ context.Context, ports []NetworkPort) error {
	for _, port := range ports {
		replaced := false
		for index := range r.ports {
			if r.ports[index].ID == port.ID {
				r.ports[index] = port
				replaced = true
				break
			}
		}
		if !replaced {
			r.ports = append(r.ports, port)
		}
	}
	return nil
}

func (r *fakeNetworkRepository) DeletePort(_ context.Context, _ ID, portID ID) error {
	r.deletedPort = portID
	r.deletedPorts = append(r.deletedPorts, portID)
	for index, port := range r.ports {
		if port.ID == portID {
			r.ports = append(r.ports[:index], r.ports[index+1:]...)
			break
		}
	}
	return nil
}

func (r *fakeNetworkRepository) UpsertPortTransceiver(_ context.Context, transceiver NetworkPortTransceiver) (NetworkPortTransceiver, error) {
	r.transceiver = transceiver
	return transceiver, nil
}

func (r *fakeNetworkRepository) GetPortTransceiver(_ context.Context, _ ID, portID ID) (NetworkPortTransceiver, error) {
	if r.transceiver.PortID == portID {
		return r.transceiver, nil
	}
	return NetworkPortTransceiver{}, sql.ErrNoRows
}

func (r *fakeNetworkRepository) ListDeviceSensors(_ context.Context, _ ID, deviceID ID) ([]NetworkDeviceSensor, error) {
	sensors := make([]NetworkDeviceSensor, 0, len(r.sensors))
	for _, sensor := range r.sensors {
		if sensor.DeviceID == deviceID {
			sensors = append(sensors, sensor)
		}
	}
	return sensors, nil
}

func (r *fakeNetworkRepository) UpsertDeviceSensors(_ context.Context, sensors []NetworkDeviceSensor) error {
	r.sensors = append(r.sensors, sensors...)
	return nil
}

func (r *fakeNetworkRepository) ListDevicePhysicalEntities(_ context.Context, _ ID, _ ID) ([]PhysicalEntity, error) {
	return r.physical, nil
}

func (r *fakeNetworkRepository) UpsertDevicePhysicalEntities(_ context.Context, _ ID, _ ID, entities []PhysicalEntity) error {
	r.physical = append(r.physical, entities...)
	return nil
}
func (r *fakeNetworkRepository) ListDeviceVLANs(_ context.Context, _ ID, _ ID) ([]DeviceVLAN, error) {
	return r.vlans, nil
}
func (r *fakeNetworkRepository) UpsertDeviceVLANs(_ context.Context, _ ID, _ ID, vlans []DeviceVLAN) error {
	r.vlans = append(r.vlans, vlans...)
	return nil
}
func (r *fakeNetworkRepository) ListDeviceLAGGroups(_ context.Context, _ ID, _ ID) ([]DeviceLAGGroup, error) {
	return r.lags, nil
}
func (r *fakeNetworkRepository) UpsertDeviceLAGGroups(_ context.Context, _ ID, _ ID, groups []DeviceLAGGroup) error {
	r.lags = append(r.lags, groups...)
	return nil
}

func (r *fakeNetworkRepository) ListBGPSessions(_ context.Context, _ ID, deviceID ID) ([]BGPSession, error) {
	sessions := make([]BGPSession, 0, len(r.bgp))
	for _, session := range r.bgp {
		if session.DeviceID == deviceID {
			sessions = append(sessions, session)
		}
	}
	return sessions, nil
}

func (r *fakeNetworkRepository) GetBGPSession(_ context.Context, _ ID, sessionID ID) (BGPSession, error) {
	for _, session := range r.bgp {
		if session.ID == sessionID {
			return session, nil
		}
	}
	return BGPSession{}, errNotFoundForTest{}
}

func (r *fakeNetworkRepository) UpsertBGPSessions(_ context.Context, sessions []BGPSession) error {
	r.bgp = append(r.bgp, sessions...)
	return nil
}

func (r *fakeNetworkRepository) GetPortPolicy(context.Context, ID, ID) (PortPolicy, error) {
	if r.policy.PortID == "" {
		return PortPolicy{}, sql.ErrNoRows
	}
	return r.policy, nil
}

func (r *fakeNetworkRepository) UpsertPortPolicy(_ context.Context, policy PortPolicy) (PortPolicy, error) {
	r.policy = policy.Normalize()
	return r.policy, nil
}

func (r *fakeNetworkRepository) GetTrafficPolicyDefaults(context.Context, ID) (TrafficPolicyDefaults, error) {
	if r.defaults.Provider.SideType == "" && r.defaults.Customer.SideType == "" {
		return BuiltinTrafficPolicyDefaults, nil
	}
	return r.defaults, nil
}

func (r *fakeNetworkRepository) UpsertTrafficPolicyDefault(_ context.Context, policyDefault TrafficPolicyDefault) (TrafficPolicyDefault, error) {
	policyDefault = policyDefault.Normalize(policyDefault.SideType)
	if policyDefault.SideType == PortSideProvider {
		r.defaults.Provider = policyDefault
	} else {
		r.defaults.Customer = policyDefault
	}
	return policyDefault, nil
}

func TestAPINetworkDevicesListFiltersByTargetPermission(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: networkTestAuth,
		Network: &fakeNetworkRepository{devices: []NetworkDevice{
			{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"},
			{ID: "device-b", TenantID: "tenant-a", TargetID: "target-b"},
		}},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "device-a") || strings.Contains(body, "device-b") {
		t.Fatalf("unexpected body = %s", body)
	}
}

func TestAPINetworkDeviceSummariesIncludeCounts(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: networkTestAuth,
		Targets: &fakeTargetRepository{targets: []Target{{
			ID:       "target-a",
			TenantID: "tenant-a",
			Name:     "core-a",
			Host:     "10.0.0.1",
			Status:   "up",
		}}},
		Agents: &fakeAgentRepository{agent: SNMPAgentConfig{
			ID:       "agent-a",
			TenantID: "tenant-a",
			TargetID: "target-a",
			Status:   "error",
		}},
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{
				{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"},
				{ID: "device-b", TenantID: "tenant-a", TargetID: "target-b"},
			},
			ports: []NetworkPort{
				{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", OperStatus: "up"},
				{ID: "port-b", TenantID: "tenant-a", DeviceID: "device-a", OperStatus: "down"},
			},
			bgp: []BGPSession{
				{ID: "bgp-a", TenantID: "tenant-a", DeviceID: "device-a", State: "established"},
				{ID: "bgp-b", TenantID: "tenant-a", DeviceID: "device-a", State: "idle"},
			},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/summary", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{`"PortCount":2`, `"UpPorts":1`, `"DownPorts":1`, `"BGPSessions":2`, `"EstablishedBGP":1`, `"Agent":{"ID":"agent-a"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %s: %s", want, body)
		}
	}
	if strings.Contains(body, "device-b") {
		t.Fatalf("body leaked inaccessible device: %s", body)
	}
}

func TestAPINetworkDeviceSummariesIncludeUndiscoveredNetworkTarget(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: networkTestAuth,
		Targets: &fakeTargetRepository{targets: []Target{{
			ID:       "target-a",
			TenantID: "tenant-a",
			Name:     "pending-switch",
			Kind:     TargetKindNetwork,
			Host:     "10.0.0.1",
			Status:   "pending",
		}}},
		Network: &fakeNetworkRepository{},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/summary", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"Target":{"id":"target-a"`) || !strings.Contains(body, "pending-switch") {
		t.Fatalf("body missing undiscovered target: %s", body)
	}
	if strings.Contains(body, `"Device":{"ID":"device-`) {
		t.Fatalf("body unexpectedly included device: %s", body)
	}
}

func TestAPINetworkDeviceSummaryRouteIsNotDeviceID(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    networkTestAuth,
		Targets: &fakeTargetRepository{},
		Network: &fakeNetworkRepository{devices: []NetworkDevice{{
			ID:       "device-a",
			TenantID: "tenant-a",
			TargetID: "target-a",
		}}},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/summary", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "Network device not found") {
		t.Fatalf("summary route was handled as device id: %s", rec.Body.String())
	}
}

func TestAPINetworkDeviceSensorsList(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: networkTestAuth,
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			sensors: []NetworkDeviceSensor{{
				ID:          "sensor-a",
				TenantID:    "tenant-a",
				DeviceID:    "device-a",
				SensorIndex: 501,
				Class:       "temperature",
				Name:        "Temp sensor",
				Unit:        "C",
				Value:       38.5,
				Status:      "ok",
			}},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/device-a/sensors", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "sensor-a") || !strings.Contains(body, "Temp sensor") {
		t.Fatalf("body = %s", body)
	}
}

func TestAPINetworkDevicesGetRejectsMissingTargetPermission(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: networkTestAuth,
		Network: &fakeNetworkRepository{devices: []NetworkDevice{
			{ID: "device-b", TenantID: "tenant-a", TargetID: "target-b"},
		}},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/device-b", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestAPINetworkDeviceCreateRequiresTargetConfigure(t *testing.T) {
	repo := &fakeNetworkRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), Network: repo})
	rec := httptest.NewRecorder()
	body := `{"ID":"device-a","TargetID":"target-a","Vendor":"juniper","Model":"mx","SysName":"core-a"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/network/devices", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.devices) != 1 || repo.devices[0].TenantID != "tenant-a" || repo.devices[0].TargetID != "target-a" {
		t.Fatalf("devices = %#v", repo.devices)
	}
}

func TestAPINetworkSNMPDiscoverUpsertsDevicePortsAndSensors(t *testing.T) {
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{
		ID: "device-a", TenantID: "tenant-a", TargetID: "target-a", SNMPProfileID: "profile-a", SNMPPort: 161,
	}}}
	discovery := &fakeSNMPInterfaceDiscoverer{result: SNMPCollectorDiscoveryResult{
		DeviceUpdates: NetworkDevice{ID: "device-a", Vendor: "cisco", Model: "n9k", SysName: "core-a", SysDescr: "core router"},
		Ports: []NetworkPort{{
			IfIndex: 101, IfName: "Eth1/1", IfDescr: "uplink", AdminStatus: "up", OperStatus: "up", SpeedBps: 10000000000, Metadata: map[string]string{"side_type": "provider"},
		}},
		Sensors: []NetworkDeviceSensor{{SensorIndex: 501, Class: "temperature", Name: "Temp sensor", OID: ".1.2.3.501", Unit: "C", Value: 38.5, Status: "ok"}},
	}}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:          configureNetworkTestAuth,
		Network:       repo,
		Targets:       &fakeTargetRepository{targets: []Target{{ID: "target-a", TenantID: "tenant-a", Name: "Core", Kind: TargetKindNetwork, Host: "10.0.0.1"}}},
		SNMP:          &fakeSNMPRepository{profiles: []SNMPProfile{{ID: "profile-a", TenantID: "tenant-a", Version: "2c", Security: map[string]string{"community": "public"}}}},
		SNMPDiscovery: discovery,
		SNMPCollector: &fakeSNMPCollectorRepository{},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/network/devices/device-a/snmp/discover", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.devices) != 1 || repo.devices[0].ID != "device-a" || repo.devices[0].TenantID != "tenant-a" || repo.devices[0].Vendor != "cisco" {
		t.Fatalf("devices = %#v", repo.devices)
	}
	if len(repo.ports) != 1 || repo.ports[0].DeviceID != "device-a" || repo.ports[0].TenantID != "tenant-a" || repo.ports[0].ID == "" {
		t.Fatalf("ports = %#v", repo.ports)
	}
	if len(repo.sensors) != 1 || repo.sensors[0].DeviceID != "device-a" || repo.sensors[0].TenantID != "tenant-a" || repo.sensors[0].ID == "" {
		t.Fatalf("sensors = %#v", repo.sensors)
	}
}

func TestAPINetworkSNMPDiscoverAppliesDeviceUpdates(t *testing.T) {
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{
		ID: "device-a", TenantID: "tenant-a", TargetID: "target-a", SNMPProfileID: "profile-a", SNMPPort: 161,
	}}}
	discovery := &fakeSNMPInterfaceDiscoverer{result: SNMPCollectorDiscoveryResult{
		DeviceUpdates: NetworkDevice{ID: "device-a", Vendor: "huawei", Model: "CE", SysObjectID: ".1.3.6.1.4.1.2011.2.23", SysName: "core-a", SysDescr: "CloudEngine"},
	}}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:          configureNetworkTestAuth,
		Network:       repo,
		Targets:       &fakeTargetRepository{targets: []Target{{ID: "target-a", TenantID: "tenant-a", Name: "Core", Kind: TargetKindNetwork, Host: "10.0.0.1"}}},
		SNMP:          &fakeSNMPRepository{profiles: []SNMPProfile{{ID: "profile-a", TenantID: "tenant-a", Version: "2c", Security: map[string]string{"community": "public"}}}},
		SNMPDiscovery: discovery,
		SNMPCollector: &fakeSNMPCollectorRepository{},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/network/devices/device-a/snmp/discover", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.devices) != 1 || repo.devices[0].Vendor != "huawei" {
		t.Fatalf("devices = %#v", repo.devices)
	}
}

func TestAPINetworkSNMPDiscoverPersistsRecipesBGPSessionsEventsAndModules(t *testing.T) {
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{
		ID: "device-a", TenantID: "tenant-a", TargetID: "target-a", SNMPProfileID: "profile-a", SNMPPort: 161,
	}}}
	portID := collectorStableID("port", "tenant-a", "device-a", "10")
	collector := &fakeSNMPCollectorRepository{}
	discovery := &fakeSNMPInterfaceDiscoverer{result: SNMPCollectorDiscoveryResult{
		DeviceUpdates: NetworkDevice{ID: "device-a", Vendor: "cisco", SysName: "core-a"},
		Ports:         []NetworkPort{{ID: portID, IfIndex: 10, IfName: "Eth1/1"}},
		Recipes: []SNMPCollectionRecipe{{
			ModuleName: snmpCollectorModulePorts, EntityType: SNMPCollectorEntityPort, EntityID: portID,
			MetricName: MetricSNMPIfInOctetsTotal, ValueType: SNMPCollectorValueCounter64,
			OID: snmpOIDIfHCInOctets + ".10", NumericOID: snmpOIDIfHCInOctets + ".10", OIDIndex: "10",
		}},
		BGPSessions: []BGPSession{{PeerAddr: "10.0.0.2", PeerAS: 65001, AFI: "ipv4", SAFI: "unicast"}},
		Events:      []SNMPEvent{{Source: "discovery", EventType: "port_discovered", Message: "port Eth1/1 discovered"}},
	}}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:          configureNetworkTestAuth,
		Network:       repo,
		Targets:       &fakeTargetRepository{targets: []Target{{ID: "target-a", TenantID: "tenant-a", Name: "Core", Kind: TargetKindNetwork, Host: "10.0.0.1"}}},
		SNMP:          &fakeSNMPRepository{profiles: []SNMPProfile{{ID: "profile-a", TenantID: "tenant-a", Version: "2c", Security: map[string]string{"community": "public"}}}},
		SNMPDiscovery: discovery,
		SNMPCollector: collector,
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/network/devices/device-a/snmp/discover", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(collector.recipes) == 0 {
		t.Fatal("no recipes persisted")
	}
	recipe := collector.recipes[0]
	if recipe.TenantID != "tenant-a" || recipe.DeviceID != "device-a" || recipe.EntityID != portID {
		t.Fatalf("recipe = %#v", recipe)
	}
	if !recipe.Enabled || recipe.SampleIntervalSeconds == 0 {
		t.Fatalf("recipe not enabled or zero interval: %#v", recipe)
	}
	if len(collector.deviceModules) == 0 || collector.deviceModules[0].ModuleName != snmpCollectorModulePorts {
		t.Fatalf("modules = %#v", collector.deviceModules)
	}
	if len(repo.bgp) == 0 || repo.bgp[0].PeerAddr != "10.0.0.2" {
		t.Fatalf("bgp sessions = %#v", repo.bgp)
	}
	if len(collector.events) == 0 || collector.events[0].EventType != "port_discovered" {
		t.Fatalf("events = %#v", collector.events)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"recipes":1`) || !strings.Contains(body, `"bgp_sessions":1`) {
		t.Fatalf("response missing recipe/bgp counts: %s", body)
	}
}

func TestAPINetworkDevicePatchPreservesSNMPOverrides(t *testing.T) {
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{
		ID: "device-a", TenantID: "tenant-a", TargetID: "target-a", SNMPProfileID: "profile-a", SNMPPort: 1161,
		SNMPSecurity: map[string]string{"community": "private-a"},
	}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: configureNetworkTestAuth, Network: repo})
	rec := httptest.NewRecorder()
	body := `{"ID":"ignored","TargetID":"target-a","Vendor":"juniper","Model":"mx","SysName":"core-a"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/api/v1/network/devices/device-a", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	device := repo.devices[0]
	if device.SNMPProfileID != "profile-a" || device.SNMPPort != 1161 || device.SNMPSecurity["community"] != "private-a" {
		t.Fatalf("device = %#v", device)
	}
}

func TestAPINetworkDevicePatchAllowsClearingInventoryOverrides(t *testing.T) {
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{
		ID: "device-a", TenantID: "tenant-a", TargetID: "target-a", Vendor: "huawei", Model: "S5720", Platform: "S5720",
		OSName: "Huawei VRP", OSVersion: "5.170", SysName: "core-a", SysObjectID: "1.3.6.1.4.1.2011", SysDescr: "old",
		SNMPProfileID: "profile-a", SNMPPort: 1161, SNMPSecurity: map[string]string{"community": "private-a"},
	}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: configureNetworkTestAuth, Network: repo})
	rec := httptest.NewRecorder()
	body := `{"ID":"ignored","TargetID":"target-a","Vendor":"","Model":"","Platform":"","OSName":"","OSVersion":"","SysName":"","SysObjectID":"","SysDescr":"manual note"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/api/v1/network/devices/device-a", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	device := repo.devices[0]
	if device.Vendor != "" || device.Model != "" || device.Platform != "" || device.OSName != "" || device.OSVersion != "" || device.SysName != "" || device.SysObjectID != "" {
		t.Fatalf("inventory was not cleared: %#v", device)
	}
	if device.SysDescr != "manual note" {
		t.Fatalf("sysDescr = %q", device.SysDescr)
	}
	if device.SNMPProfileID != "profile-a" || device.SNMPPort != 1161 || device.SNMPSecurity["community"] != "private-a" {
		t.Fatalf("snmp overrides were not preserved: %#v", device)
	}
}

func TestAPINetworkDeviceDelete(t *testing.T) {
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}}}
	cleaner := &fakeSeriesCleaner{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: configureNetworkTestAuth, Network: repo, SeriesCleaner: cleaner})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/network/devices/device-a", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.deleted != "device-a" {
		t.Fatalf("deleted = %s", repo.deleted)
	}
	if len(cleaner.calls) != 1 {
		t.Fatalf("series cleaner calls = %d, want 1", len(cleaner.calls))
	}
	if !strings.Contains(cleaner.calls[0], `device_id="device-a"`) {
		t.Fatalf("cleaner matcher = %q", cleaner.calls[0])
	}
}

func TestAPITrafficPolicyDefaultsCanBeConfigured(t *testing.T) {
	repo := &fakeNetworkRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), Network: repo})
	rec := httptest.NewRecorder()
	body := `{"Provider":{"BillingBaseBps":1073741824,"SampleStep":60000000000,"CorrectionDirection":"up","CorrectionMin":1,"CorrectionMax":5},"Customer":{"BillingBaseBps":1000000000,"SampleStep":300000000000,"CorrectionDirection":"down","CorrectionMin":2,"CorrectionMax":6}}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/network/traffic-policy-defaults", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.defaults.Provider.TenantID != "tenant-a" || repo.defaults.Provider.SideType != PortSideProvider || repo.defaults.Provider.ID == "" {
		t.Fatalf("provider default = %#v", repo.defaults.Provider)
	}
	if repo.defaults.Customer.SampleStep != 5*60*1000*1000*1000 {
		t.Fatalf("customer default = %#v", repo.defaults.Customer)
	}
}

func TestAPINetworkDeviceSNMPPatchSupportsDeviceOverrides(t *testing.T) {
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: configureNetworkTestAuth, Network: repo})
	rec := httptest.NewRecorder()
	body := `{"SNMPProfileID":"profile-b","SNMPPort":1161,"SNMPSecurity":{"community":"private-a"}}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/api/v1/network/devices/device-a/snmp", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	device := repo.devices[0]
	if device.SNMPProfileID != "profile-b" || device.SNMPPort != 1161 || device.SNMPSecurity["community"] != "private-a" {
		t.Fatalf("device = %#v", device)
	}
	if strings.Contains(rec.Body.String(), "private-a") {
		t.Fatalf("response leaked community: %s", rec.Body.String())
	}
}

type fakeSNMPInterfaceDiscoverer struct {
	request SNMPDiscoveryEngineRequest
	result  SNMPCollectorDiscoveryResult
}

func (d *fakeSNMPInterfaceDiscoverer) Discover(_ context.Context, request SNMPDiscoveryEngineRequest) (SNMPCollectorDiscoveryResult, error) {
	d.request = request
	return d.result, nil
}

func TestAPINetworkDeviceSNMPDiscoverSyncsPortsAndUsesDeviceCommunityOverride(t *testing.T) {
	repo := &fakeNetworkRepository{
		devices: []NetworkDevice{{
			ID: "device-a", TenantID: "tenant-a", TargetID: "target-a", SNMPProfileID: "profile-a", SNMPPort: 161,
			SNMPSecurity: map[string]string{"community": "private-a"},
		}},
		ports: []NetworkPort{
			{ID: "stale-port", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 99, IfName: "old"},
			{ID: "kept-port", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 10, IfName: "old-name"},
		},
	}
	discovery := &fakeSNMPInterfaceDiscoverer{result: SNMPCollectorDiscoveryResult{Ports: []NetworkPort{{
		IfIndex: 10,
		IfName:  "GigabitEthernet0/0/1",
	}}}}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:          configureNetworkTestAuth,
		Network:       repo,
		Targets:       &fakeTargetRepository{targets: []Target{{ID: "target-a", TenantID: "tenant-a", Name: "Core", Kind: TargetKindNetwork, Host: "10.0.0.1"}}},
		SNMP:          &fakeSNMPRepository{profiles: []SNMPProfile{{ID: "profile-a", TenantID: "tenant-a", Version: "2c", Security: map[string]string{"community": "public"}}}},
		SNMPDiscovery: discovery,
		SNMPCollector: &fakeSNMPCollectorRepository{},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/network/devices/device-a/snmp/discover", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if discovery.request.Profile.Security["community"] != "private-a" {
		t.Fatalf("profile override was not applied: %#v", discovery.request.Profile.Security)
	}
	if len(repo.deletedPorts) != 1 || repo.deletedPorts[0] != "stale-port" {
		t.Fatalf("deleted ports = %#v", repo.deletedPorts)
	}
	if strings.Contains(rec.Body.String(), "private-a") {
		t.Fatalf("response leaked community: %s", rec.Body.String())
	}
}

func networkTestAuth(*http.Request) (AuthContext, error) {
	return AuthContext{
		TenantID: "tenant-a",
		UserID:   "user-a",
		Grants: []Permission{{
			TenantID:     "tenant-a",
			SubjectType:  SubjectUser,
			SubjectID:    "user-a",
			ResourceType: ResourceTarget,
			ResourceID:   "target-a",
			Actions:      []Action{ActionView},
		}},
	}, nil
}

func configureNetworkTestAuth(*http.Request) (AuthContext, error) {
	auth, err := networkTestAuth(nil)
	if err != nil {
		return AuthContext{}, err
	}
	auth.Grants[0].Actions = []Action{ActionView, ActionConfigure}
	return auth, nil
}

type fakeSeriesCleaner struct {
	calls []string
}

func (f *fakeSeriesCleaner) DeleteSeries(_ context.Context, matchers []string) error {
	f.calls = append(f.calls, matchers...)
	return nil
}

type fakeDiscoveryJobRepository struct {
	enqueued []DiscoveryJob
}

func (f *fakeDiscoveryJobRepository) EnqueueDiscoveryJob(_ context.Context, tenantID, deviceID ID, reason string) error {
	f.enqueued = append(f.enqueued, DiscoveryJob{TenantID: tenantID, DeviceID: deviceID, Reason: reason})
	return nil
}
func (f *fakeDiscoveryJobRepository) ListDueDiscoveryJobs(context.Context, int, time.Time) ([]DiscoveryJob, error) {
	return nil, nil
}
func (f *fakeDiscoveryJobRepository) MarkDiscoveryJobRunning(context.Context, ID) error { return nil }
func (f *fakeDiscoveryJobRepository) MarkDiscoveryJobCompleted(context.Context, ID, string) error {
	return nil
}
