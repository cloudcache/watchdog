package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeNetworkRepository struct {
	devices      []NetworkDevice
	ports        []NetworkPort
	portQuery    NetworkPortQuery
	portTotal    int
	portCounts   NetworkPortCounts
	addresses    []NetworkInterfaceAddress
	transceiver  NetworkPortTransceiver
	sensors      []NetworkDeviceSensor
	sensorQuery  DeviceSensorQuery
	sensorTotal  int
	sensorCounts DeviceSensorCounts
	bgp          []BGPSession
	physical     []PhysicalEntity
	vlans        []DeviceVLAN
	lags         []DeviceLAGGroup
	vlanQuery    DeviceVLANQuery
	vlanTotal    int
	lagQuery     DeviceLAGQuery
	lagTotal     int
	policy       PortPolicy
	defaults     TrafficPolicyDefaults
	deleted      ID
	deletedPort  ID
	deletedPorts []ID
	// captured by ListDevicesPage / ListDeviceSummaryDevicesPage for assertions
	pagedCalled    bool
	pagedAll       bool
	pagedAllowed   []ID
	pagedFilter    NetworkDevicePageFilter
	pageNext       string
	summaryQuery   DeviceSummaryQuery
	statusCounts   DeviceStatusCounts
	bgpQuery       BGPSessionQuery
	bgpCounts      BGPSessionCounts
	bgpPageTotal   int
	inventoryQuery PhysicalEntityQuery
	inventoryTotal int
}

func (r *fakeNetworkRepository) ListDeviceSummaryDevicesPage(_ context.Context, _ ID, all bool, allowedTargetIDs []ID, q DeviceSummaryQuery) ([]NetworkDevice, error) {
	r.pagedCalled = true
	r.pagedAll = all
	r.pagedAllowed = allowedTargetIDs
	r.summaryQuery = q
	return r.devices, nil
}

func (r *fakeNetworkRepository) CountDeviceStatuses(_ context.Context, _ ID, _ bool, _ []ID, _ string) (DeviceStatusCounts, error) {
	return r.statusCounts, nil
}

func (r *fakeNetworkRepository) ListDevices(context.Context, ID) ([]NetworkDevice, error) {
	return r.devices, nil
}

func (r *fakeNetworkRepository) ListDevicesPage(_ context.Context, _ ID, all bool, allowedTargetIDs []ID, filter NetworkDevicePageFilter) ([]NetworkDevice, string, error) {
	r.pagedCalled = true
	r.pagedAll = all
	r.pagedAllowed = allowedTargetIDs
	r.pagedFilter = filter
	return r.devices, r.pageNext, nil
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

func (r *fakeNetworkRepository) ListDevicePortsPage(_ context.Context, _ ID, _ ID, query NetworkPortQuery) ([]NetworkPort, int, error) {
	r.portQuery = query
	return r.ports, r.portTotal, nil
}

func (r *fakeNetworkRepository) CountDevicePorts(_ context.Context, _ ID, _ ID) (NetworkPortCounts, error) {
	return r.portCounts, nil
}

func (r *fakeNetworkRepository) ListInterfaceAddresses(_ context.Context, _ ID, deviceID ID) ([]NetworkInterfaceAddress, error) {
	var addresses []NetworkInterfaceAddress
	for _, address := range r.addresses {
		if address.DeviceID == deviceID {
			addresses = append(addresses, address)
		}
	}
	return addresses, nil
}

func (r *fakeNetworkRepository) ListInterfaceAddressesByPorts(_ context.Context, _ ID, portIDs []ID) ([]NetworkInterfaceAddress, error) {
	wanted := make(map[ID]struct{}, len(portIDs))
	for _, portID := range portIDs {
		wanted[portID] = struct{}{}
	}
	addresses := make([]NetworkInterfaceAddress, 0)
	for _, address := range r.addresses {
		if _, ok := wanted[address.PortID]; ok {
			addresses = append(addresses, address)
		}
	}
	return addresses, nil
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

func (r *fakeNetworkRepository) ReplaceInterfaceAddresses(_ context.Context, _ ID, deviceID ID, addresses []NetworkInterfaceAddress) error {
	kept := r.addresses[:0]
	for _, address := range r.addresses {
		if address.DeviceID != deviceID {
			kept = append(kept, address)
		}
	}
	r.addresses = append(kept, addresses...)
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

func (r *fakeNetworkRepository) ListDeviceSensorsPage(_ context.Context, _ ID, _ ID, query DeviceSensorQuery) ([]NetworkDeviceSensor, int, error) {
	r.sensorQuery = query
	return r.sensors, r.sensorTotal, nil
}

func (r *fakeNetworkRepository) CountDeviceSensors(_ context.Context, _ ID, _ ID) (DeviceSensorCounts, error) {
	return r.sensorCounts, nil
}

func (r *fakeNetworkRepository) UpsertDeviceSensors(_ context.Context, sensors []NetworkDeviceSensor) error {
	r.sensors = append(r.sensors, sensors...)
	return nil
}

func (r *fakeNetworkRepository) ListDevicePhysicalEntities(_ context.Context, _ ID, _ ID) ([]PhysicalEntity, error) {
	return r.physical, nil
}

func (r *fakeNetworkRepository) ListDevicePhysicalEntitiesPage(_ context.Context, _ ID, _ ID, query PhysicalEntityQuery) ([]PhysicalEntity, int, error) {
	r.inventoryQuery = query
	return r.physical, r.inventoryTotal, nil
}

func (r *fakeNetworkRepository) UpsertDevicePhysicalEntities(_ context.Context, _ ID, _ ID, entities []PhysicalEntity) error {
	r.physical = append(r.physical, entities...)
	return nil
}
func (r *fakeNetworkRepository) ListDeviceVLANs(_ context.Context, _ ID, _ ID) ([]DeviceVLAN, error) {
	return r.vlans, nil
}
func (r *fakeNetworkRepository) ListDeviceVLANsPage(_ context.Context, _ ID, _ ID, query DeviceVLANQuery) ([]DeviceVLAN, int, error) {
	r.vlanQuery = query
	return r.vlans, r.vlanTotal, nil
}
func (r *fakeNetworkRepository) UpsertDeviceVLANs(_ context.Context, _ ID, _ ID, vlans []DeviceVLAN) error {
	r.vlans = append(r.vlans, vlans...)
	return nil
}
func (r *fakeNetworkRepository) ListDeviceLAGGroups(_ context.Context, _ ID, _ ID) ([]DeviceLAGGroup, error) {
	return r.lags, nil
}
func (r *fakeNetworkRepository) ListDeviceLAGGroupsPage(_ context.Context, _ ID, _ ID, query DeviceLAGQuery) ([]DeviceLAGGroup, int, error) {
	r.lagQuery = query
	return r.lags, r.lagTotal, nil
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

func (r *fakeNetworkRepository) ListDeviceBGPSessionsPage(_ context.Context, _ ID, deviceID ID, q BGPSessionQuery) ([]BGPSession, int, error) {
	r.bgpQuery = q
	sessions, _ := r.ListBGPSessions(context.Background(), "", deviceID)
	return sessions, r.bgpPageTotal, nil
}

func (r *fakeNetworkRepository) CountDeviceBGPSessions(_ context.Context, _ ID, _ ID) (BGPSessionCounts, error) {
	return r.bgpCounts, nil
}

func (r *fakeNetworkRepository) ListAllBGPSessions(_ context.Context, _ ID) ([]BGPSession, error) {
	return append([]BGPSession(nil), r.bgp...), nil
}

func (r *fakeNetworkRepository) ListAllBGPSessionsPage(_ context.Context, _ ID, all bool, allowedTargetIDs []ID, q BGPSessionQuery) ([]BGPSession, error) {
	r.pagedCalled = true
	r.pagedAll = all
	r.pagedAllowed = allowedTargetIDs
	r.bgpQuery = q
	return append([]BGPSession(nil), r.bgp...), nil
}

func (r *fakeNetworkRepository) CountBGPSessions(_ context.Context, _ ID, _ bool, _ []ID, _ string) (BGPSessionCounts, error) {
	return r.bgpCounts, nil
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

func (r *fakeNetworkRepository) ReplaceBGPSessions(_ context.Context, _ ID, deviceID ID, sessions []BGPSession) error {
	kept := r.bgp[:0]
	for _, session := range r.bgp {
		if session.DeviceID != deviceID {
			kept = append(kept, session)
		}
	}
	r.bgp = append(kept, sessions...)
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

func adminNetworkAuth(*http.Request) (AuthContext, error) {
	return AuthContext{TenantID: "tenant-a", UserID: "admin", IsAdmin: true}, nil
}

func tenantGrantNetworkAuth(*http.Request) (AuthContext, error) {
	return AuthContext{
		TenantID: "tenant-a",
		UserID:   "user-t",
		Grants: []Permission{{
			TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: "user-t",
			ResourceType: ResourceTenant, ResourceID: "tenant-a", Actions: []Action{ActionView},
		}},
	}, nil
}

func TestAPINetworkDevicesListPagedOptIn(t *testing.T) {
	// A per-target grant pushes down to that target only; next_cursor surfaces
	// only when the repo reports a further page.
	repo := &fakeNetworkRepository{
		devices:  []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		pageNext: "CURSOR2",
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkTestAuth, Network: repo})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices?limit=2", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !repo.pagedCalled || repo.pagedFilter.Limit != 2 {
		t.Fatalf("paged not called with limit 2: %+v", repo.pagedFilter)
	}
	if repo.pagedAll || len(repo.pagedAllowed) != 1 || repo.pagedAllowed[0] != "target-a" {
		t.Fatalf("grant pushdown wrong: all=%v allowed=%v", repo.pagedAll, repo.pagedAllowed)
	}
	if !strings.Contains(rec.Body.String(), `"next_cursor":"CURSOR2"`) {
		t.Fatalf("expected next_cursor: %s", rec.Body.String())
	}

	// A cursor alone opts in and threads through.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices?cursor=abc", nil))
	if !repo.pagedCalled || repo.pagedFilter.Cursor != "abc" {
		t.Fatalf("cursor not threaded: %+v", repo.pagedFilter)
	}

	// An admin and a tenant-scoped grant both push down as "whole tenant".
	for _, auth := range []AuthContextAdapter{adminNetworkAuth, tenantGrantNetworkAuth} {
		scopeRepo := &fakeNetworkRepository{}
		scopeRouter := NewAPIV1Router(APIV1RouterConfig{Auth: auth, Network: scopeRepo})
		rec = httptest.NewRecorder()
		scopeRouter.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices?limit=5", nil))
		if !scopeRepo.pagedAll || scopeRepo.pagedAllowed != nil {
			t.Fatalf("tenant-wide scope wrong: all=%v allowed=%v", scopeRepo.pagedAll, scopeRepo.pagedAllowed)
		}
	}
}

func TestAPINetworkDevicesListPagedGuards(t *testing.T) {
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkTestAuth, Network: repo})

	// No limit/cursor => legacy path, paged repo untouched.
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices", nil))
	if repo.pagedCalled {
		t.Fatal("full-list request must not hit the paged path")
	}
	// Bad limit rejected before the repo.
	for _, bad := range []string{"0", "-1", "abc"} {
		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices?limit="+bad, nil))
		if rec.Code != http.StatusBadRequest || repo.pagedCalled {
			t.Fatalf("limit=%q status=%d called=%v", bad, rec.Code, repo.pagedCalled)
		}
	}
}

func TestAPIDeviceEventsPushesSearchAndAlertSeveritiesToRepository(t *testing.T) {
	collector := &fakeSNMPCollectorRepository{events: []SNMPEvent{{ID: "event-a", Severity: "warning"}}}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:          networkTestAuth,
		Network:       &fakeNetworkRepository{devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}}},
		SNMPCollector: collector,
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/api/v1/network/devices/device-a/events?q=peer%201&severity=warning,error,critical&event_type=bgp&limit=25", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	filter := collector.eventFilter
	if filter.Search != "peer 1" || filter.EventType != "bgp" || filter.Limit != 25 ||
		len(filter.Severities) != 3 || filter.Severities[0] != "warning" || filter.Severities[1] != "error" || filter.Severities[2] != "critical" {
		t.Fatalf("filter = %+v", filter)
	}
}

func TestAPIDeviceEventsRejectsInvalidFilters(t *testing.T) {
	collector := &fakeSNMPCollectorRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:          networkTestAuth,
		Network:       &fakeNetworkRepository{devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}}},
		SNMPCollector: collector,
	})
	for _, query := range []string{
		"limit=0",
		"limit=501",
		"limit=nope",
		"severity=warning,,critical",
		"severity=a,b,c,d,e,f,g,h,i",
		"cursor=not-a-cursor",
	} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
			"/api/v1/network/devices/device-a/events?"+query, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("query %q status = %d body = %s", query, rec.Code, rec.Body.String())
		}
	}
}

func TestAPIDeviceSwitchingPagesPushFiltersAndReturnTotals(t *testing.T) {
	repo := &fakeNetworkRepository{
		devices:   []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		vlans:     []DeviceVLAN{{VLANID: 100, Name: "users", Status: "active"}},
		lags:      []DeviceLAGGroup{{AggregateIndex: 7, MACAddress: "00:11:22:33:44:55", Mode: "lacp"}},
		vlanTotal: 12,
		lagTotal:  4,
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkTestAuth, Network: repo})

	vlan := httptest.NewRecorder()
	router.ServeHTTP(vlan, httptest.NewRequest(http.MethodGet,
		"/api/v1/network/devices/device-a/vlans?q=user&status=active&sort=name&order=desc&limit=25&offset=25", nil))
	if vlan.Code != http.StatusOK || !strings.Contains(vlan.Body.String(), `"total":12`) {
		t.Fatalf("vlan status=%d body=%s", vlan.Code, vlan.Body.String())
	}
	if repo.vlanQuery.Search != "user" || repo.vlanQuery.Status != "active" || repo.vlanQuery.Sort != "name" ||
		!repo.vlanQuery.Desc || repo.vlanQuery.Limit != 25 || repo.vlanQuery.Offset != 25 {
		t.Fatalf("vlan query = %+v", repo.vlanQuery)
	}

	lag := httptest.NewRecorder()
	router.ServeHTTP(lag, httptest.NewRequest(http.MethodGet,
		"/api/v1/network/devices/device-a/lags?q=00%3A11&mode=lacp&sort=mac_address&order=asc&limit=50", nil))
	if lag.Code != http.StatusOK || !strings.Contains(lag.Body.String(), `"total":4`) {
		t.Fatalf("lag status=%d body=%s", lag.Code, lag.Body.String())
	}
	if repo.lagQuery.Search != "00:11" || repo.lagQuery.Mode != "lacp" || repo.lagQuery.Sort != "mac_address" ||
		repo.lagQuery.Desc || repo.lagQuery.Limit != 50 {
		t.Fatalf("lag query = %+v", repo.lagQuery)
	}
}

func TestAPIDeviceSwitchingPagesRejectInvalidQueries(t *testing.T) {
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkTestAuth, Network: repo})
	for _, path := range []string{
		"vlans?limit=0", "vlans?sort=unsafe", "vlans?order=sideways",
		"lags?limit=501", "lags?offset=-1", "lags?sort=unsafe",
	} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/device-a/"+path, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("path %q status=%d body=%s", path, rec.Code, rec.Body.String())
		}
	}
}

func TestAPIBGPSessionsPaged(t *testing.T) {
	// Server-driven opt-in: search/state/sort/offset thread into the query, the
	// grant scope is pushed down, and the first page carries the badge counts.
	repo := &fakeNetworkRepository{
		devices:   []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a", SysName: "edge1"}},
		bgp:       []BGPSession{{ID: "bgp-a", TenantID: "tenant-a", DeviceID: "device-a", PeerAddr: "10.0.0.1", State: "established"}},
		bgpCounts: BGPSessionCounts{Total: 9, Established: 4},
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkTestAuth, Network: repo})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/api/v1/network/bgp?limit=25&q=edge&state=established&sort=peer_as&order=desc", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !repo.pagedCalled {
		t.Fatal("bgp paged path not taken")
	}
	if repo.bgpQuery.Search != "edge" || repo.bgpQuery.State != "established" ||
		repo.bgpQuery.Sort != "peer_as" || !repo.bgpQuery.Desc || repo.bgpQuery.Limit != 25 {
		t.Fatalf("query not threaded: %+v", repo.bgpQuery)
	}
	if repo.pagedAll || len(repo.pagedAllowed) != 1 || repo.pagedAllowed[0] != "target-a" {
		t.Fatalf("grant pushdown wrong: all=%v allowed=%v", repo.pagedAll, repo.pagedAllowed)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"DeviceSysName":"edge1"`) || !strings.Contains(body, `"counts":{"total":9,"established":4}`) {
		t.Fatalf("bgp paged body = %s", body)
	}

	// Later pages omit counts; "all" state clears the filter; bad limit is 400.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/bgp?limit=25&offset=25&state=all", nil))
	if repo.bgpQuery.Offset != 25 || repo.bgpQuery.State != "" || strings.Contains(rec.Body.String(), "counts") {
		t.Fatalf("offset/all page: %+v body=%s", repo.bgpQuery, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/bgp?limit=0", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad limit status = %d", rec.Code)
	}
}

func TestAPINetworkDeviceSummariesPaged(t *testing.T) {
	// Server-driven opt-in: search/status/sort/offset thread into the query, the
	// grant scope is pushed down, the page is enriched, and the first page
	// carries the badge counts.
	repo := &fakeNetworkRepository{
		devices:      []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		ports:        []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", OperStatus: "up"}},
		statusCounts: DeviceStatusCounts{Total: 7, Up: 5, Down: 2, Pending: 1},
	}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    networkTestAuth,
		Targets: &fakeTargetRepository{targets: []Target{{ID: "target-a", TenantID: "tenant-a", Name: "core-a"}}},
		Network: repo,
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/api/v1/network/devices/summary?limit=25&q=core&status=up&sort=vendor&order=desc", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !repo.pagedCalled {
		t.Fatal("summary paged path not taken")
	}
	if repo.summaryQuery.Search != "core" || repo.summaryQuery.Status != "up" ||
		repo.summaryQuery.Sort != "vendor" || !repo.summaryQuery.Desc || repo.summaryQuery.Limit != 25 {
		t.Fatalf("query not threaded: %+v", repo.summaryQuery)
	}
	if repo.pagedAll || len(repo.pagedAllowed) != 1 || repo.pagedAllowed[0] != "target-a" {
		t.Fatalf("grant pushdown wrong: all=%v allowed=%v", repo.pagedAll, repo.pagedAllowed)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"PortCount":1`) || !strings.Contains(body, `"counts":{"total":7,"up":5,"down":2,"pending":1}`) {
		t.Fatalf("paged summary body = %s", body)
	}

	// Every page carries the stable searched-set counts and the active filter's
	// total so a server-driven table can render exact pagination after refresh.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/summary?limit=25&offset=25", nil))
	if repo.summaryQuery.Offset != 25 || !strings.Contains(rec.Body.String(), `"total":7`) ||
		!strings.Contains(rec.Body.String(), `"counts"`) {
		t.Fatalf("offset page: offset=%d body=%s", repo.summaryQuery.Offset, rec.Body.String())
	}

	// "all" status means no filter.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/summary?status=all", nil))
	if repo.summaryQuery.Status != "" {
		t.Fatalf("status=all must clear the filter, got %q", repo.summaryQuery.Status)
	}

	for _, query := range []string{
		"status=broken", "sort=raw_sql", "order=sideways", "limit=0", "limit=501", "offset=-1", "unknown=1",
	} {
		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/summary?"+query, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("query=%q status=%d body=%s", query, rec.Code, rec.Body.String())
		}
	}
}

func TestAPINetworkDeviceSummariesIncludeCounts(t *testing.T) {
	lastPolled := time.Date(2026, 9, 4, 1, 2, 3, 0, time.UTC)
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
		SNMPCollector: &fakeSNMPCollectorRepository{recipes: []SNMPCollectionRecipe{{
			ID: "recipe-a", TenantID: "tenant-a", DeviceID: "device-a", Enabled: true, LastPolledAt: lastPolled,
		}}},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/summary", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{`"PortCount":2`, `"UpPorts":1`, `"DownPorts":1`, `"BGPSessions":2`, `"EstablishedBGP":1`, `"Agent":{"ID":"agent-a"`, `"LastSeen":"2026-09-04T01:02:03Z"`} {
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
	if strings.Contains(body, `"Agent"`) {
		t.Fatalf("body unexpectedly included an empty agent: %s", body)
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

func TestAPINetworkDeviceSensorsPageMapsQueryAndCounts(t *testing.T) {
	repo := &fakeNetworkRepository{
		devices:      []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		sensors:      []NetworkDeviceSensor{{ID: "sensor-a", DeviceID: "device-a", Class: "temperature", Status: "warning"}},
		sensorTotal:  7,
		sensorCounts: DeviceSensorCounts{Total: 19, Problems: 4},
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkTestAuth, Network: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/device-a/sensors?q=temp&class=temperature&status=warning&health=problem&sort=value&order=desc&limit=10&offset=20", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	query := repo.sensorQuery
	if query.Search != "temp" || query.Class != "temperature" || query.Status != "warning" || query.Health != "problem" || query.Sort != "value" || !query.Desc || query.Limit != 10 || query.Offset != 20 {
		t.Fatalf("query = %+v", query)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"total":7`) || !strings.Contains(body, `"problems":4`) || !strings.Contains(body, `"limit":10`) {
		t.Fatalf("body = %s", body)
	}
}

func TestAPINetworkDeviceSensorsPageRejectsInvalidQuery(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    networkTestAuth,
		Network: &fakeNetworkRepository{devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}}},
	})
	for _, path := range []string{
		"/api/v1/network/devices/device-a/sensors?health=broken",
		"/api/v1/network/devices/device-a/sensors?sort=raw_sql",
		"/api/v1/network/devices/device-a/sensors?order=sideways",
		"/api/v1/network/devices/device-a/sensors?limit=0",
		"/api/v1/network/devices/device-a/sensors?offset=-1",
	} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("path %s: status = %d, body = %s", path, rec.Code, rec.Body.String())
		}
	}
}

func TestAPINetworkDeviceInventoryPageMapsQuery(t *testing.T) {
	repo := &fakeNetworkRepository{
		devices:        []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		physical:       []PhysicalEntity{{Index: 7, Name: "PSU-A", Class: "powerSupply", IsFRU: true}},
		inventoryTotal: 42,
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkTestAuth, Network: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/device-a/inventory?q=psu&class=powerSupply&fru=true&sort=manufacturer&order=desc&limit=10&offset=20", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	query := repo.inventoryQuery
	if query.Search != "psu" || query.Class != "powerSupply" || query.FRU == nil || !*query.FRU || query.Sort != "manufacturer" || !query.Desc || query.Limit != 10 || query.Offset != 20 {
		t.Fatalf("inventory query not mapped: %+v", query)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"total":42`) || !strings.Contains(body, `"Name":"PSU-A"`) {
		t.Fatalf("body = %s", body)
	}
}

func TestAPINetworkDeviceInventoryPageRejectsInvalidQuery(t *testing.T) {
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkTestAuth, Network: repo})
	for _, query := range []string{"limit=0", "limit=501", "offset=-1", "fru=yes", "sort=vendor_type", "order=sideways"} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/device-a/inventory?"+query, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("query %q status = %d, body = %s", query, rec.Code, rec.Body.String())
		}
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
		InterfaceAddresses: []NetworkInterfaceAddress{{
			IfIndex: 101, Address: "2001:db8::10", Family: "ipv6", PrefixLength: 64,
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
	if len(repo.addresses) != 1 || repo.addresses[0].PortID == "" || repo.addresses[0].DeviceID != "device-a" {
		t.Fatalf("interface addresses = %#v", repo.addresses)
	}
	if !strings.Contains(rec.Body.String(), `"interface_addresses":1`) {
		t.Fatalf("response missing interface address count: %s", rec.Body.String())
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
	body := `{"ID":"ignored","TargetID":"target-a","Vendor":"","Model":"","Platform":"","OSName":"","OSVersion":"","SysName":"","SysObjectID":"","SysDescr":""}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/api/v1/network/devices/device-a", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	device := repo.devices[0]
	if device.Vendor != "" || device.Model != "" || device.Platform != "" || device.OSName != "" || device.OSVersion != "" || device.SysName != "" || device.SysObjectID != "" {
		t.Fatalf("inventory was not cleared: %#v", device)
	}
	if device.SysDescr != "" {
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

func TestAPINetworkDeviceSNMPPatchPreservesSecretWhenOmitted(t *testing.T) {
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{
		ID: "device-a", TenantID: "tenant-a", TargetID: "target-a", SNMPProfileID: "profile-a", SNMPPort: 161,
		SNMPSecurity: map[string]string{"community": "private-a"},
	}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: configureNetworkTestAuth, Network: repo})
	rec := httptest.NewRecorder()
	body := `{"SNMPProfileID":"profile-b","SNMPPort":1161}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/api/v1/network/devices/device-a/snmp", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	device := repo.devices[0]
	if device.SNMPSecurity["community"] != "private-a" {
		t.Fatalf("device SNMP security = %#v", device.SNMPSecurity)
	}
	if strings.Contains(rec.Body.String(), "private-a") {
		t.Fatalf("response leaked community: %s", rec.Body.String())
	}
}

type fakeSNMPInterfaceDiscoverer struct {
	request SNMPDiscoveryEngineRequest
	result  SNMPCollectorDiscoveryResult
	err     error
}

func (d *fakeSNMPInterfaceDiscoverer) Discover(_ context.Context, request SNMPDiscoveryEngineRequest) (SNMPCollectorDiscoveryResult, error) {
	d.request = request
	return d.result, d.err
}

func TestAPINetworkDeviceSNMPDiscoverMarksFailureDown(t *testing.T) {
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{
		ID: "device-a", TenantID: "tenant-a", TargetID: "target-a", SNMPProfileID: "profile-a", SNMPPort: 161,
	}}}
	targets := &fakeTargetRepository{targets: []Target{{
		ID: "target-a", TenantID: "tenant-a", Name: "Core", Kind: TargetKindNetwork, Host: "10.0.0.1", Status: "up",
	}}}
	collector := &fakeSNMPCollectorRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:          configureNetworkTestAuth,
		Network:       repo,
		Targets:       targets,
		SNMP:          &fakeSNMPRepository{profiles: []SNMPProfile{{ID: "profile-a", TenantID: "tenant-a", Version: "2c"}}},
		SNMPDiscovery: &fakeSNMPInterfaceDiscoverer{err: errors.New("request timeout")},
		SNMPCollector: collector,
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/network/devices/device-a/snmp/discover", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if targets.updated.Status != "down" {
		t.Fatalf("target status = %q", targets.updated.Status)
	}
	if len(collector.events) != 1 || collector.events[0].EventType != "discovery_failed" {
		t.Fatalf("events = %#v", collector.events)
	}
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
func (f *fakeDiscoveryJobRepository) ListDueDiscoveryJobs(context.Context, ID, int) ([]DiscoveryJob, error) {
	return nil, nil
}
func (f *fakeDiscoveryJobRepository) MarkDiscoveryJobRunning(context.Context, ID) (bool, error) {
	return true, nil
}
func (f *fakeDiscoveryJobRepository) MarkDiscoveryJobCompleted(context.Context, ID, string) error {
	return nil
}
