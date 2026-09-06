package watchdog

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAPIPortsListRequiresDeviceTargetPermission(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: networkTestAuth,
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 101}},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/device-a/ports", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "port-a") {
		t.Fatalf("body missing port: %s", rec.Body.String())
	}
}

func TestAPIPortsListIncludesIPv4AndIPv6Addresses(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: networkTestAuth,
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 101}},
			addresses: []NetworkInterfaceAddress{
				{ID: "address-v4", TenantID: "tenant-a", DeviceID: "device-a", PortID: "port-a", IfIndex: 101, Address: "192.0.2.10", Family: "ipv4", PrefixLength: 24},
				{ID: "address-v6", TenantID: "tenant-a", DeviceID: "device-a", PortID: "port-a", IfIndex: 101, Address: "2001:db8::10", Family: "ipv6", PrefixLength: 64},
			},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/device-a/ports", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	for _, want := range []string{`"Address":"192.0.2.10"`, `"Family":"ipv4"`, `"Address":"2001:db8::10"`, `"Family":"ipv6"`} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("body missing %s: %s", want, rec.Body.String())
		}
	}
}

func TestAPIPortsPageMapsQueryAddressesAndCounts(t *testing.T) {
	repo := &fakeNetworkRepository{
		devices:    []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		ports:      []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 101, IfName: "xe-0/0/0"}},
		addresses:  []NetworkInterfaceAddress{{ID: "address-v6", PortID: "port-a", Address: "2001:db8::10", Family: "ipv6", PrefixLength: 64}},
		portTotal:  7,
		portCounts: NetworkPortCounts{Total: 19, Up: 15, Down: 4},
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkTestAuth, Network: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/device-a/ports?q=xe&admin_status=up&oper_status=down&address_family=ipv6&sort=speed&order=desc&limit=10&offset=20", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	query := repo.portQuery
	if query.Search != "xe" || query.AdminStatus != "up" || query.OperStatus != "down" || query.AddressFamily != "ipv6" || query.Sort != "speed" || !query.Desc || query.Limit != 10 || query.Offset != 20 {
		t.Fatalf("query = %+v", query)
	}
	body := rec.Body.String()
	for _, want := range []string{`"total":7`, `"up":15`, `"down":4`, `"Address":"2001:db8::10"`, `"limit":10`} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %s: %s", want, body)
		}
	}
}

func TestAPIPortsPageRejectsInvalidQuery(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    networkTestAuth,
		Network: &fakeNetworkRepository{devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}}},
	})
	for _, path := range []string{
		"/api/v1/network/devices/device-a/ports?address_family=ipv10",
		"/api/v1/network/devices/device-a/ports?sort=raw_sql",
		"/api/v1/network/devices/device-a/ports?order=sideways",
		"/api/v1/network/devices/device-a/ports?limit=0",
		"/api/v1/network/devices/device-a/ports?offset=-1",
	} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("path %s: status = %d, body = %s", path, rec.Code, rec.Body.String())
		}
	}
}

func TestAPIPortsGetAllowsTargetInheritedPermission(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: networkTestAuth,
		Network: &fakeNetworkRepository{
			devices:     []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			ports:       []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 101}},
			transceiver: NetworkPortTransceiver{ID: "xcvr-a", TenantID: "tenant-a", PortID: "port-a", ModuleType: "SFP", Serial: "ABC"},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/ports/port-a", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"ModuleType":"SFP"`) || !strings.Contains(rec.Body.String(), `"Serial":"ABC"`) {
		t.Fatalf("body missing transceiver: %s", rec.Body.String())
	}
}

func TestAPIPortsPatchUpdatesMetadata(t *testing.T) {
	repo := &fakeNetworkRepository{
		devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		ports: []NetworkPort{{
			ID:       "port-a",
			TenantID: "tenant-a",
			DeviceID: "device-a",
			IfIndex:  101,
			IfName:   "Eth1/1",
		}},
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: configureNetworkTestAuth, Network: repo})
	rec := httptest.NewRecorder()
	body := `{"IfIndex":101,"IfName":"Eth1/1","IfAlias":"Transit","IfDescr":"uplink","AdminStatus":"up","OperStatus":"up","SpeedBps":10000000000,"Metadata":{"side_type":"provider","billing_note":"95th"}}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/api/v1/network/ports/port-a", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.ports[0].IfAlias != "Transit" || repo.ports[0].Metadata["side_type"] != "provider" {
		t.Fatalf("port = %#v", repo.ports[0])
	}
}

func TestAPIPortsCreateIsDisabledForManualPorts(t *testing.T) {
	repo := &fakeNetworkRepository{
		devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: configureNetworkTestAuth, Network: repo})
	rec := httptest.NewRecorder()
	body := `{"ID":"port-a","IfIndex":101,"IfName":"Eth1/1","IfAlias":"Transit","IfDescr":"uplink","AdminStatus":"up","OperStatus":"up","SpeedBps":10000000000,"Metadata":{"side_type":"provider"}}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/network/devices/device-a/ports", strings.NewReader(body)))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.ports) != 0 {
		t.Fatalf("ports = %#v", repo.ports)
	}
}

func TestAPIPortsDeleteUsesConfigurePermission(t *testing.T) {
	repo := &fakeNetworkRepository{
		devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 101}},
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: configureNetworkTestAuth, Network: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/network/ports/port-a", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.deletedPort != "port-a" {
		t.Fatalf("deletedPort = %s", repo.deletedPort)
	}
}

func TestAPIPortsPolicyPatchUsesPortPermission(t *testing.T) {
	repo := &fakeNetworkRepository{
		devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 101}},
	}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{
				TenantID: "tenant-a",
				UserID:   "user-a",
				Grants: []Permission{{
					TenantID:     "tenant-a",
					SubjectType:  SubjectUser,
					SubjectID:    "user-a",
					ResourceType: ResourcePort,
					ResourceID:   "port-a",
					Actions:      []Action{ActionConfigure},
				}},
			}, nil
		},
		Network: repo,
	})
	rec := httptest.NewRecorder()
	body := `{"SideType":"provider","SampleStep":60000000000,"Enabled":true}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/api/v1/network/ports/port-a/policy", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.policy.PortID != "port-a" || repo.policy.SampleStep != time.Minute {
		t.Fatalf("policy = %#v", repo.policy)
	}
}

func TestAPIPortsPolicyGetReturnsStoredPolicy(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: networkTestAuth,
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 101}},
			policy: PortPolicy{
				ID:                  "policy-a",
				TenantID:            "tenant-a",
				PortID:              "port-a",
				SideType:            PortSideProvider,
				BillingBaseBps:      ProviderBillingBaseBps,
				SampleStep:          time.Minute,
				CorrectionDirection: CorrectionUp,
				CorrectionMin:       1,
				CorrectionMax:       5,
				Enabled:             true,
			},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/ports/port-a/policy", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"PortID":"port-a"`) || !strings.Contains(body, `"CorrectionDirection":"up"`) {
		t.Fatalf("body = %s", body)
	}
}

func TestAPIPortsPolicyGetReturnsAdminDefaultWhenPortPolicyMissing(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: networkTestAuth,
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			ports: []NetworkPort{{
				ID:       "port-a",
				TenantID: "tenant-a",
				DeviceID: "device-a",
				IfIndex:  101,
				Metadata: map[string]string{
					"side_type": "provider",
				},
			}},
			defaults: TrafficPolicyDefaults{
				Provider: TrafficPolicyDefault{
					SideType:            PortSideProvider,
					BillingBaseBps:      ProviderBillingBaseBps,
					SampleStep:          time.Minute,
					CorrectionDirection: CorrectionDown,
					CorrectionMin:       10,
					CorrectionMax:       20,
				},
			},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/ports/port-a/policy", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"ID":"`+string(stableID("policy", "tenant-a", "port-a"))+`"`) || !strings.Contains(body, `"SampleStep":60000000000`) || !strings.Contains(body, `"CorrectionDirection":"down"`) {
		t.Fatalf("body = %s", body)
	}
}
