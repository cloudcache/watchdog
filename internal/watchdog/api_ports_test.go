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
