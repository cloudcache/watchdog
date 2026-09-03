package watchdog

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAPIGraphDeviceOverviewReturnsLibreNMSStylePanels(t *testing.T) {
	repo := &fakeNetworkRepository{
		devices: []NetworkDevice{{
			ID:       "device-a",
			TenantID: "tenant-a",
			TargetID: "target-a",
			SysName:  "switch-a",
			Model:    "S5720",
		}},
		ports: []NetworkPort{
			{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 1, IfName: "GigabitEthernet0/0/1", OperStatus: "up", AdminStatus: "up", SpeedBps: 1_000_000_000},
			{ID: "port-b", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 2, IfName: "GigabitEthernet0/0/2", OperStatus: "down", AdminStatus: "up", SpeedBps: 1_000_000_000},
		},
		bgp:     []BGPSession{{ID: "bgp-a", TenantID: "tenant-a", DeviceID: "device-a", PeerAddr: "10.0.0.1", State: "established"}},
		sensors: []NetworkDeviceSensor{{ID: "sensor-a", TenantID: "tenant-a", DeviceID: "device-a", Class: "dbm", Unit: "dBm", Name: "Rx Power"}},
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkTestAuth, Network: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/graph/devices/device-a/overview", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var dashboard GraphDashboard
	if err := json.Unmarshal(rec.Body.Bytes(), &dashboard); err != nil {
		t.Fatal(err)
	}
	if dashboard.ID != "network-device-overview:device-a" || dashboard.Title != "switch-a" {
		t.Fatalf("dashboard = %#v", dashboard)
	}
	if len(dashboard.Panels) != 5 {
		t.Fatalf("panel count = %d", len(dashboard.Panels))
	}
	traffic := dashboard.Panels[0]
	// LibreNMS-style mirrored traffic: In above the axis, Out flipped below.
	if traffic.ID != "overall-traffic" || traffic.Stack != "signed" {
		t.Fatalf("traffic panel = %#v", traffic)
	}
	if len(traffic.Queries) != 2 || traffic.Queries[0].Metric != MetricSNMPIfInBps || traffic.Queries[1].Metric != MetricSNMPIfOutBps {
		t.Fatalf("traffic queries = %#v", traffic.Queries)
	}
	if !traffic.Queries[1].Transform.Negative {
		t.Fatalf("out traffic must mirror below the axis (signed mode)")
	}
	// Device totals pin an explicit physical-port membership so virtual
	// interfaces cannot double-count the sum.
	if len(traffic.Queries[0].PortIDs) != 2 || len(traffic.Queries[1].PortIDs) != 2 {
		t.Fatalf("traffic port ids = %#v / %#v", traffic.Queries[0].PortIDs, traffic.Queries[1].PortIDs)
	}
	if len(traffic.Links) != 2 || traffic.Links[0].Href != "/network/ports/port-a" {
		t.Fatalf("links = %#v", traffic.Links)
	}
	if traffic.QueryOptions.MaxDataPoints != 1200 || traffic.QueryOptions.MinInterval != "1m" {
		t.Fatalf("query options = %#v", traffic.QueryOptions)
	}
}

func TestAPIGraphDeviceOverviewChecksTargetPermission(t *testing.T) {
	repo := &fakeNetworkRepository{
		devices: []NetworkDevice{{ID: "device-b", TenantID: "tenant-a", TargetID: "target-b", SysName: "switch-b"}},
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkTestAuth, Network: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/graph/devices/device-b/overview", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body = %s", rec.Code, rec.Body.String())
	}
}
