package watchdog

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAPIBGPListRequiresDeviceTargetPermission(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: networkTestAuth,
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			bgp:     []BGPSession{{ID: "bgp-a", TenantID: "tenant-a", DeviceID: "device-a", PeerAddr: "192.0.2.1"}},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/device-a/bgp", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "bgp-a") {
		t.Fatalf("body missing bgp session: %s", rec.Body.String())
	}
}

func TestAPIBGPGetRejectsMissingTargetPermission(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: networkTestAuth,
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-b", TenantID: "tenant-a", TargetID: "target-b"}},
			bgp:     []BGPSession{{ID: "bgp-b", TenantID: "tenant-a", DeviceID: "device-b"}},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/bgp/bgp-b", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}
