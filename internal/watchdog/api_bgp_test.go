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

func TestAPIDeviceBGPPagePushesFiltersAndReturnsTotal(t *testing.T) {
	repo := &fakeNetworkRepository{
		devices:      []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		bgp:          []BGPSession{{ID: "bgp-a", TenantID: "tenant-a", DeviceID: "device-a", PeerAddr: "2001:db8::1"}},
		bgpPageTotal: 7,
		bgpCounts:    BGPSessionCounts{Total: 9, Established: 6},
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkTestAuth, Network: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/api/v1/network/devices/device-a/bgp?limit=25&offset=0&q=2001%3Adb8&state=established&sort=peer_as&order=desc", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if repo.bgpQuery.Search != "2001:db8" || repo.bgpQuery.State != "established" || repo.bgpQuery.Sort != "peer_as" ||
		!repo.bgpQuery.Desc || repo.bgpQuery.Limit != 25 || repo.bgpQuery.Offset != 0 {
		t.Fatalf("query = %+v", repo.bgpQuery)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"total":7`) || !strings.Contains(body, `"counts":{"total":9,"established":6}`) {
		t.Fatalf("body = %s", body)
	}
}

func TestAPIDeviceBGPPageRejectsInvalidQuery(t *testing.T) {
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkTestAuth, Network: repo})
	for _, query := range []string{"limit=0", "limit=501", "offset=-1", "sort=unsafe", "order=sideways"} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/device-a/bgp?"+query, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("query %q status = %d body = %s", query, rec.Code, rec.Body.String())
		}
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
