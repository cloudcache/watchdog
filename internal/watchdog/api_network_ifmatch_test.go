package watchdog

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func networkConfigureAuth(*http.Request) (AuthContext, error) {
	return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: true}, nil
}

func TestNetworkDeviceIfMatchOptimisticLocking(t *testing.T) {
	updatedAt := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	repo := &fakeNetworkRepository{devices: []NetworkDevice{{
		ID: "device-a", TenantID: "tenant-a", TargetID: "target-a", SysName: "core", UpdatedAt: updatedAt,
	}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkConfigureAuth, Network: repo})

	get := httptest.NewRecorder()
	router.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/v1/network/devices/device-a", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", get.Code, get.Body.String())
	}
	etag := get.Header().Get("ETag")
	if etag == "" || !strings.HasPrefix(etag, `W/"`) {
		t.Fatalf("expected weak ETag, got %q", etag)
	}

	stale := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/network/devices/device-a",
		strings.NewReader(`{"TargetID":"target-a","SysName":"renamed"}`))
	req.Header.Set("If-Match", WeakETagFromTime(updatedAt.Add(-time.Hour)))
	router.ServeHTTP(stale, req)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale patch status = %d, body = %s", stale.Code, stale.Body.String())
	}

	staleDelete := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/network/devices/device-a", nil)
	req.Header.Set("If-Match", WeakETagFromTime(updatedAt.Add(-time.Hour)))
	router.ServeHTTP(staleDelete, req)
	if staleDelete.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale delete status = %d", staleDelete.Code)
	}
	if repo.deleted != "" {
		t.Fatalf("stale delete must not delete, got %q", repo.deleted)
	}

	fresh := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/network/devices/device-a", nil)
	req.Header.Set("If-Match", etag)
	router.ServeHTTP(fresh, req)
	if fresh.Code != http.StatusNoContent {
		t.Fatalf("fresh delete status = %d, body = %s", fresh.Code, fresh.Body.String())
	}
	if repo.deleted != "device-a" {
		t.Fatalf("fresh delete did not remove device, got %q", repo.deleted)
	}
}

func TestNetworkPortIfMatchOptimisticLocking(t *testing.T) {
	updatedAt := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	repo := &fakeNetworkRepository{
		devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		ports: []NetworkPort{{
			ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 1, IfName: "ge-0/0/1", UpdatedAt: updatedAt,
		}},
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: networkConfigureAuth, Network: repo})

	get := httptest.NewRecorder()
	router.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/v1/network/ports/port-a", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", get.Code, get.Body.String())
	}
	etag := get.Header().Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag on port GET")
	}

	stale := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/network/ports/port-a",
		strings.NewReader(`{"IfName":"renamed"}`))
	req.Header.Set("If-Match", WeakETagFromTime(updatedAt.Add(-time.Hour)))
	router.ServeHTTP(stale, req)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale port patch status = %d, body = %s", stale.Code, stale.Body.String())
	}

	fresh := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPatch, "/api/v1/network/ports/port-a",
		strings.NewReader(`{"IfName":"renamed"}`))
	req.Header.Set("If-Match", etag)
	router.ServeHTTP(fresh, req)
	if fresh.Code != http.StatusOK {
		t.Fatalf("fresh port patch status = %d, body = %s", fresh.Code, fresh.Body.String())
	}
}
