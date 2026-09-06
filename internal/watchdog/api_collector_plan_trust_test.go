package watchdog

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type collectorPlanTrustControllerStub struct {
	delivery    CollectorPlanTrustBundleDelivery
	collectorID ID
	credential  CollectorMachineCredential
	calls       int
	err         error
}

func (c *collectorPlanTrustControllerStub) FetchTrustBundle(_ context.Context, collectorID ID, credential CollectorMachineCredential) (CollectorPlanTrustBundleDelivery, error) {
	c.collectorID, c.credential = collectorID, credential
	c.calls++
	return c.delivery, c.err
}

func TestCollectorPlanTrustAPIUsesMachineAuthAndMonotonicHeaders(t *testing.T) {
	controller := &collectorPlanTrustControllerStub{delivery: CollectorPlanTrustBundleDelivery{
		Payload: []byte(`{"schema_version":1,"generation":3}`), ETag: `"g3-deadbeef"`, Generation: 3, Checksum: "deadbeef",
	}}
	router := NewAPIV1Router(APIV1RouterConfig{CollectorPlanTrust: controller})
	path := "/api/v1/collectors/collector-a/trust-bundle"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("X-Watchdog-Agent-Token", "secret-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != string(controller.delivery.Payload) ||
		rec.Header().Get("ETag") != controller.delivery.ETag || rec.Header().Get("X-Watchdog-Trust-Generation") != "3" ||
		rec.Header().Get("X-Watchdog-Trust-Checksum") != "deadbeef" || controller.collectorID != "collector-a" ||
		controller.credential.Token != "secret-token" {
		t.Fatalf("status=%d headers=%v delivery=%+v calls=%d body=%s", rec.Code, rec.Header(), controller.delivery, controller.calls, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("If-None-Match", controller.delivery.ETag)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified || rec.Body.Len() != 0 || controller.calls != 2 {
		t.Fatalf("not-modified status=%d calls=%d body=%s", rec.Code, controller.calls, rec.Body.String())
	}
}

func TestCollectorPlanTrustAPIRejectsBadIdentityAndMapsUnavailable(t *testing.T) {
	controller := &collectorPlanTrustControllerStub{}
	router := NewAPIV1Router(APIV1RouterConfig{CollectorPlanTrust: controller})
	path := "/api/v1/collectors/collector-a/trust-bundle"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || controller.calls != 0 {
		t.Fatalf("unauthorized status=%d calls=%d body=%s", rec.Code, controller.calls, rec.Body.String())
	}
	controller.err = errors.New("database unavailable")
	req = httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("X-Watchdog-Agent-Token", "secret-token")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || controller.calls != 1 {
		t.Fatalf("unavailable status=%d calls=%d body=%s", rec.Code, controller.calls, rec.Body.String())
	}
}
