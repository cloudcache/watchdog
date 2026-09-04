package watchdog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type collectorPlanDeliveryControllerStub struct {
	delivery     CollectorPlanDelivery
	collectorID  ID
	credential   CollectorMachineCredential
	ack          CollectorPlanAcknowledgement
	failure      CollectorPlanFailureReport
	fetchCalls   int
	ackCalls     int
	failureCalls int
	err          error
}

func (c *collectorPlanDeliveryControllerStub) Fetch(_ context.Context, collectorID ID, credential CollectorMachineCredential) (CollectorPlanDelivery, error) {
	c.collectorID, c.credential = collectorID, credential
	c.fetchCalls++
	return c.delivery, c.err
}

func (c *collectorPlanDeliveryControllerStub) Acknowledge(_ context.Context, collectorID ID, credential CollectorMachineCredential, acknowledgement CollectorPlanAcknowledgement) error {
	c.collectorID, c.credential, c.ack = collectorID, credential, acknowledgement
	c.ackCalls++
	return c.err
}

func (c *collectorPlanDeliveryControllerStub) ReportFailure(_ context.Context, collectorID ID, credential CollectorMachineCredential, report CollectorPlanFailureReport) error {
	c.collectorID, c.credential, c.failure = collectorID, credential, report
	c.failureCalls++
	return c.err
}

func TestCollectorPlanDeliveryAPIUsesVersionedETagAndStrictMachineDTOs(t *testing.T) {
	controller := &collectorPlanDeliveryControllerStub{delivery: CollectorPlanDelivery{
		Envelope: []byte(`{"schema_version":2}`), ETag: `"v7-deadbeef"`, ConfigVersion: 7,
		ExpiresAt: time.Unix(2_000_000_000, 0).UTC(),
	}}
	router := NewAPIV1Router(APIV1RouterConfig{CollectorPlans: controller})
	path := "/api/v1/collectors/collector-a/plan"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("X-Watchdog-Agent-Token", "secret-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Header().Get("ETag") != controller.delivery.ETag || rec.Body.String() != string(controller.delivery.Envelope) || controller.fetchCalls != 1 {
		t.Fatalf("status=%d etag=%q calls=%d body=%s", rec.Code, rec.Header().Get("ETag"), controller.fetchCalls, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("If-None-Match", controller.delivery.ETag)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified || rec.Body.Len() != 0 || controller.fetchCalls != 2 {
		t.Fatalf("304 status=%d calls=%d body=%s", rec.Code, controller.fetchCalls, rec.Body.String())
	}

	ackPath := "/api/v1/collectors/collector-a/plan-ack"
	req = httptest.NewRequest(http.MethodPost, ackPath, strings.NewReader(`{"tenant_id":"forbidden","config_version":7,"spec_hash":"`+strings.Repeat("a", 64)+`","boot_id":"boot-a","software_version":"1.0.0"}`))
	req.Header.Set("X-Watchdog-Agent-Token", "secret-token")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || controller.ackCalls != 0 {
		t.Fatalf("identity injection status=%d calls=%d body=%s", rec.Code, controller.ackCalls, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, ackPath, strings.NewReader(`{"config_version":7,"spec_hash":"`+strings.Repeat("a", 64)+`","boot_id":"boot-a","software_version":"1.0.0"}`))
	req.Header.Set("X-Watchdog-Agent-Token", "secret-token")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || controller.ackCalls != 1 || controller.ack.TenantID != "" || controller.ack.CollectorID != "" {
		t.Fatalf("ack status=%d calls=%d ack=%+v body=%s", rec.Code, controller.ackCalls, controller.ack, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, ackPath, strings.NewReader(`{"config_version":7,"spec_hash":"bad","boot_id":"boot-a"}`))
	req.Header.Set("X-Watchdog-Agent-Token", "secret-token")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || controller.ackCalls != 1 {
		t.Fatalf("invalid ack status=%d calls=%d body=%s", rec.Code, controller.ackCalls, rec.Body.String())
	}
}

func TestCollectorPlanDeliveryAPIRejectsInvalidFailureAndMixedCredentials(t *testing.T) {
	controller := &collectorPlanDeliveryControllerStub{}
	router := NewAPIV1Router(APIV1RouterConfig{CollectorPlans: controller})
	path := "/api/v1/collectors/collector-a/heartbeat"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"boot_id":"boot-a","stage":"unknown","code":"FAILED","detail":"failed"}`))
	req.Header.Set("X-Watchdog-Agent-Token", "token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || controller.failureCalls != 0 {
		t.Fatalf("invalid failure status=%d calls=%d body=%s", rec.Code, controller.failureCalls, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"boot_id":"boot-a","stage":"verify","code":"SIGNATURE_INVALID","detail":"failed"}`))
	req.Header.Set("X-Watchdog-Agent-Token", "token")
	req.Header.Set("Authorization", "Bearer token")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || controller.failureCalls != 0 {
		t.Fatalf("mixed credentials status=%d calls=%d body=%s", rec.Code, controller.failureCalls, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"failed_config_version":7,"boot_id":"boot-a","software_version":"1.0.0","stage":"verify","code":"SIGNATURE_INVALID","detail":"signature verification failed"}`))
	req.Header.Set("Authorization", "Bearer token")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || controller.failureCalls != 1 || controller.failure.FailedConfigVersion != 7 {
		t.Fatalf("failure status=%d calls=%d failure=%+v body=%s", rec.Code, controller.failureCalls, controller.failure, rec.Body.String())
	}
}
