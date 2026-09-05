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
	runtime      CollectorRuntimeHeartbeatReport
	fetchCalls   int
	ackCalls     int
	failureCalls int
	runtimeCalls int
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

func (c *collectorPlanDeliveryControllerStub) ReportRuntime(_ context.Context, collectorID ID, credential CollectorMachineCredential, report CollectorRuntimeHeartbeatReport) error {
	c.collectorID, c.credential, c.runtime = collectorID, credential, report
	c.runtimeCalls++
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
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"schema_version":1,"kind":"plan_failure","plan_failure":{"boot_id":"boot-a","stage":"unknown","code":"FAILED","detail":"failed"}}`))
	req.Header.Set("X-Watchdog-Agent-Token", "token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || controller.failureCalls != 0 {
		t.Fatalf("invalid failure status=%d calls=%d body=%s", rec.Code, controller.failureCalls, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"schema_version":1,"kind":"plan_failure","plan_failure":{"boot_id":"boot-a","stage":"verify","code":"SIGNATURE_INVALID","detail":"failed"}}`))
	req.Header.Set("X-Watchdog-Agent-Token", "token")
	req.Header.Set("Authorization", "Bearer token")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || controller.failureCalls != 0 {
		t.Fatalf("mixed credentials status=%d calls=%d body=%s", rec.Code, controller.failureCalls, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"schema_version":1,"kind":"plan_failure","plan_failure":{"failed_config_version":7,"boot_id":"boot-a","software_version":"1.0.0","stage":"verify","code":"SIGNATURE_INVALID","detail":"signature verification failed"}}`))
	req.Header.Set("Authorization", "Bearer token")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || controller.failureCalls != 1 || controller.failure.FailedConfigVersion != 7 {
		t.Fatalf("failure status=%d calls=%d failure=%+v body=%s", rec.Code, controller.failureCalls, controller.failure, rec.Body.String())
	}
}

func TestCollectorPlanDeliveryAPIAcceptsStrictRuntimeHeartbeat(t *testing.T) {
	controller := &collectorPlanDeliveryControllerStub{}
	router := NewAPIV1Router(APIV1RouterConfig{CollectorPlans: controller})
	path := "/api/v1/collectors/collector-a/heartbeat"
	body := `{"schema_version":1,"kind":"runtime","runtime":` + collectorRuntimeHeartbeatFixtureJSON() + `}`
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("X-Watchdog-Agent-Token", "token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || controller.runtimeCalls != 1 || controller.runtime.Sequence != 1 || controller.runtime.BootID != "boot-a" || controller.collectorID != "collector-a" || controller.credential.Token != "token" {
		t.Fatalf("runtime status=%d calls=%d report=%+v body=%s", rec.Code, controller.runtimeCalls, controller.runtime, rec.Body.String())
	}

	for _, invalid := range []string{
		`{"schema_version":1,"kind":"runtime","runtime":` + collectorRuntimeHeartbeatFixtureJSON() + `,"plan_failure":{"boot_id":"boot-a"}}`,
		`{"schema_version":1,"kind":"runtime","runtime":` + collectorRuntimeHeartbeatFixtureJSON() + `,"tenant_id":"forbidden"}`,
		`{"kind":"runtime","runtime":` + collectorRuntimeHeartbeatFixtureJSON() + `}`,
		`{"schema_version":2,"kind":"runtime","runtime":` + collectorRuntimeHeartbeatFixtureJSON() + `}`,
		`{"schema_version":1,"kind":"unknown"}`,
	} {
		req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(invalid))
		req.Header.Set("X-Watchdog-Agent-Token", "token")
		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("invalid heartbeat status=%d body=%s request=%s", rec.Code, rec.Body.String(), invalid)
		}
	}
	if controller.runtimeCalls != 1 || controller.failureCalls != 0 {
		t.Fatalf("invalid heartbeat reached controller runtime=%d failure=%d", controller.runtimeCalls, controller.failureCalls)
	}
	oversized := body + strings.Repeat(" ", collectorEvidenceRequestMaxBytes-len(body)+1)
	req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(oversized))
	req.Header.Set("X-Watchdog-Agent-Token", "token")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || controller.runtimeCalls != 1 {
		t.Fatalf("oversized heartbeat status=%d calls=%d body=%s", rec.Code, controller.runtimeCalls, rec.Body.String())
	}
	controller.err = ErrCollectorHeartbeatFenced
	req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("X-Watchdog-Agent-Token", "token")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed || controller.runtimeCalls != 2 {
		t.Fatalf("fenced heartbeat status=%d calls=%d body=%s", rec.Code, controller.runtimeCalls, rec.Body.String())
	}
}

func collectorRuntimeHeartbeatFixtureJSON() string {
	return `{"schema_version":1,"sequence":1,"sent_at_unix_ms":2000000000000,"boot_id":"boot-a","software_version":"1.0.0","agent_api_version":1,"plan_schema_min":1,"plan_schema_max":1,"active_config_version":7,"active_spec_hash":"` + strings.Repeat("a", 64) + `","capabilities":{"schema_version":1,"protocols":["ipfix","netflow5","netflow9","sflow5"],"plan_envelope_versions":[2]},"observation":{"running":true,"uptime_seconds":10,"plan_accepting":true,"plan_using_lkg":false,"control_plane_healthy":true,"kafka_healthy":true,"queues":{"kafka":{"depth":2,"capacity":20}},"counters":{"received_datagrams":100,"rejected_sources":1,"invalid_datagrams":0,"udp_kernel_drops":2,"kafka_records":99,"kafka_bytes":4096,"publish_failures":4}}}`
}
