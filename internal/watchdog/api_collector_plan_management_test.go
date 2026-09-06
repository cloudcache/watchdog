package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type collectorPlanManagementControllerStub struct {
	createRequest CollectorPlanCreateRequest
	listTenant    ID
	listCollector ID
	listFilter    CollectorPlanPageFilter
	activation    CollectorPlanActivation
	created       CollectorPlanRevision
	items         []CollectorPlanRevision
	next          string
	err           error
	createCalls   int
	listCalls     int
	activateCalls int
}

func (c *collectorPlanManagementControllerStub) CreateCollectorPlanRevision(_ context.Context, request CollectorPlanCreateRequest) (CollectorPlanRevision, error) {
	c.createRequest = request
	c.createCalls++
	return c.created, c.err
}

func (c *collectorPlanManagementControllerStub) ListCollectorPlanRevisions(_ context.Context, tenantID, collectorID ID, filter CollectorPlanPageFilter) ([]CollectorPlanRevision, string, error) {
	c.listTenant, c.listCollector, c.listFilter = tenantID, collectorID, filter
	c.listCalls++
	return c.items, c.next, c.err
}

func (c *collectorPlanManagementControllerStub) ActivateCollectorPlanRevision(_ context.Context, activation CollectorPlanActivation) (CollectorPlanRevision, error) {
	c.activation = activation
	c.activateCalls++
	return c.created, c.err
}

func TestCollectorPlanManagementAPICreateInjectsIdentityAndHidesEnvelope(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	controller := &collectorPlanManagementControllerStub{created: collectorPlanAPIRecord(now, CollectorPlanValidated, 1)}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPlanManagementAuth(true), PlanManagement: controller})
	body := `{"plan_schema_version":1,"spec":{"kafka":{"topic":"flow"}},"expires_at":"` + now.Add(time.Hour).Format(time.RFC3339Nano) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/collectors/collector-a/plan-revisions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated || rec.Header().Get("ETag") != `"1"` || controller.createCalls != 1 {
		t.Fatalf("status=%d etag=%q calls=%d body=%s", rec.Code, rec.Header().Get("ETag"), controller.createCalls, rec.Body.String())
	}
	if controller.createRequest.TenantID != "tenant-a" || controller.createRequest.CollectorID != "collector-a" || controller.createRequest.ActorID != "user-a" || controller.createRequest.PlanSchemaVersion != 1 || string(controller.createRequest.SpecJSON) != `{"kafka":{"topic":"flow"}}` {
		t.Fatalf("request=%+v", controller.createRequest)
	}
	for _, forbidden := range []string{"spec\"", "signature", "verified", "private_key"} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Fatalf("response leaked %q: %s", forbidden, rec.Body.String())
		}
	}

	body = `{"from_config_version":1,"expires_at":"` + now.Add(time.Hour).Format(time.RFC3339Nano) + `","signature":"forbidden"}`
	req = httptest.NewRequest(http.MethodPost, "/api/v1/collectors/collector-a/plan-revisions", strings.NewReader(body))
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || controller.createCalls != 1 {
		t.Fatalf("signature injection status=%d calls=%d body=%s", rec.Code, controller.createCalls, rec.Body.String())
	}
}

func TestCollectorPlanManagementAPIListUsesKeysetCursor(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	controller := &collectorPlanManagementControllerStub{
		items: []CollectorPlanRevision{collectorPlanAPIRecord(now, CollectorPlanRetired, 7)},
		next:  encodeCollectorPlanCursor(7),
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPlanManagementAuth(true), PlanManagement: controller})
	path := "/api/v1/collectors/collector-a/plan-revisions?limit=10&cursor=" + encodeCollectorPlanCursor(9)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != http.StatusOK || controller.listCalls != 1 || controller.listTenant != "tenant-a" || controller.listCollector != "collector-a" || controller.listFilter.Limit != 10 || controller.listFilter.BeforeVersion != 9 || !strings.Contains(rec.Body.String(), `"next_cursor":"`+controller.next+`"`) {
		t.Fatalf("status=%d controller=%+v body=%s", rec.Code, controller, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/collectors/collector-a/plan-revisions?cursor=bad!", nil))
	if rec.Code != http.StatusBadRequest || controller.listCalls != 1 {
		t.Fatalf("invalid cursor status=%d calls=%d", rec.Code, controller.listCalls)
	}
}

func TestCollectorPlanManagementAPIActivateUsesDualOptimisticLocks(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	controller := &collectorPlanManagementControllerStub{created: collectorPlanAPIRecord(now, CollectorPlanActive, 2)}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPlanManagementAuth(true), PlanManagement: controller})
	path := "/api/v1/collectors/collector-a/plan-revisions/8/activate"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"collector_row_version":4}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionRequired || controller.activateCalls != 0 {
		t.Fatalf("missing precondition status=%d calls=%d", rec.Code, controller.activateCalls)
	}
	req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"collector_row_version":4}`))
	req.Header.Set("If-Match", `"1"`)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || controller.activateCalls != 1 || controller.activation.TenantID != "tenant-a" || controller.activation.CollectorID != "collector-a" || controller.activation.ConfigVersion != 8 || controller.activation.ExpectedCollectorRowVersion != 4 || controller.activation.ExpectedPlanRowVersion != 1 || controller.activation.ActorID != "user-a" {
		t.Fatalf("status=%d activation=%+v body=%s", rec.Code, controller.activation, rec.Body.String())
	}
	controller.err = ErrCollectorPlanConflict
	req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"collector_row_version":4}`))
	req.Header.Set("If-Match", `"1"`)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("conflict status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestCollectorPlanManagementAPIPermissions(t *testing.T) {
	controller := &collectorPlanManagementControllerStub{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPlanManagementAuth(false), PlanManagement: controller})
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/api/v1/collectors/collector-a/plan-revisions", nil),
		httptest.NewRequest(http.MethodPost, "/api/v1/collectors/collector-a/plan-revisions", strings.NewReader(`{}`)),
		httptest.NewRequest(http.MethodPost, "/api/v1/collectors/collector-a/plan-revisions/1/activate", strings.NewReader(`{}`)),
	} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, request)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s %s status=%d body=%s", request.Method, request.URL.Path, rec.Code, rec.Body.String())
		}
	}
}

func TestCollectorPlanManagementAPICreateFailsClosedWithoutSigner(t *testing.T) {
	repository := &collectorPlanManagementRepositoryStub{}
	service, err := NewCollectorPlanManagementService(repository, nil)
	if err != nil {
		t.Fatal(err)
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPlanManagementAuth(true), PlanManagement: service})
	expiresAt := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond).Format(time.RFC3339Nano)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/collectors/collector-a/plan-revisions", strings.NewReader(`{"plan_schema_version":1,"spec":{"a":1},"expires_at":"`+expiresAt+`"}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || repository.createCalls != 0 {
		t.Fatalf("status=%d calls=%d body=%s", rec.Code, repository.createCalls, rec.Body.String())
	}
}

func collectorPlanManagementAuth(admin bool) AuthContextAdapter {
	return func(*http.Request) (AuthContext, error) {
		return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: admin}, nil
	}
}

func collectorPlanAPIRecord(now time.Time, status CollectorPlanStatus, rowVersion uint64) CollectorPlanRevision {
	plan := CollectorPlanRevision{
		ID: "plan-a", TenantID: "tenant-a", CollectorID: "collector-a",
		ConfigVersion: 8, PlanSchemaVersion: 1, Status: status,
		SpecJSON: json.RawMessage(`{"secret_ref":"secret://hidden"}`), SpecHash: strings.Repeat("a", 64),
		SigningKeyID: "key-a", Signature: []byte("hidden-signature"),
		ExpiresAt: now.Add(time.Hour), RowVersion: rowVersion, CreatedAt: now, UpdatedAt: now,
	}
	if status == CollectorPlanActive || status == CollectorPlanRetired {
		plan.ActivatedAt = now
	}
	if status == CollectorPlanRetired {
		plan.RetiredAt = now.Add(time.Minute)
	}
	return plan
}

func TestWriteCollectorPlanManagementErrorDoesNotLeakInternalError(t *testing.T) {
	rec := httptest.NewRecorder()
	writeCollectorPlanManagementError(rec, errors.New("private signer path"))
	if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "private signer path") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
