package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type collectorPlanRolloutControllerStub struct {
	created        CollectorPlanRollout
	preview        CollectorPlanRolloutPreview
	items          []CollectorPlanRollout
	targets        []CollectorPlanRolloutTarget
	summary        CollectorPlanRolloutSummary
	total          int64
	err            error
	createRequest  CollectorPlanRolloutCreateRequest
	previewRequest CollectorPlanRolloutPreviewRequest
	listTenant     ID
	getTenant      ID
	getRollout     ID
	targetTenant   ID
	targetRollout  ID
	listFilter     CollectorPlanRolloutListFilter
	targetFilter   CollectorPlanRolloutTargetListFilter
	createCalls    int
	previewCalls   int
	listCalls      int
	getCalls       int
	targetCalls    int
}

func (c *collectorPlanRolloutControllerStub) ListCollectorPlanRollouts(_ context.Context, tenantID ID, filter CollectorPlanRolloutListFilter) ([]CollectorPlanRollout, int64, error) {
	c.listTenant, c.listFilter = tenantID, filter
	c.listCalls++
	return c.items, c.total, c.err
}

func (c *collectorPlanRolloutControllerStub) GetCollectorPlanRollout(_ context.Context, tenantID, rolloutID ID) (CollectorPlanRollout, CollectorPlanRolloutSummary, error) {
	c.getTenant, c.getRollout = tenantID, rolloutID
	c.getCalls++
	return c.created, c.summary, c.err
}

func (c *collectorPlanRolloutControllerStub) ListCollectorPlanRolloutTargets(_ context.Context, tenantID, rolloutID ID, filter CollectorPlanRolloutTargetListFilter) ([]CollectorPlanRolloutTarget, int64, error) {
	c.targetTenant, c.targetRollout, c.targetFilter = tenantID, rolloutID, filter
	c.targetCalls++
	return c.targets, c.total, c.err
}

func (c *collectorPlanRolloutControllerStub) CreateCollectorPlanRollout(_ context.Context, request CollectorPlanRolloutCreateRequest) (CollectorPlanRollout, error) {
	c.createRequest = request
	c.createCalls++
	return c.created, c.err
}

func (c *collectorPlanRolloutControllerStub) PreviewCollectorPlanRollout(_ context.Context, request CollectorPlanRolloutPreviewRequest) (CollectorPlanRolloutPreview, error) {
	c.previewRequest = request
	c.previewCalls++
	return c.preview, c.err
}

func TestCollectorPlanRolloutAPICreateInjectsIdentityAndHidesSpec(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	created := collectorPlanRolloutAPIRecord(t, now, CollectorPlanRolloutDraft, 1)
	controller := &collectorPlanRolloutControllerStub{created: created}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPlanManagementAuth(true), PlanRollouts: controller})
	body := `{"selector":{"module_key":"flow"},"plan_schema_version":1,"spec":{"secret_ref":"secret://hidden"},"strategy":{"canary_count":1,"wave_size":10,"min_soak_seconds":600,"failure_budget":1},"expires_at":"` + now.Add(time.Hour).Format(time.RFC3339Nano) + `"}`
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/plan-rollouts", strings.NewReader(body)))
	if recorder.Code != http.StatusCreated || recorder.Header().Get("ETag") != `"1"` || controller.createCalls != 1 {
		t.Fatalf("status=%d etag=%q calls=%d body=%s", recorder.Code, recorder.Header().Get("ETag"), controller.createCalls, recorder.Body.String())
	}
	if controller.createRequest.TenantID != "tenant-a" || controller.createRequest.ActorID != "user-a" || controller.createRequest.Selector.ModuleKey != "flow" || string(controller.createRequest.SpecJSON) != `{"secret_ref":"secret://hidden"}` {
		t.Fatalf("request=%+v", controller.createRequest)
	}
	if strings.Contains(recorder.Body.String(), "secret://hidden") || strings.Contains(recorder.Body.String(), `"spec":`) {
		t.Fatalf("response leaked plan spec: %s", recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	forbidden := strings.TrimSuffix(body, "}") + `,"tenant_id":"other"}`
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/plan-rollouts", strings.NewReader(forbidden)))
	if recorder.Code != http.StatusBadRequest || controller.createCalls != 1 {
		t.Fatalf("identity injection status=%d calls=%d body=%s", recorder.Code, controller.createCalls, recorder.Body.String())
	}
}

func TestCollectorPlanRolloutAPIPreviewRequiresVersionAndInjectsIdentity(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	rollout := collectorPlanRolloutAPIRecord(t, now, CollectorPlanRolloutPreviewed, 2)
	rollout.PreviewedAt = now
	controller := &collectorPlanRolloutControllerStub{preview: CollectorPlanRolloutPreview{
		Rollout: rollout, MatchedCount: 5, EligibleCount: 4, SkippedCount: 1, WaveCount: 2,
	}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPlanManagementAuth(true), PlanRollouts: controller})
	path := "/api/v1/plan-rollouts/rollout-a/preview"
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, nil))
	if recorder.Code != http.StatusPreconditionRequired || controller.previewCalls != 0 {
		t.Fatalf("missing version status=%d calls=%d", recorder.Code, controller.previewCalls)
	}
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
	request.Header.Set("If-Match", `"1"`)
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || recorder.Header().Get("ETag") != `"2"` || controller.previewCalls != 1 || controller.previewRequest.TenantID != "tenant-a" || controller.previewRequest.RolloutID != "rollout-a" || controller.previewRequest.ActorID != "user-a" || controller.previewRequest.ExpectedRowVersion != 1 || !strings.Contains(recorder.Body.String(), `"matched_count":5`) {
		t.Fatalf("status=%d request=%+v body=%s", recorder.Code, controller.previewRequest, recorder.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"force":true}`))
	request.Header.Set("If-Match", `"2"`)
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || controller.previewCalls != 1 {
		t.Fatalf("unknown field status=%d calls=%d", recorder.Code, controller.previewCalls)
	}
}

func TestCollectorPlanRolloutAPIPermissionsAndErrors(t *testing.T) {
	controller := &collectorPlanRolloutControllerStub{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPlanManagementAuth(false), PlanRollouts: controller})
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/api/v1/plan-rollouts", nil),
		httptest.NewRequest(http.MethodGet, "/api/v1/plan-rollouts/rollout-a", nil),
		httptest.NewRequest(http.MethodGet, "/api/v1/plan-rollouts/rollout-a/targets", nil),
		httptest.NewRequest(http.MethodPost, "/api/v1/plan-rollouts", strings.NewReader(`{}`)),
		httptest.NewRequest(http.MethodPost, "/api/v1/plan-rollouts/rollout-a/preview", nil),
	} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("%s status=%d body=%s", request.URL.Path, recorder.Code, recorder.Body.String())
		}
	}
	for _, test := range []struct {
		err  error
		want int
	}{
		{ErrCollectorPlanRolloutInvalid, http.StatusBadRequest},
		{sql.ErrNoRows, http.StatusNotFound},
		{ErrCollectorPlanRolloutConflict, http.StatusPreconditionFailed},
		{ErrCollectorPlanRolloutEmpty, http.StatusConflict},
		{ErrCollectorPlanInvalidTransition, http.StatusConflict},
		{errors.New("private database detail"), http.StatusServiceUnavailable},
	} {
		recorder := httptest.NewRecorder()
		writeCollectorPlanRolloutError(recorder, test.err)
		if recorder.Code != test.want || strings.Contains(recorder.Body.String(), "private database detail") {
			t.Fatalf("error=%v status=%d body=%s", test.err, recorder.Code, recorder.Body.String())
		}
	}
}

func TestCollectorPlanRolloutAPIReadEndpointsUseServerFiltersAndHideSpec(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	rollout := collectorPlanRolloutAPIRecord(t, now, CollectorPlanRolloutPreviewed, 2)
	rollout.PreviewedAt = now
	controller := &collectorPlanRolloutControllerStub{
		created: rollout, items: []CollectorPlanRollout{rollout}, total: 7,
		summary: CollectorPlanRolloutSummary{Matched: 6, Eligible: 5, Pending: 5, Skipped: 1},
		targets: []CollectorPlanRolloutTarget{{
			TenantID: "tenant-a", RolloutID: "rollout-a", CollectorID: "collector-a",
			CollectorName: "edge-a", AgentType: "flow-collect", CollectorStatus: "active",
			ObservedHealth: "healthy", Wave: CollectorPlanRolloutSkippedWave,
			PriorConfigVersion: 4, Status: CollectorPlanRolloutTargetSkipped,
			FailureReason: "schema mismatch", RowVersion: 1, CreatedAt: now, UpdatedAt: now,
		}},
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPlanManagementAuth(true), PlanRollouts: controller})

	listPath := "/api/v1/plan-rollouts?q=abc&module_key=flow&status=previewed&sort=expires_at&order=asc&limit=20&offset=40"
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, listPath, nil))
	if recorder.Code != http.StatusOK || controller.listCalls != 1 || controller.listTenant != "tenant-a" || controller.listFilter.Query != "abc" || controller.listFilter.ModuleKey != "flow" || controller.listFilter.Status != CollectorPlanRolloutPreviewed || controller.listFilter.Sort != "expires_at" || controller.listFilter.Desc || controller.listFilter.Limit != 20 || controller.listFilter.Offset != 40 || !strings.Contains(recorder.Body.String(), `"total":7`) || strings.Contains(recorder.Body.String(), "secret://hidden") || strings.Contains(recorder.Body.String(), `"spec":`) {
		t.Fatalf("list status=%d filter=%+v body=%s", recorder.Code, controller.listFilter, recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/plan-rollouts/rollout-a", nil))
	if recorder.Code != http.StatusOK || recorder.Header().Get("ETag") != `"2"` || controller.getTenant != "tenant-a" || controller.getRollout != "rollout-a" || !strings.Contains(recorder.Body.String(), `"eligible":5`) || strings.Contains(recorder.Body.String(), "secret://hidden") {
		t.Fatalf("get status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	targetPath := "/api/v1/plan-rollouts/rollout-a/targets?q=edge&status=skipped&health=healthy&wave=skipped&sort=collector&order=desc&limit=10&offset=20"
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, targetPath, nil))
	if recorder.Code != http.StatusOK || controller.targetCalls != 1 || controller.targetTenant != "tenant-a" || controller.targetRollout != "rollout-a" || controller.targetFilter.Wave == nil || *controller.targetFilter.Wave != CollectorPlanRolloutSkippedWave || controller.targetFilter.Status != CollectorPlanRolloutTargetSkipped || controller.targetFilter.Health != "healthy" || !controller.targetFilter.Desc || !strings.Contains(recorder.Body.String(), `"failure_reason":"schema mismatch"`) || strings.Contains(recorder.Body.String(), `"config_version":0`) {
		t.Fatalf("targets status=%d filter=%+v body=%s", recorder.Code, controller.targetFilter, recorder.Body.String())
	}
}

func TestCollectorPlanRolloutAPIReadRejectsMalformedFilters(t *testing.T) {
	controller := &collectorPlanRolloutControllerStub{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPlanManagementAuth(true), PlanRollouts: controller})
	for _, path := range []string{
		"/api/v1/plan-rollouts?status=bad",
		"/api/v1/plan-rollouts?sort=spec_json",
		"/api/v1/plan-rollouts?limit=0",
		"/api/v1/plan-rollouts?order=sideways",
		"/api/v1/plan-rollouts/rollout-a/targets?wave=-1",
		"/api/v1/plan-rollouts/rollout-a/targets?health=up",
	} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("path=%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
	if controller.listCalls != 0 || controller.targetCalls != 0 {
		t.Fatalf("malformed filters reached controller: list=%d target=%d", controller.listCalls, controller.targetCalls)
	}
}

func collectorPlanRolloutAPIRecord(t *testing.T, now time.Time, status CollectorPlanRolloutStatus, rowVersion uint64) CollectorPlanRollout {
	t.Helper()
	selector, selectorHash, err := canonicalCollectorPlanRolloutValue(CollectorPlanRolloutSelector{ModuleKey: "flow", Status: "active"})
	if err != nil {
		t.Fatal(err)
	}
	strategy, strategyHash, err := canonicalCollectorPlanRolloutValue(CollectorPlanRolloutStrategy{CanaryCount: 1, WaveSize: 10, MinSoakSeconds: 600, FailureBudget: 0})
	if err != nil {
		t.Fatal(err)
	}
	spec, specHash, err := CanonicalCollectorPlanJSON(json.RawMessage(`{"secret_ref":"secret://hidden"}`))
	if err != nil {
		t.Fatal(err)
	}
	return CollectorPlanRollout{
		ID: "rollout-a", TenantID: "tenant-a", ModuleKey: "flow", RolloutSchemaVersion: CollectorPlanRolloutSchemaVersion,
		SelectorJSON: selector, SelectorHash: selectorHash, SpecJSON: spec, SpecHash: specHash,
		PlanSchemaVersion: 1, StrategyJSON: strategy, StrategyHash: strategyHash,
		Status: status, ExpiresAt: now.Add(time.Hour), RowVersion: rowVersion,
		CreatedBy: "user-a", UpdatedBy: "user-a", CreatedAt: now, UpdatedAt: now,
	}
}
