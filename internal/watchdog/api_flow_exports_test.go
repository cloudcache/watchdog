package watchdog

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAPIFlowExportsCreatesTenantScopedOperationExport(t *testing.T) {
	repo := &fakeExportRepository{}
	gateway := newFlowExportGatewayFixture(t, &queryProviderStub{})
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    func(*http.Request) (AuthContext, error) { return flowExportAuth(), nil },
		Exports: repo, QueryGateway: gateway,
	})
	rec := httptest.NewRecorder()
	body := `{"query":{"dataset":"flow.traffic","from":"2026-09-07T01:00:00Z","to":"2026-09-07T02:00:00Z","limit":250000,"value_layer":"customer","parameters":{"metric":"estimated_bps","dimension":"geo.country","top_n":20,"include_other":true,"target_points":300,"timezone":"UTC"}},"format":"csv"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/flow/exports", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if len(repo.tasks) != 1 || repo.tasks[0].DatasetKey != FlowTrafficDataset || repo.tasks[0].TargetID != "" || repo.tasks[0].CreatedBy != "user-a" {
		t.Fatalf("tasks = %+v", repo.tasks)
	}
	if !strings.Contains(rec.Body.String(), `"DatasetKey":"flow.traffic"`) {
		t.Fatalf("body=%s", rec.Body.String())
	}
}

func TestAPIFlowExportsRejectsMissingTenantExportPermission(t *testing.T) {
	repo := &fakeExportRepository{}
	gateway := newFlowExportGatewayFixture(t, &queryProviderStub{})
	auth := flowExportAuth()
	auth.Grants[0].Actions = []Action{ActionViewCustomer}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    func(*http.Request) (AuthContext, error) { return auth, nil },
		Exports: repo, QueryGateway: gateway,
	})
	rec := httptest.NewRecorder()
	body := `{"query":{"dataset":"flow.traffic","from":"2026-09-07T01:00:00Z","to":"2026-09-07T02:00:00Z","value_layer":"customer","parameters":{"metric":"estimated_bps","dimension":"geo.country","top_n":20,"include_other":true,"target_points":300}},"format":"csv"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/flow/exports", strings.NewReader(body)))
	if rec.Code != http.StatusForbidden || len(repo.tasks) != 0 {
		t.Fatalf("status=%d tasks=%+v body=%s", rec.Code, repo.tasks, rec.Body.String())
	}
}
