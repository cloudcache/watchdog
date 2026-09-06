package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

type flowDetailAPIRunnerStub struct {
	compiled flowquery.CompiledDetail
	result   flowquery.DetailResult
	facet    flowquery.CompiledDetailFacet
	items    flowquery.DetailFacetResult
	err      error
}

func (s *flowDetailAPIRunnerStub) Run(_ context.Context, compiled flowquery.CompiledDetail) (flowquery.DetailResult, error) {
	s.compiled = compiled
	return s.result, s.err
}

func (s *flowDetailAPIRunnerStub) RunFacet(_ context.Context, compiled flowquery.CompiledDetailFacet) (flowquery.DetailFacetResult, error) {
	s.facet = compiled
	return s.items, s.err
}

func TestFlowRecordSearchInjectsTenantAndReturnsCursorEnvelope(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	runner := &flowDetailAPIRunnerStub{result: flowquery.DetailResult{
		View: flowquery.ViewCustomer, Fields: []flowquery.DetailField{flowquery.DetailFieldSourceIP},
		Rows:    []flowquery.DetailRow{{EventTime: now.Add(-time.Minute), RecordID: strings.Repeat("a", 64), Values: map[flowquery.DetailField]any{flowquery.DetailFieldSourceIP: "203.0.113.1"}}},
		HasMore: true, NextCursor: "v1.cursor",
	}}
	audit := &recordingAuditRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: true}, nil
		},
		FlowRecords: runner, FlowRecordNow: func() time.Time { return now }, Audit: audit,
	})
	body := `{
		"ip":"203.0.113.1","endpoint":"source",
		"from":"2026-09-07T11:00:00Z","to":"2026-09-07T12:00:00Z",
		"view":"customer","fields":["src_ip"],"limit":100
	}`
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/flow/records/search", strings.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if runner.compiled.Query.Body == "" || runner.compiled.View != flowquery.ViewCustomer || runner.compiled.Limit != 100 {
		t.Fatalf("compiled=%+v", runner.compiled)
	}
	var envelope struct {
		Data flowquery.DetailResult `json:"data"`
		Meta struct {
			Sort     string `json:"sort"`
			PageSize uint16 `json:"page_size"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.Data.HasMore || envelope.Data.NextCursor != "v1.cursor" || envelope.Meta.PageSize != 100 || envelope.Meta.Sort != "event_time:desc,record_id:desc" {
		t.Fatalf("envelope=%+v", envelope)
	}
	if len(audit.logs) != 1 || audit.logs[0].TenantID != "tenant-a" || audit.logs[0].Action != "query.sensitive_viewed" {
		t.Fatalf("audit=%+v", audit.logs)
	}
}

func TestFlowRecordSearchEnforcesLayerPermissionAndStrictInput(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	runner := &flowDetailAPIRunnerStub{}
	auth := func(*http.Request) (AuthContext, error) {
		return AuthContext{
			TenantID: "tenant-a", UserID: "user-a",
			Grants: []Permission{{
				TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: "user-a",
				ResourceType: ResourceTenant, ResourceID: "tenant-a", Actions: []Action{ActionViewCustomer},
			}},
		}, nil
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: auth, FlowRecords: runner, FlowRecordNow: func() time.Time { return now }})
	requestBody := func(view, suffix string) string {
		return `{"ip":"203.0.113.1","endpoint":"either","from":"2026-09-07T11:00:00Z","to":"2026-09-07T12:00:00Z","view":"` + view + `","limit":50` + suffix + `}`
	}

	denied := httptest.NewRecorder()
	router.ServeHTTP(denied, httptest.NewRequest(http.MethodPost, "/api/v1/flow/records/search", strings.NewReader(requestBody("raw", ""))))
	if denied.Code != http.StatusForbidden || runner.compiled.Query.Body != "" {
		t.Fatalf("denied status=%d body=%s compiled=%+v", denied.Code, denied.Body.String(), runner.compiled)
	}

	unknown := httptest.NewRecorder()
	router.ServeHTTP(unknown, httptest.NewRequest(http.MethodPost, "/api/v1/flow/records/search", strings.NewReader(requestBody("customer", `,"tenant_id":"tenant-b"`))))
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown status=%d body=%s", unknown.Code, unknown.Body.String())
	}

	sorted := httptest.NewRecorder()
	router.ServeHTTP(sorted, httptest.NewRequest(http.MethodPost, "/api/v1/flow/records/search", strings.NewReader(requestBody("customer", `,"fields":["src_ip"],"sort":{"field":"src_ip","direction":"asc"}`))))
	if sorted.Code != http.StatusOK || runner.compiled.Sort.Field != "src_ip" || runner.compiled.Sort.Direction != "asc" ||
		!strings.Contains(sorted.Body.String(), `"sort":"src_ip:asc,event_time:asc,record_id:asc"`) {
		t.Fatalf("sorted status=%d body=%s compiled=%+v", sorted.Code, sorted.Body.String(), runner.compiled)
	}

	capabilities := httptest.NewRecorder()
	router.ServeHTTP(capabilities, httptest.NewRequest(http.MethodGet, "/api/v1/flow/records/capabilities", nil))
	if capabilities.Code != http.StatusOK || !strings.Contains(capabilities.Body.String(), `"default_fields"`) {
		t.Fatalf("capabilities status=%d body=%s", capabilities.Code, capabilities.Body.String())
	}
}

func TestFlowRecordFacetsInjectTenantAndAuthorizeColumnResources(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	runner := &flowDetailAPIRunnerStub{items: flowquery.DetailFacetResult{
		Field: "remote_country", Items: []flowquery.DetailFacetOption{{Value: "CN", Count: 42}},
	}}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: true}, nil
		},
		FlowRecords: runner, FlowRecordNow: func() time.Time { return now },
	})
	body := `{
		"ip":"203.0.113.1","endpoint":"source","from":"2026-09-07T11:00:00Z","to":"2026-09-07T12:00:00Z",
		"view":"customer","field":"remote_country","column_filters":[{"field":"src_port","values":["443"]}],"limit":50
	}`
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/flow/records/facets", strings.NewReader(body)))
	if response.Code != http.StatusOK || runner.facet.Query.Body == "" || runner.facet.Field != "remote_country" ||
		!strings.Contains(response.Body.String(), `"value":"CN"`) {
		t.Fatalf("status=%d body=%s compiled=%+v", response.Code, response.Body.String(), runner.facet)
	}
}
