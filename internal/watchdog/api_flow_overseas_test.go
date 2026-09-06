package watchdog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

type flowOverseasAPIRunnerStub struct {
	compiled flowquery.CompiledOverseas
	result   flowquery.OverseasResult
}

func (s *flowOverseasAPIRunnerStub) Run(_ context.Context, compiled flowquery.CompiledOverseas) (flowquery.OverseasResult, error) {
	s.compiled = compiled
	return s.result, nil
}

func TestFlowOverseasQueryInjectsTenantAndReturnsTypedEnvelope(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	runner := &flowOverseasAPIRunnerStub{result: flowquery.OverseasResult{
		GeoLevel: flowquery.OverseasGeoCountry, TopN: 20, IncludeOther: true,
		Points: []flowquery.OverseasPoint{{
			Kind: flowquery.OverseasRowGeo, GeoScope: flowquery.OverseasScopeOverseas,
			GeoVersion: "test-1", GeoValue: "810000",
		}},
	}}
	geo := NewFlowGeoService(writeFlowGeoBundle(t))
	if err := geo.Reload(); err != nil {
		t.Fatal(err)
	}
	audit := &recordingAuditRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: true}, nil
		},
		FlowOverseas: runner, FlowOverseasNow: func() time.Time { return now }, FlowGeo: geo, Audit: audit,
	})
	body := `{"from":"2026-09-07T11:00:00Z","to":"2026-09-07T12:00:00Z","bucket":"1m","metric":"estimated_bps","geo_level":"country","view":"customer","top_n":20,"include_other":true}`
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/flow/overseas/query", strings.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if runner.compiled.Query.Body == "" || runner.compiled.From != now.Add(-time.Hour) || runner.compiled.GeoLevel != flowquery.OverseasGeoCountry {
		t.Fatalf("compiled=%+v", runner.compiled)
	}
	if !strings.Contains(response.Body.String(), `"source":"1m"`) || !strings.Contains(response.Body.String(), `"step_seconds":60`) {
		t.Fatalf("body=%s", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"test-1:810000":{"code":"810000","name":"香港","breadcrumb":["香港"]}`) {
		t.Fatalf("geo labels missing from body=%s", response.Body.String())
	}
	if len(audit.logs) != 1 || audit.logs[0].TenantID != "tenant-a" || audit.logs[0].Action != "query.executed" {
		t.Fatalf("audit=%+v", audit.logs)
	}
}

func TestFlowOverseasQueryEnforcesPermissionResourcesAndStrictInput(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	runner := &flowOverseasAPIRunnerStub{}
	auth := func(*http.Request) (AuthContext, error) {
		return AuthContext{TenantID: "tenant-a", UserID: "user-a"}, nil
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: auth, FlowOverseas: runner, FlowOverseasNow: func() time.Time { return now }})
	base := `{"from":"2026-09-07T11:00:00Z","to":"2026-09-07T12:00:00Z","bucket":"1m","metric":"estimated_bps","geo_level":"country","view":"customer","top_n":20}`

	denied := httptest.NewRecorder()
	router.ServeHTTP(denied, httptest.NewRequest(http.MethodPost, "/api/v1/flow/overseas/query", strings.NewReader(base)))
	if denied.Code != http.StatusForbidden || runner.compiled.Query.Body != "" {
		t.Fatalf("denied status=%d body=%s", denied.Code, denied.Body.String())
	}

	adminRouter := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: true}, nil
		},
		FlowOverseas: runner, FlowOverseasNow: func() time.Time { return now },
	})
	unknown := httptest.NewRecorder()
	adminRouter.ServeHTTP(unknown, httptest.NewRequest(http.MethodPost, "/api/v1/flow/overseas/query", strings.NewReader(strings.Replace(base, `"from":`, `"tenant_id":"tenant-b","from":`, 1))))
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown status=%d body=%s", unknown.Code, unknown.Body.String())
	}
}
