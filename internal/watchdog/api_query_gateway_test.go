package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestQueryGatewayAPIUsesStableEnvelopeAndRequestID(t *testing.T) {
	repo := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	provider := &queryProviderStub{query: func(_ context.Context, request QueryProviderRequest) (QueryProviderResult, error) {
		if request.TenantID != "tenant-a" || request.ValueLayer != QueryValueCustomer {
			t.Fatalf("provider request = %#v", request)
		}
		return QueryProviderResult{Data: json.RawMessage(`[{"value":1}]`), Unit: "bytes"}, nil
	}}
	gateway, _ := newQueryGatewayFixture(t, repo, nil, provider, 2)
	registries := &PlatformRegistries{Modules: gateway.Modules, Datasets: gateway.Datasets}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: true}, nil
		},
		QueryGateway: gateway, QueryPolicies: repo, Registries: registries,
	})
	body := `{
		"dataset":"query.test","from":"2026-08-24T11:00:00Z","to":"2026-08-24T12:00:00Z",
		"value_layer":"customer","parameters":{"dimension":"category"}
	}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/query", strings.NewReader(body))
	request.Header.Set(RequestIDHeader, "query-request-a")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get(RequestIDHeader) != "query-request-a" {
		t.Fatalf("status=%d request-id=%q body=%s", response.Code, response.Header().Get(RequestIDHeader), response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"schema_version":"query-result-v2"`) ||
		!strings.Contains(response.Body.String(), `"request_id":"query-request-a"`) ||
		!strings.Contains(response.Body.String(), `"source":"clickhouse"`) {
		t.Fatalf("body = %s", response.Body.String())
	}

	unknown := httptest.NewRequest(http.MethodPost, "/api/v1/query", strings.NewReader(strings.Replace(body, `"parameters":`, `"tenant_id":"tenant-b","parameters":`, 1)))
	unknownResponse := httptest.NewRecorder()
	router.ServeHTTP(unknownResponse, unknown)
	if unknownResponse.Code != http.StatusBadRequest || !strings.Contains(unknownResponse.Body.String(), `QUERY_INVALID`) {
		t.Fatalf("unknown field status=%d body=%s", unknownResponse.Code, unknownResponse.Body.String())
	}
}

func TestFlowQueryAPIInjectsTheFlowDatasetAndRejectsOtherDatasets(t *testing.T) {
	repo := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	provider := &queryProviderStub{query: func(_ context.Context, request QueryProviderRequest) (QueryProviderResult, error) {
		if request.Dataset.Key != FlowTrafficDataset || request.TenantID != "tenant-a" {
			t.Fatalf("provider request = %#v", request)
		}
		return QueryProviderResult{Data: json.RawMessage(`[]`), Versions: map[string]string{
			"operator_id": "operator-a", "operator_flow_isp_id": "17", "operator_publication_ids": "publication-1",
			"operator_dimension_snapshot_ids": "snapshot-1", "operator_classification_versions": "4",
		}}, nil
	}}
	audit := &recordingAuditRepository{}
	registries, err := NewBuiltinPlatformRegistries()
	if err != nil {
		t.Fatal(err)
	}
	providers := NewQueryProviderRegistry()
	if err := providers.Register(QueryProviderRegistration{Kind: DatasetProviderClickHouse, Provider: provider, Enabled: true, MaxConcurrent: 2}); err != nil {
		t.Fatal(err)
	}
	gateway, err := NewQueryGateway(registries, nil, repo, providers)
	if err != nil {
		t.Fatal(err)
	}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: true}, nil
		},
		QueryGateway: gateway, Audit: audit,
	})
	body := `{"from":"2026-08-24T11:00:00Z","to":"2026-08-24T12:00:00Z","value_layer":"customer","parameters":{"dimension":"category"}}`
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/flow/query", strings.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if len(audit.logs) != 1 || audit.logs[0].Action != "query.executed" || audit.logs[0].Detail["operator_id"] != "operator-a" ||
		audit.logs[0].Detail["operator_classification_versions"] != "4" {
		t.Fatalf("operator query audit = %+v", audit.logs)
	}

	wrong := httptest.NewRecorder()
	router.ServeHTTP(wrong, httptest.NewRequest(http.MethodPost, "/api/v1/flow/query", strings.NewReader(strings.Replace(body, `"from":`, `"dataset":"query.test","from":`, 1))))
	if wrong.Code != http.StatusBadRequest || !strings.Contains(wrong.Body.String(), "QUERY_INVALID") {
		t.Fatalf("wrong dataset status=%d body=%s", wrong.Code, wrong.Body.String())
	}
}

func TestQueryDatasetPolicyAPICRUDUsesETagAndRegisteredDataset(t *testing.T) {
	repo := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	gateway, _ := newQueryGatewayFixture(t, repo, nil, &queryProviderStub{}, 2)
	registries := &PlatformRegistries{Modules: gateway.Modules, Datasets: gateway.Datasets}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: true}, nil
		},
		QueryGateway: gateway, QueryPolicies: repo, Registries: registries,
	})

	defaults := httptest.NewRecorder()
	router.ServeHTTP(defaults, httptest.NewRequest(http.MethodGet, "/api/v1/query-policies/query.test", nil))
	if defaults.Code != http.StatusOK || defaults.Header().Get("ETag") != `"0"` || !strings.Contains(defaults.Body.String(), `"configured":false`) {
		t.Fatalf("defaults status=%d etag=%q body=%s", defaults.Code, defaults.Header().Get("ETag"), defaults.Body.String())
	}

	policyBody := `{
		"enabled":true,"allow_raw":true,"allow_supplier":false,"allow_customer":true,
		"max_range_seconds":86400,"max_concurrent":3,"max_result_rows":10000,"query_timeout_ms":30000
	}`
	missingPrecondition := httptest.NewRecorder()
	router.ServeHTTP(missingPrecondition, httptest.NewRequest(http.MethodPut, "/api/v1/query-policies/query.test", strings.NewReader(policyBody)))
	if missingPrecondition.Code != http.StatusPreconditionRequired {
		t.Fatalf("missing If-Match status=%d body=%s", missingPrecondition.Code, missingPrecondition.Body.String())
	}

	putRequest := httptest.NewRequest(http.MethodPut, "/api/v1/query-policies/query.test", strings.NewReader(policyBody))
	putRequest.Header.Set("If-Match", "*")
	putResponse := httptest.NewRecorder()
	router.ServeHTTP(putResponse, putRequest)
	if putResponse.Code != http.StatusOK || putResponse.Header().Get("ETag") != `"1"` || !strings.Contains(putResponse.Body.String(), `"configured":true`) {
		t.Fatalf("put status=%d etag=%q body=%s", putResponse.Code, putResponse.Header().Get("ETag"), putResponse.Body.String())
	}

	staleRequest := httptest.NewRequest(http.MethodPut, "/api/v1/query-policies/query.test", strings.NewReader(policyBody))
	staleRequest.Header.Set("If-Match", `"0"`)
	staleResponse := httptest.NewRecorder()
	router.ServeHTTP(staleResponse, staleRequest)
	if staleResponse.Code != http.StatusPreconditionFailed || !strings.Contains(staleResponse.Body.String(), "RESOURCE_VERSION_CONFLICT") {
		t.Fatalf("stale status=%d body=%s", staleResponse.Code, staleResponse.Body.String())
	}

	listed := httptest.NewRecorder()
	router.ServeHTTP(listed, httptest.NewRequest(http.MethodGet, "/api/v1/query-policies", nil))
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), `"total":1`) {
		t.Fatalf("list status=%d body=%s", listed.Code, listed.Body.String())
	}

	deleteRequest := httptest.NewRequest(http.MethodDelete, "/api/v1/query-policies/query.test", nil)
	deleteRequest.Header.Set("If-Match", `"1"`)
	deleted := httptest.NewRecorder()
	router.ServeHTTP(deleted, deleteRequest)
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%s", deleted.Code, deleted.Body.String())
	}

	unknown := httptest.NewRequest(http.MethodGet, "/api/v1/query-policies/not.registered", nil)
	unknownResponse := httptest.NewRecorder()
	router.ServeHTTP(unknownResponse, unknown)
	if unknownResponse.Code != http.StatusNotFound {
		t.Fatalf("unknown status=%d body=%s", unknownResponse.Code, unknownResponse.Body.String())
	}
}

func TestQueryGatewayAPIMapsConcurrencyAndPermissionErrors(t *testing.T) {
	concurrency := &QueryGatewayError{Code: QueryErrorConcurrencyLimit, Message: "busy", Retryable: true}
	response := httptest.NewRecorder()
	writeQueryGatewayError(response, concurrency)
	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "1" || !strings.Contains(response.Body.String(), `"retryable":true`) {
		t.Fatalf("concurrency status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}

	permission := httptest.NewRecorder()
	writeQueryGatewayError(permission, &QueryGatewayError{Code: QueryErrorPermissionDenied, Message: "denied"})
	if permission.Code != http.StatusForbidden || !strings.Contains(permission.Body.String(), "QUERY_PERMISSION_DENIED") {
		t.Fatalf("permission status=%d body=%s", permission.Code, permission.Body.String())
	}
}
