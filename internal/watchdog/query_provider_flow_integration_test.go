package watchdog

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowquery"
)

// This opt-in test uses only a historical empty interval in the existing
// development Flow database. It proves the production HTTP envelope, gateway,
// provider, compiler and native runner are connected without mutating data.
func TestRealClickHouseFlowQueryGatewayHTTP(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_CLICKHOUSE_GATEWAY_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_CLICKHOUSE_GATEWAY_INTEGRATION=1 to run")
	}
	passwordFile := os.Getenv("WATCHDOG_CLICKHOUSE_PASSWORD_FILE")
	if passwordFile == "" {
		t.Fatal("WATCHDOG_CLICKHOUSE_PASSWORD_FILE is required")
	}
	secret, err := os.ReadFile(passwordFile)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	native, err := flowch.NewNativeInserter(ctx, flowch.NativeConfig{
		Address: "127.0.0.1:9000", Database: "watchdog_flow", User: "default", Password: strings.TrimSpace(string(secret)),
		ClientName: "watchdog-flow-gateway-integration", DialTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second,
		OperationTimeout: 10 * time.Second, MaxConns: 2, MinConns: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	if err := native.Ready(ctx); err != nil {
		t.Fatalf("ClickHouse readiness: %v", err)
	}
	runner, err := flowquery.NewRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	jointRunner, err := flowquery.NewJointRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	detailRunner, err := flowquery.NewDetailRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	overseasRunner, err := flowquery.NewOverseasRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	registries, err := NewBuiltinPlatformRegistries()
	if err != nil {
		t.Fatal(err)
	}
	policies := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	providers := NewQueryProviderRegistry()
	if err := providers.Register(QueryProviderRegistration{
		Kind:     DatasetProviderClickHouse,
		Provider: ClickHouseFlowQueryProvider{Runner: runner, JointRunner: jointRunner, Readiness: native},
		Enabled:  true, MaxConcurrent: 2,
	}); err != nil {
		t.Fatal(err)
	}
	gateway, err := NewQueryGateway(registries, &fakeTenantModuleRepository{}, policies, providers)
	if err != nil {
		t.Fatal(err)
	}
	auth := func(*http.Request) (AuthContext, error) {
		return AuthContext{TenantID: "tenant-flow-http-it", UserID: "user-flow-http-it", IsAdmin: true}, nil
	}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: auth, QueryGateway: gateway, FlowRecords: detailRunner, FlowOverseas: overseasRunner,
		FlowRecordNow:   func() time.Time { return time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC) },
		FlowOverseasNow: func() time.Time { return time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC) },
	})
	body := map[string]any{
		"from": "2020-01-01T00:00:00Z", "to": "2020-01-01T00:01:00Z",
		"step_seconds": 0, "limit": 1, "value_layer": "customer",
		"parameters": map[string]any{
			"metric": "estimated_bps", "dimension": "total", "top_n": 1,
			"include_other": false, "timezone": "UTC", "target_points": 300,
		},
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/flow/query", bytes.NewReader(encoded)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data flowquery.Result `json:"data"`
		Meta QueryResultMeta  `json:"meta"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Data.Plan == nil || response.Data.Plan.Source != flowquery.BucketOneMinute ||
		response.Data.Plan.StepSeconds != 60 || response.Meta.StepSeconds != 60 ||
		len(response.Data.Points) != 0 || response.Data.RollupCompleteness.ExpectedBuckets != 1 ||
		response.Data.RollupCompleteness.CoveredBuckets != 0 || !response.Meta.Partial || response.Meta.Source != "clickhouse" {
		t.Fatalf("response=%+v", response)
	}

	jointBody := map[string]any{
		"dataset": FlowTrafficDataset, "from": "2020-01-01T00:00:00Z", "to": "2020-01-01T00:02:00Z",
		"step_seconds": 0, "limit": 10, "value_layer": "customer",
		"parameters": map[string]any{
			"metric": "estimated_bps", "dimensions": []string{"geo.country", "asn"}, "top_n": 2,
			"include_other": true, "timezone": "UTC", "target_points": 300,
		},
	}
	encoded, err = json.Marshal(jointBody)
	if err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/query", bytes.NewReader(encoded)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("joint status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var jointResponse struct {
		Data flowquery.JointResult `json:"data"`
		Meta QueryResultMeta       `json:"meta"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &jointResponse); err != nil {
		t.Fatal(err)
	}
	if len(jointResponse.Data.Points) != 0 || jointResponse.Data.Plan.Source != "flow_records" ||
		jointResponse.Data.Plan.StepSeconds != 60 || jointResponse.Meta.StepSeconds != 60 ||
		!jointResponse.Meta.Partial || jointResponse.Meta.Source != "clickhouse" {
		t.Fatalf("joint response=%+v", jointResponse)
	}

	validateRecorder := httptest.NewRecorder()
	router.ServeHTTP(validateRecorder, httptest.NewRequest(http.MethodPost, "/api/v1/flow/filters/validate", strings.NewReader(`{
		"filter":{"op":"and","args":[
			{"op":"predicate","field":"protocol","operator":"in","values":["UDP","tcp"]},
			{"op":"predicate","field":"src_ip","operator":"in","values":["10.0.0.9/8"]}
		]}}
	`)))
	if validateRecorder.Code != http.StatusOK {
		t.Fatalf("filter validate status=%d body=%s", validateRecorder.Code, validateRecorder.Body.String())
	}
	var validated struct {
		Filter flowquery.FilterExpression `json:"filter"`
	}
	if err := json.Unmarshal(validateRecorder.Body.Bytes(), &validated); err != nil {
		t.Fatal(err)
	}
	typedBody := map[string]any{
		"dataset": FlowTrafficDataset, "from": "2020-01-01T00:00:00Z", "to": "2020-01-01T00:02:00Z",
		"step_seconds": 0, "limit": 10, "value_layer": "customer",
		"parameters": map[string]any{
			"metric": "estimated_bps", "dimension": "geo.country", "top_n": 2,
			"include_other": true, "timezone": "UTC", "target_points": 300, "filter": validated.Filter,
		},
	}
	encoded, err = json.Marshal(typedBody)
	if err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/query", bytes.NewReader(encoded)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("typed filter status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var typedResponse struct {
		Data flowquery.JointResult `json:"data"`
		Meta QueryResultMeta       `json:"meta"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &typedResponse); err != nil {
		t.Fatal(err)
	}
	if len(typedResponse.Data.Points) != 0 || typedResponse.Data.Plan.Source != "flow_records" ||
		typedResponse.Data.Plan.StepSeconds != 60 || typedResponse.Meta.StepSeconds != 60 ||
		!typedResponse.Meta.Partial || typedResponse.Meta.Source != "clickhouse" {
		t.Fatalf("typed filter response=%+v", typedResponse)
	}

	detailRecorder := httptest.NewRecorder()
	router.ServeHTTP(detailRecorder, httptest.NewRequest(http.MethodPost, "/api/v1/flow/records/search", strings.NewReader(`{
		"ip":"203.0.113.1","endpoint":"source","from":"2020-01-01T00:00:00Z","to":"2020-01-01T00:01:00Z",
		"view":"customer","fields":["src_ip","dst_ip","business_direction","category","estimated_bytes"],
		"filters":{"directions":["in"],"categories":["overseas"]},"limit":25
	}`)))
	if detailRecorder.Code != http.StatusOK {
		t.Fatalf("detail status=%d body=%s", detailRecorder.Code, detailRecorder.Body.String())
	}
	var detailResponse struct {
		Data flowquery.DetailResult `json:"data"`
		Meta struct {
			Sort     string `json:"sort"`
			PageSize uint16 `json:"page_size"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(detailRecorder.Body.Bytes(), &detailResponse); err != nil {
		t.Fatal(err)
	}
	if len(detailResponse.Data.Rows) != 0 || detailResponse.Data.HasMore ||
		detailResponse.Meta.Sort != "event_time:desc,record_id:desc" || detailResponse.Meta.PageSize != 25 {
		t.Fatalf("detail response=%+v", detailResponse)
	}
	for _, field := range []string{
		"event_time", "src_ip", "src_port", "dst_ip", "dst_port", "ip_protocol", "business_direction",
		"category", "remote_asn", "remote_country", "estimated_bytes", "sampling_rate", "quality_flags",
	} {
		fields := []string{"src_ip"}
		if field != "event_time" && field != "src_ip" {
			fields = append(fields, field)
		}
		sortedBody, err := json.Marshal(map[string]any{
			"ip": "203.0.113.1", "endpoint": "source", "from": "2020-01-01T00:00:00Z", "to": "2020-01-01T00:01:00Z",
			"view": "customer", "fields": fields, "sort": map[string]string{"field": field, "direction": "asc"}, "limit": 25,
		})
		if err != nil {
			t.Fatal(err)
		}
		sortedRecorder := httptest.NewRecorder()
		router.ServeHTTP(sortedRecorder, httptest.NewRequest(http.MethodPost, "/api/v1/flow/records/search", bytes.NewReader(sortedBody)))
		if sortedRecorder.Code != http.StatusOK {
			t.Fatalf("detail sort %s status=%d body=%s", field, sortedRecorder.Code, sortedRecorder.Body.String())
		}
	}

	for _, field := range []string{
		"event_time", "src_ip", "src_port", "dst_ip", "dst_port", "ip_protocol", "business_direction",
		"category", "remote_asn", "remote_country", "estimated_bytes", "sampling_rate", "quality_flags",
	} {
		facetRecorder := httptest.NewRecorder()
		facetBody := `{
			"ip":"203.0.113.1","endpoint":"source","from":"2020-01-01T00:00:00Z","to":"2020-01-01T00:01:00Z",
			"view":"customer","field":"` + field + `","column_filters":[{"field":"src_port","values":["443"]}],
			"search":"c","limit":50
		}`
		router.ServeHTTP(facetRecorder, httptest.NewRequest(http.MethodPost, "/api/v1/flow/records/facets", strings.NewReader(facetBody)))
		if facetRecorder.Code != http.StatusOK {
			t.Fatalf("facet %s status=%d body=%s", field, facetRecorder.Code, facetRecorder.Body.String())
		}
		var facetResponse struct {
			Data flowquery.DetailFacetResult `json:"data"`
		}
		if err := json.Unmarshal(facetRecorder.Body.Bytes(), &facetResponse); err != nil {
			t.Fatal(err)
		}
		if facetResponse.Data.Field != field || len(facetResponse.Data.Items) != 0 {
			t.Fatalf("facet response=%+v", facetResponse)
		}
	}

	overseasRecorder := httptest.NewRecorder()
	router.ServeHTTP(overseasRecorder, httptest.NewRequest(http.MethodPost, "/api/v1/flow/overseas/query", strings.NewReader(`{
		"from":"2020-01-01T00:00:00Z","to":"2020-01-01T00:01:00Z","bucket":"1m",
		"metric":"estimated_bps","geo_level":"country","view":"customer","top_n":20,"include_other":true
	}`)))
	if overseasRecorder.Code != http.StatusOK {
		t.Fatalf("overseas status=%d body=%s", overseasRecorder.Code, overseasRecorder.Body.String())
	}
	var overseasResponse struct {
		Data flowquery.OverseasResult `json:"data"`
		Meta struct {
			Source      string `json:"source"`
			StepSeconds uint32 `json:"step_seconds"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(overseasRecorder.Body.Bytes(), &overseasResponse); err != nil {
		t.Fatal(err)
	}
	if len(overseasResponse.Data.Points) != 0 || overseasResponse.Data.RollupCompleteness.ExpectedBuckets != 1 ||
		overseasResponse.Data.RollupCompleteness.CoveredBuckets != 0 || overseasResponse.Meta.Source != "1m" ||
		overseasResponse.Meta.StepSeconds != 60 {
		t.Fatalf("overseas response=%+v", overseasResponse)
	}
}
