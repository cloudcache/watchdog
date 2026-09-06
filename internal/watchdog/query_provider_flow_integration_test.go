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
	registries, err := NewBuiltinPlatformRegistries()
	if err != nil {
		t.Fatal(err)
	}
	policies := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	providers := NewQueryProviderRegistry()
	if err := providers.Register(QueryProviderRegistration{
		Kind:     DatasetProviderClickHouse,
		Provider: ClickHouseFlowQueryProvider{Runner: runner, Readiness: native},
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
	router := NewAPIV1Router(APIV1RouterConfig{Auth: auth, QueryGateway: gateway})
	body := map[string]any{
		"dataset": FlowTrafficDataset, "from": "2020-01-01T00:00:00Z", "to": "2020-01-01T00:01:00Z",
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
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/query", bytes.NewReader(encoded)))
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
}
