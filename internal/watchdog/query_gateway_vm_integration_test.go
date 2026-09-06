package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestQueryGatewayVictoriaMetricsIntegration is opt-in because it writes two
// uniquely tenant-scoped samples to a real VictoriaMetrics and removes that
// exact series on exit. A local fault proxy makes timeout, caller cancellation
// and provider interruption deterministic while recovery still reaches the
// same real dependency.
func TestQueryGatewayVictoriaMetricsIntegration(t *testing.T) {
	if os.Getenv("WATCHDOG_QUERY_VM_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_QUERY_VM_INTEGRATION=1 to run the QueryGateway VictoriaMetrics integration test")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("WATCHDOG_VICTORIAMETRICS_URL")), "/")
	if baseURL == "" {
		baseURL = "http://127.0.0.1:8428"
	}
	upstream, err := url.Parse(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	direct := VictoriaMetricsClient{BaseURL: baseURL, HTTPClient: &http.Client{Timeout: 5 * time.Second}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := direct.Ready(ctx); err != nil {
		t.Fatalf("VictoriaMetrics is not ready at %s: %v", baseURL, err)
	}

	runID := fmt.Sprintf("query_vm_%d", time.Now().UnixNano())
	tenantID, targetID := ID("tenant_"+runID), ID("target_"+runID)
	now := time.Now().UTC().Truncate(time.Minute)
	payload := []byte(fmt.Sprintf(
		"%s{tenant_id=%q,target_id=%q} 41 %d\n%s{tenant_id=%q,target_id=%q} 42 %d\n",
		MetricSystemCPUPercent, tenantID, targetID, now.Add(-2*time.Minute).UnixMilli(),
		MetricSystemCPUPercent, tenantID, targetID, now.Add(-time.Minute).UnixMilli(),
	))
	if err := direct.ImportPrometheus(ctx, payload); err != nil {
		t.Fatalf("import test samples: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := direct.DeleteSeries(cleanupCtx, []string{fmt.Sprintf(`{tenant_id=%q,target_id=%q}`, tenantID, targetID)}); err != nil {
			t.Errorf("delete test series: %v", err)
		}
	}()
	waitForVMSeriesVisible(t, baseURL, fmt.Sprintf(`%s{tenant_id=%q,target_id=%q}`, MetricSystemCPUPercent, tenantID, targetID), now.Add(-3*time.Minute), now)

	reverseProxy := httputil.NewSingleHostReverseProxy(upstream)
	var fault atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch fault.Load() {
		case 1:
			select {
			case <-r.Context().Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
		case 2:
			http.Error(w, "injected VictoriaMetrics interruption", http.StatusServiceUnavailable)
			return
		}
		reverseProxy.ServeHTTP(w, r)
	}))
	defer proxy.Close()

	policies := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	policy := QueryDatasetPolicy{
		TenantID: tenantID, DatasetKey: "host.agent_metrics", Enabled: true, AllowCustomer: true,
		MaxRangeSeconds: 86_400, MaxConcurrent: 2, MaxResultRows: 1_000, QueryTimeoutMS: 5_000, RowVersion: 1,
	}
	policies.policies[policies.key(tenantID, policy.DatasetKey)] = policy
	client := VictoriaMetricsClient{BaseURL: proxy.URL, HTTPClient: &http.Client{}}
	gateway := newMetricsGatewayForTest(t, client, nil, policies)
	auth := AuthContext{TenantID: tenantID, UserID: "integration-user", IsAdmin: true}
	request := QueryRequest{
		Dataset: "host.agent_metrics", From: now.Add(-3 * time.Minute), To: now,
		StepSeconds: 60, Limit: 100, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(fmt.Sprintf(`{"metric":%q,"target_id":%q}`, MetricSystemCPUPercent, targetID)),
	}

	result := executeVMGatewayEventually(t, gateway, auth, request)
	if result.Meta.Source != string(DatasetProviderVM) || result.Meta.UnknownRatio != 1 || result.Meta.Partial || result.Meta.CompleteRatio != 0 {
		t.Fatalf("real VM completeness metadata = %#v", result.Meta.QueryCompleteness)
	}
	var vmResponse VictoriaMetricsResponse
	if err := json.Unmarshal(result.Data, &vmResponse); err != nil || victoriaMetricsPointCount(vmResponse) < 2 {
		t.Fatalf("real VM result points=%d decode=%v data=%s", victoriaMetricsPointCount(vmResponse), err, result.Data)
	}

	requireComplete := request
	requireComplete.RequireComplete = true
	assertVMGatewayError(t, gateway, auth, requireComplete, QueryErrorIncomplete)

	fault.Store(1)
	policy.QueryTimeoutMS = 100
	policies.mu.Lock()
	policies.policies[policies.key(tenantID, policy.DatasetKey)] = policy
	policies.mu.Unlock()
	assertVMGatewayError(t, gateway, auth, request, QueryErrorTimeout)

	policy.QueryTimeoutMS = 5_000
	policies.mu.Lock()
	policies.policies[policies.key(tenantID, policy.DatasetKey)] = policy
	policies.mu.Unlock()
	canceledContext, cancelQuery := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancelQuery()
	}()
	_, err = gateway.Execute(canceledContext, auth, "real-vm-canceled", request)
	var queryErr *QueryGatewayError
	if !errors.As(err, &queryErr) || queryErr.Code != QueryErrorCanceled {
		t.Fatalf("caller cancellation error = %#v", err)
	}

	fault.Store(2)
	assertVMGatewayError(t, gateway, auth, request, QueryErrorProviderFailure)
	fault.Store(0)
	recovered := executeVMGatewayEventually(t, gateway, auth, request)
	if recovered.Meta.UnknownRatio != 1 {
		t.Fatalf("recovered completeness = %#v", recovered.Meta.QueryCompleteness)
	}
}

func waitForVMSeriesVisible(t *testing.T, baseURL, query string, from, to time.Time) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		values := url.Values{}
		values.Set("query", query)
		values.Set("start", fmt.Sprint(from.Unix()))
		values.Set("end", fmt.Sprint(to.Unix()))
		values.Set("step", "60")
		values.Set("nocache", "1")
		request, err := http.NewRequest(http.MethodGet, baseURL+"/api/v1/query_range?"+values.Encode(), nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			var result VictoriaMetricsResponse
			decodeErr := json.NewDecoder(response.Body).Decode(&result)
			closeErr := response.Body.Close()
			if decodeErr == nil && closeErr == nil && response.StatusCode == http.StatusOK && victoriaMetricsPointCount(result) >= 2 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("imported VictoriaMetrics series did not become visible: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func executeVMGatewayEventually(t *testing.T, gateway *QueryGateway, auth AuthContext, request QueryRequest) QueryResult {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		result, err := gateway.Execute(context.Background(), auth, "real-vm", request)
		if err == nil {
			var response VictoriaMetricsResponse
			if json.Unmarshal(result.Data, &response) == nil && victoriaMetricsPointCount(response) >= 2 {
				return result
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("real VictoriaMetrics query did not converge: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func assertVMGatewayError(t *testing.T, gateway *QueryGateway, auth AuthContext, request QueryRequest, code QueryErrorCode) {
	t.Helper()
	_, err := gateway.Execute(context.Background(), auth, "real-vm-error", request)
	var queryErr *QueryGatewayError
	if !errors.As(err, &queryErr) || queryErr.Code != code {
		t.Fatalf("query error = %#v, want %s", err, code)
	}
}
