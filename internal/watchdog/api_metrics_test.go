package watchdog

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAPIMetricsRealtimeDefaultsTimeMode(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: metricsTestAuth(false)})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/realtime?target_id=target-a&metric=watchdog_system_cpu_percent", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"TimeMode":"realtime"`) {
		t.Fatalf("body missing realtime mode: %s", rec.Body.String())
	}
}

func TestAPIMetricsRealtimeQueriesRecentWindow(t *testing.T) {
	client := &fakeMetricsQueryClient{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    metricsTestAuth(true),
		Metrics: MetricsService{Client: client},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/realtime?target_id=target-a&metric=watchdog_system_cpu_percent", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if client.query.Start.IsZero() || client.query.End.IsZero() || !client.query.End.After(client.query.Start) {
		t.Fatalf("range = %s - %s", client.query.Start, client.query.End)
	}
	if got := client.query.End.Sub(client.query.Start); got != 10*time.Minute {
		t.Fatalf("window = %s", got)
	}
	if client.query.Step != 10*time.Second {
		t.Fatalf("step = %s", client.query.Step)
	}
}

func TestAPIMetricsRejectsRawForNonAdmin(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: metricsTestAuth(false)})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/query?target_id=target-a&metric=watchdog_snmp_if_in_bps&time_mode=custom&start=2026-06-01T00:00:00Z&end=2026-06-01T01:00:00Z&step=300&value_mode=raw", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestAPIMetricsRejectsCorrectedTrafficWithoutPortID(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: metricsTestAuth(true)})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/query?target_id=target-a&metric=watchdog_snmp_if_in_bps&time_mode=custom&start=2026-06-01T00:00:00Z&end=2026-06-01T01:00:00Z&step=300&value_mode=corrected", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (corrected requires port_id)", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "port_id") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestAPIMetricsRejectsUnknownMetric(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: metricsTestAuth(true)})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/query?target_id=target-a&metric=sum(up)&time_mode=custom&start=2026-06-01T00:00:00Z&end=2026-06-01T01:00:00Z&step=300", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "unsupported metric") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestAPIMetricsCatalogListsSNMPAndTargetMetrics(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: metricsTestAuth(false)})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/catalog", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{MetricSNMPIfInBps, "watchdog_system_cpu_percent", "watchdog_container_cpu_percent", `"Scope":"port"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %s: %s", want, body)
		}
	}
}

func TestAPIMetricsAggregateBuildsSinglePromQLQuery(t *testing.T) {
	client := &fakeMetricsQueryClient{response: vmResponseForMetricsTest(42)}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    metricsTestAuth(true),
		Metrics: MetricsService{Client: client},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/aggregate?target_ids=target-a,target-b&metric=watchdog_system_cpu_percent&aggregate=sum&time_mode=custom&start=2026-06-01T00:00:00Z&end=2026-06-01T01:00:00Z&step=300", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	want := `sum(watchdog_system_cpu_percent{tenant_id="tenant-a",target_id=~"target-a|target-b"}) and (count(watchdog_system_cpu_percent{tenant_id="tenant-a",target_id=~"target-a|target-b"}) > 0)`
	if client.query.Query != want {
		t.Fatalf("query = %s, want %s", client.query.Query, want)
	}
}

func TestAPIMetricsAggregateSNMPTrafficResolvesPortsByIfIndex(t *testing.T) {
	client := &fakeMetricsQueryClient{response: vmResponseForMetricsTest(42)}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    metricsTestAuth(true),
		Metrics: MetricsService{Client: client},
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			ports: []NetworkPort{
				{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 101},
				{ID: "port-b", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 102},
			},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/aggregate?port_ids=port-a,port-b&metric=watchdog_snmp_if_in_bps&aggregate=sum&value_mode=raw&time_mode=custom&start=2026-06-01T00:00:00Z&end=2026-06-01T01:00:00Z&step=300", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(client.queries) != 2 {
		t.Fatalf("queries = %d, want 2", len(client.queries))
	}
	for _, want := range []string{`device_id="device-a"`, `watchdog_snmp_if_in_octets_total{`} {
		if !strings.Contains(client.queries[0].Query, want) {
			t.Fatalf("query = %q, missing %q", client.queries[0].Query, want)
		}
	}
	if !strings.Contains(client.queries[0].Query, `if_index="101"`) || !strings.Contains(client.queries[1].Query, `if_index="102"`) {
		t.Fatalf("queries = %#v", client.queries)
	}
}

func TestAPIMetricsTrafficViewCustomerUsesOneMinuteQueryAndFiveMinuteMax(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	base := float64(0)
	delta1 := float64(30000)
	delta2 := float64(20000)
	response := VictoriaMetricsResponse{Status: "success"}
	response.Data.Result = []VMRangeQueryItem{{
		Metric: map[string]string{"__name__": MetricSNMPIfInOctetsTotal},
		Values: []VMValue{
			{Time: start, Value: base},
			{Time: start.Add(time.Minute), Value: base + delta1},
			{Time: start.Add(2 * time.Minute), Value: base + delta1 + delta2},
			{Time: start.Add(5 * time.Minute), Value: base + delta1*2},
			{Time: start.Add(9 * time.Minute), Value: base + delta1*3},
		},
	}}
	client := &fakeMetricsQueryClient{response: response}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    metricsTestAuth(true),
		Metrics: MetricsService{Client: client},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/query?target_id=target-a&port_id=port-a&metric=watchdog_snmp_if_in_bps&traffic_view=customer&value_mode=raw&time_mode=custom&start=2026-06-01T00:00:00Z&end=2026-06-01T00:10:00Z&step=300", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if client.query.Step != time.Minute {
		t.Fatalf("step = %s, want 1m", client.query.Step)
	}
	var got VictoriaMetricsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	values := got.Data.Result[0].Values
	if len(values) != 2 {
		t.Fatalf("values = %#v, want 2 buckets", values)
	}
	if !values[0].Time.Equal(start) || values[0].Value != 4000 {
		t.Fatalf("first bucket = %#v, want %s value 4000", values[0], start)
	}
	if !values[1].Time.Equal(start.Add(5*time.Minute)) || values[1].Value != 1000 {
		t.Fatalf("second bucket = %#v, want %s value 1000", values[1], start.Add(5*time.Minute))
	}
}

func TestAPIMetricsTrafficViewSupplierUsesFiveMinuteQuery(t *testing.T) {
	client := &fakeMetricsQueryClient{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    metricsTestAuth(true),
		Metrics: MetricsService{Client: client},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/query?target_id=target-a&port_id=port-a&metric=watchdog_snmp_if_in_bps&traffic_view=supplier&value_mode=raw&time_mode=custom&start=2026-06-01T00:00:00Z&end=2026-06-01T01:00:00Z&step=60", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if client.query.Step != 5*time.Minute {
		t.Fatalf("step = %s, want 5m", client.query.Step)
	}
}

func TestAPIMetricsAggregateRequiresSelection(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: metricsTestAuth(false)})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/aggregate?metric=watchdog_system_cpu_percent&aggregate=sum&time_mode=custom&start=2026-06-01T00:00:00Z&end=2026-06-01T01:00:00Z&step=300", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func vmResponseForMetricsTest(value float64) VictoriaMetricsResponse {
	response := VictoriaMetricsResponse{Status: "success"}
	response.Data.Result = []VMRangeQueryItem{{
		Metric: map[string]string{"__name__": "watchdog_system_cpu_percent"},
		Values: []VMValue{{Time: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), Value: value}},
	}}
	return response
}

func TestAPIMetricsAllowsCatalogTargetMetric(t *testing.T) {
	client := &fakeMetricsQueryClient{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    metricsTestAuth(false),
		Metrics: MetricsService{Client: client},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/query?target_id=target-a&metric=watchdog_system_cpu_percent&time_mode=custom&start=2026-06-01T00:00:00Z&end=2026-06-01T01:00:00Z&max_data_points=600", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	wantSelector := `watchdog_system_cpu_percent{tenant_id="tenant-a",target_id="target-a"}`
	if client.query.Query != wantSelector {
		t.Fatalf("selector = %q", client.query.Query)
	}
}

func TestAPIMetricsAllowsRawForAdmin(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: metricsTestAuth(true)})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/query?target_id=target-a&metric=watchdog_snmp_if_in_bps&time_mode=custom&start=2026-06-01T00:00:00Z&end=2026-06-01T01:00:00Z&step=300&value_mode=raw", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestAPIMetricsQueriesVictoriaMetricsWithSelectorAndAutoStep(t *testing.T) {
	client := &fakeMetricsQueryClient{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    metricsTestAuth(true),
		Metrics: MetricsService{Client: client},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/query?target_id=target-a&port_id=port-a&metric=watchdog_snmp_if_in_bps&time_mode=custom&start=2026-06-01T00:00:00Z&end=2026-06-01T01:00:00Z&max_data_points=600", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	wantSelector := `watchdog_snmp_if_in_octets_total{tenant_id="tenant-a",target_id="target-a",port_id="port-a"}`
	if client.query.Query != wantSelector {
		t.Fatalf("selector = %q, want %q", client.query.Query, wantSelector)
	}
	// Auto-step would pick 10s for 600 points over 1h, but traffic counters
	// are collected once a minute, so the query clamps to the 1m floor.
	if client.query.Step != time.Minute {
		t.Fatalf("step = %s, want 1m", client.query.Step)
	}
}

func TestAPIMetricsReturnsServiceUnavailableWhenVictoriaMetricsIsDown(t *testing.T) {
	client := &fakeMetricsQueryClient{err: ErrVictoriaMetricsUnavailable}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    metricsTestAuth(true),
		Metrics: MetricsService{Client: client},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/query?target_id=target-a&metric=watchdog_container_cpu_percent&time_mode=fixed&window=1h", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "VictoriaMetrics is unavailable") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestAPIMetricsResolvesPortTargetForPermissionAndSelector(t *testing.T) {
	client := &fakeMetricsQueryClient{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    metricsTestAuth(false),
		Metrics: MetricsService{Client: client},
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 101}},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/query?port_id=port-a&metric=watchdog_snmp_if_in_bps&time_mode=custom&start=2026-06-01T00:00:00Z&end=2026-06-01T01:00:00Z&max_data_points=600", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	wantParts := []string{
		`watchdog_snmp_if_in_octets_total{`,
		`tenant_id="tenant-a"`,
		`device_id="device-a"`,
		`if_index="101"`,
		`}`,
	}
	for _, part := range wantParts {
		if !strings.Contains(client.query.Query, part) {
			t.Fatalf("selector = %q, missing %q", client.query.Query, part)
		}
	}
}

func TestAPIMetricsRejectsMismatchedPortTarget(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: metricsTestAuth(true),
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a"}},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/query?target_id=target-b&port_id=port-a&metric=watchdog_snmp_if_in_bps&time_mode=custom&start=2026-06-01T00:00:00Z&end=2026-06-01T01:00:00Z&step=300", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", rec.Code, rec.Body.String())
	}
}

func metricsTestAuth(admin bool) AuthContextAdapter {
	return func(*http.Request) (AuthContext, error) {
		return AuthContext{
			TenantID: "tenant-a",
			UserID:   "user-a",
			IsAdmin:  admin,
			Grants: []Permission{{
				TenantID:     "tenant-a",
				SubjectType:  SubjectUser,
				SubjectID:    "user-a",
				ResourceType: ResourceTarget,
				ResourceID:   "target-a",
				Actions:      []Action{ActionView},
			}},
		}, nil
	}
}
