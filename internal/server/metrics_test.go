package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/gin-gonic/gin"
)

type metricQueryExecutor struct {
	query ch.Query
}

func (e *metricQueryExecutor) Do(ctx context.Context, query ch.Query) error {
	e.query = query
	result := query.Result.(proto.Results)
	bucket := result[0].Data.(*proto.ColDateTime)
	device := result[1].Data.(*proto.ColStr)
	entityKind := result[2].Data.(*proto.ColLowCardinality[string])
	entityID := result[3].Data.(*proto.ColStr)
	value := result[4].Data.(*proto.ColFloat64)
	bucket.Append(time.Date(2026, 9, 9, 1, 1, 0, 0, time.UTC))
	device.Append("device-a")
	entityKind.Append("port")
	entityID.Append("port-a")
	value.Append(8_000)
	return query.OnResult(ctx, proto.Block{Rows: 1})
}

func TestQueryMetricsUsesClickHouseAndKeepsChartContract(t *testing.T) {
	exec := &metricQueryExecutor{}
	store, err := snmpch.New(exec)
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{snmpMetrics: store}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet,
		"/api/v1/metrics/range?device_id=device-a&port_id=&metric=watchdog_snmp_if_in_bps&time_mode=custom&start=2026-09-09T01:00:00Z&end=2026-09-09T01:02:00Z&step=60&max_data_points=10", nil)
	c.Set(principalKey, &principal{IsAdmin: true})

	server.queryMetrics(c)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string   `json:"metric"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Status != "success" || response.Data.ResultType != "matrix" || len(response.Data.Result) != 1 {
		t.Fatalf("response=%+v", response)
	}
	item := response.Data.Result[0]
	var value string
	if len(item.Values) == 1 && len(item.Values[0]) == 2 {
		_ = json.Unmarshal(item.Values[0][1], &value)
	}
	if item.Metric["__name__"] != snmpch.MetricIfInBPS || item.Metric["device_id"] != "device-a" || item.Metric["port_id"] != "port-a" || value != "8000" {
		t.Fatalf("result=%+v", item)
	}
	if !strings.Contains(exec.query.Body, "FROM snmp_samples") || strings.Contains(strings.ToLower(exec.query.Body), "tenant") {
		t.Fatalf("unexpected ClickHouse query: %s", exec.query.Body)
	}
	if !strings.Contains(exec.query.Body, "LIMIT 250001") {
		t.Fatalf("device-scoped query reused the per-series max_data_points as its intermediate row budget: %s", exec.query.Body)
	}
}

func TestSNMPIntermediateRowBudgetSeparatesChartPointsFromCorrectionRows(t *testing.T) {
	from := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	to := from.Add(24 * time.Hour)
	if got := snmpIntermediateRowBudget(180, []snmpch.Scope{{DeviceID: "device-a", PortID: "port-a"}}, from, to, 5*time.Minute, 250000); got != 289 {
		t.Fatalf("single-port budget=%d, want 289", got)
	}
	if got := snmpIntermediateRowBudget(180, []snmpch.Scope{{DeviceID: "device-a", PortID: "port-a"}, {DeviceID: "device-a", PortID: "port-b"}}, from, to, 5*time.Minute, 250000); got != 578 {
		t.Fatalf("multi-port intermediate budget=%d, want 578", got)
	}
	if got := snmpIntermediateRowBudget(180, []snmpch.Scope{{DeviceID: "device-a"}}, from, to, 5*time.Minute, 250000); got != 250000 {
		t.Fatalf("device-wide intermediate budget=%d, want 250000", got)
	}
}

func TestDownsampleSNMPPointsAfterCorrection(t *testing.T) {
	base := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	points := []snmpch.Point{
		{Time: base, Value: 1}, {Time: base.Add(time.Minute), Value: 3},
		{Time: base.Add(2 * time.Minute), Value: 5}, {Time: base.Add(3 * time.Minute), Value: 7},
	}
	averages := downsampleSNMPPoints(points, 2, false)
	if len(averages) != 2 || averages[0].Value != 2 || averages[1].Value != 6 || !averages[1].Time.Equal(points[3].Time) {
		t.Fatalf("average downsample=%+v", averages)
	}
	last := downsampleSNMPPoints(points, 2, true)
	if len(last) != 2 || last[0].Value != 3 || last[1].Value != 7 {
		t.Fatalf("last-value downsample=%+v", last)
	}
}

func TestQueryMetricsRejectsUnknownParameterBeforeClickHouse(t *testing.T) {
	server := &Server{}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/metrics/range?device_id=device-a&metric=watchdog_snmp_if_in_bps&tenant_id=legacy", nil)
	c.Set(principalKey, &principal{IsAdmin: true})

	server.queryMetrics(c)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "unsupported query parameter") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestQueryMetricsDoesNotRouteContainerMetricToSNMP(t *testing.T) {
	exec := &metricQueryExecutor{}
	store, err := snmpch.New(exec)
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{snmpMetrics: store}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/metrics/query?target_id=host-a&metric=watchdog_container_cpu_percent&time_mode=fixed&window=1h", nil)
	c.Set(principalKey, &principal{IsAdmin: true})

	server.queryMetrics(c)
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "metric_provider_unavailable") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if exec.query.Body != "" {
		t.Fatalf("container metric was routed to SNMP ClickHouse query: %s", exec.query.Body)
	}
}

func TestMetricWindowDurationSupportsSNMPPagePresets(t *testing.T) {
	for input, expected := range map[string]time.Duration{
		"5m": 5 * time.Minute, "30m": 30 * time.Minute, "1h": time.Hour,
		"6h": 6 * time.Hour, "12h": 12 * time.Hour, "24h": 24 * time.Hour,
		"7d": 7 * 24 * time.Hour, "30d": 30 * 24 * time.Hour,
	} {
		actual, ok := metricWindowDuration(input)
		if !ok || actual != expected {
			t.Fatalf("metricWindowDuration(%q) = %s, %v; want %s, true", input, actual, ok, expected)
		}
	}
}
