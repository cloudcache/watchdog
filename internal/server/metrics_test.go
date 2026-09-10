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
