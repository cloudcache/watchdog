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
	var response metricRangeResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Status != "success" || response.Data.ResultType != "matrix" || len(response.Data.Result) != 1 {
		t.Fatalf("response=%+v", response)
	}
	item := response.Data.Result[0]
	if item.Metric["__name__"] != snmpch.MetricIfInBPS || item.Metric["device_id"] != "device-a" || item.Metric["port_id"] != "port-a" || len(item.Values) != 1 || item.Values[0].Value != 8_000 {
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
