package snmpch

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type queryExecutor struct{ query ch.Query }

func (e *queryExecutor) Do(ctx context.Context, query ch.Query) error {
	e.query = query
	if query.OnResult == nil {
		return nil
	}
	result := query.Result.(proto.Results)
	bucket := result[0].Data.(*proto.ColDateTime)
	device := result[1].Data.(*proto.ColStr)
	kind := result[2].Data.(*proto.ColLowCardinality[string])
	entity := result[3].Data.(*proto.ColStr)
	value := result[4].Data.(*proto.ColFloat64)
	bucket.Append(time.Unix(120, 0).UTC())
	device.Append("device-a")
	kind.Append("port")
	entity.Append("port-a")
	value.Append(8000)
	return query.OnResult(ctx, proto.Block{Rows: 1})
}

func TestQueryTrafficUsesCounterRateAndNoTenantScope(t *testing.T) {
	exec := &queryExecutor{}
	store, _ := New(exec)
	series, err := store.Query(context.Background(), QueryRequest{
		DeviceID: "device-a", EntityID: "port-a", Metric: MetricIfInBPS,
		From: time.Unix(0, 0), To: time.Unix(300, 0), Step: time.Minute, MaxRows: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Points) != 1 || series[0].Metric != MetricIfInBPS {
		t.Fatalf("series=%+v", series)
	}
	for _, required := range []string{"lagInFrame", "counter_width=32", "interval_ms", "entity_id={entity:String}"} {
		if !strings.Contains(exec.query.Body, required) {
			t.Fatalf("rate query missing %q: %s", required, exec.query.Body)
		}
	}
	if strings.Contains(exec.query.Body, "tenant") || strings.Contains(exec.query.Body, "source_kind") {
		t.Fatalf("single-domain SNMP query retained legacy scope: %s", exec.query.Body)
	}
}

func TestQueryRejectsUnboundedRequest(t *testing.T) {
	store, _ := New(&queryExecutor{})
	if _, err := store.Query(context.Background(), QueryRequest{DeviceID: "d", Metric: "m", From: time.Unix(0, 0), To: time.Unix(1, 0)}); err == nil {
		t.Fatal("zero step and row budget were accepted")
	}
}

func TestQueryUsesConfiguredClickHouseBudgets(t *testing.T) {
	exec := &queryExecutor{}
	limits := DefaultQueryLimits()
	limits.MaxResultRows = 20
	limits.MaxExecutionTime = 30 * time.Second
	limits.MaxRowsToRead = 1000
	limits.MaxBytesToRead = 2000
	limits.MaxMemoryBytes = 3000
	store, err := NewWithQueryLimits(exec, limits)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Query(context.Background(), QueryRequest{
		DeviceID: "device-a", EntityID: "port-a", Metric: MetricIfInBPS,
		From: time.Unix(0, 0), To: time.Unix(300, 0), Step: time.Minute, MaxRows: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{}
	for _, setting := range exec.query.Settings {
		settings[setting.Key] = setting.Value
	}
	for key, want := range map[string]string{
		"max_execution_time": "30", "max_result_rows": "11", "max_rows_to_read": "1000",
		"max_bytes_to_read": "2000", "max_memory_usage": "3000",
	} {
		if settings[key] != want {
			t.Fatalf("setting %s=%q, want %q; all=%v", key, settings[key], want, settings)
		}
	}
	if _, err := store.Query(context.Background(), QueryRequest{
		DeviceID: "device-a", Metric: "metric", From: time.Unix(0, 0), To: time.Unix(300, 0), Step: time.Minute, MaxRows: 21,
	}); err == nil {
		t.Fatal("request above configured result-row budget was accepted")
	}
}
