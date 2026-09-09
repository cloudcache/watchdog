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
