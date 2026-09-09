package snmpch

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type aggregateExecutor struct {
	query ch.Query
}

func (e *aggregateExecutor) Do(ctx context.Context, query ch.Query) error {
	e.query = query
	result := query.Result.(proto.Results)
	buckets := result[0].Data.(*proto.ColDateTime)
	values := result[1].Data.(*proto.ColFloat64)
	buckets.Append(time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC))
	values.Append(12_000)
	return query.OnResult(ctx, proto.Block{Rows: 1})
}

func TestAggregateUsesOneScopedRateQuery(t *testing.T) {
	exec := &aggregateExecutor{}
	store, _ := New(exec)
	result, err := store.Aggregate(context.Background(), AggregateRequest{
		Scopes: []Scope{
			{DeviceID: "device-b", PortID: "port-b"},
			{DeviceID: "device-a", PortID: "port-a"},
			{DeviceID: "device-a"}, // device-wide scope supersedes port-a
		},
		Metric: MetricIfInBPS, Method: "sum",
		From: time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 9, 9, 2, 0, 0, 0, time.UTC), Step: 5 * time.Minute, MaxRows: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Points) != 1 || result.Points[0].Value != 12_000 {
		t.Fatalf("result=%+v", result)
	}
	if !strings.Contains(exec.query.Body, "FROM snmp_samples AS s FINAL") || !strings.Contains(exec.query.Body, "sum(value)") || strings.Contains(strings.ToLower(exec.query.Body), "tenant") {
		t.Fatalf("unexpected aggregate SQL: %s", exec.query.Body)
	}
	if exec.query.ExternalTable != "snmp_scope" || len(exec.query.ExternalData) != 2 {
		t.Fatalf("external scope=%q/%d", exec.query.ExternalTable, len(exec.query.ExternalData))
	}
	devices := exec.query.ExternalData[0].Data.(*proto.ColStr)
	ports := exec.query.ExternalData[1].Data.(*proto.ColStr)
	if devices.Rows() != 2 || devices.Row(0) != "device-a" || ports.Row(0) != "" || devices.Row(1) != "device-b" || ports.Row(1) != "port-b" {
		t.Fatalf("normalized scope devices=%v ports=%v", devices, ports)
	}
}

func TestAggregateRejectsSQLMethodAndBudget(t *testing.T) {
	store, _ := New(&aggregateExecutor{})
	base := AggregateRequest{
		Scopes: []Scope{{DeviceID: "device-a"}}, Metric: MetricIfInBPS,
		From: time.Unix(0, 0), To: time.Unix(3600, 0), Step: time.Minute, MaxRows: 10,
	}
	base.Method = "sum); DROP TABLE snmp_samples; --"
	if _, err := store.Aggregate(context.Background(), base); err == nil {
		t.Fatal("unsafe aggregate method was accepted")
	}
	base.Method = "sum"
	base.MaxRows = maxAggregateRows + 1
	if _, err := store.Aggregate(context.Background(), base); err == nil {
		t.Fatal("oversized aggregate budget was accepted")
	}
}
