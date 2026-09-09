package snmpch

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type billingExecutor struct {
	query ch.Query
}

func (e *billingExecutor) Do(ctx context.Context, query ch.Query) error {
	e.query = query
	result := query.Result.(proto.Results)
	buckets := result[0].Data.(*proto.ColDateTime)
	inBytes := result[1].Data.(*proto.ColUInt64)
	outBytes := result[2].Data.(*proto.ColUInt64)
	selectedBytes := result[3].Data.(*proto.ColUInt64)
	inBPS := result[4].Data.(*proto.ColFloat64)
	outBPS := result[5].Data.(*proto.ColFloat64)
	selectedBPS := result[6].Data.(*proto.ColFloat64)
	coverage := result[7].Data.(*proto.ColFloat64)
	reset := result[8].Data.(*proto.ColUInt8)
	gap := result[9].Data.(*proto.ColUInt8)
	generation := result[10].Data.(*proto.ColUInt64)
	presentPorts := result[11].Data.(*proto.ColUInt64)
	base := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	for i, row := range []struct {
		in, out, selected                       uint64
		inRate, outRate, selectedRate, coverage float64
		reset, gap                              uint8
		generation                              uint64
	}{
		{100, 200, 300, 10, 20, 30, 1, 0, 0, 7},
		{150, 250, 400, 15, 25, 40, .5, 1, 0, 8},
	} {
		buckets.Append(base.Add(time.Duration(i) * 5 * time.Minute))
		inBytes.Append(row.in)
		outBytes.Append(row.out)
		selectedBytes.Append(row.selected)
		inBPS.Append(row.inRate)
		outBPS.Append(row.outRate)
		selectedBPS.Append(row.selectedRate)
		coverage.Append(row.coverage)
		reset.Append(row.reset)
		gap.Append(row.gap)
		generation.Append(row.generation)
		presentPorts.Append(2)
	}
	return query.OnResult(ctx, proto.Block{Rows: 2})
}

func TestReadBillingConservesClosedBucketTotals(t *testing.T) {
	exec := &billingExecutor{}
	store, _ := New(exec)
	from := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	result, err := store.ReadBilling(context.Background(), BillingRequest{
		Ports: []BillingPort{{PortID: "port-b", Direction: "out"}, {PortID: "port-a", Direction: "agg"}},
		From:  from, To: from.Add(10 * time.Minute), Now: from.Add(time.Hour), MaxBuckets: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.TotalInBytes != 250 || result.TotalOutBytes != 450 || result.TotalSelectedBytes != 700 {
		t.Fatalf("billing totals=%+v", result)
	}
	if result.ExpectedBuckets != 2 || result.ObservedBuckets != 2 || result.ExpectedPorts != 2 || result.Coverage != .75 || result.ResetBuckets != 1 || result.GapBuckets != 0 {
		t.Fatalf("billing quality=%+v", result)
	}
	if result.Rate95Selected != 40 || result.RateAverageSelected != 35 || result.GenerationMin != 7 || result.GenerationMax != 8 {
		t.Fatalf("billing rates/generation=%+v", result)
	}
	if !strings.Contains(exec.query.Body, "snmp_interface_traffic_5m FINAL") || !strings.Contains(exec.query.Body, "CROSS JOIN snmp_billing_scope") || !strings.Contains(exec.query.Body, "LEFT JOIN published") || strings.Contains(strings.ToLower(exec.query.Body), "tenant") {
		t.Fatalf("unexpected billing SQL: %s", exec.query.Body)
	}
	ports := exec.query.ExternalData[0].Data.(*proto.ColStr)
	directions := exec.query.ExternalData[1].Data.(*proto.ColStr)
	if ports.Row(0) != "port-a" || directions.Row(0) != "agg" || ports.Row(1) != "port-b" || directions.Row(1) != "out" {
		t.Fatalf("billing scope ports=%v directions=%v", ports, directions)
	}
}

func TestReadBillingRejectsOpenRangeAndConflictingPort(t *testing.T) {
	store, _ := New(&billingExecutor{})
	now := time.Date(2026, 9, 9, 2, 0, 0, 0, time.UTC)
	request := BillingRequest{
		Ports: []BillingPort{{PortID: "port-a", Direction: "in"}, {PortID: "port-a", Direction: "out"}},
		From:  now.Add(-time.Hour), To: now, Now: now,
	}
	if _, err := store.ReadBilling(context.Background(), request); err == nil {
		t.Fatal("conflicting port directions were accepted")
	}
	request.Ports = []BillingPort{{PortID: "port-a", Direction: "in"}}
	request.To = now.Add(5 * time.Minute)
	if _, err := store.ReadBilling(context.Background(), request); err == nil {
		t.Fatal("open billing bucket was accepted")
	}
}
