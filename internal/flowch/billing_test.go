// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type flowBillingExecutor struct {
	query ch.Query
}

func (e *flowBillingExecutor) Do(ctx context.Context, query ch.Query) error {
	e.query = query
	result := query.Result.(proto.Results)
	bucket := result[0].Data.(*proto.ColDateTime)
	columns := make([]*proto.ColUInt64, 0, 16)
	for _, index := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17} {
		columns = append(columns, result[index].Data.(*proto.ColUInt64))
	}
	refs := []*proto.ColStr{result[18].Data.(*proto.ColStr), result[19].Data.(*proto.ColStr), result[20].Data.(*proto.ColStr)}
	base := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	rows := [][]uint64{
		{100, 200, 300, 90, 180, 270, 80, 160, 240, 4, 3, 4, 2, 4, 3, 7, 8},
		{150, 250, 400, 140, 230, 370, 120, 210, 330, 2, 2, 2, 2, 2, 2, 8, 9},
	}
	for rowIndex, row := range rows {
		bucket.Append(base.Add(time.Duration(rowIndex) * 5 * time.Minute))
		for columnIndex, value := range row {
			columns[columnIndex].Append(value)
		}
		refs[0].Append([]string{"dim-b,dim-a", "dim-b"}[rowIndex])
		refs[1].Append([]string{"geo-2", "geo-1"}[rowIndex])
		refs[2].Append([]string{"3", "4,3"}[rowIndex])
	}
	return query.OnResult(ctx, proto.Block{Rows: len(rows)})
}

func TestReadFlowBillingConservesLayerEvidence(t *testing.T) {
	exec := &flowBillingExecutor{}
	store := &NativeInserter{executor: exec}
	from := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	result, err := store.ReadBilling(context.Background(), FlowBillingRequest{
		Ports: []BillingPortScope{
			{DeviceID: "device-b", IfIndex: 8, Direction: "out"},
			{DeviceID: "device-a", IfIndex: 3, Direction: "agg"},
		},
		From: from, To: from.Add(10 * time.Minute), Now: from.Add(time.Hour), MaxBuckets: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Layers) != 3 {
		t.Fatalf("layers=%+v", result.Layers)
	}
	if raw := result.Layers[0]; raw.Layer != "raw" || raw.InBytes != 250 || raw.OutBytes != 450 || raw.SelectedBytes != 700 || raw.Coverage != .875 || raw.UnknownSamplingRecords != 1 {
		t.Fatalf("raw=%+v", raw)
	}
	if supplier := result.Layers[1]; supplier.Layer != "supplier" || supplier.SelectedBytes != 640 || supplier.Coverage != .75 || supplier.UnknownSamplingRecords != 2 {
		t.Fatalf("supplier=%+v", supplier)
	}
	if customer := result.Layers[2]; customer.Layer != "customer" || customer.SelectedBytes != 570 || customer.Coverage != .875 || customer.UnknownSamplingRecords != 1 {
		t.Fatalf("customer=%+v", customer)
	}
	if result.SourceGenerationMin != 7 || result.SourceGenerationMax != 9 || strings.Join(result.DimensionSnapshotRefs, ",") != "dim-a,dim-b" || strings.Join(result.GeoVersionRefs, ",") != "geo-1,geo-2" || strings.Join(result.ClassificationVersions, ",") != "3,4" {
		t.Fatalf("provenance=%+v", result)
	}
	if !strings.Contains(exec.query.Body, "flow_records AS records FINAL") || !strings.Contains(exec.query.Body, "CROSS JOIN flow_billing_scope") || !strings.Contains(exec.query.Body, "records.fact_schema>=2") || !strings.Contains(exec.query.Body, "records.disposition='count'") || strings.Contains(strings.ToLower(exec.query.Body), "tenant") {
		t.Fatalf("unexpected billing SQL: %s", exec.query.Body)
	}
	devices := exec.query.ExternalData[0].Data.(*proto.ColStr)
	indexes := exec.query.ExternalData[1].Data.(*proto.ColUInt32)
	if devices.Row(0) != "device-a" || indexes.Row(0) != 3 || devices.Row(1) != "device-b" || indexes.Row(1) != 8 {
		t.Fatalf("unsorted scope devices=%v indexes=%v", devices, indexes)
	}
}

func TestReadFlowBillingRejectsOpenRangeAndConflictingPort(t *testing.T) {
	store := &NativeInserter{executor: &flowBillingExecutor{}}
	now := time.Date(2026, 9, 9, 2, 0, 0, 0, time.UTC)
	request := FlowBillingRequest{
		Ports: []BillingPortScope{{DeviceID: "device-a", IfIndex: 7, Direction: "in"}, {DeviceID: "device-a", IfIndex: 7, Direction: "out"}},
		From:  now.Add(-time.Hour), To: now, Now: now,
	}
	if _, err := store.ReadBilling(context.Background(), request); err == nil {
		t.Fatal("conflicting directions were accepted")
	}
	request.Ports = []BillingPortScope{{DeviceID: "device-a", IfIndex: 7, Direction: "in"}}
	request.To = now.Add(5 * time.Minute)
	if _, err := store.ReadBilling(context.Background(), request); err == nil {
		t.Fatal("open billing bucket was accepted")
	}
}

func TestFlowBillingUnknownSamplingNeverAddsZeroRate(t *testing.T) {
	acc := &flowBillingAccumulator{result: BillingLayerResult{Layer: "raw", ExpectedBuckets: 2}}
	from := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	accumulateFlowBillingLayer(acc, from, 30000, 0, 30000, 1, 1)
	accumulateFlowBillingLayer(acc, from.Add(5*time.Minute), 0, 0, 0, 1, 0)
	if len(acc.result.SelectedRates) != 1 || acc.result.SelectedRates[0] != 800 || acc.result.UnknownSamplingRecords != 1 {
		t.Fatalf("unknown-only bucket polluted the rate series: %+v", acc.result)
	}
	if len(acc.result.RateBuckets) != 2 || !acc.result.RateBuckets[0].Complete || acc.result.RateBuckets[1].Complete {
		t.Fatalf("rate completeness=%+v", acc.result.RateBuckets)
	}
}

func TestRealClickHouseFlowBillingEvidence(t *testing.T) {
	ctx, store := openDataIntegrationClickHouse(t, "watchdog_flow_it_billing")
	from := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	first := testEnrichedRecord(1, 100, 1000)
	first.EventTime, first.DeviceID, first.InIf, first.OutIf = from.Add(time.Minute), "device-a", 7, 8
	first.Dimensions.SnapshotID, first.RemoteGeo.Version, first.ClassificationVersion = "dimension-1", "geo-1", 3
	second := testEnrichedRecord(2, 200, 2000)
	second.EventTime, second.DeviceID, second.InIf, second.OutIf = from.Add(6*time.Minute), "device-a", 7, 8
	second.Dimensions.SnapshotID, second.RemoteGeo.Version, second.ClassificationVersion = "dimension-2", "geo-2", 4
	second.EstimatedValid = false
	second.EstimatedBytes, second.EstimatedPackets = 0, 0
	insertIntegrationBatch(t, ctx, store, integrationBatch(30, from.Add(10*time.Minute), first, second))

	result, err := store.ReadBilling(ctx, FlowBillingRequest{
		Ports: []BillingPortScope{{DeviceID: "device-a", IfIndex: 7, Direction: "in"}},
		From:  from, To: from.Add(10 * time.Minute), Now: from.Add(time.Hour), MaxBuckets: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Layers) != 3 {
		t.Fatalf("layers=%+v", result.Layers)
	}
	for _, layer := range result.Layers {
		if layer.InBytes != 1000 || layer.OutBytes != 0 || layer.SelectedBytes != 1000 ||
			layer.ExpectedBuckets != 2 || layer.ObservedBuckets != 2 || layer.Coverage != .5 || layer.UnknownSamplingRecords != 1 {
			t.Fatalf("layer=%+v", layer)
		}
	}
	if result.SourceGenerationMin == 0 || result.SourceGenerationMax == 0 ||
		strings.Join(result.DimensionSnapshotRefs, ",") != "dimension-1,dimension-2" ||
		strings.Join(result.GeoVersionRefs, ",") != "geo-1,geo-2" ||
		strings.Join(result.ClassificationVersions, ",") != "3,4" {
		t.Fatalf("provenance=%+v", result)
	}
}
