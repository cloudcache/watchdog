// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

func TestRealClickHouseDetailViewsAndProvenance(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_provenance")

	window := time.Date(2026, 9, 5, 13, 0, 0, 0, time.UTC)
	wanted := netip.MustParseAddr("192.0.2.20")
	remote := netip.MustParseAddr("2001:db8::20")
	legacy := integrationProvenanceRecord(1, window.Add(10*time.Second), wanted, remote, 100)
	insertLegacyIntegrationBatch(t, ctx, native, integrationBatch(40, window.Add(time.Minute), legacy))

	dropped := integrationProvenanceRecord(2, window.Add(20*time.Second), wanted, remote, 200)
	dropped.Disposition = flowdimension.DispositionDrop
	third := integrationProvenanceRecord(3, window.Add(30*time.Second), wanted, remote, 300)
	fourth := integrationProvenanceRecord(4, window.Add(30*time.Second), wanted, remote, 400)
	insertIntegrationBatch(t, ctx, native, integrationBatch(41, window.Add(2*time.Minute), dropped, third, fourth))

	runner, err := flowquery.NewDetailRunner(&integrationBlockExecutor{executor: native.executor})
	if err != nil {
		t.Fatal(err)
	}
	rawRequest := integrationProvenanceRequest(window, wanted, flowquery.ViewRaw, 2)
	rawFirst := runIntegrationDetail(t, ctx, runner, rawRequest)
	assertDetailPage(t, rawFirst, []byte{4, 3}, []uint64{400, 300}, true)
	rawRequest.Cursor = rawFirst.NextCursor
	rawSecond := runIntegrationDetail(t, ctx, runner, rawRequest)
	assertDetailPage(t, rawSecond, []byte{2, 1}, []uint64{200, 100}, false)

	customer := runIntegrationDetail(t, ctx, runner, integrationProvenanceRequest(window, wanted, flowquery.ViewCustomer, 10))
	assertDetailPage(t, customer, []byte{4, 3, 1}, []uint64{400, 300, 100}, false)

	fullSupplier := integrationProvenanceRequest(window, wanted, flowquery.ViewSupplier, 10)
	fullResult, err := runner.Run(ctx, compileIntegrationDetail(t, fullSupplier))
	if !errors.Is(err, flowquery.ErrSupplierProvenanceUnavailable) || len(fullResult.Rows) != 0 {
		t.Fatalf("mixed-schema supplier result=%+v error=%v", fullResult, err)
	}

	supplierRequest := integrationProvenanceRequest(window, wanted, flowquery.ViewSupplier, 1)
	supplierRequest.From = window.Add(20 * time.Second)
	supplierFirst := runIntegrationDetail(t, ctx, runner, supplierRequest)
	assertSupplierPage(t, supplierFirst, []byte{4}, true)
	supplierRequest.Cursor = supplierFirst.NextCursor
	supplierSecond := runIntegrationDetail(t, ctx, runner, supplierRequest)
	assertSupplierPage(t, supplierSecond, []byte{3}, false)

	emptyRequest := integrationProvenanceRequest(window, netip.MustParseAddr("192.0.2.99"), flowquery.ViewSupplier, 10)
	empty := runIntegrationDetail(t, ctx, runner, emptyRequest)
	if len(empty.Rows) != 0 || !empty.SupplierProvenanceComplete || empty.MinimumFactSchema != 0 {
		t.Fatalf("empty supplier result=%+v", empty)
	}

	compiledRaw := compileIntegrationDetail(t, integrationProvenanceRequest(window, wanted, flowquery.ViewRaw, 10))
	for _, setting := range []string{"max_rows_to_read", "max_bytes_to_read"} {
		limitedRunner, createErr := flowquery.NewDetailRunner(&integrationSettingExecutor{
			executor: native.executor, overrides: map[string]string{setting: "1"},
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		limited, runErr := limitedRunner.Run(ctx, compiledRaw)
		if runErr == nil || len(limited.Rows) != 0 {
			t.Fatalf("%s limited result=%+v error=%v", setting, limited, runErr)
		}
	}
}

func integrationProvenanceRecord(index byte, eventTime time.Time, source, destination netip.Addr, rawBytes uint64) flowworker.EnrichedRecord {
	record := integrationDetailRecord(index, eventTime, source, destination, rawBytes)
	record.SupplierRemoteGeo = flowdimension.GeoInfo{
		Country: "US", Version: "supplier-geo-a", Source: flowdimension.GeoSchemaV2,
		ContinentID: "NorthAmerica", RegionID: "NorthernAmerica", CountryID: "US", CityID: "SFO",
	}
	record.SupplierRemoteASN = 64_512
	record.SupplierRemoteASNSource = flowworker.ASNSourceGeoV2
	record.SupplierCategory = flowdimension.CategoryOverseas
	return record
}

func integrationProvenanceRequest(window time.Time, ip netip.Addr, view flowquery.View, limit uint16) flowquery.DetailRequest {
	request := integrationDetailRequest(window.Add(30*time.Second), ip, flowquery.DetailEndpointSource, limit)
	request.From = window
	request.To = window.Add(time.Minute)
	request.View = view
	if view == flowquery.ViewSupplier {
		request.Fields = append(request.Fields,
			flowquery.DetailFieldCategory,
			flowquery.DetailFieldRemoteASN,
			flowquery.DetailFieldRemoteCountry,
			flowquery.DetailFieldGeoVersion,
		)
	}
	return request
}

func insertLegacyIntegrationBatch(t *testing.T, ctx context.Context, native *NativeInserter, batch *flowworker.EnrichedBatch) {
	t.Helper()
	blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{batch}, BatchLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || len(blocks[0].Records) != 1 {
		t.Fatalf("legacy prepared blocks=%d records=%d, want 1/1", len(blocks), len(blocks[0].Records))
	}
	records, err := buildRecordInput(blocks[0])
	if err != nil {
		t.Fatal(err)
	}
	for index := range records {
		if records[index].Name == "fact_schema" {
			records[index].Data = proto.ColUInt16{1}
		}
	}
	token := blockDeduplicationToken(blocks[0]) + ":legacy-schema"
	if err := native.executor.Do(ctx, insertQuery(flowRecordsTable, token, records)); err != nil {
		t.Fatal(err)
	}
}

func assertSupplierPage(t *testing.T, result flowquery.DetailResult, ids []byte, hasMore bool) {
	t.Helper()
	raw := make([]uint64, len(ids))
	for index, id := range ids {
		raw[index] = uint64(id) * 100
	}
	assertDetailPage(t, result, ids, raw, hasMore)
	if !result.SupplierProvenanceComplete || result.MinimumFactSchema != 2 {
		t.Fatalf("supplier completeness=%+v", result)
	}
	for _, row := range result.Rows {
		if row.Values[flowquery.DetailFieldCategory] != string(flowdimension.CategoryOverseas) ||
			row.Values[flowquery.DetailFieldRemoteASN] != uint64(64_512) ||
			row.Values[flowquery.DetailFieldRemoteCountry] != "US" ||
			row.Values[flowquery.DetailFieldGeoVersion] != "supplier-geo-a" {
			t.Fatalf("supplier row=%+v", row)
		}
	}
}
