// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type fakeOverseasRow struct {
	bucket                             time.Time
	rowKind, geoScope                  string
	direction, ipFamily, geoValue      string
	other                              uint8
	dimensionSnapshotID, geoVersion    string
	classificationVersion              uint32
	value                              float64
	remoteIPs, localHosts              uint64
	received, unknownSampling, quality uint64
	generatedAt                        time.Time
	endpointConsistent, metadata       uint8
	coveredBuckets                     uint64
}

type fakeOverseasExecutor struct {
	blocks     [][]fakeOverseasRow
	err        error
	skipColumn string
}

func (e fakeOverseasExecutor) Do(ctx context.Context, query ch.Query) error {
	results, ok := query.Result.(proto.Results)
	if !ok {
		return errors.New("overseas runner did not install typed results")
	}
	for _, block := range e.blocks {
		for _, result := range results {
			result.Data.Reset()
		}
		for _, row := range block {
			appendFakeOverseasRow(results, row, e.skipColumn)
		}
		if query.OnResult != nil {
			if err := query.OnResult(ctx, proto.Block{Columns: len(results), Rows: len(block)}); err != nil {
				return err
			}
		}
	}
	return e.err
}

func TestOverseasRunnerDecodesKPIKnownAndUnknownGeoAcrossBlocks(t *testing.T) {
	compiled := compiledOverseasQuery(t)
	kpi := overseasKPIDataRow(compiled.From)
	kpi.value, kpi.remoteIPs, kpi.localHosts = 800, 7, 3
	kpi.received, kpi.unknownSampling, kpi.quality = 10, 1, 2
	known := overseasGeoDataRow(compiled.From, OverseasScopeOverseas, "US")
	unknown := overseasGeoDataRow(compiled.From, OverseasScopeUnknownGeo, "_unassigned")
	unknown.dimensionSnapshotID, unknown.geoVersion, unknown.classificationVersion = "snapshot-b", "geo-b", 2
	metadata := overseasMetadataRow(59)
	runner, err := NewOverseasRunner(fakeOverseasExecutor{blocks: [][]fakeOverseasRow{{kpi, known}, {unknown, metadata}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Points) != 3 || result.Metric.Name != MetricEstimatedBPS || result.GeoLevel != OverseasGeoCountry ||
		result.TopN != 5 || !result.IncludeOther || !result.MixedVersions || result.VersionCount != 2 {
		t.Fatalf("result=%+v", result)
	}
	if result.RollupCompleteness.ExpectedBuckets != 60 || result.RollupCompleteness.CoveredBuckets != 59 ||
		result.RollupCompleteness.Complete || result.RollupCompleteness.Ratio != float64(59)/60 {
		t.Fatalf("completeness=%+v", result.RollupCompleteness)
	}
	if result.Points[0].ObservedRemoteIPs != 7 || result.Points[0].ObservedLocalHosts != 3 ||
		result.Points[0].SamplingCompleteness != 0.9 || !result.Points[0].SamplingCompletenessKnown ||
		result.Points[0].QualityRecordRatio != 0.2 || !result.Points[0].QualityRecordRatioKnown {
		t.Fatalf("KPI point=%+v", result.Points[0])
	}
	if result.Points[1].GeoScope != OverseasScopeOverseas || result.Points[1].GeoValue != "US" ||
		result.Points[2].GeoScope != OverseasScopeUnknownGeo || result.Points[2].GeoValue != "_unassigned" {
		t.Fatalf("Geo points=%+v", result.Points[1:])
	}
}

func TestOverseasRunnerPreservesCompleteEmptyResult(t *testing.T) {
	compiled := compiledOverseasQuery(t)
	runner, err := NewOverseasRunner(fakeOverseasExecutor{blocks: [][]fakeOverseasRow{{overseasMetadataRow(60)}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background(), compiled)
	if err != nil || len(result.Points) != 0 || !result.RollupCompleteness.Complete || result.MixedVersions || result.VersionCount != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestOverseasRunnerRejectsMalformedOrInconsistentResults(t *testing.T) {
	compiled := compiledOverseasQuery(t)
	valid := overseasKPIDataRow(compiled.From)
	metadata := overseasMetadataRow(60)
	tests := []struct {
		name       string
		rows       []fakeOverseasRow
		skipColumn string
		mutate     func(*CompiledOverseas)
	}{
		{"duplicate", []fakeOverseasRow{valid, valid, metadata}, "", nil},
		{"data after metadata", []fakeOverseasRow{metadata, valid}, "", nil},
		{"duplicate metadata", []fakeOverseasRow{metadata, metadata}, "", nil},
		{"outside bucket", []fakeOverseasRow{func() fakeOverseasRow { row := valid; row.bucket = compiled.To; return row }(), metadata}, "", nil},
		{"unaligned bucket", []fakeOverseasRow{func() fakeOverseasRow { row := valid; row.bucket = row.bucket.Add(time.Second); return row }(), metadata}, "", nil},
		{"row kind", []fakeOverseasRow{func() fakeOverseasRow { row := valid; row.rowKind = "summary"; return row }(), metadata}, "", nil},
		{"KPI scope", []fakeOverseasRow{func() fakeOverseasRow { row := valid; row.geoScope = "unknown_geo"; return row }(), metadata}, "", nil},
		{"KPI family", []fakeOverseasRow{func() fakeOverseasRow { row := valid; row.ipFamily = "ipv5"; return row }(), metadata}, "", nil},
		{"endpoint mismatch", []fakeOverseasRow{func() fakeOverseasRow { row := valid; row.endpointConsistent = 0; return row }(), metadata}, "", nil},
		{"observed count overflow", []fakeOverseasRow{func() fakeOverseasRow { row := valid; row.remoteIPs = 2; return row }(), metadata}, "", nil},
		{"missing version", []fakeOverseasRow{func() fakeOverseasRow { row := valid; row.geoVersion = ""; return row }(), metadata}, "", nil},
		{"nan", []fakeOverseasRow{func() fakeOverseasRow { row := valid; row.value = math.NaN(); return row }(), metadata}, "", nil},
		{"sampling overflow", []fakeOverseasRow{func() fakeOverseasRow { row := valid; row.received, row.unknownSampling = 1, 2; return row }(), metadata}, "", nil},
		{"unknown Geo identity", []fakeOverseasRow{func() fakeOverseasRow {
			return overseasGeoDataRow(compiled.From, OverseasScopeUnknownGeo, "US")
		}(), metadata}, "", nil},
		{"Geo count", []fakeOverseasRow{func() fakeOverseasRow {
			row := overseasGeoDataRow(compiled.From, OverseasScopeOverseas, "US")
			row.remoteIPs = 1
			return row
		}(), metadata}, "", nil},
		{"malformed metadata", []fakeOverseasRow{func() fakeOverseasRow { row := metadata; row.rowKind = "kpi"; return row }()}, "", nil},
		{"covered data", []fakeOverseasRow{func() fakeOverseasRow { row := valid; row.coveredBuckets = 1; return row }(), metadata}, "", nil},
		{"column mismatch", []fakeOverseasRow{valid, metadata}, "quality_records", nil},
		{"row limit", []fakeOverseasRow{valid, overseasGeoDataRow(compiled.From, OverseasScopeOverseas, "US"), metadata}, "", func(value *CompiledOverseas) { value.MaxResultRows = 2 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := compiled
			if test.mutate != nil {
				test.mutate(&current)
			}
			runner, err := NewOverseasRunner(fakeOverseasExecutor{blocks: [][]fakeOverseasRow{test.rows}, skipColumn: test.skipColumn})
			if err != nil {
				t.Fatal(err)
			}
			result, err := runner.Run(context.Background(), current)
			if err == nil || len(result.Points) != 0 || !strings.Contains(err.Error(), "Flow overseas") {
				t.Fatalf("result=%+v error=%v", result, err)
			}
		})
	}
}

func TestOverseasRunnerRejectsMissingMetadataAndExcessCoverage(t *testing.T) {
	compiled := compiledOverseasQuery(t)
	for _, rows := range [][]fakeOverseasRow{{overseasKPIDataRow(compiled.From)}, {overseasMetadataRow(61)}} {
		runner, err := NewOverseasRunner(fakeOverseasExecutor{blocks: [][]fakeOverseasRow{rows}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := runner.Run(context.Background(), compiled); err == nil {
			t.Fatalf("invalid rows were accepted: %+v", rows)
		}
	}
}

func TestOverseasRunnerReturnsExecutorFailureWithoutPartialResult(t *testing.T) {
	compiled := compiledOverseasQuery(t)
	want := errors.New("query cancelled")
	runner, err := NewOverseasRunner(fakeOverseasExecutor{
		blocks: [][]fakeOverseasRow{{overseasKPIDataRow(compiled.From)}}, err: want,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result, runErr := runner.Run(context.Background(), compiled); !errors.Is(runErr, want) || len(result.Points) != 0 {
		t.Fatalf("result=%+v error=%v", result, runErr)
	}
}

func TestOverseasRunnerRejectsMissingExecutorOrCompiledContract(t *testing.T) {
	if _, err := NewOverseasRunner(nil); err == nil {
		t.Fatal("nil executor was accepted")
	}
	compiled := compiledOverseasQuery(t)
	if _, err := (&OverseasRunner{}).Run(context.Background(), compiled); err == nil {
		t.Fatal("uninitialized runner was accepted")
	}
	runner, err := NewOverseasRunner(fakeOverseasExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	compiled.GeoLevel = "city"
	if _, err := runner.Run(context.Background(), compiled); err == nil {
		t.Fatal("invalid compiled query was accepted")
	}
}

func compiledOverseasQuery(t *testing.T) CompiledOverseas {
	t.Helper()
	compiled, err := CompileOverseas(Scope{TenantID: "tenant-a"}, validOverseasRequest(), overseasNow())
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func overseasKPIDataRow(bucket time.Time) fakeOverseasRow {
	return fakeOverseasRow{
		bucket: bucket, rowKind: "kpi", geoScope: "overseas", direction: "combined", ipFamily: "all",
		dimensionSnapshotID: "snapshot-a", geoVersion: "geo-a", classificationVersion: 1,
		value: 1, received: 1, generatedAt: bucket.Add(time.Minute), endpointConsistent: 1,
	}
}

func overseasGeoDataRow(bucket time.Time, scope OverseasGeoScope, value string) fakeOverseasRow {
	return fakeOverseasRow{
		bucket: bucket, rowKind: "geo", geoScope: string(scope), direction: "combined", ipFamily: "all", geoValue: value,
		dimensionSnapshotID: "snapshot-a", geoVersion: "geo-a", classificationVersion: 1,
		value: 1, received: 1, generatedAt: bucket.Add(time.Minute), endpointConsistent: 1,
	}
}

func overseasMetadataRow(covered uint64) fakeOverseasRow {
	return fakeOverseasRow{
		bucket: time.Unix(0, 0).UTC(), generatedAt: time.Unix(0, 0).UTC(), metadata: 1, coveredBuckets: covered,
	}
}

func appendFakeOverseasRow(results proto.Results, row fakeOverseasRow, skipColumn string) {
	for _, result := range results {
		if result.Name == skipColumn {
			continue
		}
		switch result.Name {
		case "bucket":
			result.Data.(*proto.ColDateTime).Append(row.bucket)
		case "row_kind":
			result.Data.(*proto.ColStr).Append(row.rowKind)
		case "geo_scope":
			result.Data.(*proto.ColStr).Append(row.geoScope)
		case "direction":
			result.Data.(*proto.ColStr).Append(row.direction)
		case "ip_family":
			result.Data.(*proto.ColStr).Append(row.ipFamily)
		case "geo_value":
			result.Data.(*proto.ColStr).Append(row.geoValue)
		case "is_other":
			result.Data.(*proto.ColUInt8).Append(row.other)
		case "dimension_snapshot_id":
			result.Data.(*proto.ColStr).Append(row.dimensionSnapshotID)
		case "geo_version":
			result.Data.(*proto.ColStr).Append(row.geoVersion)
		case "classification_version":
			result.Data.(*proto.ColUInt32).Append(row.classificationVersion)
		case "value":
			result.Data.(*proto.ColFloat64).Append(row.value)
		case "observed_remote_ips":
			result.Data.(*proto.ColUInt64).Append(row.remoteIPs)
		case "observed_local_hosts":
			result.Data.(*proto.ColUInt64).Append(row.localHosts)
		case "received_records":
			result.Data.(*proto.ColUInt64).Append(row.received)
		case "unknown_sampling_records":
			result.Data.(*proto.ColUInt64).Append(row.unknownSampling)
		case "quality_records":
			result.Data.(*proto.ColUInt64).Append(row.quality)
		case "generated_at":
			result.Data.(*proto.ColDateTime64).Append(row.generatedAt)
		case "endpoint_consistent":
			result.Data.(*proto.ColUInt8).Append(row.endpointConsistent)
		case "is_metadata":
			result.Data.(*proto.ColUInt8).Append(row.metadata)
		case "covered_buckets":
			result.Data.(*proto.ColUInt64).Append(row.coveredBuckets)
		}
	}
}
