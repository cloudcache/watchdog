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

type fakeAddressSetRow struct {
	bucket                             time.Time
	dimensionSnapshotID, geoVersion    string
	classificationVersion              uint32
	value                              float64
	received, unknownSampling, quality uint64
}

type fakeAddressSetExecutor struct {
	blocks     [][]fakeAddressSetRow
	err        error
	skipColumn string
}

func (e fakeAddressSetExecutor) Do(ctx context.Context, query ch.Query) error {
	results, ok := query.Result.(proto.Results)
	if !ok {
		return errors.New("address-set runner did not install typed results")
	}
	for _, block := range e.blocks {
		for _, result := range results {
			result.Data.Reset()
		}
		for _, row := range block {
			appendFakeAddressSetRow(results, row, e.skipColumn)
		}
		if query.OnResult != nil {
			if err := query.OnResult(ctx, proto.Block{Columns: len(results), Rows: len(block)}); err != nil {
				return err
			}
		}
	}
	return e.err
}

func TestAddressSetRunnerDecodesMultipleBlocksAndVersions(t *testing.T) {
	compiled := compiledAddressSetQuery(t)
	first := addressSetDataRow(compiled.From, "snapshot-a", "geo-a", 1)
	first.value, first.received, first.unknownSampling, first.quality = 800, 10, 1, 2
	second := addressSetDataRow(compiled.From, "snapshot-b", "geo-b", 2)
	second.value, second.received = 400, 5
	third := addressSetDataRow(compiled.From.Add(time.Minute), "snapshot-b", "geo-b", 2)
	runner, err := NewAddressSetRunner(fakeAddressSetExecutor{blocks: [][]fakeAddressSetRow{{first}, {second, third}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Points) != 3 || !result.MixedVersions || result.VersionCount != 2 || result.Metric.Name != MetricEstimatedBPS || result.Endpoint != AddressSetEndpointEither {
		t.Fatalf("result=%+v", result)
	}
	if result.Points[0].SamplingCompleteness != 0.9 || !result.Points[0].SamplingCompletenessKnown || result.Points[0].QualityRecordRatio != 0.2 || !result.Points[0].QualityRecordRatioKnown {
		t.Fatalf("quality ratios=%+v", result.Points[0])
	}
	result.Sets.IncludeAny[0] = "mutated"
	if compiled.Sets.IncludeAny[0] != "set-a" {
		t.Fatal("result selection aliases compiled query memory")
	}
}

func TestAddressSetRunnerPreservesEmptyResult(t *testing.T) {
	compiled := compiledAddressSetQuery(t)
	runner, err := NewAddressSetRunner(fakeAddressSetExecutor{blocks: [][]fakeAddressSetRow{{}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background(), compiled)
	if err != nil || len(result.Points) != 0 || result.MixedVersions || result.VersionCount != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestAddressSetRunnerRejectsMalformedUnorderedOrUnboundedResults(t *testing.T) {
	compiled := compiledAddressSetQuery(t)
	valid := addressSetDataRow(compiled.From, "snapshot-a", "geo-a", 1)
	tests := []struct {
		name       string
		blocks     [][]fakeAddressSetRow
		skipColumn string
		mutate     func(*CompiledAddressSet)
	}{
		{"duplicate", [][]fakeAddressSetRow{{valid, valid}}, "", nil},
		{"bucket order", [][]fakeAddressSetRow{{addressSetDataRow(compiled.From.Add(time.Minute), "snapshot-a", "geo-a", 1), valid}}, "", nil},
		{"version order", [][]fakeAddressSetRow{{addressSetDataRow(compiled.From, "snapshot-b", "geo-a", 1), valid}}, "", nil},
		{"outside bucket", [][]fakeAddressSetRow{{addressSetDataRow(compiled.To, "snapshot-a", "geo-a", 1)}}, "", nil},
		{"unaligned bucket", [][]fakeAddressSetRow{{addressSetDataRow(compiled.From.Add(time.Second), "snapshot-a", "geo-a", 1)}}, "", nil},
		{"missing snapshot", [][]fakeAddressSetRow{{addressSetDataRow(compiled.From, "", "geo-a", 1)}}, "", nil},
		{"missing geo", [][]fakeAddressSetRow{{addressSetDataRow(compiled.From, "snapshot-a", "", 1)}}, "", nil},
		{"zero classification", [][]fakeAddressSetRow{{addressSetDataRow(compiled.From, "snapshot-a", "geo-a", 0)}}, "", nil},
		{"nan", [][]fakeAddressSetRow{{func() fakeAddressSetRow { row := valid; row.value = math.NaN(); return row }()}}, "", nil},
		{"negative", [][]fakeAddressSetRow{{func() fakeAddressSetRow { row := valid; row.value = -1; return row }()}}, "", nil},
		{"sampling overflow", [][]fakeAddressSetRow{{func() fakeAddressSetRow { row := valid; row.received, row.unknownSampling = 1, 2; return row }()}}, "", nil},
		{"quality overflow", [][]fakeAddressSetRow{{func() fakeAddressSetRow { row := valid; row.received, row.quality = 1, 2; return row }()}}, "", nil},
		{"column mismatch", [][]fakeAddressSetRow{{valid}}, "quality_records", nil},
		{"row bound", [][]fakeAddressSetRow{{valid, addressSetDataRow(compiled.From.Add(time.Minute), "snapshot-a", "geo-a", 1)}}, "", func(value *CompiledAddressSet) { value.MaxResultRows = 1 }},
		{"invalid endpoint", [][]fakeAddressSetRow{{valid}}, "", func(value *CompiledAddressSet) { value.Endpoint = "source" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := compiled
			if test.mutate != nil {
				test.mutate(&current)
			}
			runner, err := NewAddressSetRunner(fakeAddressSetExecutor{blocks: test.blocks, skipColumn: test.skipColumn})
			if err != nil {
				t.Fatal(err)
			}
			result, err := runner.Run(context.Background(), current)
			if err == nil || len(result.Points) != 0 || !strings.Contains(err.Error(), "Flow address-set") {
				t.Fatalf("result=%+v error=%v", result, err)
			}
		})
	}
}

func TestAddressSetRunnerReturnsExecutorFailureWithoutPartialResult(t *testing.T) {
	compiled := compiledAddressSetQuery(t)
	want := errors.New("query cancelled")
	runner, err := NewAddressSetRunner(fakeAddressSetExecutor{blocks: [][]fakeAddressSetRow{{addressSetDataRow(compiled.From, "snapshot-a", "geo-a", 1)}}, err: want})
	if err != nil {
		t.Fatal(err)
	}
	if result, runErr := runner.Run(context.Background(), compiled); !errors.Is(runErr, want) || len(result.Points) != 0 {
		t.Fatalf("result=%+v error=%v", result, runErr)
	}
}

func TestAddressSetRunnerRejectsMissingExecutorOrCompiledContract(t *testing.T) {
	if _, err := NewAddressSetRunner(nil); err == nil {
		t.Fatal("nil executor was accepted")
	}
	compiled := compiledAddressSetQuery(t)
	if _, err := (&AddressSetRunner{}).Run(context.Background(), compiled); err == nil {
		t.Fatal("uninitialized runner was accepted")
	}
	runner, err := NewAddressSetRunner(fakeAddressSetExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	compiled.Query.Body = ""
	if _, err := runner.Run(context.Background(), compiled); err == nil {
		t.Fatal("invalid compiled query was accepted")
	}
}

func compiledAddressSetQuery(t *testing.T) CompiledAddressSet {
	t.Helper()
	compiled, err := CompileAddressSet(Scope{}, validAddressSetRequest(), addressSetNow())
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func addressSetDataRow(bucket time.Time, snapshot, geo string, classification uint32) fakeAddressSetRow {
	return fakeAddressSetRow{
		bucket: bucket, dimensionSnapshotID: snapshot, geoVersion: geo, classificationVersion: classification,
		value: 1, received: 1,
	}
}

func appendFakeAddressSetRow(results proto.Results, row fakeAddressSetRow, skipColumn string) {
	for _, result := range results {
		if result.Name == skipColumn {
			continue
		}
		switch result.Name {
		case "bucket":
			result.Data.(*proto.ColDateTime).Append(row.bucket)
		case "dimension_snapshot_id":
			result.Data.(*proto.ColStr).Append(row.dimensionSnapshotID)
		case "geo_version":
			result.Data.(*proto.ColStr).Append(row.geoVersion)
		case "classification_version":
			result.Data.(*proto.ColUInt32).Append(row.classificationVersion)
		case "value":
			result.Data.(*proto.ColFloat64).Append(row.value)
		case "received_records":
			result.Data.(*proto.ColUInt64).Append(row.received)
		case "unknown_sampling_records":
			result.Data.(*proto.ColUInt64).Append(row.unknownSampling)
		case "quality_records":
			result.Data.(*proto.ColUInt64).Append(row.quality)
		}
	}
}
