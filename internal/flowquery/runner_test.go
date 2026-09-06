// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func TestClassifyExecutionErrorMapsResourceLimitsToTypedError(t *testing.T) {
	// Every Flow query sets read/memory guards; when a range trips one — most
	// importantly a mixed-version raw scan whose pre-FINAL row count the caller
	// cannot predict — the engine overflow must surface as the typed
	// limit_exceeded code, not a generic internal error.
	for _, code := range []proto.Error{
		proto.ErrTooManyRows,
		proto.ErrTooManyBytes,
		proto.ErrTooManyRowsOrBytes,
		proto.ErrMemoryLimitExceeded,
		proto.ErrSetSizeLimitExceeded,
	} {
		wrapped := fmt.Errorf("execute ClickHouse Flow query: %w", &ch.Exception{Code: code, Message: "limit"})
		if got := classifyExecutionError(wrapped); !IsRequestError(got, "from/to", ErrorLimitExceeded) {
			t.Fatalf("code %v: got %v, want a from/to limit_exceeded RequestError", code, got)
		}
	}
}

func TestClassifyExecutionErrorPassesThroughNonLimitErrors(t *testing.T) {
	nonLimit := fmt.Errorf("execute ClickHouse Flow query: %w", &ch.Exception{Code: proto.ErrUnknownTable})
	if got := classifyExecutionError(nonLimit); IsRequestError(got, "from/to", ErrorLimitExceeded) {
		t.Fatalf("a non-limit ClickHouse exception was mistyped as limit_exceeded: %v", got)
	}
	plain := errors.New("connection reset")
	if got := classifyExecutionError(plain); got != plain {
		t.Fatalf("a plain transport error must pass through unchanged, got %v", got)
	}
}

type fakeResultRow struct {
	bucket, generatedAt                             time.Time
	dimensionValue, dimensionSnapshotID, geoVersion string
	isOther, isMetadata                             uint8
	classificationVersion                           uint32
	value                                           float64
	received, unknownSampling, quality, covered     uint64
}

type fakeResultExecutor struct {
	blocks [][]fakeResultRow
	err    error
}

func (e fakeResultExecutor) Do(ctx context.Context, query ch.Query) error {
	results, ok := query.Result.(proto.Results)
	if !ok {
		return errors.New("runner did not install typed results")
	}
	for _, block := range e.blocks {
		for _, column := range results {
			column.Data.Reset()
		}
		for _, row := range block {
			appendFakeRow(results, row)
		}
		if query.OnResult != nil {
			if err := query.OnResult(ctx, proto.Block{Columns: len(results), Rows: len(block)}); err != nil {
				return err
			}
		}
	}
	return e.err
}

func TestRunnerDecodesMultipleBlocksAndComputesCompleteness(t *testing.T) {
	compiled := compiledQuery(t)
	first := dataRow(compiled.From, "64512", false)
	first.received, first.unknownSampling, first.quality, first.value = 10, 1, 2, 800
	second := dataRow(compiled.From.Add(time.Minute), "_other", true)
	second.dimensionSnapshotID, second.geoVersion, second.classificationVersion = "snapshot-2", "geo-2", 8
	metadata := metadataRow(60)
	runner, err := NewRunner(fakeResultExecutor{blocks: [][]fakeResultRow{{first}, {second, metadata}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Points) != 2 || !result.MixedVersions || result.VersionCount != 2 || !result.Dimension.Additive {
		t.Fatalf("result metadata=%+v", result)
	}
	if !result.RollupCompleteness.Complete || result.RollupCompleteness.Ratio != 1 || result.RollupCompleteness.CoveredBuckets != 60 {
		t.Fatalf("rollup completeness=%+v", result.RollupCompleteness)
	}
	if !result.Points[0].SamplingCompletenessKnown || result.Points[0].SamplingCompleteness != 0.9 || result.Points[0].QualityRecordRatio != 0.2 {
		t.Fatalf("point completeness=%+v", result.Points[0])
	}
	if !result.Points[1].Other || result.Points[1].DimensionValue != "_other" {
		t.Fatalf("other point=%+v", result.Points[1])
	}
}

func TestRunnerPreservesEmptyResultAndIncompleteRollup(t *testing.T) {
	compiled := compiledQuery(t)
	runner, err := NewRunner(fakeResultExecutor{blocks: [][]fakeResultRow{{metadataRow(30)}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Points) != 0 || result.RollupCompleteness.Complete || result.RollupCompleteness.Ratio != 0.5 {
		t.Fatalf("empty result=%+v", result)
	}
}

func TestRunnerRejectsMalformedOrUnboundedResults(t *testing.T) {
	compiled := compiledQuery(t)
	valid := dataRow(compiled.From, "64512", false)
	tests := []struct {
		name   string
		blocks [][]fakeResultRow
		mutate func(*Compiled)
	}{
		{"missing metadata", [][]fakeResultRow{{valid}}, nil},
		{"duplicate metadata", [][]fakeResultRow{{metadataRow(60), metadataRow(60)}}, nil},
		{"coverage overflow", [][]fakeResultRow{{metadataRow(61)}}, nil},
		{"quality overflow", [][]fakeResultRow{{func() fakeResultRow { row := valid; row.unknownSampling = 2; row.received = 1; return row }(), metadataRow(60)}}, nil},
		{"invalid number", [][]fakeResultRow{{func() fakeResultRow { row := valid; row.value = math.Inf(1); return row }(), metadataRow(60)}}, nil},
		{"outside bucket", [][]fakeResultRow{{func() fakeResultRow { row := valid; row.bucket = compiled.To; return row }(), metadataRow(60)}}, nil},
		{"duplicate point", [][]fakeResultRow{{valid}, {valid, metadataRow(60)}}, nil},
		{"row limit", [][]fakeResultRow{{valid, metadataRow(60)}}, func(value *Compiled) { value.MaxResultRows = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := compiled
			if test.mutate != nil {
				test.mutate(&current)
			}
			runner, err := NewRunner(fakeResultExecutor{blocks: test.blocks})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runner.Run(context.Background(), current); err == nil || !strings.Contains(err.Error(), "invalid ClickHouse Flow result") {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestRunnerReturnsExecutorFailureWithoutPartialResult(t *testing.T) {
	compiled := compiledQuery(t)
	want := errors.New("connection reset")
	runner, err := NewRunner(fakeResultExecutor{blocks: [][]fakeResultRow{{dataRow(compiled.From, "64512", false)}}, err: want})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := runner.Run(context.Background(), compiled); !errors.Is(err, want) || len(result.Points) != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestRunnerRejectsMissingExecutorOrCompiledContract(t *testing.T) {
	if _, err := NewRunner(nil); err == nil {
		t.Fatal("nil executor was accepted")
	}
	compiled := compiledQuery(t)
	if _, err := (&Runner{}).Run(context.Background(), compiled); err == nil {
		t.Fatal("uninitialized runner was accepted")
	}
	runner, err := NewRunner(fakeResultExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	compiled.Query.Body = ""
	if _, err := runner.Run(context.Background(), compiled); err == nil {
		t.Fatal("invalid compiled query was accepted")
	}
}

func compiledQuery(t *testing.T) Compiled {
	t.Helper()
	request := validRequest()
	request.Dimension = DimensionASN
	compiled, err := Compile(Scope{TenantID: "tenant-a"}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func dataRow(bucket time.Time, value string, other bool) fakeResultRow {
	row := fakeResultRow{
		bucket: bucket, generatedAt: bucket.Add(2 * time.Minute), dimensionValue: value,
		dimensionSnapshotID: "snapshot-1", geoVersion: "geo-1", classificationVersion: 7,
		value: 1, received: 1,
	}
	if other {
		row.isOther = 1
	}
	return row
}

func metadataRow(covered uint64) fakeResultRow {
	return fakeResultRow{bucket: time.Unix(0, 0).UTC(), generatedAt: time.Unix(0, 0).UTC(), isMetadata: 1, covered: covered}
}

func appendFakeRow(results proto.Results, row fakeResultRow) {
	for _, result := range results {
		switch result.Name {
		case "bucket":
			result.Data.(*proto.ColDateTime).Append(row.bucket)
		case "dimension_value":
			result.Data.(*proto.ColStr).Append(row.dimensionValue)
		case "is_other":
			column := result.Data.(*proto.ColUInt8)
			*column = append(*column, row.isOther)
		case "dimension_snapshot_id":
			result.Data.(*proto.ColStr).Append(row.dimensionSnapshotID)
		case "geo_version":
			result.Data.(*proto.ColStr).Append(row.geoVersion)
		case "classification_version":
			column := result.Data.(*proto.ColUInt32)
			*column = append(*column, row.classificationVersion)
		case "value":
			column := result.Data.(*proto.ColFloat64)
			*column = append(*column, row.value)
		case "received_records":
			column := result.Data.(*proto.ColUInt64)
			*column = append(*column, row.received)
		case "unknown_sampling_records":
			column := result.Data.(*proto.ColUInt64)
			*column = append(*column, row.unknownSampling)
		case "quality_records":
			column := result.Data.(*proto.ColUInt64)
			*column = append(*column, row.quality)
		case "generated_at":
			result.Data.(*proto.ColDateTime64).Append(row.generatedAt)
		case "is_metadata":
			column := result.Data.(*proto.ColUInt8)
			*column = append(*column, row.isMetadata)
		case "covered_buckets":
			column := result.Data.(*proto.ColUInt64)
			*column = append(*column, row.covered)
		}
	}
}
