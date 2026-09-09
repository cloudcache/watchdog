// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type fakeJointRow struct {
	bucket, observedAt                 time.Time
	dimensions                         []string
	isOther                            uint8
	dimensionSnapshotID, geoVersion    string
	classificationVersion              uint32
	value                              float64
	received, unknownSampling, quality uint64
}

type fakeJointExecutor struct {
	blocks [][]fakeJointRow
	err    error
}

func (e fakeJointExecutor) Do(ctx context.Context, query ch.Query) error {
	results, ok := query.Result.(proto.Results)
	if !ok {
		return errors.New("joint runner did not install typed results")
	}
	for _, block := range e.blocks {
		for _, column := range results {
			column.Data.Reset()
		}
		for _, row := range block {
			appendFakeJointRow(results, row)
		}
		if query.OnResult != nil {
			if err := query.OnResult(ctx, proto.Block{Columns: len(results), Rows: len(block)}); err != nil {
				return err
			}
		}
	}
	return e.err
}

func TestJointRunnerDecodesTrueTuplesAcrossBlocks(t *testing.T) {
	compiled := compiledJointQuery(t)
	first := validFakeJointRow(compiled.From, []string{"CN", "4134"})
	first.received, first.unknownSampling, first.quality, first.value = 10, 1, 2, 800
	second := validFakeJointRow(compiled.From.Add(time.Minute), []string{"_other", "_other"})
	second.isOther = 1
	second.dimensionSnapshotID, second.geoVersion, second.classificationVersion = "snapshot-2", "geo-2", 8
	runner, err := NewJointRunner(fakeJointExecutor{blocks: [][]fakeJointRow{{first}, {second}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Points) != 2 || !result.MixedVersions || result.VersionCount != 2 || result.Plan.Source != "flow_records" {
		t.Fatalf("result=%+v", result)
	}
	if got := result.Points[0]; len(got.DimensionValues) != 2 || got.DimensionValues[0] != "CN" || got.DimensionValues[1] != "4134" ||
		!got.SamplingCompletenessKnown || got.SamplingCompleteness != 0.9 || got.QualityRecordRatio != 0.2 {
		t.Fatalf("point=%+v", got)
	}
	if !result.Points[1].Other {
		t.Fatalf("other=%+v", result.Points[1])
	}
}

func TestJointRunnerFailsClosedOnMalformedTupleOrExecutionFailure(t *testing.T) {
	compiled := compiledJointQuery(t)
	malformed := validFakeJointRow(compiled.From, []string{"CN"})
	runner, _ := NewJointRunner(fakeJointExecutor{blocks: [][]fakeJointRow{{malformed}}})
	if result, err := runner.Run(context.Background(), compiled); err == nil || len(result.Points) != 0 {
		t.Fatalf("malformed result=%+v err=%v", result, err)
	}
	valid := validFakeJointRow(compiled.From, []string{"CN", "4134"})
	runner, _ = NewJointRunner(fakeJointExecutor{blocks: [][]fakeJointRow{{valid}}, err: errors.New("connection lost")})
	if result, err := runner.Run(context.Background(), compiled); err == nil || len(result.Points) != 0 {
		t.Fatalf("partial result escaped=%+v err=%v", result, err)
	}
}

func compiledJointQuery(t *testing.T) CompiledJoint {
	t.Helper()
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	compiled, err := CompileJoint(Scope{}, validJointRequest(now), now)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func validFakeJointRow(bucket time.Time, dimensions []string) fakeJointRow {
	return fakeJointRow{
		bucket: bucket, observedAt: bucket.Add(time.Minute), dimensions: dimensions,
		dimensionSnapshotID: "snapshot-1", geoVersion: "geo-1", classificationVersion: 7,
		value: 1, received: 1,
	}
}

func appendFakeJointRow(results proto.Results, row fakeJointRow) {
	for _, result := range results {
		switch result.Name {
		case "bucket":
			result.Data.(*proto.ColDateTime).Append(row.bucket)
		case "dimension_values":
			result.Data.(*proto.ColArr[string]).Append(row.dimensions)
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
		case "observed_at":
			result.Data.(*proto.ColDateTime64).Append(row.observedAt)
		}
	}
}
