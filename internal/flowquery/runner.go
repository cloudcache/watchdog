// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type Executor interface {
	Do(context.Context, ch.Query) error
}

type Runner struct {
	executor Executor
}

type Point struct {
	Bucket                    time.Time
	DimensionValue            string
	Other                     bool
	DimensionSnapshotID       string
	GeoVersion                string
	ClassificationVersion     uint32
	Value                     float64
	ReceivedRecords           uint64
	UnknownSamplingRecords    uint64
	QualityRecords            uint64
	GeneratedAt               time.Time
	SamplingCompleteness      float64
	SamplingCompletenessKnown bool
	QualityRecordRatio        float64
	QualityRecordRatioKnown   bool
}

type RollupCompleteness struct {
	ExpectedBuckets uint64
	CoveredBuckets  uint64
	Ratio           float64
	Complete        bool
}

type Result struct {
	Points             []Point
	Metric             MetricDefinition
	Dimension          DimensionDefinition
	RollupCompleteness RollupCompleteness
	MixedVersions      bool
	VersionCount       uint64
}

func NewRunner(executor Executor) (*Runner, error) {
	if executor == nil {
		return nil, errors.New("ClickHouse Flow query executor is required")
	}
	return &Runner{executor: executor}, nil
}

func (r *Runner) Run(ctx context.Context, compiled Compiled) (Result, error) {
	if r == nil || r.executor == nil {
		return Result{}, errors.New("ClickHouse Flow query runner is not initialized")
	}
	if ctx == nil || compiled.Query.Body == "" || compiled.BucketDuration <= 0 || !compiled.To.After(compiled.From) || compiled.MaxResultRows == 0 {
		return Result{}, errors.New("compiled Flow query is invalid")
	}
	columns := newResultColumns()
	query := compiled.Query
	query.Result = columns.results()
	state := resultState{
		maxRows: compiled.MaxResultRows,
		from:    compiled.From, to: compiled.To, bucketDuration: compiled.BucketDuration,
		seen: make(map[resultPointKey]struct{}), versions: make(map[resultVersion]struct{}),
	}
	query.OnResult = func(_ context.Context, _ proto.Block) error {
		if state.err != nil {
			return state.err
		}
		state.err = state.consume(columns)
		return state.err
	}
	if err := r.executor.Do(ctx, query); err != nil {
		if state.err != nil {
			return Result{}, state.err
		}
		return Result{}, classifyExecutionError(fmt.Errorf("execute ClickHouse Flow query: %w", err))
	}
	if state.err != nil {
		return Result{}, state.err
	}
	if state.metadataRows != 1 {
		return Result{}, fmt.Errorf("invalid ClickHouse Flow result: metadata rows=%d, want 1", state.metadataRows)
	}
	expected := uint64(compiled.To.Sub(compiled.From) / compiled.BucketDuration)
	if state.coveredBuckets > expected {
		return Result{}, fmt.Errorf("invalid ClickHouse Flow result: covered buckets=%d exceed expected=%d", state.coveredBuckets, expected)
	}
	completeness := RollupCompleteness{ExpectedBuckets: expected, CoveredBuckets: state.coveredBuckets}
	if expected > 0 {
		completeness.Ratio = float64(state.coveredBuckets) / float64(expected)
	}
	completeness.Complete = expected > 0 && state.coveredBuckets == expected
	return Result{
		Points: state.points, Metric: compiled.Metric, Dimension: compiled.Dimension,
		RollupCompleteness: completeness, MixedVersions: len(state.versions) > 1,
		VersionCount: uint64(len(state.versions)),
	}, nil
}

type resultColumns struct {
	bucket                proto.ColDateTime
	dimensionValue        proto.ColStr
	isOther               proto.ColUInt8
	dimensionSnapshotID   proto.ColStr
	geoVersion            proto.ColStr
	classificationVersion proto.ColUInt32
	value                 proto.ColFloat64
	receivedRecords       proto.ColUInt64
	unknownSampling       proto.ColUInt64
	qualityRecords        proto.ColUInt64
	generatedAt           *proto.ColDateTime64
	isMetadata            proto.ColUInt8
	coveredBuckets        proto.ColUInt64
}

func newResultColumns() *resultColumns {
	return &resultColumns{
		bucket:      proto.ColDateTime{Location: time.UTC},
		generatedAt: new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC),
	}
}

func (c *resultColumns) results() proto.Results {
	return proto.Results{
		{Name: "bucket", Data: &c.bucket},
		{Name: "dimension_value", Data: &c.dimensionValue},
		{Name: "is_other", Data: &c.isOther},
		{Name: "dimension_snapshot_id", Data: &c.dimensionSnapshotID},
		{Name: "geo_version", Data: &c.geoVersion},
		{Name: "classification_version", Data: &c.classificationVersion},
		{Name: "value", Data: &c.value},
		{Name: "received_records", Data: &c.receivedRecords},
		{Name: "unknown_sampling_records", Data: &c.unknownSampling},
		{Name: "quality_records", Data: &c.qualityRecords},
		{Name: "generated_at", Data: c.generatedAt},
		{Name: "is_metadata", Data: &c.isMetadata},
		{Name: "covered_buckets", Data: &c.coveredBuckets},
	}
}

func (c *resultColumns) rows() (int, error) {
	if c == nil {
		return 0, errors.New("result columns are not initialized")
	}
	want := c.bucket.Rows()
	for name, rows := range map[string]int{
		"dimension_value": c.dimensionValue.Rows(), "is_other": c.isOther.Rows(),
		"dimension_snapshot_id": c.dimensionSnapshotID.Rows(), "geo_version": c.geoVersion.Rows(),
		"classification_version": c.classificationVersion.Rows(), "value": c.value.Rows(),
		"received_records": c.receivedRecords.Rows(), "unknown_sampling_records": c.unknownSampling.Rows(),
		"quality_records": c.qualityRecords.Rows(), "generated_at": c.generatedAt.Rows(),
		"is_metadata": c.isMetadata.Rows(), "covered_buckets": c.coveredBuckets.Rows(),
	} {
		if rows != want {
			return 0, fmt.Errorf("invalid ClickHouse Flow result: column %s has %d rows, want %d", name, rows, want)
		}
	}
	return want, nil
}

type resultVersion struct {
	dimensionSnapshotID   string
	geoVersion            string
	classificationVersion uint32
}

type resultPointKey struct {
	bucket         int64
	dimensionValue string
	other          bool
	version        resultVersion
}

type resultState struct {
	points         []Point
	metadataRows   uint64
	coveredBuckets uint64
	rows           uint64
	maxRows        uint64
	from, to       time.Time
	bucketDuration time.Duration
	seen           map[resultPointKey]struct{}
	versions       map[resultVersion]struct{}
	err            error
}

func (s *resultState) consume(columns *resultColumns) error {
	rows, err := columns.rows()
	if err != nil {
		return err
	}
	if s.rows+uint64(rows) > s.maxRows {
		return fmt.Errorf("invalid ClickHouse Flow result: rows exceed hard limit %d", s.maxRows)
	}
	s.rows += uint64(rows)
	for index := 0; index < rows; index++ {
		metadata := columns.isMetadata[index]
		if metadata > 1 {
			return fmt.Errorf("invalid ClickHouse Flow result: is_metadata=%d", metadata)
		}
		if metadata == 1 {
			if err := s.consumeMetadata(columns, index); err != nil {
				return err
			}
			continue
		}
		point, version, key, err := s.point(columns, index)
		if err != nil {
			return err
		}
		if _, exists := s.seen[key]; exists {
			return errors.New("invalid ClickHouse Flow result: duplicate point key")
		}
		s.seen[key] = struct{}{}
		s.versions[version] = struct{}{}
		s.points = append(s.points, point)
	}
	return nil
}

func (s *resultState) consumeMetadata(columns *resultColumns, index int) error {
	s.metadataRows++
	if s.metadataRows > 1 {
		return errors.New("invalid ClickHouse Flow result: duplicate metadata sentinel")
	}
	if columns.dimensionValue.Row(index) != "" || columns.isOther[index] != 0 || columns.dimensionSnapshotID.Row(index) != "" ||
		columns.geoVersion.Row(index) != "" || columns.classificationVersion[index] != 0 || columns.value[index] != 0 ||
		columns.receivedRecords[index] != 0 || columns.unknownSampling[index] != 0 || columns.qualityRecords[index] != 0 ||
		columns.bucket.Row(index).Unix() != 0 || columns.generatedAt.Row(index).UnixMilli() != 0 {
		return errors.New("invalid ClickHouse Flow result: malformed metadata sentinel")
	}
	s.coveredBuckets = columns.coveredBuckets[index]
	return nil
}

func (s *resultState) point(columns *resultColumns, index int) (Point, resultVersion, resultPointKey, error) {
	bucket := columns.bucket.Row(index).UTC()
	dimensionValue := columns.dimensionValue.Row(index)
	isOther := columns.isOther[index]
	dimensionSnapshotID := columns.dimensionSnapshotID.Row(index)
	geoVersion := columns.geoVersion.Row(index)
	classificationVersion := columns.classificationVersion[index]
	value := columns.value[index]
	received := columns.receivedRecords[index]
	unknownSampling := columns.unknownSampling[index]
	qualityRecords := columns.qualityRecords[index]
	generatedAt := columns.generatedAt.Row(index).UTC()
	if bucket.Before(s.from) || !bucket.Before(s.to) || bucket.Truncate(s.bucketDuration) != bucket {
		return Point{}, resultVersion{}, resultPointKey{}, errors.New("invalid ClickHouse Flow result: point bucket is outside or unaligned")
	}
	if isOther > 1 || (isOther == 1 && dimensionValue != "_other") || dimensionValue == "" || dimensionSnapshotID == "" || geoVersion == "" || classificationVersion == 0 || columns.coveredBuckets[index] != 0 {
		return Point{}, resultVersion{}, resultPointKey{}, errors.New("invalid ClickHouse Flow result: point identity is incomplete")
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return Point{}, resultVersion{}, resultPointKey{}, errors.New("invalid ClickHouse Flow result: point value is not finite and non-negative")
	}
	if unknownSampling > received || qualityRecords > received || generatedAt.IsZero() {
		return Point{}, resultVersion{}, resultPointKey{}, errors.New("invalid ClickHouse Flow result: point quality counters are inconsistent")
	}
	point := Point{
		Bucket: bucket, DimensionValue: dimensionValue, Other: isOther == 1,
		DimensionSnapshotID: dimensionSnapshotID, GeoVersion: geoVersion,
		ClassificationVersion: classificationVersion, Value: value,
		ReceivedRecords: received, UnknownSamplingRecords: unknownSampling,
		QualityRecords: qualityRecords, GeneratedAt: generatedAt,
	}
	if received > 0 {
		point.SamplingCompletenessKnown = true
		point.SamplingCompleteness = float64(received-unknownSampling) / float64(received)
		point.QualityRecordRatioKnown = true
		point.QualityRecordRatio = float64(qualityRecords) / float64(received)
	}
	version := resultVersion{
		dimensionSnapshotID: dimensionSnapshotID, geoVersion: geoVersion,
		classificationVersion: classificationVersion,
	}
	key := resultPointKey{bucket: bucket.Unix(), dimensionValue: dimensionValue, other: isOther == 1, version: version}
	return point, version, key, nil
}
