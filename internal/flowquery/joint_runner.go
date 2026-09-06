// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

type JointPoint struct {
	Bucket                    time.Time `json:"bucket"`
	DimensionValues           []string  `json:"dimension_values"`
	Other                     bool      `json:"other"`
	DimensionSnapshotID       string    `json:"dimension_snapshot_id"`
	GeoVersion                string    `json:"geo_version"`
	ClassificationVersion     uint32    `json:"classification_version"`
	Value                     float64   `json:"value"`
	ReceivedRecords           uint64    `json:"received_records"`
	UnknownSamplingRecords    uint64    `json:"unknown_sampling_records"`
	QualityRecords            uint64    `json:"quality_records"`
	ObservedAt                time.Time `json:"observed_at"`
	SamplingCompleteness      float64   `json:"sampling_completeness,omitempty"`
	SamplingCompletenessKnown bool      `json:"sampling_completeness_known"`
	QualityRecordRatio        float64   `json:"quality_record_ratio,omitempty"`
	QualityRecordRatioKnown   bool      `json:"quality_record_ratio_known"`
}

type JointResult struct {
	Points        []JointPoint          `json:"points"`
	Metric        MetricDefinition      `json:"metric"`
	Dimensions    []DimensionDefinition `json:"dimensions"`
	Plan          JointPlan             `json:"plan"`
	MixedVersions bool                  `json:"mixed_versions"`
	VersionCount  uint64                `json:"version_count"`
}

type JointRunner struct{ executor Executor }

func NewJointRunner(executor Executor) (*JointRunner, error) {
	if executor == nil {
		return nil, errors.New("ClickHouse Flow joint-query executor is required")
	}
	return &JointRunner{executor: executor}, nil
}

func (r *JointRunner) Run(ctx context.Context, compiled CompiledJoint) (JointResult, error) {
	if r == nil || r.executor == nil {
		return JointResult{}, errors.New("ClickHouse Flow joint-query runner is not initialized")
	}
	if ctx == nil || compiled.Query.Body == "" || compiled.BucketDuration <= 0 || !compiled.To.After(compiled.From) ||
		len(compiled.Dimensions) < MinJointDimensions || len(compiled.Dimensions) > MaxJointDimensions || compiled.MaxResultRows == 0 {
		return JointResult{}, errors.New("compiled Flow joint query is invalid")
	}
	columns := newJointResultColumns()
	query := compiled.Query
	query.Result = columns.results()
	state := jointResultState{
		compiled: compiled, seen: make(map[jointPointKey]struct{}), versions: make(map[resultVersion]struct{}),
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
			return JointResult{}, state.err
		}
		return JointResult{}, classifyExecutionError(fmt.Errorf("execute ClickHouse Flow joint query: %w", err))
	}
	if state.err != nil {
		return JointResult{}, state.err
	}
	return JointResult{
		Points: state.points, Metric: compiled.Metric, Dimensions: append([]DimensionDefinition(nil), compiled.Dimensions...),
		Plan: compiled.Plan, MixedVersions: len(state.versions) > 1, VersionCount: uint64(len(state.versions)),
	}, nil
}

type jointResultColumns struct {
	bucket                proto.ColDateTime
	dimensionValues       *proto.ColArr[string]
	isOther               proto.ColUInt8
	dimensionSnapshotID   proto.ColStr
	geoVersion            proto.ColStr
	classificationVersion proto.ColUInt32
	value                 proto.ColFloat64
	receivedRecords       proto.ColUInt64
	unknownSampling       proto.ColUInt64
	qualityRecords        proto.ColUInt64
	observedAt            *proto.ColDateTime64
}

func newJointResultColumns() *jointResultColumns {
	return &jointResultColumns{
		bucket:          proto.ColDateTime{Location: time.UTC},
		dimensionValues: new(proto.ColStr).Array(),
		observedAt:      new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC),
	}
}

func (c *jointResultColumns) results() proto.Results {
	return proto.Results{
		{Name: "bucket", Data: &c.bucket},
		{Name: "dimension_values", Data: c.dimensionValues},
		{Name: "is_other", Data: &c.isOther},
		{Name: "dimension_snapshot_id", Data: &c.dimensionSnapshotID},
		{Name: "geo_version", Data: &c.geoVersion},
		{Name: "classification_version", Data: &c.classificationVersion},
		{Name: "value", Data: &c.value},
		{Name: "received_records", Data: &c.receivedRecords},
		{Name: "unknown_sampling_records", Data: &c.unknownSampling},
		{Name: "quality_records", Data: &c.qualityRecords},
		{Name: "observed_at", Data: c.observedAt},
	}
}

func (c *jointResultColumns) rows() (int, error) {
	if c == nil {
		return 0, errors.New("joint result columns are not initialized")
	}
	want := c.bucket.Rows()
	for name, rows := range map[string]int{
		"dimension_values": c.dimensionValues.Rows(), "is_other": c.isOther.Rows(),
		"dimension_snapshot_id": c.dimensionSnapshotID.Rows(), "geo_version": c.geoVersion.Rows(),
		"classification_version": c.classificationVersion.Rows(), "value": c.value.Rows(),
		"received_records": c.receivedRecords.Rows(), "unknown_sampling_records": c.unknownSampling.Rows(),
		"quality_records": c.qualityRecords.Rows(), "observed_at": c.observedAt.Rows(),
	} {
		if rows != want {
			return 0, fmt.Errorf("invalid ClickHouse Flow joint result: column %s has %d rows, want %d", name, rows, want)
		}
	}
	return want, nil
}

type jointPointKey struct {
	bucket     int64
	dimensions string
	other      bool
	version    resultVersion
}

type jointResultState struct {
	compiled CompiledJoint
	points   []JointPoint
	rows     uint64
	seen     map[jointPointKey]struct{}
	versions map[resultVersion]struct{}
	err      error
}

func (s *jointResultState) consume(columns *jointResultColumns) error {
	rows, err := columns.rows()
	if err != nil {
		return err
	}
	if s.rows+uint64(rows) > s.compiled.MaxResultRows {
		return fmt.Errorf("invalid ClickHouse Flow joint result: rows exceed hard limit %d", s.compiled.MaxResultRows)
	}
	s.rows += uint64(rows)
	for index := 0; index < rows; index++ {
		point, version, key, err := s.point(columns, index)
		if err != nil {
			return err
		}
		if _, exists := s.seen[key]; exists {
			return errors.New("invalid ClickHouse Flow joint result: duplicate point key")
		}
		s.seen[key] = struct{}{}
		s.versions[version] = struct{}{}
		s.points = append(s.points, point)
	}
	return nil
}

func (s *jointResultState) point(columns *jointResultColumns, index int) (JointPoint, resultVersion, jointPointKey, error) {
	bucket := columns.bucket.Row(index).UTC()
	dimensions := append([]string(nil), columns.dimensionValues.Row(index)...)
	other := columns.isOther[index]
	version := resultVersion{
		dimensionSnapshotID: columns.dimensionSnapshotID.Row(index), geoVersion: columns.geoVersion.Row(index),
		classificationVersion: columns.classificationVersion[index],
	}
	value := columns.value[index]
	received := columns.receivedRecords[index]
	unknown := columns.unknownSampling[index]
	quality := columns.qualityRecords[index]
	observedAt := columns.observedAt.Row(index).UTC()
	if bucket.Before(s.compiled.From) || !bucket.Before(s.compiled.To) || bucket.Sub(s.compiled.From)%s.compiled.BucketDuration != 0 {
		return JointPoint{}, resultVersion{}, jointPointKey{}, errors.New("invalid ClickHouse Flow joint result: point bucket is outside or unaligned")
	}
	if len(dimensions) != len(s.compiled.Dimensions) || other > 1 || version.dimensionSnapshotID == "" || version.geoVersion == "" || version.classificationVersion == 0 {
		return JointPoint{}, resultVersion{}, jointPointKey{}, errors.New("invalid ClickHouse Flow joint result: point identity is incomplete")
	}
	for _, dimension := range dimensions {
		if dimension == "" || (other == 1 && dimension != "_other") {
			return JointPoint{}, resultVersion{}, jointPointKey{}, errors.New("invalid ClickHouse Flow joint result: dimension tuple is malformed")
		}
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || unknown > received || quality > received || observedAt.IsZero() {
		return JointPoint{}, resultVersion{}, jointPointKey{}, errors.New("invalid ClickHouse Flow joint result: point counters are inconsistent")
	}
	point := JointPoint{
		Bucket: bucket, DimensionValues: dimensions, Other: other == 1,
		DimensionSnapshotID: version.dimensionSnapshotID, GeoVersion: version.geoVersion,
		ClassificationVersion: version.classificationVersion, Value: value,
		ReceivedRecords: received, UnknownSamplingRecords: unknown, QualityRecords: quality, ObservedAt: observedAt,
	}
	if received > 0 {
		point.SamplingCompletenessKnown = true
		point.SamplingCompleteness = float64(received-unknown) / float64(received)
		point.QualityRecordRatioKnown = true
		point.QualityRecordRatio = float64(quality) / float64(received)
	}
	key := jointPointKey{
		bucket: bucket.Unix(), dimensions: jointDimensionsKey(dimensions), other: other == 1, version: version,
	}
	return point, version, key, nil
}

func jointDimensionsKey(values []string) string {
	key := ""
	for _, value := range values {
		key += fmt.Sprintf("%d:%s", len(value), value)
	}
	return key
}
