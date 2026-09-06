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
	"github.com/cloudcache/watchdog/internal/flowdimension"
)

type AddressSetRunner struct {
	executor Executor
}

type AddressSetPoint struct {
	Bucket                    time.Time `json:"bucket"`
	DimensionSnapshotID       string    `json:"dimension_snapshot_id"`
	GeoVersion                string    `json:"geo_version"`
	ClassificationVersion     uint32    `json:"classification_version"`
	Value                     float64   `json:"value"`
	ReceivedRecords           uint64    `json:"received_records"`
	UnknownSamplingRecords    uint64    `json:"unknown_sampling_records"`
	QualityRecords            uint64    `json:"quality_records"`
	SamplingCompleteness      float64   `json:"sampling_completeness,omitempty"`
	SamplingCompletenessKnown bool      `json:"sampling_completeness_known"`
	QualityRecordRatio        float64   `json:"quality_record_ratio,omitempty"`
	QualityRecordRatioKnown   bool      `json:"quality_record_ratio_known"`
}

type AddressSetResult struct {
	Points        []AddressSetPoint              `json:"points"`
	Metric        MetricDefinition               `json:"metric"`
	Endpoint      AddressSetEndpoint             `json:"endpoint"`
	Sets          flowdimension.AddressSetFilter `json:"address_set_filter"`
	MixedVersions bool                           `json:"mixed_versions"`
	VersionCount  uint64                         `json:"version_count"`
}

func NewAddressSetRunner(executor Executor) (*AddressSetRunner, error) {
	if executor == nil {
		return nil, errors.New("ClickHouse Flow address-set executor is required")
	}
	return &AddressSetRunner{executor: executor}, nil
}

func (r *AddressSetRunner) Run(ctx context.Context, compiled CompiledAddressSet) (AddressSetResult, error) {
	if r == nil || r.executor == nil {
		return AddressSetResult{}, errors.New("ClickHouse Flow address-set runner is not initialized")
	}
	if ctx == nil || compiled.Query.Body == "" || !compiled.To.After(compiled.From) || compiled.BucketDuration <= 0 ||
		!validAddressSetEndpoint(compiled.Endpoint) || compiled.MaxResultRows == 0 || compiled.Metric.Name == "" {
		return AddressSetResult{}, errors.New("compiled Flow address-set query is invalid")
	}
	columns := newAddressSetResultColumns()
	query := compiled.Query
	query.Result = columns.results()
	state := addressSetResultState{
		compiled: compiled, seen: make(map[addressSetPointKey]struct{}), versions: make(map[resultVersion]struct{}),
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
			return AddressSetResult{}, state.err
		}
		return AddressSetResult{}, classifyExecutionError(fmt.Errorf("execute ClickHouse Flow address-set query: %w", err))
	}
	if state.err != nil {
		return AddressSetResult{}, state.err
	}
	return AddressSetResult{
		Points: state.points, Metric: compiled.Metric, Endpoint: compiled.Endpoint, Sets: cloneAddressSetFilter(compiled.Sets),
		MixedVersions: len(state.versions) > 1, VersionCount: uint64(len(state.versions)),
	}, nil
}

type addressSetResultColumns struct {
	bucket                proto.ColDateTime
	dimensionSnapshotID   proto.ColStr
	geoVersion            proto.ColStr
	classificationVersion proto.ColUInt32
	value                 proto.ColFloat64
	receivedRecords       proto.ColUInt64
	unknownSampling       proto.ColUInt64
	qualityRecords        proto.ColUInt64
}

func newAddressSetResultColumns() *addressSetResultColumns {
	return &addressSetResultColumns{bucket: proto.ColDateTime{Location: time.UTC}}
}

func (c *addressSetResultColumns) results() proto.Results {
	return proto.Results{
		{Name: "bucket", Data: &c.bucket},
		{Name: "dimension_snapshot_id", Data: &c.dimensionSnapshotID},
		{Name: "geo_version", Data: &c.geoVersion},
		{Name: "classification_version", Data: &c.classificationVersion},
		{Name: "value", Data: &c.value},
		{Name: "received_records", Data: &c.receivedRecords},
		{Name: "unknown_sampling_records", Data: &c.unknownSampling},
		{Name: "quality_records", Data: &c.qualityRecords},
	}
}

func (c *addressSetResultColumns) rowCount() (int, error) {
	if c == nil {
		return 0, errors.New("address-set result columns are not initialized")
	}
	want := c.bucket.Rows()
	for name, rows := range map[string]int{
		"dimension_snapshot_id": c.dimensionSnapshotID.Rows(), "geo_version": c.geoVersion.Rows(),
		"classification_version": c.classificationVersion.Rows(), "value": c.value.Rows(),
		"received_records": c.receivedRecords.Rows(), "unknown_sampling_records": c.unknownSampling.Rows(),
		"quality_records": c.qualityRecords.Rows(),
	} {
		if rows != want {
			return 0, fmt.Errorf("invalid ClickHouse Flow address-set result: column %s has %d rows, want %d", name, rows, want)
		}
	}
	return want, nil
}

type addressSetPointKey struct {
	bucket  int64
	version resultVersion
}

type addressSetResultState struct {
	compiled CompiledAddressSet
	points   []AddressSetPoint
	seen     map[addressSetPointKey]struct{}
	versions map[resultVersion]struct{}
	previous *addressSetPointKey
	err      error
}

func (s *addressSetResultState) consume(columns *addressSetResultColumns) error {
	count, err := columns.rowCount()
	if err != nil {
		return err
	}
	if len(s.points)+count > int(s.compiled.MaxResultRows) {
		return fmt.Errorf("invalid ClickHouse Flow address-set result: rows exceed hard limit %d", s.compiled.MaxResultRows)
	}
	for index := 0; index < count; index++ {
		point, key, version, err := s.point(columns, index)
		if err != nil {
			return err
		}
		if _, exists := s.seen[key]; exists {
			return errors.New("invalid ClickHouse Flow address-set result: duplicate point key")
		}
		if s.previous != nil && !addressSetKeyAfter(key, *s.previous) {
			return errors.New("invalid ClickHouse Flow address-set result: rows are not strictly ordered")
		}
		s.seen[key] = struct{}{}
		s.versions[version] = struct{}{}
		current := key
		s.previous = &current
		s.points = append(s.points, point)
	}
	return nil
}

func (s *addressSetResultState) point(columns *addressSetResultColumns, index int) (AddressSetPoint, addressSetPointKey, resultVersion, error) {
	bucket := columns.bucket.Row(index).UTC()
	version := resultVersion{
		dimensionSnapshotID: columns.dimensionSnapshotID.Row(index), geoVersion: columns.geoVersion.Row(index),
		classificationVersion: columns.classificationVersion[index],
	}
	value := columns.value[index]
	received := columns.receivedRecords[index]
	unknownSampling := columns.unknownSampling[index]
	quality := columns.qualityRecords[index]
	if bucket.Before(s.compiled.From) || !bucket.Before(s.compiled.To) || bucket.Truncate(s.compiled.BucketDuration) != bucket {
		return AddressSetPoint{}, addressSetPointKey{}, resultVersion{}, errors.New("invalid ClickHouse Flow address-set result: bucket is outside or unaligned")
	}
	if version.dimensionSnapshotID == "" || version.geoVersion == "" || version.classificationVersion == 0 {
		return AddressSetPoint{}, addressSetPointKey{}, resultVersion{}, errors.New("invalid ClickHouse Flow address-set result: version identity is incomplete")
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || unknownSampling > received || quality > received {
		return AddressSetPoint{}, addressSetPointKey{}, resultVersion{}, errors.New("invalid ClickHouse Flow address-set result: value or quality counters are inconsistent")
	}
	point := AddressSetPoint{
		Bucket: bucket, DimensionSnapshotID: version.dimensionSnapshotID, GeoVersion: version.geoVersion,
		ClassificationVersion: version.classificationVersion, Value: value, ReceivedRecords: received,
		UnknownSamplingRecords: unknownSampling, QualityRecords: quality,
	}
	if received > 0 {
		point.SamplingCompletenessKnown = true
		point.SamplingCompleteness = float64(received-unknownSampling) / float64(received)
		point.QualityRecordRatioKnown = true
		point.QualityRecordRatio = float64(quality) / float64(received)
	}
	return point, addressSetPointKey{bucket: bucket.Unix(), version: version}, version, nil
}

func addressSetKeyAfter(left, right addressSetPointKey) bool {
	if left.bucket != right.bucket {
		return left.bucket > right.bucket
	}
	if left.version.dimensionSnapshotID != right.version.dimensionSnapshotID {
		return left.version.dimensionSnapshotID > right.version.dimensionSnapshotID
	}
	if left.version.geoVersion != right.version.geoVersion {
		return left.version.geoVersion > right.version.geoVersion
	}
	return left.version.classificationVersion > right.version.classificationVersion
}

func validAddressSetEndpoint(value AddressSetEndpoint) bool {
	return value == AddressSetEndpointLocal || value == AddressSetEndpointRemote || value == AddressSetEndpointEither
}

func cloneAddressSetFilter(value flowdimension.AddressSetFilter) flowdimension.AddressSetFilter {
	return flowdimension.AddressSetFilter{
		IncludeAny: append([]string(nil), value.IncludeAny...), IncludeAll: append([]string(nil), value.IncludeAll...),
		ExcludeAny: append([]string(nil), value.ExcludeAny...),
	}
}
