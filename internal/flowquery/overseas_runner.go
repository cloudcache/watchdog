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

type OverseasRowKind string

const (
	OverseasRowKPI OverseasRowKind = "kpi"
	OverseasRowGeo OverseasRowKind = "geo"
)

type OverseasGeoScope string

const (
	OverseasScopeOverseas   OverseasGeoScope = "overseas"
	OverseasScopeUnknownGeo OverseasGeoScope = "unknown_geo"
)

type OverseasDirection string

const (
	OverseasDirectionIn       OverseasDirection = "in"
	OverseasDirectionOut      OverseasDirection = "out"
	OverseasDirectionCombined OverseasDirection = "combined"
)

type OverseasIPFamily string

const (
	OverseasIPFamilyIPv4    OverseasIPFamily = "ipv4"
	OverseasIPFamilyIPv6    OverseasIPFamily = "ipv6"
	OverseasIPFamilyUnknown OverseasIPFamily = "unknown"
	OverseasIPFamilyAll     OverseasIPFamily = "all"
)

type OverseasPoint struct {
	Bucket                    time.Time         `json:"bucket"`
	Kind                      OverseasRowKind   `json:"kind"`
	GeoScope                  OverseasGeoScope  `json:"geo_scope"`
	Direction                 OverseasDirection `json:"direction"`
	IPFamily                  OverseasIPFamily  `json:"ip_family"`
	GeoValue                  string            `json:"geo_value,omitempty"`
	Other                     bool              `json:"other"`
	DimensionSnapshotID       string            `json:"dimension_snapshot_id"`
	GeoVersion                string            `json:"geo_version"`
	ClassificationVersion     uint32            `json:"classification_version"`
	Value                     float64           `json:"value"`
	ObservedRemoteIPs         uint64            `json:"observed_remote_ips"`
	ObservedLocalHosts        uint64            `json:"observed_local_hosts"`
	ReceivedRecords           uint64            `json:"received_records"`
	UnknownSamplingRecords    uint64            `json:"unknown_sampling_records"`
	QualityRecords            uint64            `json:"quality_records"`
	GeneratedAt               time.Time         `json:"generated_at"`
	SamplingCompleteness      float64           `json:"sampling_completeness,omitempty"`
	SamplingCompletenessKnown bool              `json:"sampling_completeness_known"`
	QualityRecordRatio        float64           `json:"quality_record_ratio,omitempty"`
	QualityRecordRatioKnown   bool              `json:"quality_record_ratio_known"`
}

type OverseasResult struct {
	Points             []OverseasPoint    `json:"points"`
	Metric             MetricDefinition   `json:"metric"`
	GeoLevel           OverseasGeoLevel   `json:"geo_level"`
	TopN               uint16             `json:"top_n"`
	IncludeOther       bool               `json:"include_other"`
	RollupCompleteness RollupCompleteness `json:"rollup_completeness"`
	MixedVersions      bool               `json:"mixed_versions"`
	VersionCount       uint64             `json:"version_count"`
}

type OverseasRunner struct {
	executor Executor
}

func NewOverseasRunner(executor Executor) (*OverseasRunner, error) {
	if executor == nil {
		return nil, errors.New("ClickHouse Flow overseas executor is required")
	}
	return &OverseasRunner{executor: executor}, nil
}

func (r *OverseasRunner) Run(ctx context.Context, compiled CompiledOverseas) (OverseasResult, error) {
	if r == nil || r.executor == nil {
		return OverseasResult{}, errors.New("ClickHouse Flow overseas runner is not initialized")
	}
	if ctx == nil || compiled.Query.Body == "" || compiled.BucketDuration <= 0 || !compiled.To.After(compiled.From) ||
		compiled.MaxResultRows == 0 || compiled.Metric.Name == "" || compiled.TopN == 0 {
		return OverseasResult{}, errors.New("compiled Flow overseas query is invalid")
	}
	if _, err := overseasGeoDimension(compiled.GeoLevel); err != nil {
		return OverseasResult{}, errors.New("compiled Flow overseas query is invalid")
	}

	columns := newOverseasResultColumns()
	query := compiled.Query
	query.Result = columns.results()
	state := overseasResultState{
		compiled: compiled, seen: make(map[overseasPointKey]struct{}), versions: make(map[resultVersion]struct{}),
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
			return OverseasResult{}, state.err
		}
		return OverseasResult{}, classifyExecutionError(fmt.Errorf("execute ClickHouse Flow overseas query: %w", err))
	}
	if state.err != nil {
		return OverseasResult{}, state.err
	}
	if state.metadataRows != 1 {
		return OverseasResult{}, fmt.Errorf("invalid ClickHouse Flow overseas result: metadata rows=%d, want 1", state.metadataRows)
	}
	expected := uint64(compiled.To.Sub(compiled.From) / compiled.BucketDuration)
	if state.coveredBuckets > expected {
		return OverseasResult{}, fmt.Errorf("invalid ClickHouse Flow overseas result: covered buckets=%d exceed expected=%d", state.coveredBuckets, expected)
	}
	completeness := RollupCompleteness{ExpectedBuckets: expected, CoveredBuckets: state.coveredBuckets}
	if expected > 0 {
		completeness.Ratio = float64(state.coveredBuckets) / float64(expected)
	}
	completeness.Complete = expected > 0 && state.coveredBuckets == expected
	return OverseasResult{
		Points: state.points, Metric: compiled.Metric, GeoLevel: compiled.GeoLevel,
		TopN: compiled.TopN, IncludeOther: compiled.IncludeOther, RollupCompleteness: completeness,
		MixedVersions: len(state.versions) > 1, VersionCount: uint64(len(state.versions)),
	}, nil
}

type overseasResultColumns struct {
	bucket                proto.ColDateTime
	rowKind               proto.ColStr
	geoScope              proto.ColStr
	direction             proto.ColStr
	ipFamily              proto.ColStr
	geoValue              proto.ColStr
	isOther               proto.ColUInt8
	dimensionSnapshotID   proto.ColStr
	geoVersion            proto.ColStr
	classificationVersion proto.ColUInt32
	value                 proto.ColFloat64
	observedRemoteIPs     proto.ColUInt64
	observedLocalHosts    proto.ColUInt64
	receivedRecords       proto.ColUInt64
	unknownSampling       proto.ColUInt64
	qualityRecords        proto.ColUInt64
	generatedAt           *proto.ColDateTime64
	endpointConsistent    proto.ColUInt8
	isMetadata            proto.ColUInt8
	coveredBuckets        proto.ColUInt64
}

func newOverseasResultColumns() *overseasResultColumns {
	return &overseasResultColumns{
		bucket:      proto.ColDateTime{Location: time.UTC},
		generatedAt: new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC),
	}
}

func (c *overseasResultColumns) results() proto.Results {
	return proto.Results{
		{Name: "bucket", Data: &c.bucket}, {Name: "row_kind", Data: &c.rowKind},
		{Name: "geo_scope", Data: &c.geoScope}, {Name: "direction", Data: &c.direction},
		{Name: "ip_family", Data: &c.ipFamily}, {Name: "geo_value", Data: &c.geoValue},
		{Name: "is_other", Data: &c.isOther}, {Name: "dimension_snapshot_id", Data: &c.dimensionSnapshotID},
		{Name: "geo_version", Data: &c.geoVersion}, {Name: "classification_version", Data: &c.classificationVersion},
		{Name: "value", Data: &c.value}, {Name: "observed_remote_ips", Data: &c.observedRemoteIPs},
		{Name: "observed_local_hosts", Data: &c.observedLocalHosts}, {Name: "received_records", Data: &c.receivedRecords},
		{Name: "unknown_sampling_records", Data: &c.unknownSampling}, {Name: "quality_records", Data: &c.qualityRecords},
		{Name: "generated_at", Data: c.generatedAt}, {Name: "endpoint_consistent", Data: &c.endpointConsistent},
		{Name: "is_metadata", Data: &c.isMetadata}, {Name: "covered_buckets", Data: &c.coveredBuckets},
	}
}

func (c *overseasResultColumns) rowCount() (int, error) {
	if c == nil {
		return 0, errors.New("overseas result columns are not initialized")
	}
	want := c.bucket.Rows()
	for name, rows := range map[string]int{
		"row_kind": c.rowKind.Rows(), "geo_scope": c.geoScope.Rows(), "direction": c.direction.Rows(),
		"ip_family": c.ipFamily.Rows(), "geo_value": c.geoValue.Rows(), "is_other": c.isOther.Rows(),
		"dimension_snapshot_id": c.dimensionSnapshotID.Rows(), "geo_version": c.geoVersion.Rows(),
		"classification_version": c.classificationVersion.Rows(), "value": c.value.Rows(),
		"observed_remote_ips": c.observedRemoteIPs.Rows(), "observed_local_hosts": c.observedLocalHosts.Rows(),
		"received_records": c.receivedRecords.Rows(), "unknown_sampling_records": c.unknownSampling.Rows(),
		"quality_records": c.qualityRecords.Rows(), "generated_at": c.generatedAt.Rows(),
		"endpoint_consistent": c.endpointConsistent.Rows(), "is_metadata": c.isMetadata.Rows(),
		"covered_buckets": c.coveredBuckets.Rows(),
	} {
		if rows != want {
			return 0, fmt.Errorf("invalid ClickHouse Flow overseas result: column %s has %d rows, want %d", name, rows, want)
		}
	}
	return want, nil
}

type overseasPointKey struct {
	bucket                int64
	kind                  OverseasRowKind
	scope                 OverseasGeoScope
	direction             OverseasDirection
	family                OverseasIPFamily
	geoValue              string
	other                 bool
	dimensionSnapshotID   string
	geoVersion            string
	classificationVersion uint32
}

type overseasResultState struct {
	compiled       CompiledOverseas
	points         []OverseasPoint
	seen           map[overseasPointKey]struct{}
	versions       map[resultVersion]struct{}
	metadataRows   uint64
	coveredBuckets uint64
	rows           uint64
	sawMetadata    bool
	err            error
}

func (s *overseasResultState) consume(columns *overseasResultColumns) error {
	rows, err := columns.rowCount()
	if err != nil {
		return err
	}
	if s.rows+uint64(rows) > s.compiled.MaxResultRows {
		return fmt.Errorf("invalid ClickHouse Flow overseas result: rows exceed hard limit %d", s.compiled.MaxResultRows)
	}
	s.rows += uint64(rows)
	for index := 0; index < rows; index++ {
		if columns.isMetadata[index] > 1 {
			return errors.New("invalid ClickHouse Flow overseas result: invalid metadata flag")
		}
		if columns.isMetadata[index] == 1 {
			if err := s.consumeMetadata(columns, index); err != nil {
				return err
			}
			continue
		}
		if s.sawMetadata {
			return errors.New("invalid ClickHouse Flow overseas result: data follows metadata sentinel")
		}
		point, key, version, err := s.point(columns, index)
		if err != nil {
			return err
		}
		if _, exists := s.seen[key]; exists {
			return errors.New("invalid ClickHouse Flow overseas result: duplicate point key")
		}
		s.seen[key] = struct{}{}
		s.versions[version] = struct{}{}
		s.points = append(s.points, point)
	}
	return nil
}

func (s *overseasResultState) consumeMetadata(columns *overseasResultColumns, index int) error {
	s.metadataRows++
	s.sawMetadata = true
	if s.metadataRows > 1 || columns.bucket.Row(index).Unix() != 0 || columns.rowKind.Row(index) != "" ||
		columns.geoScope.Row(index) != "" || columns.direction.Row(index) != "" || columns.ipFamily.Row(index) != "" ||
		columns.geoValue.Row(index) != "" || columns.isOther[index] != 0 || columns.dimensionSnapshotID.Row(index) != "" ||
		columns.geoVersion.Row(index) != "" || columns.classificationVersion[index] != 0 || columns.value[index] != 0 ||
		columns.observedRemoteIPs[index] != 0 || columns.observedLocalHosts[index] != 0 || columns.receivedRecords[index] != 0 ||
		columns.unknownSampling[index] != 0 || columns.qualityRecords[index] != 0 ||
		columns.generatedAt.Row(index).UnixMilli() != 0 || columns.endpointConsistent[index] != 0 {
		return errors.New("invalid ClickHouse Flow overseas result: malformed metadata sentinel")
	}
	s.coveredBuckets = columns.coveredBuckets[index]
	return nil
}

func (s *overseasResultState) point(columns *overseasResultColumns, index int) (OverseasPoint, overseasPointKey, resultVersion, error) {
	bucket := columns.bucket.Row(index).UTC()
	kind := OverseasRowKind(columns.rowKind.Row(index))
	scope := OverseasGeoScope(columns.geoScope.Row(index))
	direction := OverseasDirection(columns.direction.Row(index))
	family := OverseasIPFamily(columns.ipFamily.Row(index))
	geoValue := columns.geoValue.Row(index)
	other := columns.isOther[index]
	version := resultVersion{
		dimensionSnapshotID: columns.dimensionSnapshotID.Row(index), geoVersion: columns.geoVersion.Row(index),
		classificationVersion: columns.classificationVersion[index],
	}
	value := columns.value[index]
	received := columns.receivedRecords[index]
	unknownSampling := columns.unknownSampling[index]
	quality := columns.qualityRecords[index]
	generatedAt := columns.generatedAt.Row(index).UTC()
	if bucket.Before(s.compiled.From) || !bucket.Before(s.compiled.To) || bucket.Truncate(s.compiled.BucketDuration) != bucket {
		return OverseasPoint{}, overseasPointKey{}, resultVersion{}, errors.New("invalid ClickHouse Flow overseas result: bucket is outside or unaligned")
	}
	if version.dimensionSnapshotID == "" || version.geoVersion == "" || version.classificationVersion == 0 {
		return OverseasPoint{}, overseasPointKey{}, resultVersion{}, errors.New("invalid ClickHouse Flow overseas result: version identity is incomplete")
	}
	if !validOverseasDirection(direction) || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 ||
		other > 1 || unknownSampling > received || quality > received || generatedAt.IsZero() ||
		columns.endpointConsistent[index] != 1 || columns.coveredBuckets[index] != 0 {
		return OverseasPoint{}, overseasPointKey{}, resultVersion{}, errors.New("invalid ClickHouse Flow overseas result: value, quality, or endpoint counters are inconsistent")
	}
	if err := validateOverseasIdentity(kind, scope, family, geoValue, other == 1, columns.observedRemoteIPs[index], columns.observedLocalHosts[index], received); err != nil {
		return OverseasPoint{}, overseasPointKey{}, resultVersion{}, err
	}
	point := OverseasPoint{
		Bucket: bucket, Kind: kind, GeoScope: scope, Direction: direction, IPFamily: family,
		GeoValue: geoValue, Other: other == 1, DimensionSnapshotID: version.dimensionSnapshotID,
		GeoVersion: version.geoVersion, ClassificationVersion: version.classificationVersion,
		Value: value, ObservedRemoteIPs: columns.observedRemoteIPs[index], ObservedLocalHosts: columns.observedLocalHosts[index],
		ReceivedRecords: received, UnknownSamplingRecords: unknownSampling, QualityRecords: quality, GeneratedAt: generatedAt,
	}
	if received > 0 {
		point.SamplingCompletenessKnown = true
		point.SamplingCompleteness = float64(received-unknownSampling) / float64(received)
		point.QualityRecordRatioKnown = true
		point.QualityRecordRatio = float64(quality) / float64(received)
	}
	key := overseasPointKey{
		bucket: bucket.Unix(), kind: kind, scope: scope, direction: direction, family: family,
		geoValue: geoValue, other: other == 1, dimensionSnapshotID: version.dimensionSnapshotID,
		geoVersion: version.geoVersion, classificationVersion: version.classificationVersion,
	}
	return point, key, version, nil
}

func validateOverseasIdentity(kind OverseasRowKind, scope OverseasGeoScope, family OverseasIPFamily, geoValue string, other bool, remoteIPs, localHosts, received uint64) error {
	switch kind {
	case OverseasRowKPI:
		if scope != OverseasScopeOverseas || !validOverseasIPFamily(family) || geoValue != "" || other || remoteIPs > received || localHosts > received {
			return errors.New("invalid ClickHouse Flow overseas result: KPI identity is invalid")
		}
	case OverseasRowGeo:
		if family != OverseasIPFamilyAll || geoValue == "" || remoteIPs != 0 || localHosts != 0 {
			return errors.New("invalid ClickHouse Flow overseas result: Geo identity is invalid")
		}
		if scope == OverseasScopeUnknownGeo {
			if geoValue != "_unassigned" || other {
				return errors.New("invalid ClickHouse Flow overseas result: unknown Geo identity is invalid")
			}
		} else if scope != OverseasScopeOverseas || (geoValue == "_other") != other || geoValue == "_unassigned" {
			return errors.New("invalid ClickHouse Flow overseas result: overseas Geo identity is invalid")
		}
	default:
		return errors.New("invalid ClickHouse Flow overseas result: row kind is invalid")
	}
	return nil
}

func validOverseasDirection(value OverseasDirection) bool {
	return value == OverseasDirectionIn || value == OverseasDirectionOut || value == OverseasDirectionCombined
}

func validOverseasIPFamily(value OverseasIPFamily) bool {
	return value == OverseasIPFamilyIPv4 || value == OverseasIPFamilyIPv6 || value == OverseasIPFamilyUnknown || value == OverseasIPFamilyAll
}
