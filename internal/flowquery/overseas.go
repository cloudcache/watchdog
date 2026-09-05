// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type OverseasGeoLevel string

const (
	OverseasGeoCountry OverseasGeoLevel = "country"
	OverseasGeoRegion  OverseasGeoLevel = "region"
)

type OverseasFilters struct {
	Directions  []string `json:"directions,omitempty"`
	Businesses  []string `json:"businesses,omitempty"`
	TargetIDs   []string `json:"target_ids,omitempty"`
	DeviceIDs   []string `json:"device_ids,omitempty"`
	ExporterIDs []string `json:"exporter_ids,omitempty"`
}

type OverseasRequest struct {
	From         time.Time        `json:"from"`
	To           time.Time        `json:"to"`
	Bucket       Bucket           `json:"bucket"`
	Metric       Metric           `json:"metric"`
	GeoLevel     OverseasGeoLevel `json:"geo_level"`
	View         View             `json:"view"`
	TopN         uint16           `json:"top_n"`
	IncludeOther bool             `json:"include_other"`
	Filters      OverseasFilters  `json:"filters,omitempty"`
}

type CompiledOverseas struct {
	Query          ch.Query
	From           time.Time
	To             time.Time
	Bucket         Bucket
	BucketDuration time.Duration
	Metric         MetricDefinition
	GeoLevel       OverseasGeoLevel
	TopN           uint16
	IncludeOther   bool
	EstimatedRows  uint64
	MaxResultRows  uint64
}

// CompileOverseas builds one latest-generation aggregate query for the
// overseas KPI and Geo series. Overseas is taken only from the immutable
// classification result. A missing country/region is returned as unknown Geo
// and is never inferred to be overseas by the query layer.
func CompileOverseas(scope Scope, request OverseasRequest, now time.Time) (CompiledOverseas, error) {
	if !validTenant(scope.TenantID) {
		return CompiledOverseas{}, requestError("scope.tenant_id", ErrorInvalid, "authenticated tenant identity is invalid")
	}
	if request.View == "" {
		return CompiledOverseas{}, requestError("view", ErrorRequired, "view is required")
	}
	if request.View != ViewCustomer {
		return CompiledOverseas{}, requestError("view", ErrorUnsupported, "only the materialized customer view is queryable in overseas schema v1")
	}
	metric, exists := metricRegistry[request.Metric]
	if !exists {
		return CompiledOverseas{}, requestError("metric", ErrorUnsupported, "metric is not in the Flow registry")
	}
	geoDimension, err := overseasGeoDimension(request.GeoLevel)
	if err != nil {
		return CompiledOverseas{}, err
	}
	if request.TopN < 1 || request.TopN > maxTopN {
		return CompiledOverseas{}, requestError("top_n", ErrorLimitExceeded, "top_n must be 1..100")
	}

	duration, table, maxPoints, err := bucketSpec(request.Bucket)
	if err != nil {
		return CompiledOverseas{}, err
	}
	from, to := request.From.UTC(), request.To.UTC()
	if request.From.IsZero() || request.To.IsZero() {
		return CompiledOverseas{}, requestError("from/to", ErrorRequired, "from and to are required")
	}
	if !to.After(from) || from.Truncate(duration) != from || to.Truncate(duration) != to {
		return CompiledOverseas{}, requestError("from/to", ErrorInvalid, "range must be increasing and aligned to UTC bucket boundaries")
	}
	points := int(to.Sub(from) / duration)
	if points < 1 || points > maxPoints {
		return CompiledOverseas{}, requestError("from/to", ErrorLimitExceeded, fmt.Sprintf("range produces %d points; maximum for %s is %d", points, request.Bucket, maxPoints))
	}
	// KPI emits at most 3 directions x (IPv4, IPv6, unknown, all). Geo emits
	// at most 3 directions x (TopN, other, unknown). The estimate is
	// deliberately conservative when the caller filters a direction.
	estimatedRows := points*(12+3*(int(request.TopN)+2)) + 1
	if estimatedRows > maxResultRows {
		return CompiledOverseas{}, requestError("top_n", ErrorLimitExceeded, fmt.Sprintf("range and top_n can produce %d rows; maximum is %d", estimatedRows, maxResultRows))
	}
	if to.After(now.UTC().Truncate(duration)) {
		return CompiledOverseas{}, requestError("to", ErrorIncompleteRange, "to includes a bucket that is not closed")
	}

	filters, filterParameters, err := compileOverseasFilters(request.Filters)
	if err != nil {
		return CompiledOverseas{}, err
	}
	parameters := []proto.Parameter{
		stringParameter("tenant", scope.TenantID),
		stringParameter("from", from.Format("2006-01-02 15:04:05")),
		stringParameter("to", to.Format("2006-01-02 15:04:05")),
		stringParameter("geo_dimension", string(geoDimension)),
		uintParameter("top_n", uint64(request.TopN)),
		uintParameter("include_other", boolUint(request.IncludeOther)),
		uintParameter("bucket_seconds", uint64(duration/time.Second)),
	}
	parameters = append(parameters, filterParameters...)

	valueExpression := "toFloat64(metric_total)"
	if metric.rate {
		multiplier := uint64(1)
		if request.Metric == MetricRawBitsPerSecond || request.Metric == MetricEstimatedBPS {
			multiplier = 8
		}
		valueExpression = fmt.Sprintf("toFloat64(metric_total) * %d / {bucket_seconds:UInt32}", multiplier)
	}
	body := strings.NewReplacer(
		"{{TABLE}}", table,
		"{{FILTERS}}", strings.Join(filters, "\n      "),
		"{{METRIC_COLUMN}}", metric.column,
		"{{VALUE_EXPRESSION}}", valueExpression,
	).Replace(overseasQuerySQL)
	query := ch.Query{
		Body: body, Parameters: parameters,
		Settings: []ch.Setting{
			{Key: "max_execution_time", Value: "15", Important: true},
			{Key: "max_memory_usage", Value: "4294967296", Important: true},
			{Key: "max_result_rows", Value: strconv.Itoa(maxResultRows), Important: true},
			{Key: "result_overflow_mode", Value: "throw", Important: true},
			{Key: "max_rows_to_read", Value: "50000000", Important: true},
			{Key: "max_bytes_to_read", Value: "4294967296", Important: true},
			{Key: "read_overflow_mode", Value: "throw", Important: true},
			{Key: "join_use_nulls", Value: "0", Important: true},
		},
	}
	return CompiledOverseas{
		Query: query, From: from, To: to, Bucket: request.Bucket, BucketDuration: duration,
		Metric: metric.definition, GeoLevel: request.GeoLevel, TopN: request.TopN,
		IncludeOther: request.IncludeOther, EstimatedRows: uint64(estimatedRows), MaxResultRows: maxResultRows,
	}, nil
}

func overseasGeoDimension(level OverseasGeoLevel) (Dimension, error) {
	switch level {
	case OverseasGeoCountry:
		return DimensionGeoCountry, nil
	case OverseasGeoRegion:
		return DimensionGeoRegion, nil
	default:
		return "", requestError("geo_level", ErrorUnsupported, "geo_level must be country or region")
	}
}

func compileOverseasFilters(filters OverseasFilters) ([]string, []proto.Parameter, error) {
	directions, err := normalizeStrings("filters.directions", filters.Directions, map[string]struct{}{"in": {}, "out": {}})
	if err != nil {
		return nil, nil, err
	}
	conditions, parameters, err := compileDetailFilters(DetailFilters{
		Directions: directions, Businesses: filters.Businesses, TargetIDs: filters.TargetIDs,
		DeviceIDs: filters.DeviceIDs, ExporterIDs: filters.ExporterIDs,
	})
	if err != nil {
		return nil, nil, err
	}
	// Keep parameter and SQL generation deterministic even though the shared
	// helper already normalizes every individual filter.
	sort.Slice(parameters, func(i, j int) bool { return parameters[i].Key < parameters[j].Key })
	return conditions, parameters, nil
}

const overseasQuerySQL = `WITH
  latest AS (
    SELECT tenant_id, bucket, max(generation) AS generation
    FROM {{TABLE}} FINAL
    WHERE tenant_id = {tenant:String}
      AND bucket >= {from:DateTime('UTC')} AND bucket < {to:DateTime('UTC')}
      AND dimension_kind = '_generation'
    GROUP BY tenant_id, bucket
  ),
  selected AS (
    SELECT source.*
    FROM {{TABLE}} FINAL AS source
    INNER JOIN latest USING (tenant_id, bucket, generation)
    WHERE tenant_id = {tenant:String}
      AND bucket >= {from:DateTime('UTC')} AND bucket < {to:DateTime('UTC')}
      AND business_direction IN ('in', 'out')
      AND dimension_kind IN ('src_ip', 'dst_ip', {geo_dimension:String})
      {{FILTERS}}
  ),
  remote_endpoints AS (
    SELECT
      bucket, toString(business_direction) AS direction,
      multiIf(dimension_value = '::', 'unknown', startsWith(dimension_value, '::ffff:'), 'ipv4', 'ipv6') AS ip_family,
      dimension_value AS endpoint_ip,
      dimension_snapshot_id, geo_version, classification_version,
      {{METRIC_COLUMN}} AS metric_value,
      received_records, unknown_sampling_records, quality_records, generated_at
    FROM selected
    WHERE category = 'overseas'
      AND ((business_direction = 'in' AND dimension_kind = 'src_ip')
        OR (business_direction = 'out' AND dimension_kind = 'dst_ip'))
  ),
  local_endpoints AS (
    SELECT
      bucket, toString(business_direction) AS direction,
      multiIf(dimension_value = '::', 'unknown', startsWith(dimension_value, '::ffff:'), 'ipv4', 'ipv6') AS ip_family,
      dimension_value AS endpoint_ip,
      dimension_snapshot_id, geo_version, classification_version,
      {{METRIC_COLUMN}} AS metric_value,
      received_records, unknown_sampling_records, quality_records
    FROM selected
    WHERE category = 'overseas'
      AND ((business_direction = 'in' AND dimension_kind = 'dst_ip')
        OR (business_direction = 'out' AND dimension_kind = 'src_ip'))
  ),
  remote_expanded AS (
    SELECT *, tupleElement(grouping, 1) AS grouped_direction, tupleElement(grouping, 2) AS grouped_family
    FROM remote_endpoints
    ARRAY JOIN [
      tuple(direction, ip_family), tuple(direction, 'all'),
      tuple('combined', ip_family), tuple('combined', 'all')
    ] AS grouping
  ),
  local_expanded AS (
    SELECT *, tupleElement(grouping, 1) AS grouped_direction, tupleElement(grouping, 2) AS grouped_family
    FROM local_endpoints
    ARRAY JOIN [
      tuple(direction, ip_family), tuple(direction, 'all'),
      tuple('combined', ip_family), tuple('combined', 'all')
    ] AS grouping
  ),
  remote_summary AS (
    SELECT
      bucket, grouped_direction, grouped_family,
      dimension_snapshot_id, geo_version, classification_version,
      sum(metric_value) AS metric_total,
      uniqExact(endpoint_ip) AS observed_remote_ips,
      sum(received_records) AS received_records,
      sum(unknown_sampling_records) AS unknown_sampling_records,
      sum(quality_records) AS quality_records,
      max(generated_at) AS generated_at
    FROM remote_expanded
    GROUP BY bucket, grouped_direction, grouped_family,
      dimension_snapshot_id, geo_version, classification_version
  ),
  local_summary AS (
    SELECT
      bucket, grouped_direction, grouped_family,
      dimension_snapshot_id, geo_version, classification_version,
      sum(metric_value) AS local_metric_total,
      uniqExact(endpoint_ip) AS observed_local_hosts,
      sum(received_records) AS local_received_records,
      sum(unknown_sampling_records) AS local_unknown_sampling_records,
      sum(quality_records) AS local_quality_records
    FROM local_expanded
    GROUP BY bucket, grouped_direction, grouped_family,
      dimension_snapshot_id, geo_version, classification_version
  ),
  geo_source AS (
    SELECT
      bucket, toString(business_direction) AS direction,
      if(dimension_value = '_unassigned', 'unknown_geo', 'overseas') AS geo_scope,
      dimension_value AS geo_value,
      dimension_snapshot_id, geo_version, classification_version,
      {{METRIC_COLUMN}} AS metric_value,
      received_records, unknown_sampling_records, quality_records, generated_at
    FROM selected
    WHERE dimension_kind = {geo_dimension:String}
      AND (dimension_value = '_unassigned' OR category = 'overseas')
  ),
  top_geo AS (
    SELECT geo_value, dimension_snapshot_id, geo_version, classification_version,
      sum(metric_value) AS rank_value
    FROM geo_source
    WHERE geo_scope = 'overseas'
    GROUP BY geo_value, dimension_snapshot_id, geo_version, classification_version
    ORDER BY rank_value DESC, geo_value ASC,
      dimension_snapshot_id ASC, geo_version ASC, classification_version ASC
    LIMIT {top_n:UInt16}
  ),
  geo_tagged AS (
    SELECT *,
      geo_scope = 'unknown_geo' OR tuple(geo_value, dimension_snapshot_id, geo_version, classification_version) IN (
        SELECT tuple(geo_value, dimension_snapshot_id, geo_version, classification_version) FROM top_geo
      ) AS is_top
    FROM geo_source
  ),
  geo_expanded AS (
    SELECT *, arrayJoin([direction, 'combined']) AS grouped_direction
    FROM geo_tagged
    WHERE geo_scope = 'unknown_geo' OR is_top OR {include_other:UInt8} = 1
  ),
  geo_summary AS (
    SELECT
      bucket, geo_scope, grouped_direction,
      if(geo_scope = 'unknown_geo', '_unassigned', if(is_top, geo_value, '_other')) AS grouped_geo_value,
      toUInt8(geo_scope = 'overseas' AND NOT is_top) AS is_other,
      dimension_snapshot_id, geo_version, classification_version,
      sum(metric_value) AS metric_total,
      sum(received_records) AS received_records,
      sum(unknown_sampling_records) AS unknown_sampling_records,
      sum(quality_records) AS quality_records,
      max(generated_at) AS generated_at
    FROM geo_expanded
    GROUP BY bucket, geo_scope, grouped_direction, grouped_geo_value, is_other,
      dimension_snapshot_id, geo_version, classification_version
  )
SELECT *
FROM (
  SELECT
    remote.bucket AS bucket, 'kpi' AS row_kind, 'overseas' AS geo_scope,
    remote.grouped_direction AS direction, remote.grouped_family AS ip_family,
    '' AS geo_value, toUInt8(0) AS is_other,
    remote.dimension_snapshot_id AS dimension_snapshot_id,
    remote.geo_version AS geo_version,
    remote.classification_version AS classification_version,
    {{VALUE_EXPRESSION}} AS value,
    remote.observed_remote_ips AS observed_remote_ips,
    local_side.observed_local_hosts AS observed_local_hosts,
    remote.received_records AS received_records,
    remote.unknown_sampling_records AS unknown_sampling_records,
    remote.quality_records AS quality_records,
    remote.generated_at AS generated_at,
    toUInt8(
      local_side.local_metric_total = remote.metric_total
      AND local_side.local_received_records = remote.received_records
      AND local_side.local_unknown_sampling_records = remote.unknown_sampling_records
      AND local_side.local_quality_records = remote.quality_records
    ) AS endpoint_consistent,
    toUInt8(0) AS is_metadata, toUInt64(0) AS covered_buckets
  FROM remote_summary AS remote
  LEFT JOIN local_summary AS local_side USING (
    bucket, grouped_direction, grouped_family,
    dimension_snapshot_id, geo_version, classification_version
  )
  UNION ALL
  SELECT
    bucket, 'geo', geo_scope, grouped_direction, 'all', grouped_geo_value, is_other,
    dimension_snapshot_id, geo_version, classification_version,
    {{VALUE_EXPRESSION}}, toUInt64(0), toUInt64(0),
    received_records, unknown_sampling_records, quality_records, generated_at,
    toUInt8(1), toUInt8(0), toUInt64(0)
  FROM geo_summary
  UNION ALL
  SELECT
    toDateTime(0, 'UTC'), '', '', '', '', '', toUInt8(0),
    '', '', toUInt32(0), toFloat64(0), toUInt64(0), toUInt64(0),
    toUInt64(0), toUInt64(0), toUInt64(0), toDateTime64(0, 3, 'UTC'),
    toUInt8(0), toUInt8(1), toUInt64(count())
  FROM latest
)
ORDER BY
  is_metadata ASC, bucket ASC, row_kind ASC, geo_scope ASC,
  direction ASC, ip_family ASC, is_other ASC, value DESC, geo_value ASC,
  dimension_snapshot_id ASC, geo_version ASC, classification_version ASC`
