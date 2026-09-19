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
	From           time.Time        `json:"from"`
	To             time.Time        `json:"to"`
	Bucket         Bucket           `json:"bucket"`
	Metric         Metric           `json:"metric"`
	GeoLevel       OverseasGeoLevel `json:"geo_level"`
	View           View             `json:"view"`
	TopN           uint16           `json:"top_n"`
	IncludeOther   bool             `json:"include_other"`
	Filters        OverseasFilters  `json:"filters,omitempty"`
	StorageV2      bool             `json:"-"`
	ArchiveThrough time.Time        `json:"-"`
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
	UsesRawFacts   bool
	ArchiveThrough time.Time
}

// CompileOverseas builds one latest-generation aggregate query for the
// overseas KPI and Geo series. Overseas is taken only from the immutable
// classification result. A missing country/region is returned as unknown Geo
// and is never inferred to be overseas by the query layer.
func CompileOverseas(scope Scope, request OverseasRequest, now time.Time) (CompiledOverseas, error) {
	if request.View == "" {
		return CompiledOverseas{}, requestError("view", ErrorRequired, "view is required")
	}
	if request.View != ViewCustomer {
		return CompiledOverseas{}, requestError("view", ErrorUnsupported, "only the materialized customer view is queryable in overseas schema v1")
	}
	if !scope.allowsView(ViewCustomer) {
		return CompiledOverseas{}, requestError("view", ErrorPermissionDenied, "principal is not entitled to this value-layer view")
	}
	metric, exists := metricRegistry[request.Metric]
	if !exists {
		return CompiledOverseas{}, requestError("metric", ErrorUnsupported, "metric is not in the Flow registry")
	}
	geoDimension, err := overseasGeoDimension(request.GeoLevel)
	if err != nil {
		return CompiledOverseas{}, err
	}
	if request.TopN < 1 || request.TopN > MaxTopN {
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
		stringParameter("from", from.Format("2006-01-02 15:04:05")),
		stringParameter("to", to.Format("2006-01-02 15:04:05")),
		stringParameter("geo_dimension", string(geoDimension)),
		uintParameter("top_n", uint64(request.TopN)),
		uintParameter("include_other", boolUint(request.IncludeOther)),
		uintParameter("bucket_seconds", uint64(duration/time.Second)),
	}
	parameters = append(parameters, filterParameters...)
	template := overseasLegacySourceSQL + overseasAnalysisSQL
	usesRawFacts := false
	archiveThrough := time.Time{}
	if request.StorageV2 {
		archiveThrough = request.ArchiveThrough.UTC()
		if archiveThrough.IsZero() {
			archiveThrough = from
		}
		if archiveThrough.Before(from) || archiveThrough.After(to) || archiveThrough.Truncate(duration) != archiveThrough {
			return CompiledOverseas{}, requestError("archive_through", ErrorInvalid, "archive boundary must be within the range and bucket-aligned")
		}
		if request.Bucket == BucketOneMinute && archiveThrough.After(from) {
			return CompiledOverseas{}, requestError("archive_through", ErrorUnsupported, "Storage V2 has no one-minute overseas archive")
		}
		if archiveThrough.After(from) && archiveThrough.Before(to) && archiveThrough.Truncate(24*time.Hour) != archiveThrough {
			return CompiledOverseas{}, requestError("archive_through", ErrorInvalid, "a Storage V2 overseas archive/raw split must be a UTC day boundary")
		}
		parameters = append(parameters,
			stringParameter("archive_through", archiveThrough.Format("2006-01-02 15:04:05")),
			uintParameter("source_seconds", uint64(duration/time.Second)),
		)
		template = overseasStorageV2SourceSQL + overseasAnalysisSQL
		usesRawFacts = archiveThrough.Before(to)
	}

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
	).Replace(template)
	identityScoped := len(request.Filters.DeviceIDs) > 0 || len(request.Filters.TargetIDs) > 0 || len(request.Filters.ExporterIDs) > 0
	maxRowsToRead, maxBytesToRead := rawScanBudgets(usesRawFacts && identityScoped)
	query := ch.Query{
		Body: body, Parameters: parameters,
		Settings: []ch.Setting{
			{Key: "max_execution_time", Value: "15", Important: true},
			{Key: "max_memory_usage", Value: "4294967296", Important: true},
			{Key: "max_result_rows", Value: strconv.Itoa(maxResultRows), Important: true},
			{Key: "result_overflow_mode", Value: "throw", Important: true},
			{Key: "max_rows_to_read", Value: maxRowsToRead, Important: true},
			{Key: "max_bytes_to_read", Value: maxBytesToRead, Important: true},
			{Key: "read_overflow_mode", Value: "throw", Important: true},
			{Key: "join_use_nulls", Value: "0", Important: true},
			{Key: "max_bytes_before_external_group_by", Value: "1073741824", Important: true},
			{Key: "max_bytes_before_external_sort", Value: "1073741824", Important: true},
		},
	}
	return CompiledOverseas{
		Query: query, From: from, To: to, Bucket: request.Bucket, BucketDuration: duration,
		Metric: metric.definition, GeoLevel: request.GeoLevel, TopN: request.TopN,
		IncludeOther: request.IncludeOther, EstimatedRows: uint64(estimatedRows), MaxResultRows: maxResultRows,
		UsesRawFacts: usesRawFacts, ArchiveThrough: archiveThrough,
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
	conditions, parameters, err := compileDetailFilters(ViewCustomer, DetailFilters{
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

const overseasLegacySourceSQL = `WITH
  latest AS (
    SELECT bucket, max(generation) AS generation
    FROM {{TABLE}} FINAL
    WHERE bucket >= {from:DateTime('UTC')} AND bucket < {to:DateTime('UTC')}
      AND dimension_kind = '_generation'
    GROUP BY bucket
  ),
  selected AS (
    SELECT source.*
    FROM {{TABLE}} AS source FINAL
    INNER JOIN latest USING (bucket, generation)
    WHERE bucket >= {from:DateTime('UTC')} AND bucket < {to:DateTime('UTC')}
      AND business_direction IN ('in', 'out')
	      AND dimension_kind IN ('src_ip', 'dst_ip', {geo_dimension:String})
	      {{FILTERS}}
	  ),
	  coverage AS (
	    SELECT toUInt64(count()) AS covered_buckets FROM latest
	  )`

const overseasStorageV2SourceSQL = `WITH
  archive_latest AS (
    SELECT bucket, max(generation) AS generation
    FROM {{TABLE}} FINAL
    WHERE bucket >= {from:DateTime('UTC')} AND bucket < {archive_through:DateTime('UTC')}
      AND dimension_kind = '_generation'
    GROUP BY bucket
  ),
  archive_selected AS (
    SELECT
      source.bucket, source.target_id, source.device_id, source.exporter_id,
      source.business_direction, source.category, source.business,
      source.dimension_kind, source.dimension_value,
      source.dimension_snapshot_id, source.geo_version, source.classification_version,
      source.raw_bytes, source.raw_packets, source.estimated_bytes, source.estimated_packets,
      source.received_records, source.unknown_sampling_records, source.quality_records,
      source.generated_at
    FROM {{TABLE}} AS source FINAL
    INNER JOIN archive_latest USING (bucket, generation)
    WHERE source.bucket >= {from:DateTime('UTC')} AND source.bucket < {archive_through:DateTime('UTC')}
      AND source.dimension_kind IN ('src_ip', 'dst_ip', {geo_dimension:String})
  ),
  raw_selected AS (
    SELECT
      toStartOfInterval(event_time, toIntervalSecond({source_seconds:UInt32}), 'UTC') AS bucket,
      target_id, device_id, exporter_id,
      toString(business_direction) AS business_direction,
      toString(category) AS category,
      business,
      tupleElement(dimension, 1) AS dimension_kind,
      tupleElement(dimension, 2) AS dimension_value,
      CAST(dimension_snapshot_id AS String) AS dimension_snapshot_id,
      CAST(geo_version AS String) AS geo_version,
      classification_version,
      sum(raw_bytes) AS raw_bytes,
      sum(raw_packets) AS raw_packets,
      sum(estimated_bytes) AS estimated_bytes,
      sum(estimated_packets) AS estimated_packets,
      count() AS received_records,
      countIf(NOT estimated_valid) AS unknown_sampling_records,
      countIf(quality_flags != 0) AS quality_records,
      max(received_time) AS generated_at
    FROM flow_records FINAL
    ARRAY JOIN [
      tuple('src_ip', toString(src_ip)),
      tuple('dst_ip', toString(dst_ip)),
      tuple(
        {geo_dimension:String},
        if(
          {geo_dimension:String} = 'geo.country',
          if(empty(remote_geo_country_id), '_unassigned', remote_geo_country_id),
          if(empty(remote_geo_region_id), '_unassigned', remote_geo_region_id)
        )
      )
    ] AS dimension
    WHERE event_time >= {archive_through:DateTime('UTC')} AND event_time < {to:DateTime('UTC')}
      AND disposition = 'count'
    GROUP BY bucket, target_id, device_id, exporter_id, business_direction,
      category, business, dimension_kind, dimension_value, dimension_snapshot_id,
      geo_version, classification_version
  ),
  selected AS (
    SELECT * FROM (
      SELECT * FROM archive_selected
      UNION ALL
      SELECT * FROM raw_selected
    )
    WHERE business_direction IN ('in', 'out')
      {{FILTERS}}
  ),
  coverage AS (
    SELECT assumeNotNull(
      toUInt64((SELECT count() FROM archive_latest)) +
      toUInt64(intDiv(dateDiff('second', {archive_through:DateTime('UTC')}, {to:DateTime('UTC')}), toInt64({source_seconds:UInt32})))
    ) AS covered_buckets
  )`

const overseasAnalysisSQL = `,
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
	    toUInt8(0), toUInt8(1), any(covered_buckets)
	  FROM coverage
)
ORDER BY
  is_metadata ASC, bucket ASC, row_kind ASC, geo_scope ASC,
  direction ASC, ip_family ASC, is_other ASC, value DESC, geo_value ASC,
  dimension_snapshot_id ASC, geo_version ASC, classification_version ASC`
