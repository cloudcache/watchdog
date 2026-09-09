// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

const (
	MinJointDimensions = 1
	MaxJointDimensions = 4
	MaxJointRange      = 24 * time.Hour
)

// JointRequest queries one or more ordered dimensions from the same base
// facts. It is deliberately separate from Request: a single-dimension rollup
// cannot reconstruct correlations or apply filters on dimensions it did not
// materialize.
type JointRequest struct {
	From         time.Time
	To           time.Time
	Interval     time.Duration
	TargetPoints uint16
	Metric       Metric
	Dimensions   []Dimension
	Filters      Filters
	Filter       *FilterExpression
	View         View
	TopN         uint16
	IncludeOther bool
	Timezone     string
	TimeWindows  []LocalTimeWindow
}

type JointPlan struct {
	RequestedFrom   time.Time `json:"requested_from"`
	RequestedTo     time.Time `json:"requested_to"`
	EffectiveFrom   time.Time `json:"effective_from"`
	EffectiveTo     time.Time `json:"effective_to"`
	Source          string    `json:"source"`
	StepSeconds     uint32    `json:"step_seconds"`
	TargetPoints    uint16    `json:"target_points"`
	MaxRangeSeconds uint32    `json:"max_range_seconds"`
}

type CompiledJoint struct {
	Query          ch.Query
	From           time.Time
	To             time.Time
	BucketDuration time.Duration
	Metric         MetricDefinition
	Dimensions     []DimensionDefinition
	Timezone       string
	EstimatedRows  uint64
	MaxResultRows  uint64
	Plan           JointPlan
}

func CompileJoint(scope Scope, request JointRequest, now time.Time) (CompiledJoint, error) {
	metric, exists := metricRegistry[request.Metric]
	if !exists {
		return CompiledJoint{}, requestError("metric", ErrorUnsupported, "metric is not in the Flow registry")
	}
	if request.View == "" {
		return CompiledJoint{}, requestError("view", ErrorRequired, "view is required")
	}
	if request.View != ViewCustomer {
		return CompiledJoint{}, requestError("view", ErrorUnsupported, "only the materialized customer view is queryable in joint schema v1")
	}
	if !scope.allowsView(ViewCustomer) {
		return CompiledJoint{}, requestError("view", ErrorPermissionDenied, "principal is not entitled to this value-layer view")
	}
	dimensions, expressions, err := compileJointDimensions(request.Dimensions)
	if err != nil {
		return CompiledJoint{}, err
	}
	if request.TopN < 1 || request.TopN > maxTopN {
		return CompiledJoint{}, requestError("top_n", ErrorLimitExceeded, "top_n must be 1..100")
	}
	from, to := request.From.UTC(), request.To.UTC()
	if from.IsZero() || to.IsZero() {
		return CompiledJoint{}, requestError("from/to", ErrorRequired, "from and to are required")
	}
	if !to.After(from) || from.Truncate(time.Minute) != from || to.Truncate(time.Minute) != to {
		return CompiledJoint{}, requestError("from/to", ErrorInvalid, "joint range must be increasing and aligned to UTC minute boundaries")
	}
	if to.Sub(from) > MaxJointRange {
		return CompiledJoint{}, requestError("from/to", ErrorLimitExceeded, "synchronous joint queries are limited to 24 hours; publish an asynchronous joint index for longer ranges")
	}
	if to.After(now.UTC().Truncate(time.Minute)) {
		return CompiledJoint{}, requestError("to", ErrorIncompleteRange, "to includes a minute that is not closed")
	}
	targetPoints := request.TargetPoints
	if targetPoints == 0 {
		targetPoints = DefaultTargetPoints
	}
	if targetPoints < MinTargetPoints || targetPoints > MaxTargetPoints {
		return CompiledJoint{}, requestError("target_points", ErrorLimitExceeded, fmt.Sprintf("target_points must be %d..%d", MinTargetPoints, MaxTargetPoints))
	}
	interval := request.Interval
	if interval == 0 {
		minimum := time.Duration((to.Sub(from) + time.Duration(targetPoints) - 1) / time.Duration(targetPoints))
		interval = chooseNiceInterval(minimum)
	}
	if interval < time.Minute || interval%time.Minute != 0 || interval > MaxJointRange {
		return CompiledJoint{}, requestError("step_seconds", ErrorInvalid, "joint step_seconds must be a whole number of minutes no greater than 24 hours")
	}
	points := int((to.Sub(from) + interval - 1) / interval)
	series := int(request.TopN)
	if request.IncludeOther {
		series++
	}
	estimatedRows := points * series
	if estimatedRows > maxResultRows {
		return CompiledJoint{}, requestError("top_n", ErrorLimitExceeded, fmt.Sprintf("range and top_n can produce %d rows; maximum is %d", estimatedRows, maxResultRows))
	}
	timezone := request.Timezone
	if timezone == "" {
		timezone = "UTC"
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return CompiledJoint{}, requestError("timezone", ErrorInvalid, "timezone is not a valid IANA location")
	}

	parameters := []proto.Parameter{
		stringParameter("from", from.Format("2006-01-02 15:04:05")),
		stringParameter("to", to.Format("2006-01-02 15:04:05")),
		uintParameter("top_n", uint64(request.TopN)),
		uintParameter("include_other", boolUint(request.IncludeOther)),
		uintParameter("bucket_seconds", uint64(interval/time.Second)),
	}
	conditions, filterParameters, err := compileBaseFilters(request.Filters, expressions, request.Filter)
	if err != nil {
		return CompiledJoint{}, err
	}
	parameters = append(parameters, filterParameters...)
	timeCondition, timeParameters, err := compileLocalTimeWindows(request.TimeWindows, timezone, "event_time")
	if err != nil {
		return CompiledJoint{}, err
	}
	if timeCondition != "" {
		conditions = append(conditions, timeCondition)
		parameters = append(parameters, timeParameters...)
	}
	valueExpression := fmt.Sprintf("toFloat64(sum(%s))", metric.column)
	if metric.rate {
		multiplier := uint64(1)
		if request.Metric == MetricRawBitsPerSecond || request.Metric == MetricEstimatedBPS {
			multiplier = 8
		}
		denominator := "greatest(toUInt32(1), least({bucket_seconds:UInt32}, toUInt32(dateDiff('second', output_bucket, {to:DateTime('UTC')}))))"
		valueExpression = fmt.Sprintf("toFloat64(sum(%s)) * %d / %s", metric.column, multiplier, denominator)
	}
	otherValues := strings.TrimSuffix(strings.Repeat("'_other', ", len(dimensions)), ", ")
	body := fmt.Sprintf(jointQuerySQL,
		strings.Join(expressions, ",\n        "),
		strings.Join(conditions, "\n      "),
		metric.column,
		otherValues,
		valueExpression,
	)
	plan := JointPlan{
		RequestedFrom: from, RequestedTo: to, EffectiveFrom: from, EffectiveTo: to,
		Source: "flow_records", StepSeconds: uint32(interval / time.Second), TargetPoints: targetPoints,
		MaxRangeSeconds: uint32(MaxJointRange / time.Second),
	}
	return CompiledJoint{
		Query: ch.Query{
			Body: body, Parameters: parameters,
			Settings: []ch.Setting{
				{Key: "max_execution_time", Value: "15", Important: true},
				{Key: "max_result_rows", Value: fmt.Sprint(maxResultRows), Important: true},
				{Key: "result_overflow_mode", Value: "throw", Important: true},
				{Key: "max_rows_to_read", Value: "50000000", Important: true},
				{Key: "max_bytes_to_read", Value: "4294967296", Important: true},
				{Key: "read_overflow_mode", Value: "throw", Important: true},
				{Key: "max_memory_usage", Value: "4294967296", Important: true},
				{Key: "max_bytes_before_external_group_by", Value: "1073741824", Important: true},
			},
		},
		From: from, To: to, BucketDuration: interval, Metric: metric.definition,
		Dimensions: dimensions, Timezone: timezone, EstimatedRows: uint64(estimatedRows),
		MaxResultRows: maxResultRows, Plan: plan,
	}, nil
}

func compileJointDimensions(input []Dimension) ([]DimensionDefinition, []string, error) {
	if len(input) < MinJointDimensions || len(input) > MaxJointDimensions {
		return nil, nil, requestError("dimensions", ErrorLimitExceeded, "base-fact dimensions must contain 1..4 ordered entries")
	}
	seen := make(map[Dimension]struct{}, len(input))
	definitions := make([]DimensionDefinition, 0, len(input))
	expressions := make([]string, 0, len(input))
	for _, dimension := range input {
		definition, exists := dimensionRegistry[dimension]
		if !exists {
			return nil, nil, requestError("dimensions", ErrorUnsupported, fmt.Sprintf("dimension %q is not in the Flow registry", dimension))
		}
		if _, exists := seen[dimension]; exists {
			return nil, nil, requestError("dimensions", ErrorInvalid, "joint dimensions cannot contain duplicates")
		}
		if dimension == DimensionTotal && len(input) != 1 {
			return nil, nil, requestError("dimensions", ErrorUnsupported, "total is only valid as the sole base-fact result dimension")
		}
		seen[dimension] = struct{}{}
		expression, exists := jointDimensionExpressions[dimension]
		if !exists {
			return nil, nil, requestError("dimensions", ErrorUnsupported, fmt.Sprintf("dimension %q requires an asynchronous joint index", dimension))
		}
		definitions = append(definitions, definition)
		expressions = append(expressions, expression)
	}
	return definitions, expressions, nil
}

func compileBaseFilters(filters Filters, dimensionExpressions []string, filter *FilterExpression) ([]string, []proto.Parameter, error) {
	dimensionValues := filters.DimensionValues
	filters.DimensionValues = nil
	conditions, parameters, err := compileFilters(filters)
	if err != nil {
		return nil, nil, err
	}
	if len(dimensionValues) > 0 {
		if len(dimensionExpressions) != 1 {
			return nil, nil, requestError("filters.dimension_values", ErrorUnsupported, "dimension_values is ambiguous for a multi-dimension base query")
		}
		if len(dimensionValues) > maxValuesPerFilter {
			return nil, nil, requestError("filters.dimension_values", ErrorLimitExceeded, "too many filter values")
		}
		values, err := normalizeStrings("filters.dimension_values", dimensionValues, nil)
		if err != nil {
			return nil, nil, err
		}
		placeholders := make([]string, 0, len(values))
		for index, value := range values {
			key := fmt.Sprintf("base_dimension_value_%d", index)
			placeholders = append(placeholders, fmt.Sprintf("{%s:String}", key))
			parameters = append(parameters, stringParameter(key, value))
		}
		conditions = append(conditions, fmt.Sprintf("AND (%s) IN (%s)", dimensionExpressions[0], strings.Join(placeholders, ", ")))
	}
	if filter != nil {
		expression, typedParameters, err := compileBaseFilter(*filter)
		if err != nil {
			return nil, nil, err
		}
		conditions = append(conditions, "AND ("+expression+")")
		parameters = append(parameters, typedParameters...)
	}
	return conditions, parameters, nil
}

var jointDimensionExpressions = map[Dimension]string{
	DimensionTotal:                "'total'",
	DimensionCategory:             "toString(category)",
	DimensionGeoContinent:         "if(empty(remote_geo_continent_id), '_unassigned', remote_geo_continent_id)",
	DimensionGeoRegion:            "if(empty(remote_geo_region_id), '_unassigned', remote_geo_region_id)",
	DimensionGeoCountry:           "if(empty(remote_geo_country_id), '_unassigned', remote_geo_country_id)",
	DimensionGeoProvince:          "if(empty(remote_geo_province_id), '_unassigned', remote_geo_province_id)",
	DimensionGeoCity:              "if(empty(remote_geo_city_id), '_unassigned', remote_geo_city_id)",
	DimensionISP:                  "if(remote_isp_id = 0, '_unassigned', toString(remote_isp_id))",
	DimensionASN:                  "if(remote_asn = 0, '_unassigned', toString(remote_asn))",
	DimensionBusiness:             "if(empty(business), '_unassigned', business)",
	DimensionLocalPrefix:          "if(empty(local_prefix_id), '_unassigned', local_prefix_id)",
	DimensionRemotePrefix:         "if(empty(remote_prefix_id), '_unassigned', remote_prefix_id)",
	DimensionSourceIP:             "toString(src_ip)",
	DimensionDestinationIP:        "toString(dst_ip)",
	DimensionRemotePort:           "if(remote_port = 0, '_unassigned', toString(remote_port))",
	DimensionProtocol:             "toString(ip_protocol)",
	DimensionObservationInterface: "if(observation_if_index = 0, '_unassigned', toString(observation_if_index))",
}

const jointQuerySQL = `WITH
  grouped AS (
    SELECT
      toDateTime(
        toUnixTimestamp({from:DateTime('UTC')}) +
        intDiv(toUnixTimestamp(event_time) - toUnixTimestamp({from:DateTime('UTC')}), {bucket_seconds:UInt32}) * {bucket_seconds:UInt32},
        'UTC'
      ) AS output_bucket,
      [
        %s
      ] AS source_dimensions,
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
      max(received_time) AS observed_at
    FROM flow_records FINAL
    WHERE event_time >= {from:DateTime('UTC')} AND event_time < {to:DateTime('UTC')}
      AND disposition = 'count'
      %s
    GROUP BY output_bucket, source_dimensions, dimension_snapshot_id, geo_version, classification_version
  ),
  top_series AS (
    SELECT source_dimensions, dimension_snapshot_id, geo_version, classification_version,
      sum(%s) AS rank_value
    FROM grouped
    GROUP BY source_dimensions, dimension_snapshot_id, geo_version, classification_version
    ORDER BY rank_value DESC, source_dimensions ASC, dimension_snapshot_id ASC, geo_version ASC, classification_version ASC
    LIMIT {top_n:UInt16}
  ),
  tagged AS (
    SELECT *, tuple(source_dimensions, dimension_snapshot_id, geo_version, classification_version) IN (
      SELECT tuple(source_dimensions, dimension_snapshot_id, geo_version, classification_version) FROM top_series
    ) AS is_top
    FROM grouped
  )
SELECT
  output_bucket AS bucket,
  if(is_top, source_dimensions, [%s]) AS dimension_values,
  if(is_top, toUInt8(0), toUInt8(1)) AS is_other,
  dimension_snapshot_id, geo_version, classification_version,
  %s AS value,
  sum(received_records) AS received_records,
  sum(unknown_sampling_records) AS unknown_sampling_records,
  sum(quality_records) AS quality_records,
  max(observed_at) AS observed_at
FROM tagged
WHERE {include_other:UInt8} = 1 OR is_top
GROUP BY output_bucket, is_top, dimension_values, dimension_snapshot_id, geo_version, classification_version
ORDER BY bucket ASC, is_other ASC, value DESC, dimension_values ASC,
  dimension_snapshot_id ASC, geo_version ASC, classification_version ASC`
