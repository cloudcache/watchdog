// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// CompileEndpointRollupJoint compiles the two correlations retained on endpoint
// aggregate rows: endpoint×category and endpoint×business. It is used only
// after the server has proved complete 1h generation-marker coverage for the
// range. Other correlations still require base facts.
func CompileEndpointRollupJoint(scope Scope, request JointRequest, now time.Time) (CompiledJoint, error) {
	if request.View != ViewCustomer || !scope.allowsView(ViewCustomer) {
		return CompiledJoint{}, requestError("view", ErrorPermissionDenied, "endpoint aggregate correlation requires the customer view")
	}
	if len(request.Dimensions) != 2 ||
		(request.Dimensions[0] != DimensionSourceIP && request.Dimensions[0] != DimensionDestinationIP) ||
		(request.Dimensions[1] != DimensionCategory && request.Dimensions[1] != DimensionBusiness) {
		return CompiledJoint{}, requestError("dimensions", ErrorUnsupported, "the aggregate correlation path supports endpoint with category or business")
	}
	metric, exists := metricRegistry[request.Metric]
	if !exists {
		return CompiledJoint{}, requestError("metric", ErrorUnsupported, "metric is not in the Flow registry")
	}
	if request.TopN < 1 || request.TopN > MaxTopN {
		return CompiledJoint{}, requestError("top_n", ErrorLimitExceeded, "top_n must be 1..100")
	}
	from, to := request.From.UTC(), request.To.UTC()
	if from.IsZero() || !to.After(from) || from.Truncate(time.Hour) != from || to.Truncate(time.Hour) != to {
		return CompiledJoint{}, requestError("from/to", ErrorInvalid, "endpoint aggregate range must be increasing and hour-aligned")
	}
	if to.After(now.UTC().Truncate(time.Hour)) {
		return CompiledJoint{}, requestError("to", ErrorIncompleteRange, "to includes an hourly bucket that is not closed")
	}
	if hours := int(to.Sub(from) / time.Hour); hours < 1 || hours > 9600 {
		return CompiledJoint{}, requestError("from/to", ErrorLimitExceeded, "endpoint aggregate range exceeds 9,600 hourly buckets")
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
		interval = chooseNiceInterval(max(minimum, time.Hour))
	}
	if interval < time.Hour || interval%time.Hour != 0 || interval > 30*24*time.Hour {
		return CompiledJoint{}, requestError("step_seconds", ErrorInvalid, "endpoint aggregate step must be a whole number of hours no greater than 30 days")
	}
	points := int((to.Sub(from) + interval - 1) / interval)
	series := int(request.TopN)
	if request.IncludeOther {
		series++
	}
	if estimated := points * series; estimated > maxResultRows {
		return CompiledJoint{}, requestError("top_n", ErrorLimitExceeded, fmt.Sprintf("range and top_n can produce %d rows; maximum is %d", estimated, maxResultRows))
	}
	timezone := request.Timezone
	if timezone == "" {
		timezone = "UTC"
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return CompiledJoint{}, requestError("timezone", ErrorInvalid, "timezone is not a valid IANA location")
	}

	normalizedDimensionValues, err := storageDimensionValues(request.Dimensions[0], request.Filters.DimensionValues)
	if err != nil {
		return CompiledJoint{}, err
	}
	request.Filters.DimensionValues = normalizedDimensionValues
	columns := aggregateFilterColumns{
		direction: "source.business_direction", category: "source.category", business: "source.business",
		target: "source.target_id", device: "source.device_id", exporter: "source.exporter_id",
		dimensionValue: "source.dimension_value", dimensionSnapshot: "source.dimension_snapshot_id",
		geoVersion: "source.geo_version", classificationVersion: "source.classification_version",
	}
	conditions, filterParameters, err := compileFiltersWithColumns(request.Filters, columns)
	if err != nil {
		return CompiledJoint{}, err
	}
	if request.Filter != nil {
		supported, err := AggregateFilterSupported(*request.Filter)
		if err != nil {
			return CompiledJoint{}, err
		}
		if !supported {
			return CompiledJoint{}, requestError("filter", ErrorUnsupported, "endpoint aggregate correlation cannot evaluate a base-fact-only filter")
		}
		expression, typedParameters, err := compileBaseFilter(*request.Filter)
		if err != nil {
			return CompiledJoint{}, err
		}
		conditions = append(conditions, "AND ("+expression+")")
		filterParameters = append(filterParameters, typedParameters...)
	}
	timeCondition, timeParameters, err := compileLocalTimeWindows(request.TimeWindows, timezone, "source.bucket")
	if err != nil {
		return CompiledJoint{}, err
	}
	if timeCondition != "" {
		conditions = append(conditions, timeCondition)
		filterParameters = append(filterParameters, timeParameters...)
	}
	parameters := []proto.Parameter{
		stringParameter("from", from.Format("2006-01-02 15:04:05")),
		stringParameter("to", to.Format("2006-01-02 15:04:05")),
		stringParameter("dimension", string(request.Dimensions[0])),
		uintParameter("top_n", uint64(request.TopN)),
		uintParameter("include_other", boolUint(request.IncludeOther)),
		uintParameter("bucket_seconds", uint64(interval/time.Second)),
	}
	parameters = append(parameters, filterParameters...)
	secondary := "CAST(source.category AS String)"
	if request.Dimensions[1] == DimensionBusiness {
		secondary = "if(empty(source.business), '_unassigned', source.business)"
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
	body := fmt.Sprintf(endpointRollupJointSQL, secondary, strings.Join(conditions, "\n      "), metric.column, valueExpression)
	settings := executionTimeSetting(request.ExecutionTimeout)
	settings = append(settings, []ch.Setting{
		{Key: "do_not_merge_across_partitions_select_final", Value: "1", Important: true},
		{Key: "max_result_rows", Value: strconv.Itoa(maxResultRows), Important: true},
		{Key: "result_overflow_mode", Value: "throw", Important: true},
		{Key: "max_rows_to_read", Value: "50000000", Important: true},
		{Key: "max_bytes_to_read", Value: "4294967296", Important: true},
		{Key: "read_overflow_mode", Value: "throw", Important: true},
		{Key: "max_memory_usage", Value: "2147483648", Important: true},
		{Key: "max_bytes_before_external_group_by", Value: "536870912", Important: true},
		{Key: "max_bytes_before_external_sort", Value: "536870912", Important: true},
	}...)
	dimensions, _, err := compileJointDimensions(request.Dimensions, request.View)
	if err != nil {
		return CompiledJoint{}, err
	}
	return CompiledJoint{
		Query: ch.Query{Body: body, Parameters: parameters, Settings: settings},
		View:  request.View, From: from, To: to, BucketDuration: interval, Metric: metric.definition,
		Dimensions: dimensions, Timezone: timezone, EstimatedRows: uint64(points * series), MaxResultRows: maxResultRows,
		Plan: JointPlan{
			RequestedFrom: from, RequestedTo: to, EffectiveFrom: from, EffectiveTo: to,
			Source: "flow_aggregate_1h", StepSeconds: uint32(interval / time.Second), TargetPoints: targetPoints,
			MaxRangeSeconds: uint32((400 * 24 * time.Hour) / time.Second),
		},
	}, nil
}

const endpointRollupJointSQL = `WITH
  latest AS (
    SELECT bucket, max(generation) AS generation
    FROM flow_aggregate_1h
    WHERE bucket >= {from:DateTime('UTC')} AND bucket < {to:DateTime('UTC')}
      AND dimension_kind = '_generation'
    GROUP BY bucket
  ),
  grouped AS (
    SELECT
      toDateTime(
        toUnixTimestamp({from:DateTime('UTC')}) +
        intDiv(toUnixTimestamp(source.bucket) - toUnixTimestamp({from:DateTime('UTC')}), {bucket_seconds:UInt32}) * {bucket_seconds:UInt32},
        'UTC'
      ) AS output_bucket,
      [CAST(source.dimension_value AS String), %s] AS source_dimensions,
      CAST(source.dimension_snapshot_id AS String) AS dimension_snapshot_id,
      CAST(source.geo_version AS String) AS geo_version,
      source.classification_version,
      sum(source.raw_bytes) AS raw_bytes,
      sum(source.raw_packets) AS raw_packets,
      sum(source.estimated_bytes) AS estimated_bytes,
      sum(source.estimated_packets) AS estimated_packets,
      sum(source.received_records) AS received_records,
      sum(source.unknown_sampling_records) AS unknown_sampling_records,
      sum(source.quality_records) AS quality_records,
      max(source.generated_at) AS observed_at
    FROM flow_aggregate_1h AS source FINAL
    INNER JOIN latest USING (bucket, generation)
    WHERE source.bucket >= {from:DateTime('UTC')} AND source.bucket < {to:DateTime('UTC')}
      AND source.dimension_kind = {dimension:String}
      AND source.dimension_value != '_other'
      %s
    GROUP BY output_bucket, source_dimensions, dimension_snapshot_id, geo_version, classification_version
  ),
  scored AS (
    SELECT *, sum(%s) OVER (
      PARTITION BY source_dimensions, dimension_snapshot_id, geo_version, classification_version
    ) AS rank_value
    FROM grouped
  ),
  ranked AS (
    SELECT *, dense_rank() OVER (
      ORDER BY rank_value DESC, source_dimensions ASC, dimension_snapshot_id ASC,
        geo_version ASC, classification_version ASC
    ) AS series_rank
    FROM scored
  ),
  tagged AS (
    SELECT *, toUInt8(series_rank <= {top_n:UInt16}) AS is_top FROM ranked
  )
SELECT
  output_bucket AS bucket,
  if(is_top, source_dimensions, ['_other', '_other']) AS dimension_values,
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
