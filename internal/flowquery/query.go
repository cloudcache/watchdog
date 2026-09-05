// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

// Package flowquery owns the provider-side Flow query contract. It accepts an
// authenticated tenant scope separately from the client request and compiles
// only fixed registry entries into ClickHouse SQL.
package flowquery

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

const (
	maxTopN             = 100
	maxFilterValues     = 4_096
	maxValuesPerFilter  = 2_048
	maxFilterValueBytes = 256
	maxResultRows       = 250_000
)

type Bucket string

const (
	BucketOneMinute Bucket = "1m"
	BucketOneHour   Bucket = "1h"
)

type Metric string

const (
	MetricRawBytes         Metric = "raw_bytes"
	MetricRawBitsPerSecond Metric = "raw_bps"
	MetricRawPackets       Metric = "raw_packets"
	MetricRawPacketsSecond Metric = "raw_pps"
	MetricEstimatedBytes   Metric = "estimated_bytes"
	MetricEstimatedBPS     Metric = "estimated_bps"
	MetricEstimatedPackets Metric = "estimated_packets"
	MetricEstimatedPPS     Metric = "estimated_pps"
	MetricReceivedRecords  Metric = "received_records"
)

type Dimension string

const (
	DimensionTotal                Dimension = "total"
	DimensionCategory             Dimension = "category"
	DimensionGeoContinent         Dimension = "geo.continent"
	DimensionGeoRegion            Dimension = "geo.region"
	DimensionGeoCountry           Dimension = "geo.country"
	DimensionGeoProvince          Dimension = "geo.province"
	DimensionGeoCity              Dimension = "geo.city"
	DimensionISP                  Dimension = "isp"
	DimensionASN                  Dimension = "asn"
	DimensionBusiness             Dimension = "business"
	DimensionLocalPrefix          Dimension = "local_prefix"
	DimensionRemotePrefix         Dimension = "remote_prefix"
	DimensionAddressSet           Dimension = "address_set"
	DimensionSourceIP             Dimension = "src_ip"
	DimensionDestinationIP        Dimension = "dst_ip"
	DimensionRemotePort           Dimension = "remote_port"
	DimensionProtocol             Dimension = "protocol"
	DimensionObservationInterface Dimension = "observation_interface"
)

type View string

const ViewCustomer View = "customer"

type Scope struct {
	TenantID string
}

type Filters struct {
	Directions             []string `json:"directions,omitempty"`
	Categories             []string `json:"categories,omitempty"`
	Businesses             []string `json:"businesses,omitempty"`
	TargetIDs              []string `json:"target_ids,omitempty"`
	DeviceIDs              []string `json:"device_ids,omitempty"`
	ExporterIDs            []string `json:"exporter_ids,omitempty"`
	DimensionValues        []string `json:"dimension_values,omitempty"`
	DimensionSnapshotIDs   []string `json:"dimension_snapshot_ids,omitempty"`
	GeoVersions            []string `json:"geo_versions,omitempty"`
	ClassificationVersions []uint32 `json:"classification_versions,omitempty"`
}

type Request struct {
	From         time.Time `json:"from"`
	To           time.Time `json:"to"`
	Bucket       Bucket    `json:"bucket"`
	Metric       Metric    `json:"metric"`
	Dimension    Dimension `json:"dimension"`
	Filters      Filters   `json:"filters,omitempty"`
	View         View      `json:"view"`
	TopN         uint16    `json:"top_n"`
	IncludeOther bool      `json:"include_other"`
	Timezone     string    `json:"timezone,omitempty"`
}

type DimensionDefinition struct {
	Kind     Dimension
	Additive bool
}

type MetricDefinition struct {
	Name Metric
	Unit string
}

type Compiled struct {
	Query          ch.Query
	From           time.Time
	To             time.Time
	Bucket         Bucket
	BucketDuration time.Duration
	Metric         MetricDefinition
	Dimension      DimensionDefinition
	Timezone       string
	EstimatedRows  uint64
	MaxResultRows  uint64
}

type ErrorCode string

const (
	ErrorRequired        ErrorCode = "required"
	ErrorInvalid         ErrorCode = "invalid"
	ErrorUnsupported     ErrorCode = "unsupported"
	ErrorLimitExceeded   ErrorCode = "limit_exceeded"
	ErrorIncompleteRange ErrorCode = "incomplete_range"
)

type RequestError struct {
	Field   string
	Code    ErrorCode
	Message string
}

func (e *RequestError) Error() string {
	if e == nil {
		return "invalid Flow query"
	}
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

type metricSpec struct {
	definition MetricDefinition
	column     string
	rate       bool
}

var metricRegistry = map[Metric]metricSpec{
	MetricRawBytes:         {MetricDefinition{MetricRawBytes, "bytes"}, "raw_bytes", false},
	MetricRawBitsPerSecond: {MetricDefinition{MetricRawBitsPerSecond, "bits_per_second"}, "raw_bytes", true},
	MetricRawPackets:       {MetricDefinition{MetricRawPackets, "packets"}, "raw_packets", false},
	MetricRawPacketsSecond: {MetricDefinition{MetricRawPacketsSecond, "packets_per_second"}, "raw_packets", true},
	MetricEstimatedBytes:   {MetricDefinition{MetricEstimatedBytes, "bytes"}, "estimated_bytes", false},
	MetricEstimatedBPS:     {MetricDefinition{MetricEstimatedBPS, "bits_per_second"}, "estimated_bytes", true},
	MetricEstimatedPackets: {MetricDefinition{MetricEstimatedPackets, "packets"}, "estimated_packets", false},
	MetricEstimatedPPS:     {MetricDefinition{MetricEstimatedPPS, "packets_per_second"}, "estimated_packets", true},
	MetricReceivedRecords:  {MetricDefinition{MetricReceivedRecords, "records"}, "received_records", false},
}

var dimensionRegistry = map[Dimension]DimensionDefinition{
	DimensionTotal:                {DimensionTotal, true},
	DimensionCategory:             {DimensionCategory, true},
	DimensionGeoContinent:         {DimensionGeoContinent, true},
	DimensionGeoRegion:            {DimensionGeoRegion, true},
	DimensionGeoCountry:           {DimensionGeoCountry, true},
	DimensionGeoProvince:          {DimensionGeoProvince, true},
	DimensionGeoCity:              {DimensionGeoCity, true},
	DimensionISP:                  {DimensionISP, true},
	DimensionASN:                  {DimensionASN, true},
	DimensionBusiness:             {DimensionBusiness, true},
	DimensionLocalPrefix:          {DimensionLocalPrefix, true},
	DimensionRemotePrefix:         {DimensionRemotePrefix, true},
	DimensionAddressSet:           {DimensionAddressSet, false},
	DimensionSourceIP:             {DimensionSourceIP, true},
	DimensionDestinationIP:        {DimensionDestinationIP, true},
	DimensionRemotePort:           {DimensionRemotePort, true},
	DimensionProtocol:             {DimensionProtocol, true},
	DimensionObservationInterface: {DimensionObservationInterface, true},
}

var validDirections = map[string]struct{}{
	"ambiguous": {}, "in": {}, "out": {}, "internal": {}, "transit": {},
}

var validCategories = map[string]struct{}{
	"unknown": {}, "on_net_local_city": {}, "on_net_cross_city": {}, "on_net_cross_province": {},
	"off_net_in_province": {}, "off_net_cross_province": {}, "overseas": {}, "internal": {},
	"transit": {}, "ambiguous": {},
}

func Metrics() []MetricDefinition {
	result := make([]MetricDefinition, 0, len(metricRegistry))
	for _, current := range metricRegistry {
		result = append(result, current.definition)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func Dimensions() []DimensionDefinition {
	result := make([]DimensionDefinition, 0, len(dimensionRegistry))
	for _, current := range dimensionRegistry {
		result = append(result, current)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Kind < result[j].Kind })
	return result
}

func Compile(scope Scope, request Request, now time.Time) (Compiled, error) {
	if !validTenant(scope.TenantID) {
		return Compiled{}, requestError("scope.tenant_id", ErrorInvalid, "authenticated tenant identity is invalid")
	}
	metric, exists := metricRegistry[request.Metric]
	if !exists {
		return Compiled{}, requestError("metric", ErrorUnsupported, "metric is not in the Flow registry")
	}
	dimension, exists := dimensionRegistry[request.Dimension]
	if !exists {
		return Compiled{}, requestError("dimension", ErrorUnsupported, "dimension is not in the Flow registry")
	}
	if request.View == "" {
		return Compiled{}, requestError("view", ErrorRequired, "view is required")
	}
	if request.View != ViewCustomer {
		return Compiled{}, requestError("view", ErrorUnsupported, "only the materialized customer view is queryable in aggregate schema v1")
	}
	if request.TopN < 1 || request.TopN > maxTopN {
		return Compiled{}, requestError("top_n", ErrorLimitExceeded, "top_n must be 1..100")
	}
	if request.Dimension == DimensionTotal && request.TopN != 1 {
		return Compiled{}, requestError("top_n", ErrorInvalid, "the total dimension requires top_n=1")
	}
	if request.IncludeOther && !dimension.Additive {
		return Compiled{}, requestError("include_other", ErrorInvalid, "other is undefined for an overlapping non-additive dimension")
	}

	duration, table, maxPoints, err := bucketSpec(request.Bucket)
	if err != nil {
		return Compiled{}, err
	}
	from := request.From.UTC()
	to := request.To.UTC()
	if request.From.IsZero() || request.To.IsZero() {
		return Compiled{}, requestError("from/to", ErrorRequired, "from and to are required")
	}
	if !to.After(from) || from.Truncate(duration) != from || to.Truncate(duration) != to {
		return Compiled{}, requestError("from/to", ErrorInvalid, "range must be increasing and aligned to UTC bucket boundaries")
	}
	points := int(to.Sub(from) / duration)
	if points < 1 || points > maxPoints {
		return Compiled{}, requestError("from/to", ErrorLimitExceeded, fmt.Sprintf("range produces %d points; maximum for %s is %d", points, request.Bucket, maxPoints))
	}
	series := int(request.TopN)
	if request.IncludeOther {
		series++
	}
	estimatedRows := points*series + 1 // one internal rollup-coverage metadata row
	if estimatedRows > maxResultRows {
		return Compiled{}, requestError("top_n", ErrorLimitExceeded, fmt.Sprintf("range and top_n can produce %d rows; maximum is %d", estimatedRows, maxResultRows))
	}
	closedThrough := now.UTC().Truncate(duration)
	if to.After(closedThrough) {
		return Compiled{}, requestError("to", ErrorIncompleteRange, "to includes a bucket that is not closed")
	}
	timezone := request.Timezone
	if timezone == "" {
		timezone = "UTC"
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return Compiled{}, requestError("timezone", ErrorInvalid, "timezone is not a valid IANA location")
	}

	parameters := []proto.Parameter{
		stringParameter("tenant", scope.TenantID),
		stringParameter("from", from.Format("2006-01-02 15:04:05")),
		stringParameter("to", to.Format("2006-01-02 15:04:05")),
		stringParameter("dimension", string(request.Dimension)),
		uintParameter("top_n", uint64(request.TopN)),
		uintParameter("include_other", boolUint(request.IncludeOther)),
		uintParameter("bucket_seconds", uint64(duration/time.Second)),
	}
	conditions, filterParameters, err := compileFilters(request.Filters)
	if err != nil {
		return Compiled{}, err
	}
	parameters = append(parameters, filterParameters...)

	valueExpression := fmt.Sprintf("toFloat64(sum(%s))", metric.column)
	if metric.rate {
		multiplier := uint64(1)
		if request.Metric == MetricRawBitsPerSecond || request.Metric == MetricEstimatedBPS {
			multiplier = 8
		}
		valueExpression = fmt.Sprintf("toFloat64(sum(%s)) * %d / {bucket_seconds:UInt32}", metric.column, multiplier)
	}
	body := fmt.Sprintf(querySQL, table, table, strings.Join(conditions, "\n    "), metric.column, valueExpression)
	query := ch.Query{
		Body:       body,
		Parameters: parameters,
		Settings: []ch.Setting{
			{Key: "max_execution_time", Value: "15", Important: true},
			{Key: "max_result_rows", Value: strconv.Itoa(maxResultRows), Important: true},
			{Key: "result_overflow_mode", Value: "throw", Important: true},
			{Key: "max_rows_to_read", Value: "50000000", Important: true},
			{Key: "read_overflow_mode", Value: "throw", Important: true},
		},
	}
	return Compiled{
		Query: query, From: from, To: to, Bucket: request.Bucket, BucketDuration: duration,
		Metric: metric.definition, Dimension: dimension, Timezone: timezone,
		EstimatedRows: uint64(estimatedRows), MaxResultRows: maxResultRows,
	}, nil
}

func bucketSpec(bucket Bucket) (time.Duration, string, int, error) {
	switch bucket {
	case BucketOneMinute:
		return time.Minute, "flow_aggregate_1m", 10_080, nil
	case BucketOneHour:
		return time.Hour, "flow_aggregate_1h", 9_600, nil
	default:
		return 0, "", 0, requestError("bucket", ErrorUnsupported, "bucket must be 1m or 1h")
	}
}

func compileFilters(filters Filters) ([]string, []proto.Parameter, error) {
	type stringFilter struct {
		field, column, prefix string
		values                []string
		allowed               map[string]struct{}
	}
	stringFilters := []stringFilter{
		{"filters.directions", "business_direction", "direction", filters.Directions, validDirections},
		{"filters.categories", "category", "category", filters.Categories, validCategories},
		{"filters.businesses", "business", "business", filters.Businesses, nil},
		{"filters.target_ids", "target_id", "target", filters.TargetIDs, nil},
		{"filters.device_ids", "device_id", "device", filters.DeviceIDs, nil},
		{"filters.exporter_ids", "exporter_id", "exporter", filters.ExporterIDs, nil},
		{"filters.dimension_values", "dimension_value", "dimension_value", filters.DimensionValues, nil},
		{"filters.dimension_snapshot_ids", "dimension_snapshot_id", "dimension_snapshot", filters.DimensionSnapshotIDs, nil},
		{"filters.geo_versions", "geo_version", "geo_version", filters.GeoVersions, nil},
	}
	conditions := make([]string, 0, len(stringFilters)+1)
	parameters := make([]proto.Parameter, 0)
	total := 0
	rawTotal := 0
	for _, filter := range stringFilters {
		if len(filter.values) > maxValuesPerFilter {
			return nil, nil, requestError(filter.field, ErrorLimitExceeded, "too many filter values")
		}
		rawTotal += len(filter.values)
		if rawTotal > maxFilterValues {
			return nil, nil, requestError("filters", ErrorLimitExceeded, "too many filter values")
		}
		values, err := normalizeStrings(filter.field, filter.values, filter.allowed)
		if err != nil {
			return nil, nil, err
		}
		if len(values) > maxValuesPerFilter {
			return nil, nil, requestError(filter.field, ErrorLimitExceeded, "too many filter values")
		}
		total += len(values)
		if total > maxFilterValues {
			return nil, nil, requestError("filters", ErrorLimitExceeded, "too many filter values")
		}
		placeholders := make([]string, 0, len(values))
		for index, value := range values {
			key := fmt.Sprintf("%s_%d", filter.prefix, index)
			placeholders = append(placeholders, fmt.Sprintf("{%s:String}", key))
			parameters = append(parameters, stringParameter(key, value))
		}
		if len(placeholders) > 0 {
			conditions = append(conditions, fmt.Sprintf("AND %s IN (%s)", filter.column, strings.Join(placeholders, ", ")))
		}
	}
	if len(filters.ClassificationVersions) > maxValuesPerFilter || rawTotal+len(filters.ClassificationVersions) > maxFilterValues {
		return nil, nil, requestError("filters.classification_versions", ErrorLimitExceeded, "too many filter values")
	}
	versions := append([]uint32(nil), filters.ClassificationVersions...)
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	versions = compactVersions(versions)
	if len(versions) > maxValuesPerFilter || total+len(versions) > maxFilterValues {
		return nil, nil, requestError("filters.classification_versions", ErrorLimitExceeded, "too many filter values")
	}
	placeholders := make([]string, 0, len(versions))
	for index, version := range versions {
		if version == 0 {
			return nil, nil, requestError("filters.classification_versions", ErrorInvalid, "classification version must be positive")
		}
		key := fmt.Sprintf("classification_version_%d", index)
		placeholders = append(placeholders, fmt.Sprintf("{%s:UInt32}", key))
		parameters = append(parameters, uintParameter(key, uint64(version)))
	}
	if len(placeholders) > 0 {
		conditions = append(conditions, fmt.Sprintf("AND classification_version IN (%s)", strings.Join(placeholders, ", ")))
	}
	return conditions, parameters, nil
}

func normalizeStrings(field string, input []string, allowed map[string]struct{}) ([]string, error) {
	seen := make(map[string]struct{}, len(input))
	result := make([]string, 0, len(input))
	for _, value := range input {
		if value == "" || strings.TrimSpace(value) != value || !utf8.ValidString(value) || len(value) > maxFilterValueBytes {
			return nil, requestError(field, ErrorInvalid, "filter values must be non-empty UTF-8 without surrounding whitespace and at most 256 bytes")
		}
		for _, character := range value {
			if unicode.IsControl(character) {
				return nil, requestError(field, ErrorInvalid, "filter values cannot contain control characters")
			}
		}
		if allowed != nil {
			if _, exists := allowed[value]; !exists {
				return nil, requestError(field, ErrorUnsupported, "filter value is not in the registry")
			}
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func compactVersions(input []uint32) []uint32 {
	if len(input) == 0 {
		return input
	}
	result := input[:1]
	for _, value := range input[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}

func validTenant(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("._:-", character) {
			continue
		}
		return false
	}
	return true
}

func stringParameter(key, value string) proto.Parameter {
	escaped := strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(value)
	return proto.Parameter{Key: key, Value: "'" + escaped + "'"}
}

func uintParameter(key string, value uint64) proto.Parameter {
	return proto.Parameter{Key: key, Value: strconv.FormatUint(value, 10)}
}

func boolUint(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func requestError(field string, code ErrorCode, message string) error {
	return &RequestError{Field: field, Code: code, Message: message}
}

func IsRequestError(err error, field string, code ErrorCode) bool {
	var target *RequestError
	return errors.As(err, &target) && target.Field == field && target.Code == code
}

const querySQL = `WITH
  latest AS (
    SELECT tenant_id, bucket, max(generation) AS generation
    FROM %s FINAL
    WHERE tenant_id = {tenant:String}
      AND bucket >= {from:DateTime('UTC')} AND bucket < {to:DateTime('UTC')}
      AND dimension_kind = '_generation'
    GROUP BY tenant_id, bucket
  ),
  filtered AS (
    SELECT source.*
    FROM %s FINAL AS source
    INNER JOIN latest USING (tenant_id, bucket, generation)
    WHERE tenant_id = {tenant:String}
      AND bucket >= {from:DateTime('UTC')} AND bucket < {to:DateTime('UTC')}
      AND dimension_kind = {dimension:String}
    %s
  ),
  top_series AS (
    SELECT
      dimension_value, dimension_snapshot_id, geo_version, classification_version,
      sum(%s) AS rank_value
    FROM filtered
    GROUP BY dimension_value, dimension_snapshot_id, geo_version, classification_version
    ORDER BY rank_value DESC, dimension_value ASC, dimension_snapshot_id ASC, geo_version ASC, classification_version ASC
    LIMIT {top_n:UInt16}
  ),
  tagged AS (
    SELECT *,
      tuple(dimension_value, dimension_snapshot_id, geo_version, classification_version) IN (
        SELECT tuple(dimension_value, dimension_snapshot_id, geo_version, classification_version) FROM top_series
      ) AS is_top
    FROM filtered
  ),
  series_rows AS (
    SELECT
      bucket,
      if(is_top, dimension_value, '_other') AS dimension_value,
      if(is_top, toUInt8(0), toUInt8(1)) AS is_other,
      dimension_snapshot_id,
      geo_version,
      classification_version,
      %s AS value,
      sum(received_records) AS received_records,
      sum(unknown_sampling_records) AS unknown_sampling_records,
      sum(quality_records) AS quality_records,
      max(generated_at) AS generated_at
    FROM tagged
    WHERE {include_other:UInt8} = 1 OR is_top
    GROUP BY
      bucket, is_top, if(is_top, dimension_value, '_other'),
      dimension_snapshot_id, geo_version, classification_version
  )
SELECT *
FROM (
  SELECT
    bucket, dimension_value, is_other,
    dimension_snapshot_id, geo_version, classification_version,
    value, received_records, unknown_sampling_records, quality_records, generated_at,
    toUInt8(0) AS is_metadata, toUInt64(0) AS covered_buckets
  FROM series_rows
  UNION ALL
  SELECT
    toDateTime(0, 'UTC'), '', toUInt8(0),
    '', '', toUInt32(0),
    toFloat64(0), toUInt64(0), toUInt64(0), toUInt64(0), toDateTime64(0, 3, 'UTC'),
    toUInt8(1), toUInt64(count())
  FROM latest
)
ORDER BY
  is_metadata ASC, bucket ASC, is_other ASC, value DESC, dimension_value ASC,
  dimension_snapshot_id ASC, geo_version ASC, classification_version ASC`
