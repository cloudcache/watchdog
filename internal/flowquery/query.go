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
	// MaxTopN bounds a single grouped query so callers can split wider work
	// without duplicating the query engine's cardinality contract.
	MaxTopN             = 100
	maxFilterValues     = 4_096
	maxValuesPerFilter  = 2_048
	maxFilterValueBytes = 256
	maxResultRows       = 250_000
	// A selected device/target/exporter can legitimately emit more than 50M
	// sampled records in 24 hours. Identity-scoped raw queries may therefore use
	// this larger read budget. Unscoped queries keep the ordinary fail-closed
	// guard so an interactive request cannot scan the whole installation.
	maxIdentityScopedRawScanRows  = 250_000_000
	maxIdentityScopedRawScanBytes = 16 << 30
	// Endpoint Top-N deliberately trades a second identity-scoped read for a
	// bounded candidate set: the first pass selects candidates and the second
	// computes exact bucket values. Keep its cumulative read guard separate
	// from ordinary one-pass Flow queries.
	maxEndpointCandidateScanRows  = 500_000_000
	maxEndpointCandidateScanBytes = 32 << 30
	endpointCandidateMultiplier   = 8
)

func hasIdentityScope(filters Filters) bool {
	return len(filters.DeviceIDs) > 0 || len(filters.TargetIDs) > 0 || len(filters.ExporterIDs) > 0
}

func rawScanBudgets(identityScoped bool) (string, string) {
	if identityScoped {
		return strconv.Itoa(maxIdentityScopedRawScanRows), strconv.FormatUint(maxIdentityScopedRawScanBytes, 10)
	}
	return "50000000", "4294967296"
}

func endpointCandidateScanBudgets() (string, string) {
	return strconv.Itoa(maxEndpointCandidateScanRows), strconv.FormatUint(maxEndpointCandidateScanBytes, 10)
}

func rawExecutionTime(identityScoped bool) string {
	if identityScoped {
		return "30"
	}
	return "15"
}

type Bucket string

const (
	BucketOneMinute   Bucket = "1m"
	BucketOneHour     Bucket = "1h"
	BucketFlowRecords Bucket = "flow_records"
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

const (
	ViewRaw      View = "raw"
	ViewSupplier View = "supplier"
	ViewCustomer View = "customer"
)

type Scope struct {
	// AllowedViews is the set of value-layer views (raw/supplier/customer) the
	// authenticated principal is entitled to. It is the enforcement point for
	// value-layer RBAC: the query gateway builds it from the principal's
	// permissions, and the compiler refuses a request for any view not in it.
	// The zero value (nil) is fail-closed to customer-only — the least-privileged
	// view — so a caller that forgets to populate it can never expose the raw or
	// supplier layers, which requires an explicit grant.
	AllowedViews []View
}

// allowsView reports whether the principal may query the given value-layer view.
// An empty AllowedViews permits only the customer view.
func (s Scope) allowsView(view View) bool {
	if len(s.AllowedViews) == 0 {
		return view == ViewCustomer
	}
	for _, allowed := range s.AllowedViews {
		if allowed == view {
			return true
		}
	}
	return false
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
	From   time.Time `json:"from"`
	To     time.Time `json:"to"`
	Bucket Bucket    `json:"bucket"`
	// Interval is the presentation interval. Bucket selects the physical
	// rollup table; the two are deliberately independent so a 1m table can
	// serve 5m/15m graph points and a 1h table can serve day/week points.
	// A zero interval preserves the historical one-point-per-source-bucket
	// contract.
	Interval     time.Duration     `json:"-"`
	Metric       Metric            `json:"metric"`
	Dimension    Dimension         `json:"dimension"`
	Filters      Filters           `json:"filters,omitempty"`
	Filter       *FilterExpression `json:"filter,omitempty"`
	View         View              `json:"view"`
	TopN         uint16            `json:"top_n"`
	IncludeOther bool              `json:"include_other"`
	Timezone     string            `json:"timezone,omitempty"`
	TimeWindows  []LocalTimeWindow `json:"time_windows,omitempty"`
	// StorageV2 switches the physical source from the legacy continuously
	// maintained rollup to a disjoint union: reconciled archive before
	// ArchiveThrough and raw facts from ArchiveThrough onward.
	StorageV2      bool      `json:"-"`
	ArchiveThrough time.Time `json:"-"`
}

type DimensionDefinition struct {
	Kind     Dimension `json:"kind"`
	Additive bool      `json:"additive"`
}

type MetricDefinition struct {
	Name Metric `json:"name"`
	Unit string `json:"unit"`
}

type Compiled struct {
	Query  ch.Query
	View   View
	From   time.Time
	To     time.Time
	Bucket Bucket
	// SourceBucketDuration is the resolution of the selected physical rollup
	// table. BucketDuration is the presentation interval returned to clients.
	SourceBucketDuration time.Duration
	BucketDuration       time.Duration
	Metric               MetricDefinition
	Dimension            DimensionDefinition
	Timezone             string
	EstimatedRows        uint64
	MaxResultRows        uint64
	UsesRawFacts         bool
	ArchiveThrough       time.Time
}

type ErrorCode string

const (
	ErrorRequired         ErrorCode = "required"
	ErrorInvalid          ErrorCode = "invalid"
	ErrorUnsupported      ErrorCode = "unsupported"
	ErrorLimitExceeded    ErrorCode = "limit_exceeded"
	ErrorIncompleteRange  ErrorCode = "incomplete_range"
	ErrorPermissionDenied ErrorCode = "permission_denied"
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

// classifyExecutionError maps a ClickHouse resource-limit exception to the typed
// limit_exceeded code so a caller sees a bounded, actionable error instead of a
// raw engine overflow. Every Flow query sets read/memory guards
// (max_rows_to_read, max_bytes_to_read, max_memory_usage, ...); the guard the
// caller cannot predict from the request alone is the raw scan whose pre-FINAL
// row count is inflated by unmerged ReplacingMergeTree versions — a long
// mixed-version range trips max_rows_to_read even though points*top_n is small.
// Non-limit errors pass through unchanged.
func classifyExecutionError(err error) error {
	var exception *ch.Exception
	if errors.As(err, &exception) && exception.IsCode(
		proto.ErrTooManyRows,
		proto.ErrTooManyBytes,
		proto.ErrTooManyRowsOrBytes,
		proto.ErrMemoryLimitExceeded,
		proto.ErrSetSizeLimitExceeded,
	) {
		return &RequestError{
			Field:   "from/to",
			Code:    ErrorLimitExceeded,
			Message: "query exceeded a ClickHouse resource limit; narrow the time range or top_n",
		}
	}
	return err
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

// AggregateViews returns the value layers implemented by the current
// aggregate schema. Callers must not infer support for raw or supplier from
// the detail-query capabilities.
func AggregateViews() []View {
	return []View{ViewCustomer}
}

func Compile(scope Scope, request Request, now time.Time) (Compiled, error) {
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
	if !scope.allowsView(ViewCustomer) {
		return Compiled{}, requestError("view", ErrorPermissionDenied, "principal is not entitled to this value-layer view")
	}
	if request.TopN < 1 || request.TopN > MaxTopN {
		return Compiled{}, requestError("top_n", ErrorLimitExceeded, "top_n must be 1..100")
	}
	if request.Dimension == DimensionTotal && request.TopN != 1 {
		return Compiled{}, requestError("top_n", ErrorInvalid, "the total dimension requires top_n=1")
	}
	if request.IncludeOther && !dimension.Additive {
		return Compiled{}, requestError("include_other", ErrorInvalid, "other is undefined for an overlapping non-additive dimension")
	}

	sourceDuration, table, maxSourcePoints, err := bucketSpec(request.Bucket)
	if err != nil {
		return Compiled{}, err
	}
	interval := request.Interval
	if interval == 0 {
		interval = sourceDuration
	}
	if interval < sourceDuration || interval%sourceDuration != 0 || interval > 30*24*time.Hour {
		return Compiled{}, requestError("interval", ErrorInvalid, "interval must be a whole multiple of the source resolution and no more than 30 days")
	}
	from := request.From.UTC()
	to := request.To.UTC()
	if request.From.IsZero() || request.To.IsZero() {
		return Compiled{}, requestError("from/to", ErrorRequired, "from and to are required")
	}
	if !to.After(from) || from.Truncate(sourceDuration) != from || to.Truncate(sourceDuration) != to {
		return Compiled{}, requestError("from/to", ErrorInvalid, "range must be increasing and aligned to UTC bucket boundaries")
	}
	sourcePoints := int(to.Sub(from) / sourceDuration)
	if sourcePoints < 1 || sourcePoints > maxSourcePoints {
		return Compiled{}, requestError("from/to", ErrorLimitExceeded, fmt.Sprintf("range reads %d source buckets; maximum for %s is %d", sourcePoints, request.Bucket, maxSourcePoints))
	}
	points := int((to.Sub(from) + interval - 1) / interval)
	series := int(request.TopN)
	if request.IncludeOther {
		series++
	}
	estimatedRows := points*series + 1 // one internal rollup-coverage metadata row
	if estimatedRows > maxResultRows {
		return Compiled{}, requestError("top_n", ErrorLimitExceeded, fmt.Sprintf("range and top_n can produce %d rows; maximum is %d", estimatedRows, maxResultRows))
	}
	closedThrough := now.UTC().Truncate(sourceDuration)
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
		stringParameter("from", from.Format("2006-01-02 15:04:05")),
		stringParameter("to", to.Format("2006-01-02 15:04:05")),
		stringParameter("dimension", string(request.Dimension)),
		uintParameter("top_n", uint64(request.TopN)),
		uintParameter("include_other", boolUint(request.IncludeOther)),
		uintParameter("bucket_seconds", uint64(interval/time.Second)),
	}
	request.Filters.DimensionValues, err = storageDimensionValues(request.Dimension, request.Filters.DimensionValues)
	if err != nil {
		return Compiled{}, err
	}
	conditions, filterParameters, err := compileFilters(request.Filters)
	if err != nil {
		return Compiled{}, err
	}
	namedFilterConditionCount := len(conditions)
	parameters = append(parameters, filterParameters...)
	timeCondition, timeParameters, err := compileLocalTimeWindows(request.TimeWindows, timezone, "bucket")
	if err != nil {
		return Compiled{}, err
	}
	if timeCondition != "" {
		conditions = append(conditions, timeCondition)
		parameters = append(parameters, timeParameters...)
	}
	if request.Filter != nil {
		supported, err := AggregateFilterSupported(*request.Filter)
		if err != nil {
			return Compiled{}, err
		}
		if !supported {
			return Compiled{}, requestError("filter", ErrorUnsupported, "filter requires the bounded base-fact query path")
		}
		expression, typedParameters, err := compileBaseFilter(*request.Filter)
		if err != nil {
			return Compiled{}, err
		}
		conditions = append(conditions, "AND ("+expression+")")
		parameters = append(parameters, typedParameters...)
	}

	valueExpression := fmt.Sprintf("toFloat64(sum(%s))", metric.column)
	if metric.rate {
		multiplier := uint64(1)
		if request.Metric == MetricRawBitsPerSecond || request.Metric == MetricEstimatedBPS {
			multiplier = 8
		}
		// The final presentation bucket may be shorter than Interval when a
		// custom range is not evenly divisible. Divide by its actual covered
		// seconds instead of silently understating the final rate.
		denominator := "greatest(toUInt32(1), least({bucket_seconds:UInt32}, toUInt32(dateDiff('second', output_bucket, {to:DateTime('UTC')}))))"
		valueExpression = fmt.Sprintf("toFloat64(sum(%s)) * %d / %s", metric.column, multiplier, denominator)
	}
	body := fmt.Sprintf(querySQL, table, table, strings.Join(conditions, "\n    "), metric.column, valueExpression)
	usesRawFacts := false
	archiveThrough := time.Time{}
	endpointCandidateQuery := false
	identityScopedRawQuery := false
	if request.StorageV2 {
		archiveThrough = request.ArchiveThrough.UTC()
		if archiveThrough.IsZero() {
			archiveThrough = from
		}
		if archiveThrough.Before(from) || archiveThrough.After(to) || archiveThrough.Truncate(sourceDuration) != archiveThrough {
			return Compiled{}, requestError("archive_through", ErrorInvalid, "archive boundary must be within the range and source-aligned")
		}
		if request.Bucket == BucketOneMinute && archiveThrough.After(from) {
			return Compiled{}, requestError("archive_through", ErrorUnsupported, "Storage V2 has no one-minute archive; minute queries must use raw facts")
		}
		if archiveThrough.After(from) && archiveThrough.Before(to) && archiveThrough.Truncate(24*time.Hour) != archiveThrough {
			return Compiled{}, requestError("archive_through", ErrorInvalid, "a Storage V2 archive/raw split must be a UTC day boundary")
		}
		dimensionExpression, expressionErr := rawAggregateDimensionExpression(request.Dimension)
		if expressionErr != nil {
			return Compiled{}, expressionErr
		}
		archiveFilters, rawFilters, residualFilters, filterErr := compileStorageV2Filters(request.Filters)
		if filterErr != nil {
			return Compiled{}, filterErr
		}
		residualFilters = append(residualFilters, conditions[namedFilterConditionCount:]...)
		parameters = append(parameters,
			stringParameter("archive_through", archiveThrough.Format("2006-01-02 15:04:05")),
			uintParameter("source_seconds", uint64(sourceDuration/time.Second)),
		)
		identityScopedRawQuery = archiveThrough.Before(to) && hasIdentityScope(request.Filters)
		endpointCandidateQuery = archiveThrough.Equal(from) && identityScopedRawQuery &&
			(request.Dimension == DimensionSourceIP || request.Dimension == DimensionDestinationIP) &&
			len(residualFilters) == 0
		// The endpoint report's per-direction stage passes the already-selected
		// top-N IPs as dimension values, which keeps it off the candidate path and
		// on the plain raw GROUP BY. dimension_value equals toString(src_ip/dst_ip),
		// so add that same predicate to the raw branch's WHERE: the planner prunes
		// the scan to those IPs before aggregating instead of grouping every
		// endpoint first. It is redundant with the post-UNION dimension_value
		// residual (kept as the archive/raw contract and correctness backstop), so
		// results are unchanged. Limited to the scalar IP dimensions.
		if len(request.Filters.DimensionValues) > 0 &&
			(request.Dimension == DimensionSourceIP || request.Dimension == DimensionDestinationIP) {
			pushedFilters, pushedParams, pushErr := rawDimensionValuePredicate(dimensionExpression, request.Filters.DimensionValues)
			if pushErr != nil {
				return Compiled{}, pushErr
			}
			rawFilters = append(rawFilters, pushedFilters...)
			parameters = append(parameters, pushedParams...)
		}
		if endpointCandidateQuery {
			candidateCount := uint64(request.TopN) * endpointCandidateMultiplier
			parameters = append(parameters, uintParameter("candidate_n", candidateCount))
			metricInput := metric.column
			if request.Metric == MetricReceivedRecords {
				metricInput = "toUInt64(1)"
			}
			candidateValueExpression := "toFloat64(sum(metric_value))"
			if metric.rate {
				multiplier := uint64(1)
				if request.Metric == MetricRawBitsPerSecond || request.Metric == MetricEstimatedBPS {
					multiplier = 8
				}
				denominator := "greatest(toUInt32(1), least({bucket_seconds:UInt32}, toUInt32(dateDiff('second', output_bucket, {to:DateTime('UTC')}))))"
				candidateValueExpression = fmt.Sprintf("toFloat64(sum(metric_value)) * %d / %s", multiplier, denominator)
			}
			body = fmt.Sprintf(storageV2RawEndpointQuerySQL,
				dimensionExpression, metricInput, strings.Join(rawFilters, "\n      "),
				dimensionExpression, metricInput, strings.Join(rawFilters, "\n      "),
				candidateValueExpression)
		} else {
			metricInput := metric.column
			if request.Metric == MetricReceivedRecords {
				metricInput = "toUInt64(1)"
			}
			storageValueExpression := "toFloat64(sum(metric_value))"
			if metric.rate {
				multiplier := uint64(1)
				if request.Metric == MetricRawBitsPerSecond || request.Metric == MetricEstimatedBPS {
					multiplier = 8
				}
				denominator := "greatest(toUInt32(1), least({bucket_seconds:UInt32}, toUInt32(dateDiff('second', output_bucket, {to:DateTime('UTC')}))))"
				storageValueExpression = fmt.Sprintf("toFloat64(sum(metric_value)) * %d / %s", multiplier, denominator)
			}
			body = fmt.Sprintf(storageV2QuerySQL,
				table, metric.column, table, strings.Join(archiveFilters, "\n      "),
				dimensionExpression, metricInput, strings.Join(rawFilters, "\n      "),
				strings.Join(residualFilters, "\n    "), storageValueExpression)
		}
		usesRawFacts = archiveThrough.Before(to)
	}
	maxRowsToRead, maxBytesToRead := rawScanBudgets(identityScopedRawQuery)
	if endpointCandidateQuery {
		maxRowsToRead, maxBytesToRead = endpointCandidateScanBudgets()
	}
	query := ch.Query{
		Body:       body,
		Parameters: parameters,
		Settings: []ch.Setting{
			{Key: "max_execution_time", Value: rawExecutionTime(identityScopedRawQuery), Important: true},
			{Key: "max_result_rows", Value: strconv.Itoa(maxResultRows), Important: true},
			{Key: "result_overflow_mode", Value: "throw", Important: true},
			{Key: "max_rows_to_read", Value: maxRowsToRead, Important: true},
			{Key: "max_bytes_to_read", Value: maxBytesToRead, Important: true},
			{Key: "read_overflow_mode", Value: "throw", Important: true},
			// Keep a hard memory guard. Ordinary ranking uses one grouped scan;
			// endpoint candidate ranking uses two raw reads but keeps the
			// high-cardinality endpoint set out of the final aggregation state.
			{Key: "max_memory_usage", Value: "4294967296", Important: true},
			// High-cardinality endpoint dimensions can exceed the in-memory
			// aggregation/sort budget before the final Top N is selected. Spill
			// intermediate states instead of turning a bounded 24h query into a
			// user-visible MEMORY_LIMIT_EXCEEDED response.
			{Key: "max_bytes_before_external_group_by", Value: "1073741824", Important: true},
			{Key: "max_bytes_before_external_sort", Value: "1073741824", Important: true},
		},
	}
	return Compiled{
		Query: query, View: request.View, From: from, To: to, Bucket: request.Bucket,
		SourceBucketDuration: sourceDuration, BucketDuration: interval,
		Metric: metric.definition, Dimension: dimension, Timezone: timezone,
		EstimatedRows: uint64(estimatedRows), MaxResultRows: maxResultRows,
		UsesRawFacts: usesRawFacts, ArchiveThrough: archiveThrough,
	}, nil
}

func rawAggregateDimensionExpression(dimension Dimension) (string, error) {
	if dimension == DimensionAddressSet {
		return "arrayJoin(arrayFilter(value -> value != '', arrayDistinct(arrayConcat(local_address_set_ids, remote_address_set_ids))))", nil
	}
	expression, exists := jointDimensionExpressions[dimension]
	if !exists {
		return "", requestError("dimension", ErrorUnsupported, "dimension is not available from Storage V2 raw facts")
	}
	return expression, nil
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

type aggregateFilterColumns struct {
	direction, category, business, target, device, exporter              string
	dimensionValue, dimensionSnapshot, geoVersion, classificationVersion string
}

var defaultAggregateFilterColumns = aggregateFilterColumns{
	direction: "business_direction", category: "category", business: "business",
	target: "target_id", device: "device_id", exporter: "exporter_id",
	dimensionValue: "dimension_value", dimensionSnapshot: "dimension_snapshot_id",
	geoVersion: "geo_version", classificationVersion: "classification_version",
}

func compileFilters(filters Filters) ([]string, []proto.Parameter, error) {
	return compileFiltersWithColumns(filters, defaultAggregateFilterColumns)
}

func compileFiltersWithColumns(filters Filters, columns aggregateFilterColumns) ([]string, []proto.Parameter, error) {
	type stringFilter struct {
		field, column, prefix string
		values                []string
		allowed               map[string]struct{}
	}
	stringFilters := []stringFilter{
		{"filters.directions", columns.direction, "direction", filters.Directions, validDirections},
		{"filters.categories", columns.category, "category", filters.Categories, validCategories},
		{"filters.businesses", columns.business, "business", filters.Businesses, nil},
		{"filters.target_ids", columns.target, "target", filters.TargetIDs, nil},
		{"filters.device_ids", columns.device, "device", filters.DeviceIDs, nil},
		{"filters.exporter_ids", columns.exporter, "exporter", filters.ExporterIDs, nil},
		{"filters.dimension_values", columns.dimensionValue, "dimension_value", filters.DimensionValues, nil},
		{"filters.dimension_snapshot_ids", columns.dimensionSnapshot, "dimension_snapshot", filters.DimensionSnapshotIDs, nil},
		{"filters.geo_versions", columns.geoVersion, "geo_version", filters.GeoVersions, nil},
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
		conditions = append(conditions, fmt.Sprintf("AND %s IN (%s)", columns.classificationVersion, strings.Join(placeholders, ", ")))
	}
	return conditions, parameters, nil
}

// rawDimensionValuePredicate builds a raw-branch predicate that filters scanned
// rows by the requested dimension values before the GROUP BY. It is only used
// for the scalar IP dimensions, where the raw dimension expression
// (toString(src_ip/dst_ip)) equals the post-UNION dimension_value, so the
// predicate is redundant with the residual dimension_value filter and cannot
// change results — it only prunes the raw scan. Parameters use a distinct prefix
// so they never collide with the residual's dimension_value bindings.
func rawDimensionValuePredicate(expression string, values []string) ([]string, []proto.Parameter, error) {
	normalized, err := normalizeStrings("filters.dimension_values", values, nil)
	if err != nil {
		return nil, nil, err
	}
	if len(normalized) == 0 {
		return nil, nil, nil
	}
	placeholders := make([]string, 0, len(normalized))
	parameters := make([]proto.Parameter, 0, len(normalized))
	for index, value := range normalized {
		key := fmt.Sprintf("raw_dim_value_%d", index)
		placeholders = append(placeholders, fmt.Sprintf("{%s:String}", key))
		parameters = append(parameters, stringParameter(key, value))
	}
	return []string{fmt.Sprintf("AND %s IN (%s)", expression, strings.Join(placeholders, ", "))}, parameters, nil
}

// compileStorageV2Filters pushes predicates that exist on both archive rows and
// raw facts below the raw GROUP BY. Dimension-value and publication-version
// predicates stay after the archive/raw UNION because the raw dimension value
// is a derived expression and both branches must keep the same public contract.
func compileStorageV2Filters(filters Filters) (archive, raw, residual []string, err error) {
	pushdown := filters
	pushdown.DimensionValues = nil
	pushdown.DimensionSnapshotIDs = nil
	pushdown.GeoVersions = nil
	remaining := Filters{
		DimensionValues:      filters.DimensionValues,
		DimensionSnapshotIDs: filters.DimensionSnapshotIDs,
		GeoVersions:          filters.GeoVersions,
	}
	archiveColumns := aggregateFilterColumns{
		direction: "source.business_direction", category: "source.category", business: "source.business",
		target: "source.target_id", device: "source.device_id", exporter: "source.exporter_id",
		dimensionValue: "source.dimension_value", dimensionSnapshot: "source.dimension_snapshot_id",
		geoVersion: "source.geo_version", classificationVersion: "source.classification_version",
	}
	rawColumns := aggregateFilterColumns{
		direction: "toString(business_direction)", category: "toString(category)", business: "business",
		target: "target_id", device: "device_id", exporter: "exporter_id",
		dimensionValue: "dimension_value", dimensionSnapshot: "CAST(dimension_snapshot_id AS String)",
		geoVersion: "CAST(geo_version AS String)", classificationVersion: "classification_version",
	}
	archive, _, err = compileFiltersWithColumns(pushdown, archiveColumns)
	if err != nil {
		return nil, nil, nil, err
	}
	raw, _, err = compileFiltersWithColumns(pushdown, rawColumns)
	if err != nil {
		return nil, nil, nil, err
	}
	residual, _, err = compileFilters(remaining)
	if err != nil {
		return nil, nil, nil, err
	}
	return archive, raw, residual, nil
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

func stringParameter(key, value string) proto.Parameter {
	escaped := strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(value)
	return proto.Parameter{Key: key, Value: "'" + escaped + "'"}
}

func uintParameter(key string, value uint64) proto.Parameter {
	// The native protocol serializes query parameters as custom settings. Its
	// value is a ClickHouse Field dump, not a bare SQL token; numeric dumps must
	// therefore be quoted just like ch.Parameters does before the placeholder
	// type converts them to UInt8/UInt16/UInt32.
	return proto.Parameter{Key: key, Value: "'" + strconv.FormatUint(value, 10) + "'"}
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
    SELECT bucket, max(generation) AS generation
    FROM %s FINAL
    WHERE bucket >= {from:DateTime('UTC')} AND bucket < {to:DateTime('UTC')}
      AND dimension_kind = '_generation'
    GROUP BY bucket
  ),
  filtered AS (
    SELECT source.*
    FROM %s AS source FINAL
    INNER JOIN latest USING (bucket, generation)
    WHERE bucket >= {from:DateTime('UTC')} AND bucket < {to:DateTime('UTC')}
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
	  toDateTime(
	    toUnixTimestamp({from:DateTime('UTC')}) +
	    intDiv(toUnixTimestamp(bucket) - toUnixTimestamp({from:DateTime('UTC')}), {bucket_seconds:UInt32}) * {bucket_seconds:UInt32},
	    'UTC'
	  ) AS output_bucket,
      if(is_top, dimension_value, '_other') AS grouped_dimension_value,
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
	  output_bucket, is_top, grouped_dimension_value,
      dimension_snapshot_id, geo_version, classification_version
  )
SELECT *
FROM (
  SELECT
	output_bucket AS bucket, grouped_dimension_value AS dimension_value, is_other,
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

const storageV2QuerySQL = `WITH
  archive_latest AS (
    SELECT bucket, max(generation) AS generation
    FROM %s FINAL
    WHERE bucket >= {from:DateTime('UTC')} AND bucket < {archive_through:DateTime('UTC')}
      AND dimension_kind = '_generation'
    GROUP BY bucket
  ),
  archive_rows AS (
    SELECT
      source.bucket, source.target_id, source.device_id, source.exporter_id,
      source.business_direction, source.category, source.business,
      source.dimension_value, source.dimension_snapshot_id, source.geo_version,
	  source.classification_version, source.%s AS metric_value,
	  source.received_records,
      source.unknown_sampling_records, source.quality_records, source.generated_at
    FROM %s AS source FINAL
    INNER JOIN archive_latest USING (bucket, generation)
    WHERE source.bucket >= {from:DateTime('UTC')} AND source.bucket < {archive_through:DateTime('UTC')}
      AND source.dimension_kind = {dimension:String}
	  %s
  ),
  raw_rows AS (
    SELECT
      toStartOfInterval(event_time, toIntervalSecond({source_seconds:UInt32}), 'UTC') AS bucket,
      target_id, device_id, exporter_id,
      toString(business_direction) AS business_direction,
      toString(category) AS category,
      business,
      CAST(%s AS String) AS dimension_value,
      CAST(dimension_snapshot_id AS String) AS dimension_snapshot_id,
      CAST(geo_version AS String) AS geo_version,
      classification_version,
	  sum(%s) AS metric_value,
      count() AS received_records,
      countIf(NOT estimated_valid) AS unknown_sampling_records,
      countIf(quality_flags != 0) AS quality_records,
      max(received_time) AS generated_at
    FROM flow_records FINAL
    WHERE event_time >= {archive_through:DateTime('UTC')} AND event_time < {to:DateTime('UTC')}
      AND disposition = 'count'
	  %s
    GROUP BY bucket, target_id, device_id, exporter_id, business_direction,
      category, business, dimension_value, dimension_snapshot_id, geo_version,
      classification_version
  ),
  filtered AS (
    SELECT *
    FROM (
      SELECT * FROM archive_rows
      UNION ALL
      SELECT * FROM raw_rows
    )
    WHERE 1 = 1
    %s
  ),
  scored AS (
    SELECT *,
	  sum(metric_value) OVER (
        PARTITION BY dimension_value, dimension_snapshot_id, geo_version, classification_version
      ) AS rank_value
    FROM filtered
  ),
  ranked AS (
    SELECT *,
      dense_rank() OVER (
        ORDER BY rank_value DESC, dimension_value ASC, dimension_snapshot_id ASC,
          geo_version ASC, classification_version ASC
      ) AS series_rank
    FROM scored
  ),
  tagged AS (
    SELECT *, toUInt8(series_rank <= {top_n:UInt16}) AS is_top
    FROM ranked
  ),
  series_rows AS (
    SELECT
      toDateTime(
        toUnixTimestamp({from:DateTime('UTC')}) +
        intDiv(toUnixTimestamp(bucket) - toUnixTimestamp({from:DateTime('UTC')}), {bucket_seconds:UInt32}) * {bucket_seconds:UInt32},
        'UTC'
      ) AS output_bucket,
      if(is_top, dimension_value, '_other') AS grouped_dimension_value,
      if(is_top, toUInt8(0), toUInt8(1)) AS is_other,
      dimension_snapshot_id, geo_version, classification_version,
      %s AS value,
      sum(received_records) AS received_records,
      sum(unknown_sampling_records) AS unknown_sampling_records,
      sum(quality_records) AS quality_records,
      max(generated_at) AS generated_at
    FROM tagged
    WHERE {include_other:UInt8} = 1 OR is_top
    GROUP BY output_bucket, is_top, grouped_dimension_value,
      dimension_snapshot_id, geo_version, classification_version
  )
SELECT *
FROM (
  SELECT
    output_bucket AS bucket, grouped_dimension_value AS dimension_value, is_other,
    dimension_snapshot_id, geo_version, classification_version,
    value, received_records, unknown_sampling_records, quality_records, generated_at,
    toUInt8(0) AS is_metadata, toUInt64(0) AS covered_buckets
  FROM series_rows
  UNION ALL
  SELECT
    toDateTime(0, 'UTC'), '', toUInt8(0), '', '', toUInt32(0),
    toFloat64(0), toUInt64(0), toUInt64(0), toUInt64(0), toDateTime64(0, 3, 'UTC'),
    toUInt8(1),
    assumeNotNull(
      toUInt64((SELECT count() FROM archive_latest)) +
      toUInt64(intDiv(dateDiff('second', {archive_through:DateTime('UTC')}, {to:DateTime('UTC')}), toInt64({source_seconds:UInt32})))
    )
  )
ORDER BY
  is_metadata ASC, bucket ASC, is_other ASC, value DESC, dimension_value ASC,
  dimension_snapshot_id ASC, geo_version ASC, classification_version ASC`

// storageV2RawEndpointQuerySQL is the interactive source/destination-IP path
// for an all-raw Storage V2 interval. Building an exact GROUP BY state for
// every endpoint before ranking is pathological on sampled transit traffic:
// a single selected device can have tens of millions of distinct endpoints in
// 24 hours. The first scan therefore keeps a bounded weighted heavy-hitter
// candidate set. The second scan computes exact per-bucket values for those
// candidates and one additive `_other` series; exact Top N selection is then
// performed over that bounded result. This keeps memory proportional to
// top_n*8 rather than endpoint cardinality. Only the requested metric is read
// and aggregated on the second pass; values and total conservation are exact
// for every returned bucket.
const storageV2RawEndpointQuerySQL = `WITH
  candidate_keys AS (
    SELECT arrayJoin(
      topKWeighted({candidate_n:UInt16})(
        CAST(%s AS String),
        %s
      )
    ) AS endpoint_value
    FROM flow_records FINAL
    WHERE event_time >= {archive_through:DateTime('UTC')} AND event_time < {to:DateTime('UTC')}
      AND disposition = 'count'
      %s
  ),
  tagged AS (
    SELECT
      toDateTime(
        toUnixTimestamp({from:DateTime('UTC')}) +
        intDiv(toUnixTimestamp(event_time) - toUnixTimestamp({from:DateTime('UTC')}), {bucket_seconds:UInt32}) * {bucket_seconds:UInt32},
        'UTC'
      ) AS output_bucket,
      CAST(%s AS String) AS endpoint_value,
      tuple(
        endpoint_value,
        CAST(dimension_snapshot_id AS String),
        CAST(geo_version AS String),
        classification_version
      ) AS series_key,
      endpoint_value IN (SELECT endpoint_value FROM candidate_keys) AS is_candidate,
      %s AS metric_value,
      estimated_valid, quality_flags, received_time
    FROM flow_records FINAL
    WHERE event_time >= {archive_through:DateTime('UTC')} AND event_time < {to:DateTime('UTC')}
      AND disposition = 'count'
      %s
  ),
  bucketed AS (
    SELECT
      output_bucket,
      if(
        is_candidate,
        series_key,
        tuple(
          '_other', tupleElement(series_key, 2), tupleElement(series_key, 3), tupleElement(series_key, 4)
        )
      ) AS candidate_key,
      sum(metric_value) AS metric_value,
      count() AS received_records,
      countIf(NOT estimated_valid) AS unknown_sampling_records,
      countIf(quality_flags != 0) AS quality_records,
      max(received_time) AS generated_at
    FROM tagged
    WHERE {include_other:UInt8} = 1 OR is_candidate
    GROUP BY output_bucket, candidate_key
  ),
  top_series AS (
    SELECT candidate_key AS series_key, sum(metric_value) AS rank_value
    FROM bucketed
    WHERE tupleElement(candidate_key, 1) != '_other'
    GROUP BY series_key
    ORDER BY rank_value DESC, series_key ASC
    LIMIT {top_n:UInt16}
  ),
  ranked_buckets AS (
    SELECT *, candidate_key IN (SELECT series_key FROM top_series) AS is_top
    FROM bucketed
  ),
  series_rows AS (
    SELECT
      output_bucket,
      if(is_top, tupleElement(candidate_key, 1), '_other') AS grouped_dimension_value,
      if(is_top, toUInt8(0), toUInt8(1)) AS is_other,
      tupleElement(candidate_key, 2) AS dimension_snapshot_id,
      tupleElement(candidate_key, 3) AS geo_version,
      tupleElement(candidate_key, 4) AS classification_version,
      %s AS value,
      sum(received_records) AS received_records,
      sum(unknown_sampling_records) AS unknown_sampling_records,
      sum(quality_records) AS quality_records,
      max(generated_at) AS generated_at
    FROM ranked_buckets
    WHERE {include_other:UInt8} = 1 OR is_top
    GROUP BY output_bucket, is_top, grouped_dimension_value,
      dimension_snapshot_id, geo_version, classification_version
  )
SELECT *
FROM (
  SELECT
    output_bucket AS bucket, grouped_dimension_value AS dimension_value, is_other,
    dimension_snapshot_id, geo_version, classification_version,
    value, received_records, unknown_sampling_records, quality_records, generated_at,
    toUInt8(0) AS is_metadata, toUInt64(0) AS covered_buckets
  FROM series_rows
  UNION ALL
  SELECT
    toDateTime(0, 'UTC'), '', toUInt8(0), '', '', toUInt32(0),
    toFloat64(0), toUInt64(0), toUInt64(0), toUInt64(0), toDateTime64(0, 3, 'UTC'),
    toUInt8(1),
    toUInt64(intDiv(dateDiff('second', {archive_through:DateTime('UTC')}, {to:DateTime('UTC')}), toInt64({source_seconds:UInt32})))
)
ORDER BY
  is_metadata ASC, bucket ASC, is_other ASC, value DESC, dimension_value ASC,
  dimension_snapshot_id ASC, geo_version ASC, classification_version ASC`
