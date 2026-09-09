// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowdimension"
)

const (
	maxAddressSetSyncRange  = time.Hour
	maxAddressSetResultRows = 10_000
)

type AddressSetEndpoint string

const (
	AddressSetEndpointLocal  AddressSetEndpoint = "local"
	AddressSetEndpointRemote AddressSetEndpoint = "remote"
	AddressSetEndpointEither AddressSetEndpoint = "either"
)

type AddressSetRequest struct {
	From     time.Time                      `json:"from"`
	To       time.Time                      `json:"to"`
	Bucket   Bucket                         `json:"bucket"`
	Metric   Metric                         `json:"metric"`
	View     View                           `json:"view"`
	Endpoint AddressSetEndpoint             `json:"endpoint"`
	Sets     flowdimension.AddressSetFilter `json:"address_set_filter"`
	Filters  DetailFilters                  `json:"filters,omitempty"`
}

type CompiledAddressSet struct {
	Query          ch.Query
	From           time.Time
	To             time.Time
	Bucket         Bucket
	BucketDuration time.Duration
	Metric         MetricDefinition
	Endpoint       AddressSetEndpoint
	Sets           flowdimension.AddressSetFilter
	MaxResultRows  uint64
}

func CompileAddressSet(scope Scope, request AddressSetRequest, now time.Time) (CompiledAddressSet, error) {
	if request.View == "" {
		return CompiledAddressSet{}, requestError("view", ErrorRequired, "view is required")
	}
	if request.View != ViewCustomer {
		return CompiledAddressSet{}, requestError("view", ErrorUnsupported, "only the materialized customer view is queryable in address-set schema v1")
	}
	if !scope.allowsView(ViewCustomer) {
		return CompiledAddressSet{}, requestError("view", ErrorPermissionDenied, "principal is not entitled to this value-layer view")
	}
	metric, exists := metricRegistry[request.Metric]
	if !exists {
		return CompiledAddressSet{}, requestError("metric", ErrorUnsupported, "metric is not in the Flow registry")
	}
	bucketDuration, bucketExpression, err := addressSetBucket(request.Bucket)
	if err != nil {
		return CompiledAddressSet{}, err
	}
	from, to := request.From.UTC(), request.To.UTC()
	if request.From.IsZero() || request.To.IsZero() {
		return CompiledAddressSet{}, requestError("from/to", ErrorRequired, "from and to are required")
	}
	if !to.After(from) || from.Truncate(bucketDuration) != from || to.Truncate(bucketDuration) != to {
		return CompiledAddressSet{}, requestError("from/to", ErrorInvalid, "range must be increasing and aligned to UTC bucket boundaries")
	}
	if to.Sub(from) > maxAddressSetSyncRange {
		return CompiledAddressSet{}, requestError("from/to", ErrorLimitExceeded, "synchronous address-set range is limited to one hour; use an asynchronous operation job for larger ranges")
	}
	if to.After(now.UTC().Truncate(bucketDuration)) {
		return CompiledAddressSet{}, requestError("to", ErrorIncompleteRange, "to includes a bucket that is not closed")
	}
	if request.Endpoint != AddressSetEndpointLocal && request.Endpoint != AddressSetEndpointRemote && request.Endpoint != AddressSetEndpointEither {
		return CompiledAddressSet{}, requestError("endpoint", ErrorUnsupported, "endpoint must be local, remote, or either")
	}
	compiledSets, err := flowdimension.CompileAddressSetFilter(request.Sets)
	if err != nil {
		code := ErrorInvalid
		if errors.Is(err, flowdimension.ErrAddressSetFilterLimit) {
			code = ErrorLimitExceeded
		}
		return CompiledAddressSet{}, requestError("address_set_filter", code, err.Error())
	}
	sets := compiledSets.Canonical()
	if len(sets.IncludeAny) == 0 && len(sets.IncludeAll) == 0 {
		return CompiledAddressSet{}, requestError("address_set_filter", ErrorRequired, "include_any or include_all must select a finite positive set")
	}
	conditions, filterParameters, err := compileDetailFilters(ViewCustomer, request.Filters)
	if err != nil {
		return CompiledAddressSet{}, err
	}
	parameters := []proto.Parameter{
		stringParameter("from", from.Format("2006-01-02 15:04:05")),
		stringParameter("to", to.Format("2006-01-02 15:04:05")),
		uintParameter("bucket_seconds", uint64(bucketDuration/time.Second)),
	}
	parameters = append(parameters, filterParameters...)
	membership := "arrayDistinct(arrayConcat(local_address_set_ids, remote_address_set_ids))"
	if request.Endpoint == AddressSetEndpointLocal {
		membership = "local_address_set_ids"
	} else if request.Endpoint == AddressSetEndpointRemote {
		membership = "remote_address_set_ids"
	}
	setConditions, setParameters := compileAddressSetConditions(membership, sets)
	conditions = append(conditions, setConditions...)
	parameters = append(parameters, setParameters...)
	valueExpression := fmt.Sprintf("toFloat64(sum(%s))", metric.column)
	if request.Metric == MetricReceivedRecords {
		valueExpression = "toFloat64(count())"
	} else if metric.rate {
		multiplier := uint64(1)
		if request.Metric == MetricRawBitsPerSecond || request.Metric == MetricEstimatedBPS {
			multiplier = 8
		}
		valueExpression = fmt.Sprintf("toFloat64(sum(%s)) * %d / {bucket_seconds:UInt32}", metric.column, multiplier)
	}
	body := fmt.Sprintf(addressSetQuerySQL, bucketExpression, valueExpression, strings.Join(conditions, "\n  "))
	query := ch.Query{
		Body: body, Parameters: parameters,
		Settings: []ch.Setting{
			{Key: "max_execution_time", Value: "10", Important: true},
			{Key: "max_result_rows", Value: strconv.Itoa(maxAddressSetResultRows), Important: true},
			{Key: "result_overflow_mode", Value: "throw", Important: true},
			{Key: "max_rows_to_read", Value: "5000000", Important: true},
			{Key: "max_bytes_to_read", Value: "1073741824", Important: true},
			{Key: "read_overflow_mode", Value: "throw", Important: true},
			{Key: "max_memory_usage", Value: "2147483648", Important: true},
		},
	}
	return CompiledAddressSet{
		Query: query, From: from, To: to, Bucket: request.Bucket, BucketDuration: bucketDuration,
		Metric: metric.definition, Endpoint: request.Endpoint, Sets: sets, MaxResultRows: maxAddressSetResultRows,
	}, nil
}

func addressSetBucket(bucket Bucket) (time.Duration, string, error) {
	switch bucket {
	case BucketOneMinute:
		return time.Minute, "toDateTime(toStartOfMinute(event_time), 'UTC')", nil
	case BucketOneHour:
		return time.Hour, "toDateTime(toStartOfHour(event_time), 'UTC')", nil
	default:
		return 0, "", requestError("bucket", ErrorUnsupported, "bucket must be 1m or 1h")
	}
}

func compileAddressSetConditions(membership string, filter flowdimension.AddressSetFilter) ([]string, []proto.Parameter) {
	conditions := make([]string, 0, 3)
	parameters := make([]proto.Parameter, 0, len(filter.IncludeAny)+len(filter.IncludeAll)+len(filter.ExcludeAny))
	appendCondition := func(prefix, function string, values []string, negate bool) {
		if len(values) == 0 {
			return
		}
		placeholders := make([]string, 0, len(values))
		for index, value := range values {
			key := fmt.Sprintf("address_%s_%d", prefix, index)
			placeholders = append(placeholders, fmt.Sprintf("{%s:String}", key))
			parameters = append(parameters, stringParameter(key, value))
		}
		condition := fmt.Sprintf("%s(%s, [%s])", function, membership, strings.Join(placeholders, ", "))
		if negate {
			condition = "NOT " + condition
		}
		conditions = append(conditions, "AND "+condition)
	}
	appendCondition("include_any", "hasAny", filter.IncludeAny, false)
	appendCondition("include_all", "hasAll", filter.IncludeAll, false)
	appendCondition("exclude_any", "hasAny", filter.ExcludeAny, true)
	return conditions, parameters
}

const addressSetQuerySQL = `SELECT
  %s AS bucket,
  CAST(dimension_snapshot_id AS String) AS dimension_snapshot_id,
  CAST(geo_version AS String) AS geo_version,
  classification_version,
  %s AS value,
  count() AS received_records,
  countIf(NOT estimated_valid) AS unknown_sampling_records,
  countIf(quality_flags != 0) AS quality_records
FROM flow_records FINAL
WHERE event_time >= {from:DateTime('UTC')} AND event_time < {to:DateTime('UTC')}
  AND disposition = 'count'
  %s
GROUP BY bucket, dimension_snapshot_id, geo_version, classification_version
ORDER BY bucket ASC, dimension_snapshot_id ASC, geo_version ASC, classification_version ASC`
