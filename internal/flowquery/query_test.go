// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
)

func TestRegistryIsFixedAndMarksOnlyAddressSetsNonAdditive(t *testing.T) {
	views := AggregateViews()
	if len(views) != 1 || views[0] != ViewCustomer {
		t.Fatalf("aggregate views=%v", views)
	}
	views[0] = ViewRaw
	if AggregateViews()[0] != ViewCustomer {
		t.Fatal("caller mutation changed aggregate view capabilities")
	}
	if got := len(Metrics()); got != 9 {
		t.Fatalf("metric definitions=%d", got)
	}
	dimensions := Dimensions()
	if len(dimensions) != 19 {
		t.Fatalf("dimension definitions=%d", len(dimensions))
	}
	for _, current := range dimensions {
		if current.Additive == (current.Kind == DimensionAddressSet) {
			t.Fatalf("dimension additive contract is wrong: %+v", current)
		}
	}
}

func TestCompileBuildsDeterministicLatestGenerationTopNQuery(t *testing.T) {
	request := validRequest()
	request.Filters = Filters{
		Directions: []string{"out", "in", "out"}, Categories: []string{"overseas"},
		Businesses: []string{"customer's"}, TargetIDs: []string{"target-b", "target-a", "target-a"},
		DimensionValues: []string{"330100", "330200"}, ClassificationVersions: []uint32{7, 3, 7},
	}
	first, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same request compiled to a different query")
	}
	for _, required := range []string{
		"FROM flow_aggregate_1m\n",
		"FROM flow_aggregate_1m AS source FINAL",
		"AND dimension_kind = '_generation'",
		"INNER JOIN latest USING (bucket, generation)",
		"AND dimension_kind = {dimension:String}",
		"AND business_direction IN ({direction_0:String}, {direction_1:String})",
		"ORDER BY rank_value DESC, query_dimension_value ASC, dimension_snapshot_id ASC,",
		"series_rank <= {top_n:UInt16}",
		"if(is_top, query_dimension_value, '_other') AS grouped_dimension_value",
		"toFloat64(sum(estimated_bytes)) * 8 / greatest(toUInt32(1), least({bucket_seconds:UInt32}",
		"toUInt8(1), toUInt64(count())",
		"is_metadata ASC, bucket ASC",
	} {
		if !strings.Contains(first.Query.Body, required) {
			t.Fatalf("query missing %q:\n%s", required, first.Query.Body)
		}
	}
	for _, forbidden := range []string{"top_series AS", "FROM top_series"} {
		if strings.Contains(first.Query.Body, forbidden) {
			t.Fatalf("legacy aggregate query still rescans grouped rows through %q:\n%s", forbidden, first.Query.Body)
		}
	}
	for _, secret := range []string{"tenant-a", "customer's", "target-a", "330100"} {
		if strings.Contains(first.Query.Body, secret) {
			t.Fatalf("request value %q was interpolated into SQL", secret)
		}
	}
	if got := queryParameter(first.Query, "business_0"); got != `'customer\'s'` {
		t.Fatalf("escaped parameter=%q", got)
	}
	if queryParameter(first.Query, "target_0") != "'target-a'" || queryParameter(first.Query, "target_1") != "'target-b'" || queryParameter(first.Query, "target_2") != "" {
		t.Fatalf("target filters were not sorted and deduplicated: %+v", first.Query.Parameters)
	}
	if first.Dimension.Kind != DimensionGeoCity || !first.Dimension.Additive || first.Metric.Name != MetricEstimatedBPS || first.EstimatedRows != 241 || first.MaxResultRows != 250_000 {
		t.Fatalf("compiled metadata=%+v", first)
	}
	if setting(first.Query, "max_result_rows") != "250000" || setting(first.Query, "read_overflow_mode") != "throw" {
		t.Fatalf("query safety settings=%+v", first.Query.Settings)
	}
	if setting(first.Query, "do_not_merge_across_partitions_select_final") != "1" {
		t.Fatalf("query does not constrain FINAL to physical partitions: %+v", first.Query.Settings)
	}
	// The heaviest reader must carry both a byte and a memory ceiling so a wide
	// query throws rather than starving the shared server.
	if setting(first.Query, "max_bytes_to_read") == "" || setting(first.Query, "max_memory_usage") == "" ||
		setting(first.Query, "max_bytes_before_external_group_by") != "1073741824" ||
		setting(first.Query, "max_bytes_before_external_sort") != "1073741824" {
		t.Fatalf("query missing byte/memory guards: %+v", first.Query.Settings)
	}
}

func TestCompileDirectionGroupsTotalRowsInOneScan(t *testing.T) {
	request := validRequest()
	request.Dimension = DimensionDirection
	request.TopN = 2
	request.IncludeOther = false
	request.Filters.Directions = []string{"in", "out"}
	request.Filters.DimensionValues = []string{"out"}
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if compiled.Dimension.Kind != DimensionDirection || queryParameter(compiled.Query, "dimension") != "'total'" {
		t.Fatalf("direction query metadata=%+v parameters=%+v", compiled.Dimension, compiled.Query.Parameters)
	}
	for _, required := range []string{
		"CAST(business_direction AS String) AS query_dimension_value",
		"AND business_direction IN ({direction_0:String}, {direction_1:String})",
		"AND business_direction IN ({dimension_value_0:String})",
		"FROM flow_aggregate_1m AS source FINAL",
	} {
		if !strings.Contains(compiled.Query.Body, required) {
			t.Fatalf("direction query missing %q:\n%s", required, compiled.Query.Body)
		}
	}
	if strings.Contains(compiled.Query.Body, "FROM flow_aggregate_1m FINAL") {
		t.Fatalf("generation marker scan still uses FINAL:\n%s", compiled.Query.Body)
	}

	request.From = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	request.To = time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	request.Bucket = BucketOneHour
	request.Interval = time.Hour
	request.StorageV2 = true
	request.ArchiveThrough = time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	compiled, err = Compile(Scope{}, request, time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"CAST(source.business_direction AS String) AS dimension_value",
		"CAST(toString(business_direction) AS String) AS dimension_value",
		"FROM flow_aggregate_1h AS source FINAL",
	} {
		if !strings.Contains(compiled.Query.Body, required) {
			t.Fatalf("Storage V2 direction query missing %q:\n%s", required, compiled.Query.Body)
		}
	}
	if strings.Contains(compiled.Query.Body, "FROM flow_aggregate_1h FINAL") {
		t.Fatalf("Storage V2 generation marker scan still uses FINAL:\n%s", compiled.Query.Body)
	}
}

func TestCompileUsesCallerOwnedExecutionTimeout(t *testing.T) {
	request := validRequest()
	request.ExecutionTimeout = 1500 * time.Millisecond
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if got := setting(compiled.Query, "max_execution_time"); got != "2" {
		t.Fatalf("configured max_execution_time=%q, want rounded-up 2 seconds", got)
	}
	request.ExecutionTimeout = 0
	compiled, err = Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if got := setting(compiled.Query, "max_execution_time"); got != "" {
		t.Fatalf("compiler injected an unconfigured max_execution_time=%q", got)
	}
}

func TestCompileMapsCanonicalIPv4DimensionFilterToClickHouseStorage(t *testing.T) {
	request := validRequest()
	request.Dimension = DimensionSourceIP
	request.Filters.DimensionValues = []string{"192.0.2.10", "2001:db8::10"}
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if queryParameter(compiled.Query, "dimension_value_0") != "'2001:db8::10'" ||
		queryParameter(compiled.Query, "dimension_value_1") != "'::ffff:192.0.2.10'" {
		t.Fatalf("IP dimension filter parameters were not normalized: %+v", compiled.Query.Parameters)
	}
}

func TestCompileRejectsUnsupportedUnsafeOrIncompleteRequests(t *testing.T) {
	now := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		field  string
		code   ErrorCode
		mutate func(*Scope, *Request)
	}{
		{"metric", "metric", ErrorUnsupported, func(_ *Scope, request *Request) { request.Metric = "sql" }},
		{"view", "view", ErrorUnsupported, func(_ *Scope, request *Request) { request.View = "supplier" }},
		{"overlapping other", "include_other", ErrorInvalid, func(_ *Scope, request *Request) { request.Dimension = DimensionAddressSet }},
		{"total top", "top_n", ErrorInvalid, func(_ *Scope, request *Request) { request.Dimension = DimensionTotal }},
		{"unaligned", "from/to", ErrorInvalid, func(_ *Scope, request *Request) { request.From = request.From.Add(time.Second) }},
		{"open bucket", "to", ErrorIncompleteRange, func(_ *Scope, request *Request) { request.To = now.Add(time.Minute) }},
		{"timezone", "timezone", ErrorInvalid, func(_ *Scope, request *Request) { request.Timezone = "Mars/Olympus" }},
		{"direction", "filters.directions", ErrorUnsupported, func(_ *Scope, request *Request) { request.Filters.Directions = []string{"sideways"} }},
		{"control", "filters.businesses", ErrorInvalid, func(_ *Scope, request *Request) { request.Filters.Businesses = []string{"bad\nvalue"} }},
		{"zero version", "filters.classification_versions", ErrorInvalid, func(_ *Scope, request *Request) { request.Filters.ClassificationVersions = []uint32{0} }},
		{"raw filter limit", "filters.dimension_values", ErrorLimitExceeded, func(_ *Scope, request *Request) {
			request.Filters.DimensionValues = make([]string, maxValuesPerFilter+1)
			for index := range request.Filters.DimensionValues {
				request.Filters.DimensionValues[index] = "duplicate"
			}
		}},
		{"result rows", "top_n", ErrorLimitExceeded, func(_ *Scope, request *Request) {
			request.From = request.To.Add(-7 * 24 * time.Hour)
			request.TopN = 100
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scope := Scope{}
			request := validRequest()
			test.mutate(&scope, &request)
			_, err := Compile(scope, request, now)
			if !IsRequestError(err, test.field, test.code) {
				t.Fatalf("error=%v, want field=%s code=%s", err, test.field, test.code)
			}
		})
	}
}

func TestCompileOneHourNormalizesTimesAndDefaultsPresentationTimezone(t *testing.T) {
	request := validRequest()
	request.From = time.Date(2026, 9, 5, 8, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	request.To = request.From.Add(24 * time.Hour)
	request.Bucket = BucketOneHour
	request.TopN = 2
	request.IncludeOther = false
	request.Timezone = ""
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if compiled.From.Location() != time.UTC || compiled.From.Hour() != 0 || compiled.To.Hour() != 0 || compiled.Timezone != "UTC" || compiled.EstimatedRows != 49 {
		t.Fatalf("compiled metadata=%+v", compiled)
	}
	if !strings.Contains(compiled.Query.Body, "FROM flow_aggregate_1h AS source FINAL") || queryParameter(compiled.Query, "bucket_seconds") != "'3600'" {
		t.Fatalf("one-hour query is wrong: %s", compiled.Query.Body)
	}
}

func TestCompileOneDayUsesDailyTierAndCanBridgeRawTail(t *testing.T) {
	request := validRequest()
	request.From = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	request.To = request.From.Add(4 * 24 * time.Hour)
	request.Bucket = BucketOneDay
	request.Interval = 2 * 24 * time.Hour
	request.StorageV2 = true
	request.ArchiveThrough = request.From.Add(3 * 24 * time.Hour)
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if compiled.SourceBucketDuration != 24*time.Hour || compiled.BucketDuration != 2*24*time.Hour || !compiled.UsesRawFacts ||
		!strings.Contains(compiled.Query.Body, "FROM flow_aggregate_1d AS source FINAL") ||
		queryParameter(compiled.Query, "source_seconds") != "'86400'" {
		t.Fatalf("daily query metadata=%+v\n%s", compiled, compiled.Query.Body)
	}
}

func TestCompileSeparatesSourceResolutionFromPresentationInterval(t *testing.T) {
	request := validRequest()
	request.To = request.From.Add(2*time.Hour + 5*time.Minute)
	request.Interval = 15 * time.Minute
	request.TopN = 2
	request.IncludeOther = false
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if compiled.SourceBucketDuration != time.Minute || compiled.BucketDuration != 15*time.Minute || compiled.EstimatedRows != 19 {
		t.Fatalf("compiled resolution metadata=%+v", compiled)
	}
	for _, required := range []string{
		"FROM flow_aggregate_1m AS source FINAL",
		"intDiv(toUnixTimestamp(bucket) - toUnixTimestamp({from:DateTime('UTC')}), {bucket_seconds:UInt32})",
		"output_bucket AS bucket",
	} {
		if !strings.Contains(compiled.Query.Body, required) {
			t.Fatalf("resampled query missing %q:\n%s", required, compiled.Query.Body)
		}
	}
	if queryParameter(compiled.Query, "bucket_seconds") != "'900'" {
		t.Fatalf("presentation interval parameter=%q", queryParameter(compiled.Query, "bucket_seconds"))
	}
}

func TestCompileStorageV2UsesDisjointArchiveAndRawRangesWithGlobalTopN(t *testing.T) {
	request := validRequest()
	request.From = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	request.To = time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	request.Bucket = BucketOneHour
	request.Interval = 6 * time.Hour
	request.StorageV2 = true
	request.ArchiveThrough = time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"bucket < {archive_through:DateTime('UTC')}",
		"event_time >= {archive_through:DateTime('UTC')}",
		"SELECT * FROM archive_rows",
		"UNION ALL\n      SELECT * FROM raw_rows",
		"FROM filtered",
		"source.estimated_bytes AS metric_value",
		"sum(estimated_bytes) AS metric_value",
		"sum(metric_value) OVER",
		"dense_rank() OVER",
		"series_rank <= {top_n:UInt16}",
		"SELECT count() FROM archive_latest",
	} {
		if !strings.Contains(compiled.Query.Body, required) {
			t.Fatalf("Storage V2 query missing %q:\n%s", required, compiled.Query.Body)
		}
	}
	for _, forbidden := range []string{"top_series AS", "FROM top_series"} {
		if strings.Contains(compiled.Query.Body, forbidden) {
			t.Fatalf("Storage V2 query still performs a second ranking scan via %q:\n%s", forbidden, compiled.Query.Body)
		}
	}
	if !compiled.UsesRawFacts || !compiled.ArchiveThrough.Equal(request.ArchiveThrough) ||
		queryParameter(compiled.Query, "source_seconds") != "'3600'" {
		t.Fatalf("compiled Storage V2 metadata=%+v", compiled)
	}
}

func TestCompileStorageV2ScopedRawQueryProjectsMetricAndUsesScopedBudget(t *testing.T) {
	request := validRequest()
	request.StorageV2 = true
	request.ArchiveThrough = request.From
	request.Dimension = DimensionTotal
	request.TopN = 1
	request.IncludeOther = false
	request.Filters.DeviceIDs = []string{"device-a"}
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"source.estimated_bytes AS metric_value",
		"sum(estimated_bytes) AS metric_value",
		"sum(metric_value) OVER",
		"toFloat64(sum(metric_value)) * 8 /",
	} {
		if !strings.Contains(compiled.Query.Body, required) {
			t.Fatalf("scoped raw query missing %q:\n%s", required, compiled.Query.Body)
		}
	}
	for _, forbidden := range []string{
		"sum(raw_bytes) AS raw_bytes",
		"sum(raw_packets) AS raw_packets",
		"sum(estimated_packets) AS estimated_packets",
	} {
		if strings.Contains(compiled.Query.Body, forbidden) {
			t.Fatalf("scoped raw query reads an unrequested metric via %q:\n%s", forbidden, compiled.Query.Body)
		}
	}
	if setting(compiled.Query, "max_rows_to_read") != "250000000" || setting(compiled.Query, "max_bytes_to_read") != "17179869184" ||
		setting(compiled.Query, "max_execution_time") != "60" {
		t.Fatalf("scoped raw guards rows=%q bytes=%q", setting(compiled.Query, "max_rows_to_read"), setting(compiled.Query, "max_bytes_to_read"))
	}
}

func TestCompileStorageV2PushesSelectiveFiltersBelowRawAggregation(t *testing.T) {
	request := validRequest()
	request.From = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	request.To = time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	request.Bucket = BucketOneHour
	request.Interval = time.Hour
	request.StorageV2 = true
	request.ArchiveThrough = request.From
	request.Filters = Filters{
		Directions: []string{"in"}, Categories: []string{"overseas"},
		Businesses: []string{"business-a"}, TargetIDs: []string{"target-a"},
		DeviceIDs: []string{"device-a"}, ExporterIDs: []string{"exporter-a"},
		DimensionValues: []string{"330100"}, DimensionSnapshotIDs: []string{"snapshot-a"},
		GeoVersions: []string{"geo-a"}, ClassificationVersions: []uint32{7},
	}
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	body := compiled.Query.Body
	rawStart := strings.Index(body, "FROM flow_records FINAL")
	rawEnd := strings.Index(body[rawStart:], "GROUP BY bucket")
	if rawStart < 0 || rawEnd < 0 {
		t.Fatalf("raw aggregation not found:\n%s", body)
	}
	rawWhere := body[rawStart : rawStart+rawEnd]
	for _, required := range []string{
		"AND toString(business_direction) IN ({direction_0:String})",
		"AND toString(category) IN ({category_0:String})",
		"AND business IN ({business_0:String})",
		"AND target_id IN ({target_0:String})",
		"AND device_id IN ({device_0:String})",
		"AND exporter_id IN ({exporter_0:String})",
		"AND classification_version IN ({classification_version_0:UInt32})",
	} {
		if !strings.Contains(rawWhere, required) {
			t.Fatalf("raw WHERE did not push %q below GROUP BY:\n%s", required, rawWhere)
		}
	}
	for _, forbidden := range []string{"dimension_value IN", "dimension_snapshot_id IN", "geo_version IN"} {
		if strings.Contains(rawWhere, forbidden) {
			t.Fatalf("derived/publication predicate %q was pushed into raw WHERE:\n%s", forbidden, rawWhere)
		}
	}
	filteredStart := strings.Index(body, "filtered AS")
	if filteredStart < 0 {
		t.Fatalf("filtered union not found:\n%s", body)
	}
	filtered := body[filteredStart:]
	for _, required := range []string{
		"AND dimension_value IN ({dimension_value_0:String})",
		"AND dimension_snapshot_id IN ({dimension_snapshot_0:String})",
		"AND geo_version IN ({geo_version_0:String})",
	} {
		if !strings.Contains(filtered, required) {
			t.Fatalf("residual filter missing %q after UNION:\n%s", required, filtered)
		}
	}
	for _, required := range []string{
		"AND source.business_direction IN ({direction_0:String})",
		"AND source.category IN ({category_0:String})",
		"AND source.business IN ({business_0:String})",
		"AND source.target_id IN ({target_0:String})",
		"AND source.device_id IN ({device_0:String})",
		"AND source.exporter_id IN ({exporter_0:String})",
		"AND source.classification_version IN ({classification_version_0:UInt32})",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("archive predicate missing %q:\n%s", required, body)
		}
	}
}

func TestCompileStorageV2MinuteRangeUsesCoveredHotPrefixAndRawTail(t *testing.T) {
	request := validRequest()
	request.StorageV2 = true
	request.ArchiveThrough = request.From
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !compiled.UsesRawFacts || queryParameter(compiled.Query, "source_seconds") != "'60'" ||
		!strings.Contains(compiled.Query.Body, "FROM flow_records FINAL") {
		t.Fatalf("compiled raw minute query=%+v", compiled)
	}
	request.ArchiveThrough = request.From.Add(time.Minute)
	compiled, err = Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !compiled.UsesRawFacts || !strings.Contains(compiled.Query.Body, "FROM flow_aggregate_1m AS source FINAL") ||
		queryParameter(compiled.Query, "archive_through") != "'2026-09-05 00:01:00'" {
		t.Fatalf("compiled hot-minute query=%+v\n%s", compiled, compiled.Query.Body)
	}
}

func TestCompileStorageV2RawEndpointUsesBoundedCandidateThenExactBuckets(t *testing.T) {
	request := validRequest()
	request.StorageV2 = true
	request.ArchiveThrough = request.From
	request.Dimension = DimensionDestinationIP
	request.Filters.DeviceIDs = []string{"device-a"}
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	body := compiled.Query.Body
	for _, required := range []string{
		"candidate_keys AS",
		"topKWeighted({candidate_n:UInt16})",
		"endpoint_value IN (SELECT endpoint_value FROM candidate_keys)",
		"estimated_bytes AS metric_value",
		"sum(metric_value) AS metric_value",
		"bucketed AS",
		"top_series AS",
		"sum(metric_value) AS rank_value",
		"if(is_top, tupleElement(candidate_key, 1), '_other')",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("bounded endpoint query missing %q:\n%s", required, body)
		}
	}
	for _, forbidden := range []string{"scored AS", "dense_rank() OVER"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("bounded endpoint query retained unbounded ranking %q:\n%s", forbidden, body)
		}
	}
	if count := strings.Count(body, "AND device_id IN ({device_0:String})"); count != 2 {
		t.Fatalf("device predicate must be applied to both bounded raw scans, count=%d:\n%s", count, body)
	}
	if queryParameter(compiled.Query, "candidate_n") != "'24'" {
		t.Fatalf("candidate_n=%q", queryParameter(compiled.Query, "candidate_n"))
	}
	if setting(compiled.Query, "max_rows_to_read") != "500000000" {
		t.Fatalf("endpoint scan budget=%q", setting(compiled.Query, "max_rows_to_read"))
	}
	if setting(compiled.Query, "max_bytes_to_read") != "34359738368" {
		t.Fatalf("endpoint byte budget=%q", setting(compiled.Query, "max_bytes_to_read"))
	}
}

func TestCompileStorageV2RawEndpointWithoutIdentityScopeKeepsExactGuardedPath(t *testing.T) {
	request := validRequest()
	request.StorageV2 = true
	request.ArchiveThrough = request.From
	request.Dimension = DimensionDestinationIP
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(compiled.Query.Body, "candidate_keys AS") || !strings.Contains(compiled.Query.Body, "scored AS") {
		t.Fatalf("unscoped endpoint query must retain the ordinary guarded path:\n%s", compiled.Query.Body)
	}
	if setting(compiled.Query, "max_rows_to_read") != "50000000" || setting(compiled.Query, "max_bytes_to_read") != "4294967296" {
		t.Fatalf("unscoped guards rows=%q bytes=%q", setting(compiled.Query, "max_rows_to_read"), setting(compiled.Query, "max_bytes_to_read"))
	}
}

func TestCompileStorageV2EndpointWithResidualFilterKeepsExactFallback(t *testing.T) {
	request := validRequest()
	request.StorageV2 = true
	request.ArchiveThrough = request.From
	request.Dimension = DimensionSourceIP
	request.Filters.DimensionValues = []string{"192.0.2.10"}
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(compiled.Query.Body, "candidate_keys AS") || !strings.Contains(compiled.Query.Body, "scored AS") {
		t.Fatalf("explicit endpoint filter must keep the exact residual-filter path:\n%s", compiled.Query.Body)
	}
	if setting(compiled.Query, "max_rows_to_read") != "50000000" {
		t.Fatalf("fallback scan budget=%q", setting(compiled.Query, "max_rows_to_read"))
	}
}

func TestCompileStorageV2AcceptsCoveredHourlySplitAndRejectsSourceUnalignedSplit(t *testing.T) {
	request := validRequest()
	request.From = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	request.To = time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	request.Bucket = BucketOneHour
	request.StorageV2 = true
	request.ArchiveThrough = request.From.Add(25 * time.Hour)
	if _, err := Compile(Scope{}, request, time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("covered hourly split error=%v", err)
	}
	request.ArchiveThrough = request.From.Add(25*time.Hour + time.Minute)
	if _, err := Compile(Scope{}, request, time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)); !IsRequestError(err, "archive_through", ErrorInvalid) {
		t.Fatalf("source-unaligned split error=%v", err)
	}
}

func TestCompileRejectsPresentationIntervalFinerThanSource(t *testing.T) {
	request := validRequest()
	request.Bucket = BucketOneHour
	request.Interval = 15 * time.Minute
	request.From = request.From.Truncate(time.Hour)
	request.To = request.To.Truncate(time.Hour)
	_, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if !IsRequestError(err, "interval", ErrorInvalid) {
		t.Fatalf("error=%v, want invalid interval", err)
	}
}

func validRequest() Request {
	return Request{
		From:   time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC),
		To:     time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC),
		Bucket: BucketOneMinute, Metric: MetricEstimatedBPS, Dimension: DimensionGeoCity,
		View: ViewCustomer, TopN: 3, IncludeOther: true, Timezone: "Asia/Shanghai",
		ExecutionTimeout: 60 * time.Second,
	}
}

func queryParameter(query ch.Query, key string) string {
	for _, parameter := range query.Parameters {
		if parameter.Key == key {
			return parameter.Value
		}
	}
	return ""
}

func setting(query ch.Query, key string) string {
	for _, current := range query.Settings {
		if current.Key == key {
			return current.Value
		}
	}
	return ""
}

func TestCompileStorageV2PushesIPDimensionValuesIntoRawScan(t *testing.T) {
	// The endpoint report's per-direction stage passes selected IP dimension
	// values; they must be pushed into the raw scan (redundantly with the
	// post-UNION residual) so the plain raw path does not group every endpoint.
	request := validRequest()
	request.From = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	request.To = time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	request.Bucket = BucketOneHour
	request.Interval = time.Hour
	request.Dimension = DimensionSourceIP
	request.StorageV2 = true
	request.ArchiveThrough = request.From
	request.Filters = Filters{
		DeviceIDs:       []string{"device-a"},
		DimensionValues: []string{"203.0.113.1", "198.51.100.2"},
	}
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	body := compiled.Query.Body
	rawStart := strings.Index(body, "FROM flow_records FINAL")
	rawEnd := strings.Index(body[rawStart:], "GROUP BY bucket")
	if rawStart < 0 || rawEnd < 0 {
		t.Fatalf("raw aggregation not found:\n%s", body)
	}
	rawWhere := body[rawStart : rawStart+rawEnd]
	if !strings.Contains(rawWhere, "AND toString(src_ip) IN ({raw_dim_value_0:String}, {raw_dim_value_1:String})") {
		t.Fatalf("IP dimension values were not pushed into the raw scan:\n%s", rawWhere)
	}
	// The post-UNION residual stays as the archive/raw contract and backstop.
	if !strings.Contains(body, "AND dimension_value IN ({dimension_value_0:String}") {
		t.Fatalf("post-UNION dimension_value residual missing:\n%s", body)
	}
}

func TestCompileStorageV2EndpointCandidateMarksApproximate(t *testing.T) {
	request := validRequest()
	request.From = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	request.To = time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	request.Bucket = BucketOneHour
	request.Interval = time.Hour
	request.Dimension = DimensionSourceIP
	request.StorageV2 = true
	request.ArchiveThrough = request.From
	request.Filters = Filters{DeviceIDs: []string{"device-a"}}
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !compiled.Approximate {
		t.Fatal("candidate-path top-N ranking must be marked approximate")
	}

	// Explicit IP values use the exact path (candidate sketch off).
	request.Filters.DimensionValues = []string{"203.0.113.1"}
	exact, err := Compile(Scope{}, request, time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if exact.Approximate {
		t.Fatal("explicit dimension values use the exact path, not the candidate sketch")
	}
}

func TestCompileMarksBoundedRemotePortArchiveApproximate(t *testing.T) {
	request := validRequest()
	request.Dimension = DimensionRemotePort
	request.StorageV2 = true
	request.ArchiveThrough = request.To
	compiled, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !compiled.Approximate {
		t.Fatal("bounded remote-port archive must be marked approximate")
	}
	if !strings.Contains(compiled.Query.Body, "('src_ip', 'dst_ip', 'remote_port')") {
		t.Fatal("remote-port _other rows are not recognized as materialized long-tail data")
	}
}
