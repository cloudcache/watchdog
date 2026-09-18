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
	if len(dimensions) != 18 {
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
		"FROM flow_aggregate_1m FINAL",
		"AND dimension_kind = '_generation'",
		"INNER JOIN latest USING (bucket, generation)",
		"AND dimension_kind = {dimension:String}",
		"AND business_direction IN ({direction_0:String}, {direction_1:String})",
		"ORDER BY rank_value DESC, dimension_value ASC, dimension_snapshot_id ASC, geo_version ASC, classification_version ASC",
		"if(is_top, dimension_value, '_other') AS grouped_dimension_value",
		"toFloat64(sum(estimated_bytes)) * 8 / greatest(toUInt32(1), least({bucket_seconds:UInt32}",
		"toUInt8(1), toUInt64(count())",
		"is_metadata ASC, bucket ASC",
	} {
		if !strings.Contains(first.Query.Body, required) {
			t.Fatalf("query missing %q:\n%s", required, first.Query.Body)
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
	// The heaviest reader must carry both a byte and a memory ceiling so a wide
	// query throws rather than starving the shared server.
	if setting(first.Query, "max_bytes_to_read") == "" || setting(first.Query, "max_memory_usage") == "" {
		t.Fatalf("query missing byte/memory guards: %+v", first.Query.Settings)
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
	if !strings.Contains(compiled.Query.Body, "FROM flow_aggregate_1h FINAL") || queryParameter(compiled.Query, "bucket_seconds") != "'3600'" {
		t.Fatalf("one-hour query is wrong: %s", compiled.Query.Body)
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
		"FROM flow_aggregate_1m FINAL",
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
		"sum(estimated_bytes) OVER",
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

func TestCompileStorageV2MinuteRangeIsRawOnlyAndRejectsMinuteArchive(t *testing.T) {
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
	if _, err := Compile(Scope{}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)); !IsRequestError(err, "archive_through", ErrorUnsupported) {
		t.Fatalf("minute archive error=%v", err)
	}
}

func TestCompileStorageV2RejectsUnalignedSplit(t *testing.T) {
	request := validRequest()
	request.From = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	request.To = time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	request.Bucket = BucketOneHour
	request.StorageV2 = true
	request.ArchiveThrough = request.From.Add(25 * time.Hour)
	if _, err := Compile(Scope{}, request, time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)); !IsRequestError(err, "archive_through", ErrorInvalid) {
		t.Fatalf("split error=%v", err)
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
