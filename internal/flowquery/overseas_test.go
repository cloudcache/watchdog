// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCompileOverseasBuildsDeterministicLatestGenerationQuery(t *testing.T) {
	request := validOverseasRequest()
	request.Filters = OverseasFilters{
		Directions: []string{"out", "in", "out"}, Businesses: []string{"customer's"},
		TargetIDs: []string{"target-b", "target-a", "target-a"},
	}
	first, err := CompileOverseas(Scope{}, request, overseasNow())
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompileOverseas(Scope{}, request, overseasNow())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same overseas request compiled differently")
	}
	for _, required := range []string{
		"FROM flow_aggregate_1m\n",
		"FROM flow_aggregate_1m AS source FINAL",
		"AND dimension_kind = '_generation'",
		"INNER JOIN latest USING (bucket, generation)",
		"dimension_kind IN ('src_ip', 'dst_ip', {geo_dimension:String})",
		"category = 'overseas'",
		"business_direction = 'in' AND dimension_kind = 'src_ip'",
		"business_direction = 'out' AND dimension_kind = 'dst_ip'",
		"startsWith(dimension_value, '::ffff:')",
		"tuple('combined', ip_family)",
		"uniqExact(endpoint_ip) AS observed_remote_ips",
		"dimension_value = '_unassigned' OR category = 'overseas'",
		"if(dimension_value = '_unassigned', 'unknown_geo', 'overseas')",
		"toFloat64(metric_total) * 8 / {bucket_seconds:UInt32}",
		"local_side.local_metric_total = remote.metric_total",
		"toUInt64(count())",
	} {
		if !strings.Contains(first.Query.Body, required) {
			t.Fatalf("overseas query missing %q:\n%s", required, first.Query.Body)
		}
	}
	if strings.Contains(first.Query.Body, "FROM flow_aggregate_1m FINAL") {
		t.Fatalf("overseas generation marker scan still uses FINAL:\n%s", first.Query.Body)
	}
	for _, value := range []string{"tenant-a", "customer's", "target-a", "target-b"} {
		if strings.Contains(first.Query.Body, value) {
			t.Fatalf("request value %q was interpolated into SQL", value)
		}
	}
	if strings.Contains(first.Query.Body, "{{") {
		t.Fatalf("compiled overseas query still contains a template token:\n%s", first.Query.Body)
	}
	if queryParameter(first.Query, "geo_dimension") != "'geo.country'" ||
		queryParameter(first.Query, "detail_direction_0") != "'in'" || queryParameter(first.Query, "detail_direction_1") != "'out'" ||
		queryParameter(first.Query, "detail_target_0") != "'target-a'" || queryParameter(first.Query, "detail_target_1") != "'target-b'" ||
		queryParameter(first.Query, "detail_business_0") != `'customer\'s'` {
		t.Fatalf("overseas parameters=%+v", first.Query.Parameters)
	}
	if first.GeoLevel != OverseasGeoCountry || first.Metric.Name != MetricEstimatedBPS || first.TopN != 5 ||
		!first.IncludeOther || first.EstimatedRows != 1_981 || first.MaxResultRows != maxResultRows {
		t.Fatalf("compiled overseas metadata=%+v", first)
	}
	if setting(first.Query, "max_rows_to_read") != "50000000" || setting(first.Query, "max_bytes_to_read") != "4294967296" ||
		setting(first.Query, "max_memory_usage") != "4294967296" || setting(first.Query, "max_result_rows") != "250000" ||
		setting(first.Query, "max_bytes_before_external_group_by") != "1073741824" ||
		setting(first.Query, "max_bytes_before_external_sort") != "1073741824" ||
		setting(first.Query, "join_use_nulls") != "0" ||
		setting(first.Query, "do_not_merge_across_partitions_select_final") != "1" {
		t.Fatalf("overseas query budgets=%+v", first.Query.Settings)
	}
}

func TestCompileOverseasSupportsRegionHourlyAndNonRateMetric(t *testing.T) {
	request := validOverseasRequest()
	request.From = time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	request.To = request.From.Add(24 * time.Hour)
	request.Bucket = BucketOneHour
	request.Metric = MetricRawBytes
	request.GeoLevel = OverseasGeoRegion
	request.TopN = 2
	request.IncludeOther = false
	compiled, err := CompileOverseas(Scope{}, request, overseasNow())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(compiled.Query.Body, "FROM flow_aggregate_1h AS source FINAL") ||
		!strings.Contains(compiled.Query.Body, "toFloat64(metric_total) AS value") ||
		queryParameter(compiled.Query, "geo_dimension") != "'geo.region'" || queryParameter(compiled.Query, "include_other") != "'0'" ||
		queryParameter(compiled.Query, "bucket_seconds") != "'3600'" {
		t.Fatalf("hourly region query is wrong:\n%s\n%+v", compiled.Query.Body, compiled.Query.Parameters)
	}
}

func TestCompileOverseasStorageV2UnionsArchiveAndRawBeforeAnalysis(t *testing.T) {
	request := validOverseasRequest()
	request.From = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	request.To = time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	request.Bucket = BucketOneHour
	request.StorageV2 = true
	request.ArchiveThrough = time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	request.Filters = OverseasFilters{
		Directions: []string{"out"},
		DeviceIDs:  []string{"device-a"},
	}
	compiled, err := CompileOverseas(Scope{}, request, overseasNow())
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"FROM flow_records FINAL",
		"event_time >= {archive_through:DateTime('UTC')}",
		"raw_base AS",
		"SELECT * FROM archive_selected",
		"SELECT * FROM raw_selected",
		"FROM coverage",
	} {
		if !strings.Contains(compiled.Query.Body, required) {
			t.Fatalf("Storage V2 overseas query missing %q:\n%s", required, compiled.Query.Body)
		}
	}
	if !compiled.UsesRawFacts || !compiled.ArchiveThrough.Equal(request.ArchiveThrough) ||
		queryParameter(compiled.Query, "source_seconds") != "'3600'" {
		t.Fatalf("compiled=%+v", compiled)
	}
	if compiled.PostProcessGeoTopN || setting(compiled.Query, "max_rows_to_read") != "500000000" ||
		setting(compiled.Query, "max_bytes_to_read") != "34359738368" || setting(compiled.Query, "max_execution_time") != "60" {
		t.Fatalf("mixed budgets rows=%q bytes=%q postprocess=%v", setting(compiled.Query, "max_rows_to_read"), setting(compiled.Query, "max_bytes_to_read"), compiled.PostProcessGeoTopN)
	}
	archiveEnd := strings.Index(compiled.Query.Body, "raw_base AS")
	rawBaseEnd := strings.Index(compiled.Query.Body, "raw_selected AS")
	rawEnd := -1
	if rawBaseEnd >= 0 {
		remaining := compiled.Query.Body[rawBaseEnd+len("raw_selected AS"):]
		if offset := strings.Index(remaining, "\n  selected AS"); offset >= 0 {
			rawEnd = rawBaseEnd + len("raw_selected AS") + offset
		}
	}
	if archiveEnd < 0 || rawBaseEnd < 0 || rawEnd < 0 || rawBaseEnd <= archiveEnd || rawEnd <= rawBaseEnd {
		t.Fatalf("Storage V2 overseas query has malformed source CTEs:\n%s", compiled.Query.Body)
	}
	archiveSQL := compiled.Query.Body[:archiveEnd]
	rawBaseSQL := compiled.Query.Body[archiveEnd:rawBaseEnd]
	rawExpandedSQL := compiled.Query.Body[rawBaseEnd:rawEnd]
	for _, required := range []string{
		"AND source.business_direction IN ({detail_direction_0:String})",
		"AND source.device_id IN ({detail_device_0:String})",
	} {
		if !strings.Contains(archiveSQL, required) {
			t.Fatalf("archive source is missing pushed predicate %q:\n%s", required, archiveSQL)
		}
	}
	for _, required := range []string{
		"AND business_direction IN ({detail_direction_0:String})",
		"AND device_id IN ({detail_device_0:String})",
	} {
		if !strings.Contains(rawBaseSQL, required) {
			t.Fatalf("raw base is missing pushed predicate %q:\n%s", required, rawBaseSQL)
		}
	}
	if !strings.Contains(rawExpandedSQL, "FROM raw_base") || strings.Contains(rawExpandedSQL, "FROM flow_records") {
		t.Fatalf("raw expansion must consume the prefiltered base CTE:\n%s", rawExpandedSQL)
	}
}

func TestCompileOverseasUsesLargerReadBudgetOnlyForIdentityScopedRawFacts(t *testing.T) {
	request := validOverseasRequest()
	request.StorageV2 = true
	request.ArchiveThrough = request.From
	unscoped, err := CompileOverseas(Scope{}, request, overseasNow())
	if err != nil {
		t.Fatal(err)
	}
	request.Filters.DeviceIDs = []string{"device-a"}
	scoped, err := CompileOverseas(Scope{}, request, overseasNow())
	if err != nil {
		t.Fatal(err)
	}
	if setting(unscoped.Query, "max_rows_to_read") != "50000000" || setting(unscoped.Query, "max_bytes_to_read") != "4294967296" {
		t.Fatalf("unscoped budgets rows=%q bytes=%q", setting(unscoped.Query, "max_rows_to_read"), setting(unscoped.Query, "max_bytes_to_read"))
	}
	if setting(scoped.Query, "max_rows_to_read") != "500000000" || setting(scoped.Query, "max_bytes_to_read") != "34359738368" ||
		setting(scoped.Query, "max_execution_time") != "60" {
		t.Fatalf("scoped budgets rows=%q bytes=%q", setting(scoped.Query, "max_rows_to_read"), setting(scoped.Query, "max_bytes_to_read"))
	}
	if !scoped.PostProcessGeoTopN || strings.Count(scoped.Query.Body, "FROM flow_records FINAL") != 1 ||
		!strings.Contains(scoped.Query.Body, "ARRAY JOIN arrayFilter") || strings.Contains(scoped.Query.Body, "remote_endpoints AS") {
		t.Fatalf("all-raw overseas query is not the single-scan plan:\n%s", scoped.Query.Body)
	}

}

func TestCompileOverseasRejectsUnsafeUnsupportedOrUnboundedRequests(t *testing.T) {
	tests := []struct {
		name  string
		field string
		code  ErrorCode
		apply func(*Scope, *OverseasRequest)
	}{
		{"missing view", "view", ErrorRequired, func(_ *Scope, request *OverseasRequest) { request.View = "" }},
		{"view", "view", ErrorUnsupported, func(_ *Scope, request *OverseasRequest) { request.View = "supplier" }},
		{"metric", "metric", ErrorUnsupported, func(_ *Scope, request *OverseasRequest) { request.Metric = "sql" }},
		{"geo level", "geo_level", ErrorUnsupported, func(_ *Scope, request *OverseasRequest) { request.GeoLevel = "city" }},
		{"top zero", "top_n", ErrorLimitExceeded, func(_ *Scope, request *OverseasRequest) { request.TopN = 0 }},
		{"top high", "top_n", ErrorLimitExceeded, func(_ *Scope, request *OverseasRequest) { request.TopN = 101 }},
		{"bucket", "bucket", ErrorUnsupported, func(_ *Scope, request *OverseasRequest) { request.Bucket = "5m" }},
		{"missing time", "from/to", ErrorRequired, func(_ *Scope, request *OverseasRequest) { request.From = time.Time{} }},
		{"unaligned", "from/to", ErrorInvalid, func(_ *Scope, request *OverseasRequest) { request.From = request.From.Add(time.Second) }},
		{"open bucket", "to", ErrorIncompleteRange, func(_ *Scope, request *OverseasRequest) { request.To = overseasNow().Add(time.Minute) }},
		{"direction", "filters.directions", ErrorUnsupported, func(_ *Scope, request *OverseasRequest) { request.Filters.Directions = []string{"internal"} }},
		{"control", "filters.businesses", ErrorInvalid, func(_ *Scope, request *OverseasRequest) { request.Filters.Businesses = []string{"bad\nvalue"} }},
		{"filter limit", "filters.target_ids", ErrorLimitExceeded, func(_ *Scope, request *OverseasRequest) {
			request.Filters.TargetIDs = make([]string, maxDetailValuesPerFilter+1)
		}},
		{"result rows", "top_n", ErrorLimitExceeded, func(_ *Scope, request *OverseasRequest) {
			request.From = request.To.Add(-7 * 24 * time.Hour)
			request.TopN = 100
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scope := Scope{}
			request := validOverseasRequest()
			test.apply(&scope, &request)
			_, err := CompileOverseas(scope, request, overseasNow())
			if !IsRequestError(err, test.field, test.code) {
				t.Fatalf("error=%v, want field=%s code=%s", err, test.field, test.code)
			}
		})
	}
}

func validOverseasRequest() OverseasRequest {
	return OverseasRequest{
		From:   time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC),
		To:     time.Date(2026, 9, 5, 11, 0, 0, 0, time.UTC),
		Bucket: BucketOneMinute, Metric: MetricEstimatedBPS, GeoLevel: OverseasGeoCountry,
		View: ViewCustomer, TopN: 5, IncludeOther: true, ExecutionTimeout: 60 * time.Second,
	}
}

func overseasNow() time.Time {
	return time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
}
