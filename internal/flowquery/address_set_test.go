// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

func TestCompileAddressSetBuildsDeterministicDeduplicatedBaseQuery(t *testing.T) {
	request := validAddressSetRequest()
	request.Sets = flowdimension.AddressSetFilter{
		IncludeAny: []string{"set-b", "set-a", "set-a"}, IncludeAll: []string{"set-c"}, ExcludeAny: []string{"set-d"},
	}
	request.Filters = DetailFilters{Directions: []string{"out", "in"}, Businesses: []string{"customer's"}}
	first, err := CompileAddressSet(Scope{TenantID: "tenant-a"}, request, addressSetNow())
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompileAddressSet(Scope{TenantID: "tenant-a"}, request, addressSetNow())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same address-set request compiled differently")
	}
	if !reflect.DeepEqual(first.Sets.IncludeAny, []string{"set-a", "set-b"}) || first.Metric.Name != MetricEstimatedBPS || first.Endpoint != AddressSetEndpointEither {
		t.Fatalf("compiled metadata=%+v", first)
	}
	for _, required := range []string{
		"FROM flow_records FINAL",
		"CAST(dimension_snapshot_id AS String) AS dimension_snapshot_id",
		"CAST(geo_version AS String) AS geo_version",
		"toDateTime(toStartOfMinute(event_time), 'UTC') AS bucket",
		"toFloat64(sum(estimated_bytes)) * 8 / {bucket_seconds:UInt32}",
		"hasAny(arrayDistinct(arrayConcat(local_address_set_ids, remote_address_set_ids)), [{address_include_any_0:String}, {address_include_any_1:String}])",
		"hasAll(arrayDistinct(arrayConcat(local_address_set_ids, remote_address_set_ids)), [{address_include_all_0:String}])",
		"NOT hasAny(arrayDistinct(arrayConcat(local_address_set_ids, remote_address_set_ids)), [{address_exclude_any_0:String}])",
		"AND disposition = 'count'",
		"GROUP BY bucket, dimension_snapshot_id, geo_version, classification_version",
	} {
		if !strings.Contains(first.Query.Body, required) {
			t.Fatalf("query missing %q:\n%s", required, first.Query.Body)
		}
	}
	if strings.Contains(first.Query.Body, "ARRAY JOIN") {
		t.Fatalf("address-set membership was expanded and can double count overlapping facts:\n%s", first.Query.Body)
	}
	for _, requestValue := range []string{"tenant-a", "set-a", "set-b", "set-c", "set-d", "customer's"} {
		if strings.Contains(first.Query.Body, requestValue) {
			t.Fatalf("request value %q was interpolated into SQL", requestValue)
		}
	}
	if queryParameter(first.Query, "address_include_any_0") != "'set-a'" || queryParameter(first.Query, "address_include_any_1") != "'set-b'" ||
		queryParameter(first.Query, "address_include_all_0") != "'set-c'" || queryParameter(first.Query, "address_exclude_any_0") != "'set-d'" {
		t.Fatalf("set parameters=%+v", first.Query.Parameters)
	}
	if setting(first.Query, "max_rows_to_read") != "5000000" || setting(first.Query, "max_bytes_to_read") != "1073741824" || setting(first.Query, "max_result_rows") != "10000" || setting(first.Query, "max_memory_usage") == "" {
		t.Fatalf("address-set query budgets=%+v", first.Query.Settings)
	}
	request.Sets.IncludeAny[0] = "mutated"
	if first.Sets.IncludeAny[0] != "set-a" {
		t.Fatal("compiled address-set selection aliases request memory")
	}
}

func TestCompileAddressSetEndpointAndMetricVariants(t *testing.T) {
	tests := []struct {
		name       string
		endpoint   AddressSetEndpoint
		metric     Metric
		bucket     Bucket
		membership string
		value      string
	}{
		{"local raw", AddressSetEndpointLocal, MetricRawBytes, BucketOneMinute, "hasAny(local_address_set_ids", "toFloat64(sum(raw_bytes))"},
		{"remote pps", AddressSetEndpointRemote, MetricEstimatedPPS, BucketOneMinute, "hasAny(remote_address_set_ids", "toFloat64(sum(estimated_packets)) * 1 / {bucket_seconds:UInt32}"},
		{"either records", AddressSetEndpointEither, MetricReceivedRecords, BucketOneHour, "hasAny(arrayDistinct(arrayConcat(local_address_set_ids, remote_address_set_ids))", "toFloat64(count())"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validAddressSetRequest()
			request.Endpoint, request.Metric, request.Bucket = test.endpoint, test.metric, test.bucket
			if test.bucket == BucketOneHour {
				request.From = time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)
				request.To = request.From.Add(time.Hour)
			}
			compiled, err := CompileAddressSet(Scope{TenantID: "tenant-a"}, request, addressSetNow())
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(compiled.Query.Body, test.membership) || !strings.Contains(compiled.Query.Body, test.value) {
				t.Fatalf("variant query is wrong:\n%s", compiled.Query.Body)
			}
		})
	}
}

func TestCompileAddressSetRejectsUnsafeOrAsynchronousRequests(t *testing.T) {
	tests := []struct {
		name  string
		field string
		code  ErrorCode
		apply func(*Scope, *AddressSetRequest)
	}{
		{"tenant", "scope.tenant_id", ErrorInvalid, func(scope *Scope, _ *AddressSetRequest) { scope.TenantID = "bad tenant" }},
		{"view", "view", ErrorUnsupported, func(_ *Scope, request *AddressSetRequest) { request.View = "supplier" }},
		{"metric", "metric", ErrorUnsupported, func(_ *Scope, request *AddressSetRequest) { request.Metric = "sql" }},
		{"bucket", "bucket", ErrorUnsupported, func(_ *Scope, request *AddressSetRequest) { request.Bucket = "5m" }},
		{"unaligned", "from/to", ErrorInvalid, func(_ *Scope, request *AddressSetRequest) { request.From = request.From.Add(time.Second) }},
		{"async range", "from/to", ErrorLimitExceeded, func(_ *Scope, request *AddressSetRequest) { request.From = request.To.Add(-2 * time.Hour) }},
		{"open bucket", "to", ErrorIncompleteRange, func(_ *Scope, request *AddressSetRequest) {
			request.From, request.To = addressSetNow().Add(-59*time.Minute), addressSetNow().Add(time.Minute)
		}},
		{"endpoint", "endpoint", ErrorUnsupported, func(_ *Scope, request *AddressSetRequest) { request.Endpoint = "source" }},
		{"empty selection", "address_set_filter", ErrorRequired, func(_ *Scope, request *AddressSetRequest) { request.Sets = flowdimension.AddressSetFilter{} }},
		{"exclude only", "address_set_filter", ErrorRequired, func(_ *Scope, request *AddressSetRequest) {
			request.Sets = flowdimension.AddressSetFilter{ExcludeAny: []string{"set-a"}}
		}},
		{"invalid id", "address_set_filter", ErrorInvalid, func(_ *Scope, request *AddressSetRequest) { request.Sets.IncludeAny = []string{"bad id"} }},
		{"set limit", "address_set_filter", ErrorLimitExceeded, func(_ *Scope, request *AddressSetRequest) {
			request.Sets.IncludeAny = make([]string, 257)
			for index := range request.Sets.IncludeAny {
				request.Sets.IncludeAny[index] = "set-a"
			}
		}},
		{"common filter", "filters.categories", ErrorUnsupported, func(_ *Scope, request *AddressSetRequest) { request.Filters.Categories = []string{"invalid"} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scope := Scope{TenantID: "tenant-a"}
			request := validAddressSetRequest()
			test.apply(&scope, &request)
			_, err := CompileAddressSet(scope, request, addressSetNow())
			if !IsRequestError(err, test.field, test.code) {
				t.Fatalf("error=%v, want field=%s code=%s", err, test.field, test.code)
			}
		})
	}
}

func validAddressSetRequest() AddressSetRequest {
	return AddressSetRequest{
		From:   time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC),
		To:     time.Date(2026, 9, 5, 11, 0, 0, 0, time.UTC),
		Bucket: BucketOneMinute, Metric: MetricEstimatedBPS, View: ViewCustomer,
		Endpoint: AddressSetEndpointEither,
		Sets:     flowdimension.AddressSetFilter{IncludeAny: []string{"set-a"}},
	}
}

func addressSetNow() time.Time {
	return time.Date(2026, 9, 5, 11, 30, 0, 0, time.UTC)
}
