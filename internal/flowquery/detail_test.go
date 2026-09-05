// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDetailRegistryAndDefaultMaskAreFixed(t *testing.T) {
	fields := DetailFields()
	if len(fields) != 48 {
		t.Fatalf("detail field definitions=%d", len(fields))
	}
	for index := 1; index < len(fields); index++ {
		if fields[index-1] >= fields[index] {
			t.Fatalf("registry is not strictly sorted: %q then %q", fields[index-1], fields[index])
		}
	}
	compiled, err := CompileDetail(Scope{TenantID: "tenant-a"}, validDetailRequest(), detailNow())
	if err != nil {
		t.Fatal(err)
	}
	want, err := normalizeDetailFields(ViewCustomer, defaultDetailFields)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(compiled.Fields, want) {
		t.Fatalf("default fields=%v, want %v", compiled.Fields, want)
	}
}

func TestCompileDetailBuildsParameterizedFinalQuery(t *testing.T) {
	request := validDetailRequest()
	request.IP = "2001:0DB8::1"
	request.Endpoint = DetailEndpointEither
	request.Fields = []DetailField{DetailFieldRawBytes, DetailFieldSourceIP, DetailFieldRawBytes, DetailFieldEstimatedValid}
	request.Filters = DetailFilters{
		Directions: []string{"out", "in", "out"}, Businesses: []string{"customer's"},
		TargetIDs: []string{"target-b", "target-a", "target-a"},
	}
	first, err := CompileDetail(Scope{TenantID: "tenant-a"}, request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompileDetail(Scope{TenantID: "tenant-a"}, request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same detail request compiled differently")
	}
	if first.IP.String() != "2001:db8::1" || first.Limit != 2 || first.MaxResultRows != 3 {
		t.Fatalf("compiled detail metadata=%+v", first)
	}
	wantFields := []DetailField{DetailFieldEstimatedValid, DetailFieldRawBytes, DetailFieldSourceIP}
	if !reflect.DeepEqual(first.Fields, wantFields) {
		t.Fatalf("fields=%v, want %v", first.Fields, wantFields)
	}
	for _, required := range []string{
		"FROM flow_records FINAL",
		"lower(hex(record_id)) AS record_id",
		"toString(src_ip) AS _source_ip",
		"estimated_valid AS estimated_valid",
		"raw_bytes AS raw_bytes",
		"(src_ip = toIPv6({ip:String}) OR dst_ip = toIPv6({ip:String}))",
		"AND disposition = 'count'",
		"AND business_direction IN ({detail_direction_0:String}, {detail_direction_1:String})",
		"ORDER BY event_time DESC, record_id DESC",
		"LIMIT {fetch_limit:UInt16}",
	} {
		if !strings.Contains(first.Query.Body, required) {
			t.Fatalf("query missing %q:\n%s", required, first.Query.Body)
		}
	}
	for _, value := range []string{"tenant-a", "2001:db8::1", "customer's", "target-a"} {
		if strings.Contains(first.Query.Body, value) {
			t.Fatalf("request value %q was interpolated into SQL", value)
		}
	}
	if queryParameter(first.Query, "ip") != "'2001:db8::1'" || queryParameter(first.Query, "fetch_limit") != "3" ||
		queryParameter(first.Query, "detail_target_0") != "'target-a'" || queryParameter(first.Query, "detail_target_1") != "'target-b'" {
		t.Fatalf("detail parameters=%+v", first.Query.Parameters)
	}
	if setting(first.Query, "max_result_rows") != "3" || setting(first.Query, "max_rows_to_read") != "5000000" || setting(first.Query, "max_bytes_to_read") != "1073741824" {
		t.Fatalf("detail query budgets=%+v", first.Query.Settings)
	}
}

func TestCompileDetailRawViewUsesOnlyProtocolFacts(t *testing.T) {
	request := validDetailRequest()
	request.View = ViewRaw
	request.Fields = []DetailField{
		DetailFieldSourceIP, DetailFieldSourceASN, DetailFieldDestinationASN,
		DetailFieldObservationDirection, DetailFieldRawBytes, DetailFieldEstimatedValid,
	}
	request.Filters = DetailFilters{TargetIDs: []string{"target-a"}, ExporterIDs: []string{"exporter-a"}}
	compiled, err := CompileDetail(Scope{TenantID: "tenant-a"}, request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	if compiled.View != ViewRaw {
		t.Fatalf("compiled view=%q", compiled.View)
	}
	for _, required := range []string{
		"toUInt64(source_asn) AS source_asn", "toUInt64(destination_asn) AS destination_asn",
		"toString(observation_direction) AS observation_direction", "AND target_id IN ({detail_target_0:String})",
	} {
		if !strings.Contains(compiled.Query.Body, required) {
			t.Fatalf("raw query missing %q:\n%s", required, compiled.Query.Body)
		}
	}
	for _, forbidden := range []string{"disposition = 'count'", "supplier_", "customer_", "business_direction IN", "category IN", "business IN"} {
		if strings.Contains(compiled.Query.Body, forbidden) {
			t.Fatalf("raw query contains customer/supplier expression %q:\n%s", forbidden, compiled.Query.Body)
		}
	}

	request.Fields = nil
	defaults, err := CompileDetail(Scope{TenantID: "tenant-a"}, request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	want, err := normalizeDetailFields(ViewRaw, defaultRawDetailFields)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(defaults.Fields, want) {
		t.Fatalf("raw default fields=%v want=%v", defaults.Fields, want)
	}
}

func TestCompileDetailSupplierViewMapsBaselineAndChecksFullScope(t *testing.T) {
	request := validDetailRequest()
	request.View = ViewSupplier
	request.Fields = []DetailField{
		DetailFieldCategory, DetailFieldRemoteASN, DetailFieldRemoteASNSource, DetailFieldRemoteCountry,
		DetailFieldGeoVersion, DetailFieldRemoteISPID, DetailFieldRemoteGeoCityID, DetailFieldDimensionSnapshotID,
	}
	request.Filters = DetailFilters{Directions: []string{"out"}, Categories: []string{"overseas"}, TargetIDs: []string{"target-a"}}
	cursor, err := EncodeDetailCursor(request.From.Add(30*time.Minute), strings.Repeat("01", 32))
	if err != nil {
		t.Fatal(err)
	}
	request.Cursor = cursor
	compiled, err := CompileDetail(Scope{TenantID: "tenant-a"}, request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	if compiled.View != ViewSupplier {
		t.Fatalf("compiled view=%q", compiled.View)
	}
	for _, required := range []string{
		"toString(supplier_category) AS category", "toUInt64(supplier_remote_asn) AS remote_asn",
		"toString(supplier_remote_asn_source) AS remote_asn_source", "toString(supplier_remote_country) AS remote_country",
		"supplier_geo_version AS geo_version", "toUInt64(supplier_remote_isp_id) AS remote_isp_id",
		"supplier_remote_geo_city_id AS remote_geo_city_id", "min(fact_schema) OVER () AS _minimum_fact_schema",
		"row_number() OVER (ORDER BY event_time DESC, record_id DESC) AS _scope_row",
		"AND supplier_category IN ({detail_category_0:String})", "WHERE _scope_match OR _scope_row = 1",
	} {
		if !strings.Contains(compiled.Query.Body, required) {
			t.Fatalf("supplier query missing %q:\n%s", required, compiled.Query.Body)
		}
	}
	cursorPosition := strings.Index(compiled.Query.Body, "AS _scope_match")
	windowPosition := strings.Index(compiled.Query.Body, "min(fact_schema) OVER ()")
	outerWherePosition := strings.Index(compiled.Query.Body, "WHERE _scope_match OR _scope_row = 1")
	if cursorPosition < 0 || windowPosition < cursorPosition || outerWherePosition < windowPosition {
		t.Fatalf("supplier completeness is not evaluated before cursor filtering:\n%s", compiled.Query.Body)
	}
	for _, forbidden := range []string{"toString(category) AS category", "toUInt64(remote_asn) AS remote_asn", "toString(remote_country) AS remote_country", " business AS business", "prefix_id"} {
		if strings.Contains(compiled.Query.Body, forbidden) {
			t.Fatalf("supplier query contains customer expression %q:\n%s", forbidden, compiled.Query.Body)
		}
	}

	request.Cursor, request.Fields, request.Filters = "", nil, DetailFilters{}
	defaults, err := CompileDetail(Scope{TenantID: "tenant-a"}, request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	want, err := normalizeDetailFields(ViewSupplier, defaultSupplierDetailFields)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(defaults.Fields, want) {
		t.Fatalf("supplier default fields=%v want=%v", defaults.Fields, want)
	}
}

func TestCompileDetailUsesEndpointSpecificPredicatesForIPv4AndIPv6(t *testing.T) {
	tests := []struct {
		name, ip  string
		endpoint  DetailEndpoint
		predicate string
	}{
		{"ipv4 source", "192.0.2.10", DetailEndpointSource, "AND src_ip = toIPv6({ip:String})"},
		{"ipv4 destination", "192.0.2.10", DetailEndpointDestination, "AND dst_ip = toIPv6({ip:String})"},
		{"ipv6 either", "2001:db8::10", DetailEndpointEither, "AND (src_ip = toIPv6({ip:String}) OR dst_ip = toIPv6({ip:String}))"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validDetailRequest()
			request.IP, request.Endpoint = test.ip, test.endpoint
			compiled, err := CompileDetail(Scope{TenantID: "tenant-a"}, request, detailNow())
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(compiled.Query.Body, test.predicate) {
				t.Fatalf("query missing endpoint predicate:\n%s", compiled.Query.Body)
			}
		})
	}
}

func TestDetailCursorGoldenAndBoundaryCompilation(t *testing.T) {
	eventTime := time.Date(2026, 9, 5, 10, 11, 12, 345_000_000, time.UTC)
	recordID := strings.Repeat("ab", 32)
	cursor, err := EncodeDetailCursor(eventTime, recordID)
	if err != nil {
		t.Fatal(err)
	}
	const golden = "v1.AAABoHEM_1mrq6urq6urq6urq6urq6urq6urq6urq6urq6urq6urqw"
	if cursor != golden {
		t.Fatalf("cursor=%q, want %q", cursor, golden)
	}
	request := validDetailRequest()
	request.Cursor = cursor
	compiled, err := CompileDetail(Scope{TenantID: "tenant-a"}, request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(compiled.Query.Body, "record_id < unhex({cursor_record_id:String})") ||
		queryParameter(compiled.Query, "cursor_time") != "'2026-09-05 10:11:12.345'" || queryParameter(compiled.Query, "cursor_record_id") != "'"+recordID+"'" {
		t.Fatalf("cursor boundary query=%s parameters=%+v", compiled.Query.Body, compiled.Query.Parameters)
	}
	request.Fields = []DetailField{DetailFieldCategory, DetailFieldRemoteASN}
	changedMask, err := CompileDetail(Scope{TenantID: "tenant-a"}, request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	if queryParameter(changedMask.Query, "cursor_time") != queryParameter(compiled.Query, "cursor_time") ||
		queryParameter(changedMask.Query, "cursor_record_id") != queryParameter(compiled.Query, "cursor_record_id") {
		t.Fatal("a v1 cursor changed meaning when the field mask changed")
	}
	decoded, err := decodeDetailCursor(cursor)
	if err != nil || !decoded.eventTime.Equal(eventTime) || string(decoded.recordID[:]) != string([]byte(strings.Repeat("\xab", 32))) {
		t.Fatalf("decoded=%+v error=%v", decoded, err)
	}
}

func TestCompileDetailRejectsUnsafeUnsupportedOrUnboundedRequests(t *testing.T) {
	tests := []struct {
		name  string
		field string
		code  ErrorCode
		apply func(*Scope, *DetailRequest)
	}{
		{"tenant", "scope.tenant_id", ErrorInvalid, func(scope *Scope, _ *DetailRequest) { scope.TenantID = "tenant'" }},
		{"view", "view", ErrorUnsupported, func(_ *Scope, request *DetailRequest) { request.View = "invented" }},
		{"raw customer field", "fields", ErrorUnsupported, func(_ *Scope, request *DetailRequest) {
			request.View = ViewRaw
			request.Fields = []DetailField{DetailFieldRemoteCountry}
		}},
		{"raw customer filter", "filters.categories", ErrorUnsupported, func(_ *Scope, request *DetailRequest) {
			request.View = ViewRaw
			request.Filters.Categories = []string{"overseas"}
		}},
		{"supplier customer field", "fields", ErrorUnsupported, func(_ *Scope, request *DetailRequest) {
			request.View = ViewSupplier
			request.Fields = []DetailField{DetailFieldBusiness}
		}},
		{"supplier customer filter", "filters.businesses", ErrorUnsupported, func(_ *Scope, request *DetailRequest) {
			request.View = ViewSupplier
			request.Filters.Businesses = []string{"customer-a"}
		}},
		{"ip", "ip", ErrorInvalid, func(_ *Scope, request *DetailRequest) { request.IP = "not-an-ip" }},
		{"zone", "ip", ErrorInvalid, func(_ *Scope, request *DetailRequest) { request.IP = "fe80::1%en0" }},
		{"endpoint", "endpoint", ErrorUnsupported, func(_ *Scope, request *DetailRequest) { request.Endpoint = "remote" }},
		{"future", "to", ErrorIncompleteRange, func(_ *Scope, request *DetailRequest) { request.To = detailNow().Add(time.Millisecond) }},
		{"long range", "from/to", ErrorInvalid, func(_ *Scope, request *DetailRequest) { request.From = request.To.Add(-25 * time.Hour) }},
		{"precision", "from/to", ErrorInvalid, func(_ *Scope, request *DetailRequest) { request.From = request.From.Add(time.Microsecond) }},
		{"limit", "limit", ErrorLimitExceeded, func(_ *Scope, request *DetailRequest) { request.Limit = 501 }},
		{"field injection", "fields", ErrorUnsupported, func(_ *Scope, request *DetailRequest) { request.Fields = []DetailField{"raw_bytes, sleep(1)"} }},
		{"filter", "filters.directions", ErrorUnsupported, func(_ *Scope, request *DetailRequest) { request.Filters.Directions = []string{"sideways"} }},
		{"filter limit", "filters.target_ids", ErrorLimitExceeded, func(_ *Scope, request *DetailRequest) {
			request.Filters.TargetIDs = make([]string, maxDetailValuesPerFilter+1)
		}},
		{"cursor version", "cursor", ErrorInvalid, func(_ *Scope, request *DetailRequest) { request.Cursor = "v2.bad" }},
		{"cursor payload", "cursor", ErrorInvalid, func(_ *Scope, request *DetailRequest) { request.Cursor = "v1.bad=" }},
		{"cursor range", "cursor", ErrorInvalid, func(_ *Scope, request *DetailRequest) {
			cursor, err := EncodeDetailCursor(request.From.Add(-time.Millisecond), strings.Repeat("01", 32))
			if err != nil {
				panic(err)
			}
			request.Cursor = cursor
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scope := Scope{TenantID: "tenant-a"}
			request := validDetailRequest()
			test.apply(&scope, &request)
			_, err := CompileDetail(scope, request, detailNow())
			if !IsRequestError(err, test.field, test.code) {
				t.Fatalf("error=%v, want field=%s code=%s", err, test.field, test.code)
			}
		})
	}
}

func TestEncodeDetailCursorRejectsInvalidComponents(t *testing.T) {
	validTime := time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name string
		time time.Time
		id   string
	}{
		{"sub-millisecond", validTime.Add(time.Microsecond), strings.Repeat("01", 32)},
		{"pre-epoch", time.UnixMilli(-1), strings.Repeat("01", 32)},
		{"short id", validTime, "01"},
		{"non-hex", validTime, strings.Repeat("zz", 32)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := EncodeDetailCursor(test.time, test.id); !IsRequestError(err, "cursor", ErrorInvalid) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func validDetailRequest() DetailRequest {
	return DetailRequest{
		IP: "192.0.2.10", Endpoint: DetailEndpointEither,
		From: time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 9, 5, 11, 0, 0, 0, time.UTC),
		View: ViewCustomer, Limit: 2,
	}
}

func detailNow() time.Time {
	return time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
}
