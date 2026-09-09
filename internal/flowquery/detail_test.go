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
	compiled, err := CompileDetail(fullScope("tenant-a"), validDetailRequest(), detailNow())
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

func TestDetailCapabilitiesShareValidationRegistryAndReturnCopies(t *testing.T) {
	capabilities := DetailCapabilities()
	if len(capabilities) != 3 || capabilities[0].View != ViewRaw || capabilities[1].View != ViewSupplier || capabilities[2].View != ViewCustomer {
		t.Fatalf("detail capabilities=%+v", capabilities)
	}
	for _, capability := range capabilities {
		if len(capability.Fields) == 0 || len(capability.DefaultFields) == 0 || len(capability.Filters) == 0 {
			t.Fatalf("incomplete capability=%+v", capability)
		}
		allowedFields := make(map[DetailField]struct{}, len(capability.Fields))
		for index, field := range capability.Fields {
			if index > 0 && capability.Fields[index-1] >= field {
				t.Fatalf("%s fields are not sorted: %v", capability.View, capability.Fields)
			}
			allowedFields[field] = struct{}{}
			request := validDetailRequest()
			request.View, request.Fields = capability.View, []DetailField{field}
			if _, err := CompileDetail(fullScope("tenant-a"), request, detailNow()); err != nil {
				t.Fatalf("advertised %s field %s was rejected: %v", capability.View, field, err)
			}
		}
		for _, field := range capability.DefaultFields {
			if _, ok := allowedFields[field]; !ok {
				t.Fatalf("%s default field %s is not allowed", capability.View, field)
			}
		}
		for _, filter := range capability.Filters {
			request := validDetailRequest()
			request.View = capability.View
			setDetailFilter(&request.Filters, filter)
			if _, err := CompileDetail(fullScope("tenant-a"), request, detailNow()); err != nil {
				t.Fatalf("advertised %s filter %s was rejected: %v", capability.View, filter, err)
			}
		}
	}

	capabilities[0].Fields[0] = "mutated"
	capabilities[0].DefaultFields[0] = "mutated"
	capabilities[0].Filters[0] = "mutated"
	again, err := DetailCapability(ViewRaw)
	if err != nil {
		t.Fatal(err)
	}
	if again.Fields[0] == "mutated" || again.DefaultFields[0] == "mutated" || again.Filters[0] == "mutated" {
		t.Fatal("caller mutation changed the detail capability registry")
	}
	if _, err := DetailCapability("invented"); err == nil {
		t.Fatal("unknown detail view returned capabilities")
	}
}

func TestDetailCapabilitiesDoNotAdvertiseUnsupportedFieldsOrFilters(t *testing.T) {
	allFields := DetailFields()
	allFilters := []DetailFilter{
		DetailFilterDirection, DetailFilterCategory, DetailFilterBusiness,
		DetailFilterTarget, DetailFilterDevice, DetailFilterExporter,
	}
	for _, capability := range DetailCapabilities() {
		allowedFields := make(map[DetailField]struct{}, len(capability.Fields))
		for _, field := range capability.Fields {
			allowedFields[field] = struct{}{}
		}
		for _, field := range allFields {
			if _, ok := allowedFields[field]; ok {
				continue
			}
			request := validDetailRequest()
			request.View, request.Fields = capability.View, []DetailField{field}
			if _, err := CompileDetail(fullScope("tenant-a"), request, detailNow()); err == nil {
				t.Fatalf("unadvertised %s field %s was accepted", capability.View, field)
			}
		}
		allowedFilters := make(map[DetailFilter]struct{}, len(capability.Filters))
		for _, filter := range capability.Filters {
			allowedFilters[filter] = struct{}{}
		}
		for _, filter := range allFilters {
			if _, ok := allowedFilters[filter]; ok {
				continue
			}
			request := validDetailRequest()
			request.View = capability.View
			setDetailFilter(&request.Filters, filter)
			if _, err := CompileDetail(fullScope("tenant-a"), request, detailNow()); err == nil {
				t.Fatalf("unadvertised %s filter %s was accepted", capability.View, filter)
			}
		}
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
	first, err := CompileDetail(fullScope("tenant-a"), request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompileDetail(fullScope("tenant-a"), request, detailNow())
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
		"FROM flow_records AS source FINAL",
		"source.source_stream_id",
		"source.kafka_partition",
		"source.kafka_offset",
		"source.record_index",
		"toString(source.src_ip) AS _source_ip",
		"source.estimated_valid AS estimated_valid",
		"toUInt64(source.raw_bytes) AS raw_bytes",
		"(source.src_ip = toIPv6({ip:String}) OR source.dst_ip = toIPv6({ip:String}))",
		"AND source.disposition = 'count'",
		"AND source.business_direction IN ({detail_direction_0:String}, {detail_direction_1:String})",
		"ORDER BY event_time DESC, source_stream_id DESC, kafka_partition DESC, kafka_offset DESC, record_index DESC",
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
	if queryParameter(first.Query, "ip") != "'2001:db8::1'" || queryParameter(first.Query, "fetch_limit") != "'3'" ||
		queryParameter(first.Query, "detail_target_0") != "'target-a'" || queryParameter(first.Query, "detail_target_1") != "'target-b'" {
		t.Fatalf("detail parameters=%+v", first.Query.Parameters)
	}
	if setting(first.Query, "max_result_rows") != "3" || setting(first.Query, "max_rows_to_read") != "5000000" || setting(first.Query, "max_bytes_to_read") != "1073741824" || setting(first.Query, "max_memory_usage") == "" {
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
	compiled, err := CompileDetail(fullScope("tenant-a"), request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	if compiled.View != ViewRaw {
		t.Fatalf("compiled view=%q", compiled.View)
	}
	for _, required := range []string{
		"toUInt64(source.source_asn) AS source_asn", "toUInt64(source.destination_asn) AS destination_asn",
		"CAST(source.observation_direction AS String) AS observation_direction", "AND source.target_id IN ({detail_target_0:String})",
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
	defaults, err := CompileDetail(fullScope("tenant-a"), request, detailNow())
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
	cursor, err := EncodeDetailCursor(request.From.Add(30*time.Minute), detailCoordinate(1))
	if err != nil {
		t.Fatal(err)
	}
	request.Cursor = cursor
	compiled, err := CompileDetail(fullScope("tenant-a"), request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	if compiled.View != ViewSupplier {
		t.Fatalf("compiled view=%q", compiled.View)
	}
	for _, required := range []string{
		"CAST(source.supplier_category AS String) AS category", "toUInt64(source.supplier_remote_asn) AS remote_asn",
		"CAST(source.supplier_remote_asn_source AS String) AS remote_asn_source", "CAST(source.supplier_remote_country AS String) AS remote_country",
		"CAST(source.supplier_geo_version AS String) AS geo_version", "toUInt64(source.supplier_remote_isp_id) AS remote_isp_id",
		"CAST(source.supplier_remote_geo_city_id AS String) AS remote_geo_city_id", "min(source.fact_schema) OVER () AS _minimum_fact_schema",
		"row_number() OVER (ORDER BY source.event_time DESC, source.source_stream_id DESC, source.kafka_partition DESC, source.kafka_offset DESC, source.record_index DESC) AS _scope_row",
		"CAST((source.event_time < {cursor_time:DateTime64(3, 'UTC')}", "AS Bool) AS _scope_match",
		"AND source.supplier_category IN ({detail_category_0:String})", "WHERE _scope_match OR _scope_row = 1",
	} {
		if !strings.Contains(compiled.Query.Body, required) {
			t.Fatalf("supplier query missing %q:\n%s", required, compiled.Query.Body)
		}
	}
	cursorPosition := strings.Index(compiled.Query.Body, "AS _scope_match")
	windowPosition := strings.Index(compiled.Query.Body, "min(source.fact_schema) OVER ()")
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
	defaults, err := CompileDetail(fullScope("tenant-a"), request, detailNow())
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
		{"ipv4 source", "192.0.2.10", DetailEndpointSource, "AND source.src_ip = toIPv6({ip:String})"},
		{"ipv4 destination", "192.0.2.10", DetailEndpointDestination, "AND source.dst_ip = toIPv6({ip:String})"},
		{"ipv6 either", "2001:db8::10", DetailEndpointEither, "AND (source.src_ip = toIPv6({ip:String}) OR source.dst_ip = toIPv6({ip:String}))"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validDetailRequest()
			request.IP, request.Endpoint = test.ip, test.endpoint
			compiled, err := CompileDetail(fullScope("tenant-a"), request, detailNow())
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
	coordinate := SourceCoordinate{SourceStreamID: "stream-a", KafkaPartition: 3, KafkaOffset: 99, RecordIndex: 7}
	cursor, err := EncodeDetailCursor(eventTime, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	const golden = "v3.eyJmIjoiZXZlbnRfdGltZSIsImQiOiJkZXNjIiwidiI6IjIwMjYtMDktMDVUMTA6MTE6MTIuMzQ1WiIsInQiOjE3ODg2MDMwNzIzNDUsInMiOiJzdHJlYW0tYSIsInAiOjMsIm8iOjk5LCJpIjo3fQ"
	if cursor != golden {
		t.Fatalf("cursor=%q, want %q", cursor, golden)
	}
	request := validDetailRequest()
	request.Cursor = cursor
	compiled, err := CompileDetail(fullScope("tenant-a"), request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(compiled.Query.Body, "source.record_index < {cursor_record_index:UInt32}") ||
		queryParameter(compiled.Query, "cursor_time") != "'2026-09-05 10:11:12.345'" ||
		queryParameter(compiled.Query, "cursor_source_stream_id") != "'stream-a'" ||
		queryParameter(compiled.Query, "cursor_kafka_partition") != "'3'" ||
		queryParameter(compiled.Query, "cursor_kafka_offset") != "'99'" ||
		queryParameter(compiled.Query, "cursor_record_index") != "'7'" {
		t.Fatalf("cursor boundary query=%s parameters=%+v", compiled.Query.Body, compiled.Query.Parameters)
	}
	request.Fields = []DetailField{DetailFieldCategory, DetailFieldRemoteASN}
	changedMask, err := CompileDetail(fullScope("tenant-a"), request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	if queryParameter(changedMask.Query, "cursor_time") != queryParameter(compiled.Query, "cursor_time") ||
		queryParameter(changedMask.Query, "cursor_record_index") != queryParameter(compiled.Query, "cursor_record_index") {
		t.Fatal("a v3 cursor changed meaning when the field mask changed")
	}
	decoded, err := decodeDetailCursor(cursor)
	if err != nil || !decoded.eventTime.Equal(eventTime) || decoded.sourceStreamID != coordinate.SourceStreamID ||
		decoded.kafkaPartition != coordinate.KafkaPartition || decoded.kafkaOffset != coordinate.KafkaOffset || decoded.recordIndex != coordinate.RecordIndex {
		t.Fatalf("decoded=%+v error=%v", decoded, err)
	}
}

func TestCompileDetailUsesWhitelistedStableSortAndBoundCursor(t *testing.T) {
	request := validDetailRequest()
	request.Fields = []DetailField{DetailFieldRawBytes, DetailFieldSourceIP}
	request.Sort = DetailSort{Field: string(DetailFieldRawBytes), Direction: "asc"}
	compiled, err := CompileDetail(fullScope("tenant-a"), request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	if compiled.Sort != request.Sort || !strings.Contains(compiled.Query.Body, "ORDER BY raw_bytes ASC, event_time ASC, source_stream_id ASC, kafka_partition ASC, kafka_offset ASC, record_index ASC") {
		t.Fatalf("compiled sort=%+v query=\n%s", compiled.Sort, compiled.Query.Body)
	}
	row := DetailRow{
		EventTime: request.From.Add(30 * time.Minute), SourceCoordinate: detailCoordinate(7),
		Values: map[DetailField]any{DetailFieldRawBytes: uint64(123)},
	}
	cursor, err := encodeDetailCursorForRow(compiled, row)
	if err != nil || !strings.HasPrefix(cursor, detailCursorPrefix) {
		t.Fatalf("cursor=%q error=%v", cursor, err)
	}
	request.Cursor = cursor
	next, err := CompileDetail(fullScope("tenant-a"), request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"toUInt64(source.raw_bytes) > {cursor_sort_value:UInt64}",
		"source.event_time > {cursor_time:DateTime64(3, 'UTC')}",
		"source.record_index > {cursor_record_index:UInt32}",
	} {
		if !strings.Contains(next.Query.Body, required) {
			t.Fatalf("next query missing %q:\n%s", required, next.Query.Body)
		}
	}
	if queryParameter(next.Query, "cursor_sort_value") != "'123'" || queryParameter(next.Query, "cursor_record_index") != "'7'" {
		t.Fatalf("cursor parameters=%+v", next.Query.Parameters)
	}

	request.Sort.Direction = "desc"
	if _, err := CompileDetail(fullScope("tenant-a"), request, detailNow()); !IsRequestError(err, "cursor", ErrorInvalid) {
		t.Fatalf("mismatched cursor error=%v", err)
	}
}

func TestCompileDetailColumnFiltersAreTypedCanonicalAndParameterized(t *testing.T) {
	request := validDetailRequest()
	request.ColumnFilters = []DetailColumnFilter{
		{Field: string(DetailFieldSourceIP), Values: []string{"192.0.2.1", "::ffff:192.0.2.1"}},
		{Field: string(DetailFieldSourcePort), Values: []string{"443", "80", "443"}},
		{Field: string(DetailFieldEstimatedValid), Values: []string{"true"}},
		{Field: string(DetailFieldRemoteCountry), Values: []string{"", "CN"}},
	}
	compiled, err := CompileDetail(fullScope("tenant-a"), request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"source.src_ip IN (toIPv6({detail_column_0_0:String}))",
		"toUInt64(source.src_port) IN ({detail_column_1_0:UInt64}, {detail_column_1_1:UInt64})",
		"toUInt8(source.estimated_valid) IN ({detail_column_2_0:UInt8})",
		"CAST(source.remote_country AS String) IN ({detail_column_3_0:String}, {detail_column_3_1:String})",
	} {
		if !strings.Contains(compiled.Query.Body, required) {
			t.Fatalf("query missing %q:\n%s", required, compiled.Query.Body)
		}
	}
	if queryParameter(compiled.Query, "detail_column_0_0") != "'192.0.2.1'" ||
		queryParameter(compiled.Query, "detail_column_1_0") != "'443'" || queryParameter(compiled.Query, "detail_column_1_1") != "'80'" ||
		queryParameter(compiled.Query, "detail_column_3_0") != "''" {
		t.Fatalf("column filter parameters=%+v", compiled.Query.Parameters)
	}
	for _, unsafe := range []string{"192.0.2.1", "443", "CN"} {
		if strings.Contains(compiled.Query.Body, unsafe) {
			t.Fatalf("column filter value %q was interpolated into SQL", unsafe)
		}
	}
}

func TestCompileDetailRejectsUnsafeUnsupportedOrUnboundedRequests(t *testing.T) {
	tests := []struct {
		name  string
		field string
		code  ErrorCode
		apply func(*Scope, *DetailRequest)
	}{
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
		{"sort injection", "sort.field", ErrorUnsupported, func(_ *Scope, request *DetailRequest) { request.Sort.Field = "raw_bytes DESC" }},
		{"sort direction", "sort.direction", ErrorUnsupported, func(_ *Scope, request *DetailRequest) { request.Sort.Direction = "sideways" }},
		{"sort not selected", "sort.field", ErrorInvalid, func(_ *Scope, request *DetailRequest) {
			request.Fields = []DetailField{DetailFieldSourceIP}
			request.Sort.Field = string(DetailFieldRawBytes)
		}},
		{"column duplicate", "column_filters", ErrorInvalid, func(_ *Scope, request *DetailRequest) {
			request.ColumnFilters = []DetailColumnFilter{{Field: "category", Values: []string{"overseas"}}, {Field: "category", Values: []string{"unknown"}}}
		}},
		{"column invalid uint", "column_filters[0].values", ErrorInvalid, func(_ *Scope, request *DetailRequest) {
			request.ColumnFilters = []DetailColumnFilter{{Field: "src_port", Values: []string{"080"}}}
		}},
		{"column legacy overlap", "column_filters", ErrorInvalid, func(_ *Scope, request *DetailRequest) {
			request.Filters.Categories = []string{"overseas"}
			request.ColumnFilters = []DetailColumnFilter{{Field: "category", Values: []string{"unknown"}}}
		}},
		{"filter", "filters.directions", ErrorUnsupported, func(_ *Scope, request *DetailRequest) { request.Filters.Directions = []string{"sideways"} }},
		{"filter limit", "filters.target_ids", ErrorLimitExceeded, func(_ *Scope, request *DetailRequest) {
			request.Filters.TargetIDs = make([]string, maxDetailValuesPerFilter+1)
		}},
		{"cursor version", "cursor", ErrorInvalid, func(_ *Scope, request *DetailRequest) { request.Cursor = "v2.bad" }},
		{"cursor payload", "cursor", ErrorInvalid, func(_ *Scope, request *DetailRequest) { request.Cursor = "v3.bad=" }},
		{"cursor range", "cursor", ErrorInvalid, func(_ *Scope, request *DetailRequest) {
			cursor, err := EncodeDetailCursor(request.From.Add(-time.Millisecond), detailCoordinate(1))
			if err != nil {
				panic(err)
			}
			request.Cursor = cursor
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scope := fullScope("tenant-a")
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
		name       string
		time       time.Time
		coordinate SourceCoordinate
	}{
		{"sub-millisecond", validTime.Add(time.Microsecond), detailCoordinate(1)},
		{"pre-epoch", time.UnixMilli(-1), detailCoordinate(1)},
		{"empty stream", validTime, SourceCoordinate{}},
		{"invalid stream", validTime, SourceCoordinate{SourceStreamID: "bad stream"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := EncodeDetailCursor(test.time, test.coordinate); !IsRequestError(err, "cursor", ErrorInvalid) {
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

func setDetailFilter(filters *DetailFilters, filter DetailFilter) {
	switch filter {
	case DetailFilterDirection:
		filters.Directions = []string{"in"}
	case DetailFilterCategory:
		filters.Categories = []string{"overseas"}
	case DetailFilterBusiness:
		filters.Businesses = []string{"business-a"}
	case DetailFilterTarget:
		filters.TargetIDs = []string{"target-a"}
	case DetailFilterDevice:
		filters.DeviceIDs = []string{"device-a"}
	case DetailFilterExporter:
		filters.ExporterIDs = []string{"exporter-a"}
	default:
		panic("unknown detail filter " + filter)
	}
}

// fullScope grants every value-layer view — detail data tests exercise view
// behavior, not authorization.
func fullScope(tenant string) Scope {
	return Scope{AllowedViews: []View{ViewRaw, ViewSupplier, ViewCustomer}}
}

// TestCompileDetailEnforcesValueLayerEntitlement is the value-layer RBAC gate
// (review F3 / PLAT-04H): a principal must be entitled to the requested view.
func TestCompileDetailEnforcesValueLayerEntitlement(t *testing.T) {
	customerOnly := Scope{} // nil AllowedViews => customer-only
	for _, view := range []View{ViewRaw, ViewSupplier} {
		request := validDetailRequest()
		request.View = view
		if _, err := CompileDetail(customerOnly, request, detailNow()); !IsRequestError(err, "view", ErrorPermissionDenied) {
			t.Fatalf("customer-only principal requesting %q: err=%v, want permission_denied", view, err)
		}
	}
	// The customer view is permitted by the default scope.
	if _, err := CompileDetail(Scope{}, validDetailRequest(), detailNow()); err != nil {
		t.Fatalf("customer view under default scope: %v", err)
	}
	// An explicit grant unlocks the privileged view.
	granted := Scope{AllowedViews: []View{ViewCustomer, ViewSupplier}}
	request := validDetailRequest()
	request.View = ViewSupplier
	if _, err := CompileDetail(granted, request, detailNow()); err != nil {
		t.Fatalf("supplier view with grant: %v", err)
	}
}
