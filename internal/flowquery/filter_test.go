// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCanonicalFilterNormalizesTypedValuesAndCommutativeNodes(t *testing.T) {
	input := FilterExpression{Op: FilterAnd, Args: []FilterExpression{
		{Op: FilterPredicate, Field: "protocol", Operator: FilterIn, Values: []string{"UDP", "tcp", "6"}},
		{Op: FilterPredicate, Field: "src_ip", Operator: FilterIn, Values: []string{"203.0.113.9/24", "2001:db8::1"}},
		{Op: FilterPredicate, Field: "asn", Operator: FilterEqual, Values: []string{"AS4134"}},
	}}
	first, err := CanonicalFilter(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Args[0], input.Args[2] = input.Args[2], input.Args[0]
	second, err := CanonicalFilter(input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("canonical filters differ:\n%+v\n%+v", first, second)
	}
	encoded := mustJSON(t, first)
	for _, value := range []string{`"203.0.113.0/24"`, `"2001:db8::1"`, `"4134"`, `"6"`, `"17"`} {
		if !strings.Contains(encoded, value) {
			t.Fatalf("canonical filter %s missing %s", encoded, value)
		}
	}
}

func TestAggregateFilterSupportKeepsMaterializedFieldsOnRollup(t *testing.T) {
	filter := FilterExpression{Op: FilterAnd, Args: []FilterExpression{
		{Op: FilterPredicate, Field: "direction", Operator: FilterIn, Values: []string{"out", "in"}},
		{Op: FilterPredicate, Field: "business", Operator: FilterNotEqual, Values: []string{"test"}},
	}}
	supported, err := AggregateFilterSupported(filter)
	if err != nil || !supported {
		t.Fatalf("supported=%v err=%v", supported, err)
	}
	request := validRequest()
	request.Filter = &filter
	compiled, err := Compile(Scope{TenantID: "tenant-a"}, request, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(compiled.Query.Body, "business_direction IN ({typed_filter_") ||
		!strings.Contains(compiled.Query.Body, "business != {typed_filter_") {
		t.Fatalf("query=%s", compiled.Query.Body)
	}
	baseOnly := FilterExpression{Op: FilterPredicate, Field: "src_ip", Operator: FilterEqual, Values: []string{"203.0.113.1"}}
	if supported, err := AggregateFilterSupported(baseOnly); err != nil || supported {
		t.Fatalf("base-only supported=%v err=%v", supported, err)
	}
}

func TestFilterFieldsReturnsCanonicalUniqueFields(t *testing.T) {
	fields, err := FilterFields(FilterExpression{Op: FilterAnd, Args: []FilterExpression{
		{Op: FilterPredicate, Field: "asn", Operator: FilterEqual, Values: []string{"4134"}},
		{Op: FilterPredicate, Field: "src_ip", Operator: FilterIn, Values: []string{"203.0.113.0/24"}},
		{Op: FilterPredicate, Field: "asn", Operator: FilterGreaterThan, Values: []string{"0"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fields, []string{"asn", "src_ip"}) {
		t.Fatalf("fields=%v", fields)
	}
}

func TestCompileBaseFilterUsesOnlyRegistrySQLAndTypedParameters(t *testing.T) {
	filter := FilterExpression{Op: FilterOr, Args: []FilterExpression{
		{Op: FilterPredicate, Field: "remote_ip", Operator: FilterIn, Values: []string{"203.0.113.0/24", "2001:db8::1"}},
		{Op: FilterAnd, Args: []FilterExpression{
			{Op: FilterPredicate, Field: "asn", Operator: FilterGreaterThanOrEqual, Values: []string{"64512"}},
			{Op: FilterPredicate, Field: "geo.country", Operator: FilterNotIn, Values: []string{"US", "CN"}},
		}},
	}}
	sql, parameters, err := compileBaseFilter(filter)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"isIPAddressInRange(toString(remote_ip), {typed_filter_", "remote_ip = toIPv6({typed_filter_",
		"remote_asn >= {typed_filter_", "remote_geo_country_id NOT IN (",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("compiled SQL %q missing %q", sql, fragment)
		}
	}
	if len(parameters) != 5 {
		t.Fatalf("parameters=%+v", parameters)
	}
	mappedRange := false
	for _, parameter := range parameters {
		if parameter.Value == "'::ffff:203.0.113.0/120'" {
			mappedRange = true
		}
	}
	if !mappedRange {
		t.Fatalf("IPv4 CIDR was not mapped for ClickHouse IPv6 storage: %+v", parameters)
	}
	if strings.Contains(sql, "203.0.113") || strings.Contains(sql, "64512") || strings.Contains(sql, "CN") {
		t.Fatalf("user value escaped into SQL: %s", sql)
	}
}

func TestCanonicalFilterRejectsInvalidFieldsOperatorsValuesAndLimits(t *testing.T) {
	tests := []FilterExpression{
		{Op: FilterPredicate, Field: "raw_sql", Operator: FilterEqual, Values: []string{"1=1"}},
		{Op: FilterPredicate, Field: "src_ip", Operator: FilterGreaterThan, Values: []string{"203.0.113.1"}},
		{Op: FilterPredicate, Field: "remote_port", Operator: FilterEqual, Values: []string{"65536"}},
		{Op: FilterPredicate, Field: "src_ip", Operator: FilterEqual, Values: []string{"not-an-ip"}},
		{Op: FilterAnd, Args: []FilterExpression{{Op: FilterPredicate, Field: "asn", Operator: FilterEqual, Values: []string{"1"}}}},
	}
	for _, input := range tests {
		if _, err := CanonicalFilter(input); err == nil {
			t.Fatalf("filter unexpectedly accepted: %+v", input)
		}
	}
	deep := FilterExpression{Op: FilterNot}
	cursor := &deep
	for index := 0; index < maxFilterDepth; index++ {
		cursor.Args = []FilterExpression{{Op: FilterNot}}
		cursor = &cursor.Args[0]
	}
	cursor.Args = []FilterExpression{{Op: FilterPredicate, Field: "asn", Operator: FilterEqual, Values: []string{"1"}}}
	if _, err := CanonicalFilter(deep); err == nil {
		t.Fatal("deep filter unexpectedly accepted")
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
