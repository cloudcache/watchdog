// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowquery"
)

// TestDirectionSplitAllowed pins the faithful hub gate: direction split is only
// defined for a single total series with no pre-existing direction filter.
func TestDirectionSplitAllowed(t *testing.T) {
	cases := []struct {
		name string
		in   flowQueryParameters
		want bool
	}{
		{"empty dimension (total series)", flowQueryParameters{}, true},
		{"explicit total, top_n=1", flowQueryParameters{Dimension: flowquery.DimensionTotal, TopN: 1}, true},
		{"non-total dimension", flowQueryParameters{Dimension: flowquery.DimensionCategory}, false},
		{"multi-dimension", flowQueryParameters{Dimensions: []flowquery.Dimension{flowquery.DimensionTotal}}, false},
		{"top_n > 1", flowQueryParameters{TopN: 5}, false},
		{"include_other", flowQueryParameters{IncludeOther: true}, false},
		{"pre-existing direction filter", flowQueryParameters{Filters: flowquery.Filters{Directions: []string{"in"}}}, false},
	}
	for _, tc := range cases {
		if got := directionSplitAllowed(tc.in); got != tc.want {
			t.Errorf("%s: directionSplitAllowed = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestMergeFlowDirectionResults proves the per-direction fold: relabeled points
// under a synthetic additive "direction" dimension, with completeness reduced
// conservatively (widest expected, narrowest covered, min ratio, AND complete).
func TestMergeFlowDirectionResults(t *testing.T) {
	bucket := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	metric := flowquery.MetricDefinition{Name: flowquery.Metric("estimated_bps"), Unit: "bps"}
	plan := &flowquery.AggregatePlan{Source: flowquery.BucketOneMinute, StepSeconds: 60}
	inbound := flowquery.Result{
		Metric:             metric,
		Points:             []flowquery.Point{{Bucket: bucket, DimensionValue: "total", Value: 10}},
		RollupCompleteness: flowquery.RollupCompleteness{ExpectedBuckets: 10, CoveredBuckets: 10, Ratio: 1, Complete: true},
	}
	outbound := flowquery.Result{
		Metric:             metric,
		Points:             []flowquery.Point{{Bucket: bucket, DimensionValue: "total", Value: 20}},
		RollupCompleteness: flowquery.RollupCompleteness{ExpectedBuckets: 12, CoveredBuckets: 8, Ratio: 0.5, Complete: false},
	}
	combined := mergeFlowDirectionResults(plan, []string{"Inbound", "Outbound"}, []flowquery.Result{inbound, outbound})

	if combined.Dimension.Kind != flowquery.Dimension("direction") || !combined.Dimension.Additive {
		t.Fatalf("synthetic dimension = %+v", combined.Dimension)
	}
	if len(combined.Points) != 2 || combined.Points[0].DimensionValue != "Inbound" || combined.Points[1].DimensionValue != "Outbound" {
		t.Fatalf("relabeled points = %+v", combined.Points)
	}
	if combined.Points[0].Value != 10 || combined.Points[1].Value != 20 {
		t.Fatalf("values not preserved: %+v", combined.Points)
	}
	rc := combined.RollupCompleteness
	if rc.ExpectedBuckets != 12 || rc.CoveredBuckets != 8 || rc.Ratio != 0.5 || rc.Complete {
		t.Fatalf("reduced completeness = %+v", rc)
	}
	if combined.Metric != metric || combined.Plan != plan {
		t.Fatalf("metric/plan not carried through: %+v", combined)
	}
}

// TestFlowAddressSetLabel: a lone included set names the series; anything else is a
// generic combination label.
func TestFlowAddressSetLabel(t *testing.T) {
	single := flowquery.AddressSetResult{Sets: flowdimension.AddressSetFilter{IncludeAny: []string{"set-a"}}}
	if got := flowAddressSetLabel(single); got != "set-a" {
		t.Fatalf("single include label = %q, want set-a", got)
	}
	combo := flowquery.AddressSetResult{Sets: flowdimension.AddressSetFilter{IncludeAny: []string{"set-a"}, ExcludeAny: []string{"set-b"}}}
	if got := flowAddressSetLabel(combo); got != "address-set combination" {
		t.Fatalf("include+exclude label = %q", got)
	}
	multi := flowquery.AddressSetResult{Sets: flowdimension.AddressSetFilter{IncludeAny: []string{"set-a", "set-c"}}}
	if got := flowAddressSetLabel(multi); got != "address-set combination" {
		t.Fatalf("multi-include label = %q", got)
	}
}

// TestMarshalFlowAddressSetResult: the payload echoes the resolved filter + endpoint
// the client reads back, promotes the embedded aggregate result, and attaches a table
// page only when a table request is present.
func TestMarshalFlowAddressSetResult(t *testing.T) {
	result := flowquery.Result{
		Metric:    flowquery.MetricDefinition{Name: flowquery.Metric("estimated_bps"), Unit: "bps"},
		Dimension: flowquery.DimensionDefinition{Kind: flowquery.DimensionAddressSet},
		Points:    []flowquery.Point{{Bucket: time.Now().UTC(), DimensionValue: "set-a", Value: 42}},
		Plan:      &flowquery.AggregatePlan{Source: flowquery.BucketFlowRecords, StepSeconds: 60},
	}
	source := flowquery.AddressSetResult{
		Sets:     flowdimension.AddressSetFilter{IncludeAny: []string{"set-a"}},
		Endpoint: flowquery.AddressSetEndpointEither,
	}

	raw, err := marshalFlowAddressSetResult(result, nil, source)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["address_set_endpoint"] != "either" {
		t.Fatalf("endpoint echo = %v", decoded["address_set_endpoint"])
	}
	filter, ok := decoded["address_set_filter"].(map[string]any)
	if !ok || filter["include_any"] == nil {
		t.Fatalf("filter echo = %v", decoded["address_set_filter"])
	}
	if decoded["points"] == nil {
		t.Fatalf("embedded result points not promoted")
	}
	if _, hasTable := decoded["table"]; hasTable {
		t.Fatalf("table must be omitted without a table request")
	}

	req := &flowTableRequest{Limit: 25}
	if err := normalizeFlowTableRequest(req); err != nil {
		t.Fatal(err)
	}
	rawWithTable, err := marshalFlowAddressSetResult(result, req, source)
	if err != nil {
		t.Fatal(err)
	}
	var decodedWithTable map[string]any
	if err := json.Unmarshal(rawWithTable, &decodedWithTable); err != nil {
		t.Fatal(err)
	}
	if _, hasTable := decodedWithTable["table"]; !hasTable {
		t.Fatalf("table must be attached with a table request: %s", rawWithTable)
	}
}
