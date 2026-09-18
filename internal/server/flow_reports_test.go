// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

// TestFlowReportQueryEnvelope: the hub wire envelope (nested `report` + gateway
// fields incl. operator_selection) strict-decodes and maps onto the internal
// request, and the mapped request normalizes.
func TestFlowReportQueryEnvelope(t *testing.T) {
	from, to := reportWindow()
	body := `{
		"from": "` + from.Format(time.RFC3339Nano) + `",
		"to": "` + to.Format(time.RFC3339Nano) + `",
		"step_seconds": 0, "limit": 250000, "value_layer": "customer",
		"metric": "estimated_bps", "filters": {"businesses": ["b1"]},
		"top_n": 20, "include_other": true, "timezone": "UTC", "target_points": 300,
		"operator_selection": {"operator_id": "op-1"},
		"report": {"schema_version": 1, "kind": "overview", "display_mode": "value",
			"tables": {"business_matrix": {"limit": 10}}}
	}`
	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.DisallowUnknownFields()
	var input flowReportQueryInput
	if err := decoder.Decode(&input); err != nil {
		t.Fatalf("strict decode of hub envelope failed: %v", err)
	}
	if input.Operator == nil || input.Operator.OperatorID != "op-1" {
		t.Fatalf("operator_selection not decoded: %+v", input.Operator)
	}
	req := input.toReportRequest()
	if req.Kind != flowReportOverview || req.View != flowquery.ViewCustomer || req.Metric != "estimated_bps" ||
		req.DisplayMode != flowReportValue || req.TargetPoints != 300 || req.TopN != 20 ||
		req.Tables["business_matrix"] == nil || len(req.Filters.Businesses) != 1 {
		t.Fatalf("envelope mapping = %+v", req)
	}
	if err := normalizeFlowReport(&req); err != nil {
		t.Fatalf("normalize mapped request: %v", err)
	}
}

// TestFlowReportCompleteness derives report completeness from panels: the minimum
// per-panel complete_ratio and partial when anything is degraded.
func TestFlowReportCompleteness(t *testing.T) {
	if ratio, partial := flowReportCompleteness(nil, nil); ratio != 1 || partial {
		t.Fatalf("empty = %v %v", ratio, partial)
	}
	if _, partial := flowReportCompleteness(nil, []string{"observed: unavailable"}); !partial {
		t.Fatalf("a warning must mark the report partial")
	}
	panels := []flowReportPanel{
		{ID: "a", Status: "ready", Meta: gin.H{"complete_ratio": 0.8}},
		{ID: "b", Status: "ready", Meta: gin.H{"complete_ratio": 1.0}},
		{ID: "c", Status: "unavailable"},
	}
	if ratio, partial := flowReportCompleteness(panels, nil); ratio != 0.8 || !partial {
		t.Fatalf("panels = %v %v", ratio, partial)
	}
}

func reportWindow() (time.Time, time.Time) {
	from := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	return from, from.Add(time.Hour)
}

// TestNormalizeFlowReport applies defaults and rejects unsupported kinds/groupings.
func TestNormalizeFlowReport(t *testing.T) {
	from, to := reportWindow()
	req := flowReportRequest{Kind: flowReportOverview, From: from, To: to}
	if err := normalizeFlowReport(&req); err != nil {
		t.Fatalf("overview: %v", err)
	}
	if req.Metric != flowquery.MetricEstimatedBPS || req.TopN != 20 || req.Timezone != "UTC" || req.TargetPoints != flowquery.DefaultTargetPoints {
		t.Fatalf("defaults not applied: %+v", req)
	}

	dims := flowReportRequest{Kind: flowReportDimensions, From: from, To: to}
	if err := normalizeFlowReport(&dims); err != nil || dims.GroupBy != flowquery.DimensionCategory {
		t.Fatalf("dimensions default group_by: err=%v group_by=%s", err, dims.GroupBy)
	}

	for _, tc := range []struct {
		name string
		req  flowReportRequest
	}{
		{"bad kind", flowReportRequest{Kind: "bogus", From: from, To: to}},
		{"group_by on overview", flowReportRequest{Kind: flowReportOverview, GroupBy: flowquery.DimensionASN, From: from, To: to}},
		{"bad group_by", flowReportRequest{Kind: flowReportDimensions, GroupBy: "bogus", From: from, To: to}},
		{"empty range", flowReportRequest{Kind: flowReportOverview, From: from, To: from}},
		{"top_n too high", flowReportRequest{Kind: flowReportOverview, TopN: 101, From: from, To: to}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.req
			if err := normalizeFlowReport(&r); err == nil {
				t.Fatalf("%s: expected error", tc.name)
			}
		})
	}
}

// TestReportPanelSpecs resolves the per-kind panels with directional filters, and
// honors a panel_ids subset.
func TestReportPanelSpecs(t *testing.T) {
	from, to := reportWindow()
	overview := flowReportRequest{
		Kind: flowReportOverview, TopN: 20, From: from, To: to,
		Filters: flowquery.Filters{DeviceIDs: []string{"device-1"}},
	}
	specs := reportPanelSpecs(overview)
	ids := panelIDs(specs)
	if !reflect.DeepEqual(ids, []string{"total", "category_in", "category_out", "business_category_in", "business_category_out"}) {
		t.Fatalf("overview panels = %v", ids)
	}
	// category_in filters to the in direction; business_category is a joint (2 dims) and optional.
	byID := specByID(specs)
	if byID["total"].Special != "direction_split" {
		t.Fatalf("total panel special = %q", byID["total"].Special)
	}
	for _, spec := range specs {
		if !reflect.DeepEqual(spec.Filters.DeviceIDs, []string{"device-1"}) {
			t.Fatalf("%s lost device filter: %v", spec.ID, spec.Filters.DeviceIDs)
		}
	}
	if got := byID["category_in"].Filters.Directions; !reflect.DeepEqual(got, []string{"in"}) {
		t.Fatalf("category_in directions = %v", got)
	}
	if bc := byID["business_category_out"]; !bc.Optional || len(bc.Dimensions) != 2 {
		t.Fatalf("business_category panel = %+v", bc)
	}

	dims := flowReportRequest{Kind: flowReportDimensions, GroupBy: flowquery.DimensionASN, TopN: 10, From: from, To: to}
	dimSpecs := reportPanelSpecs(dims)
	if ids := panelIDs(dimSpecs); !reflect.DeepEqual(ids, []string{"total", "dimension_in", "dimension_out"}) {
		t.Fatalf("dimensions panels = %v", ids)
	}
	if d := specByID(dimSpecs)["dimension_in"]; len(d.Dimensions) != 1 || d.Dimensions[0] != flowquery.DimensionASN {
		t.Fatalf("dimension_in dims = %+v", d.Dimensions)
	}

	// panel_ids selects a subset.
	overview.PanelIDs = []string{"total", "category_in"}
	if ids := panelIDs(reportPanelSpecs(overview)); !reflect.DeepEqual(ids, []string{"total", "category_in"}) {
		t.Fatalf("subset = %v", ids)
	}
}

// TestReportPanelSpecsOverseas: the overseas report adds geo/asn/port/protocol
// breakdowns + the special observed/vpn_share panels, and every panel carries the
// "overseas" category filter.
func TestReportPanelSpecsOverseas(t *testing.T) {
	from, to := reportWindow()
	specs := reportPanelSpecs(flowReportRequest{Kind: flowReportOverseas, TopN: 20, From: from, To: to})
	byID := specByID(specs)
	want := []string{"total", "country_in", "country_out", "region_in", "region_out", "asn_in", "asn_out", "remote_port_in", "remote_port_out", "protocol_in", "protocol_out", "observed", "vpn_share"}
	if ids := panelIDs(specs); !reflect.DeepEqual(ids, want) {
		t.Fatalf("overseas panels = %v", ids)
	}
	if !containsString(byID["total"].Filters.Categories, "overseas") || !containsString(byID["country_in"].Filters.Categories, "overseas") {
		t.Fatalf("overseas category filter missing: total=%v country_in=%v", byID["total"].Filters.Categories, byID["country_in"].Filters.Categories)
	}
	if byID["observed"].Special != "observed" || byID["vpn_share"].Special != "vpn_share" {
		t.Fatalf("special panels = %+v / %+v", byID["observed"], byID["vpn_share"])
	}
	if d := byID["country_in"].Dimensions; len(d) != 1 || d[0] != flowquery.DimensionGeoCountry {
		t.Fatalf("country_in dim = %v", d)
	}
}

// TestNormalizeFlowReportDisplayAndPeakWindows: display_mode defaults to value and
// is enum-checked; peak windows validate count, ISO weekdays and HH:MM intervals.
func TestNormalizeFlowReportDisplayAndPeakWindows(t *testing.T) {
	from, to := reportWindow()

	req := flowReportRequest{Kind: flowReportOverview, From: from, To: to}
	if err := normalizeFlowReport(&req); err != nil || req.DisplayMode != flowReportValue {
		t.Fatalf("display default: err=%v mode=%s", err, req.DisplayMode)
	}
	windowed := flowReportRequest{Kind: flowReportOverview, DisplayMode: flowReportShare, From: from, To: to,
		PeakWindows: []flowquery.LocalTimeWindow{{Days: []uint8{1, 2, 3, 4, 5}, StartLocal: "09:00", EndLocal: "17:00"}}}
	if err := normalizeFlowReport(&windowed); err != nil {
		t.Fatalf("valid share + peak windows: %v", err)
	}

	win := func(days []uint8, start, end string) []flowquery.LocalTimeWindow {
		return []flowquery.LocalTimeWindow{{Days: days, StartLocal: start, EndLocal: end}}
	}
	for _, tc := range []struct {
		name string
		req  flowReportRequest
	}{
		{"bad display", flowReportRequest{Kind: flowReportOverview, DisplayMode: "bogus", From: from, To: to}},
		{"empty days", flowReportRequest{Kind: flowReportOverview, From: from, To: to, PeakWindows: win(nil, "09:00", "17:00")}},
		{"bad weekday", flowReportRequest{Kind: flowReportOverview, From: from, To: to, PeakWindows: win([]uint8{8}, "09:00", "17:00")}},
		{"duplicate weekday", flowReportRequest{Kind: flowReportOverview, From: from, To: to, PeakWindows: win([]uint8{1, 1}, "09:00", "17:00")}},
		{"bad start", flowReportRequest{Kind: flowReportOverview, From: from, To: to, PeakWindows: win([]uint8{1}, "9am", "17:00")}},
		{"empty interval", flowReportRequest{Kind: flowReportOverview, From: from, To: to, PeakWindows: win([]uint8{1}, "09:00", "09:00")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.req
			if err := normalizeFlowReport(&r); err == nil {
				t.Fatalf("%s: expected error", tc.name)
			}
		})
	}
}

func panelIDs(specs []reportPanelSpec) []string {
	ids := make([]string, len(specs))
	for i, spec := range specs {
		ids[i] = spec.ID
	}
	return ids
}

func specByID(specs []reportPanelSpec) map[string]reportPanelSpec {
	out := make(map[string]reportPanelSpec, len(specs))
	for _, spec := range specs {
		out[spec.ID] = spec
	}
	return out
}

// TestUniqueSortedStrings: report warnings are de-duplicated and sorted, so a warning
// raised by more than one panel surfaces once (the frontend keys the warning list by
// text, so duplicates would collide).
func TestUniqueSortedStrings(t *testing.T) {
	got := uniqueSortedStrings([]string{"b: two", "a: one", "b: two", "a: one", "c: three"})
	want := []string{"a: one", "b: two", "c: three"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("uniqueSortedStrings = %v, want %v", got, want)
	}
	if out := uniqueSortedStrings(nil); out != nil {
		t.Fatalf("nil input must return nil, got %v", out)
	}
	if out := uniqueSortedStrings([]string{}); len(out) != 0 {
		t.Fatalf("empty input must return empty, got %v", out)
	}
}

func TestFlowReportNeedsBaseFacts(t *testing.T) {
	if needed, err := flowReportNeedsBaseFacts(nil); err != nil || needed {
		t.Fatalf("nil filter: needed=%v err=%v", needed, err)
	}
	rollup := &flowquery.FilterExpression{
		Op: flowquery.FilterPredicate, Field: "business", Operator: flowquery.FilterEqual, Values: []string{"customer-a"},
	}
	if needed, err := flowReportNeedsBaseFacts(rollup); err != nil || needed {
		t.Fatalf("rollup filter: needed=%v err=%v", needed, err)
	}
	geo := &flowquery.FilterExpression{
		Op: flowquery.FilterPredicate, Field: "geo.country", Operator: flowquery.FilterEqual, Values: []string{"CN"},
	}
	if needed, err := flowReportNeedsBaseFacts(geo); err != nil || !needed {
		t.Fatalf("geo filter: needed=%v err=%v", needed, err)
	}
}
