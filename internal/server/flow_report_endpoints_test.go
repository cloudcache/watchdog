// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"testing"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

// TestNormalizeFlowReportEndpoints: endpoints require a side, default a table, and
// reject a table on any other kind.
func TestNormalizeFlowReportEndpoints(t *testing.T) {
	from, to := reportWindow()

	req := flowReportRequest{Kind: flowReportEndpoints, Side: "source", From: from, To: to}
	if err := normalizeFlowReport(&req); err != nil {
		t.Fatalf("endpoints: %v", err)
	}
	if req.Table == nil || req.Table.SortBy != "maximum" || req.Table.Limit != req.TopN {
		t.Fatalf("endpoint table default not applied: %+v", req.Table)
	}

	for _, tc := range []struct {
		name string
		req  flowReportRequest
	}{
		{"missing side", flowReportRequest{Kind: flowReportEndpoints, From: from, To: to}},
		{"bad side", flowReportRequest{Kind: flowReportEndpoints, Side: "middle", From: from, To: to}},
		{"side on overview", flowReportRequest{Kind: flowReportOverview, Side: "source", From: from, To: to}},
		{"table on overview", flowReportRequest{Kind: flowReportOverview, Table: &flowTableRequest{Limit: 10}, From: from, To: to}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.req
			if err := normalizeFlowReport(&r); err == nil {
				t.Fatalf("%s: expected error", tc.name)
			}
		})
	}
}

// TestFlowReportEndpointAddresses covers both extraction paths: the table items and
// the dimension-points fallback.
func TestFlowReportEndpointAddresses(t *testing.T) {
	tablePanel := flowReportPanel{ID: "endpoint", Status: "ready", Data: rawJSON(t, map[string]any{
		"table": flowTablePage{Items: []flowTableRow{
			{Path: []string{"10.0.0.2"}}, {Path: []string{"10.0.0.1"}}, {Path: []string{""}},
		}},
	})}
	if got := flowReportEndpointAddresses([]flowReportPanel{tablePanel}, "endpoint"); !equalStrings(got, []string{"10.0.0.1", "10.0.0.2"}) {
		t.Fatalf("table path = %v", got)
	}

	pointsPanel := flowReportPanel{ID: "endpoint", Status: "ready", Data: rawJSON(t, struct {
		Points []flowquery.Point `json:"points"`
	}{Points: []flowquery.Point{
		{DimensionValue: "10.0.0.9"}, {DimensionValue: "10.0.0.8"}, {DimensionValue: "x", Other: true}, {DimensionValue: "10.0.0.9"},
	}})}
	if got := flowReportEndpointAddresses([]flowReportPanel{pointsPanel}, "endpoint"); !equalStrings(got, []string{"10.0.0.8", "10.0.0.9"}) {
		t.Fatalf("points fallback = %v", got)
	}
}

// TestComposeFlowEndpointReportTable merges direction/category/business panels into
// the enriched per-endpoint table, computing category shares and residual rollups.
func TestComposeFlowEndpointReportTable(t *testing.T) {
	panels := []flowReportPanel{
		{ID: "endpoint", Status: "ready", Data: rawJSON(t, map[string]any{
			"table": flowTablePage{Items: []flowTableRow{
				{Path: []string{"10.0.0.1"}, Label: "10.0.0.1", Maximum: 100, Total: 1200},
				{Path: []string{"10.0.0.2"}, Label: "10.0.0.2", Maximum: 50, Total: 600},
			}, Limit: 10, Total: 2, FilterOptions: map[string][]flowTableFilterOption{}},
		})},
		{ID: "endpoint_in", Status: "ready", Data: directionData(
			row("10.0.0.1", 1000), row("10.0.0.2", 500))},
		{ID: "endpoint_out", Status: "ready", Data: directionData(
			row("10.0.0.1", 200), row("10.0.0.2", 100))},
		{ID: "endpoint_category_in", Status: "ready", Data: categoryData(
			summary("10.0.0.1", "on_net_local_city", 800),
			summary("10.0.0.1", "overseas", 200),
			summary("10.0.0.1", "unknown", 50),
			summary("10.0.0.2", "on_net_local_city", 500))},
		{ID: "endpoint_category_out", Status: "ready", Data: categoryData(
			summary("10.0.0.1", "overseas", 200))},
		{ID: "endpoint_business", Status: "ready", Data: businessData(
			[]string{"10.0.0.1", "web"}, []string{"10.0.0.1", "db"}, []string{"10.0.0.2", "web"})},
	}

	merged, err := composeFlowEndpointReportTable(panels, flowTableRequest{SortBy: "maximum", SortDirection: "desc", Limit: 10})
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	var envelope struct {
		Table flowEndpointReportPage `json:"table"`
	}
	if err := json.Unmarshal(endpointPanelData(t, merged), &envelope); err != nil {
		t.Fatalf("decode merged: %v", err)
	}
	page := envelope.Table
	if page.Total != 2 || len(page.Items) != 2 {
		t.Fatalf("page = %+v", page)
	}
	// Sorted by maximum desc: 10.0.0.1 first.
	first := page.Items[0]
	if endpointAddress(first.flowTableRow) != "10.0.0.1" {
		t.Fatalf("first row = %s", endpointAddress(first.flowTableRow))
	}
	if first.Inbound.Total != 1000 || first.Outbound.Total != 200 {
		t.Fatalf("direction summary = %+v / %+v", first.Inbound, first.Outbound)
	}
	local := first.Categories["on_net_local_city"]
	if local.Inbound != 800 || local.InboundShare == nil || *local.InboundShare != 0.8 {
		t.Fatalf("on_net_local_city = %+v", local)
	}
	overseas := first.Categories["overseas"]
	if overseas.Inbound != 200 || overseas.Outbound != 200 || overseas.OutboundShare == nil || *overseas.OutboundShare != 1.0 {
		t.Fatalf("overseas = %+v", overseas)
	}
	if first.Residual.Inbound != 50 || first.Residual.InboundShare == nil || *first.Residual.InboundShare != 0.05 {
		t.Fatalf("residual = %+v", first.Residual)
	}
	if !equalStrings(first.Businesses, []string{"db", "web"}) {
		t.Fatalf("businesses = %v", first.Businesses)
	}
	if second := page.Items[1]; endpointAddress(second.flowTableRow) != "10.0.0.2" || !equalStrings(second.Businesses, []string{"web"}) {
		t.Fatalf("second row = %s biz=%v", endpointAddress(second.flowTableRow), second.Businesses)
	}
}

func rawJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

func row(address string, total float64) flowTableRow {
	return flowTableRow{Path: []string{address}, Label: address, Total: total}
}

func directionData(rows ...flowTableRow) json.RawMessage {
	data, _ := json.Marshal(map[string]any{"table": flowTablePage{Items: rows}})
	return data
}

func summary(address, category string, total float64) endpointCategorySummaryEntry {
	return endpointCategorySummaryEntry{Address: address, Category: category, Total: total}
}

func categoryData(entries ...endpointCategorySummaryEntry) json.RawMessage {
	data, _ := json.Marshal(struct {
		Summaries []endpointCategorySummaryEntry `json:"summaries"`
	}{Summaries: entries})
	return data
}

func businessData(pairs ...[]string) json.RawMessage {
	points := make([]flowquery.JointPoint, 0, len(pairs))
	for _, pair := range pairs {
		points = append(points, flowquery.JointPoint{DimensionValues: pair})
	}
	data, _ := json.Marshal(struct {
		Points []flowquery.JointPoint `json:"points"`
	}{Points: points})
	return data
}

func endpointPanelData(t *testing.T, panels []flowReportPanel) json.RawMessage {
	t.Helper()
	for _, panel := range panels {
		if panel.ID == "endpoint" {
			return panel.Data
		}
	}
	t.Fatalf("endpoint panel missing")
	return nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
