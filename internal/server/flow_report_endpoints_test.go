// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

func TestCombineEndpointJointSegmentsPreservesAggregatePrefixAndRawTail(t *testing.T) {
	from := time.Date(2026, 9, 21, 7, 0, 0, 0, time.UTC)
	point := func(bucket time.Time, value float64, version uint32) flowquery.JointPoint {
		return flowquery.JointPoint{
			Bucket: bucket, DimensionValues: []string{"2001:db8::1", "overseas"}, Value: value,
			DimensionSnapshotID: "snapshot", GeoVersion: "geo", ClassificationVersion: version,
		}
	}
	combined := combineEndpointJointSegments([]flowquery.JointResult{
		{Points: []flowquery.JointPoint{point(from, 1, 1)}, View: flowquery.ViewCustomer},
		{Points: []flowquery.JointPoint{point(from.Add(23*time.Hour), 2, 2)}, View: flowquery.ViewCustomer},
	}, flowquery.AggregatePlan{
		RequestedFrom: from, RequestedTo: from.Add(24 * time.Hour),
		EffectiveFrom: from, EffectiveTo: from.Add(24 * time.Hour), TargetPoints: 300,
	}, time.Hour)
	if len(combined.Points) != 2 || !combined.Points[0].Bucket.Equal(from) ||
		combined.Plan.Source != "flow_aggregate_1h+flow_records" || combined.Plan.StepSeconds != 3600 ||
		!combined.MixedVersions || combined.VersionCount != 2 {
		t.Fatalf("combined endpoint segments=%+v", combined)
	}
}

// TestNormalizeFlowReportEndpoints: endpoints require a side, default a table, and
// reject a table on any other kind.
func TestNormalizeFlowReportEndpoints(t *testing.T) {
	from, to := reportWindow()

	req := flowReportRequest{Kind: flowReportEndpoints, Side: "source", From: from, To: to}
	if err := normalizeFlowReport(&req); err != nil {
		t.Fatalf("endpoints: %v", err)
	}
	if req.Table == nil || req.Table.SortBy != "last" || req.Table.Limit != req.TopN || req.Metric != flowquery.MetricEstimatedBPS {
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
		{"endpoint bytes metric", flowReportRequest{Kind: flowReportEndpoints, Side: "source", Metric: flowquery.MetricEstimatedBytes, From: from, To: to}},
		{"destination source-only column", flowReportRequest{Kind: flowReportEndpoints, Side: "destination", Table: &flowTableRequest{SortBy: "p95", Limit: 10}, From: from, To: to}},
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

func TestFlowEndpointCategoryChunksBoundEachJointQuery(t *testing.T) {
	categories := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for _, tc := range []struct {
		addresses int
		chunks    int
	}{
		{addresses: 8, chunks: 1},
		{addresses: 20, chunks: 2},
		{addresses: 100, chunks: 10},
	} {
		chunks := flowEndpointCategoryChunks(tc.addresses, categories)
		if len(chunks) != tc.chunks {
			t.Fatalf("addresses=%d chunks=%d want=%d", tc.addresses, len(chunks), tc.chunks)
		}
		for _, chunk := range chunks {
			if len(chunk)*tc.addresses > flowquery.MaxTopN {
				t.Fatalf("addresses=%d chunk=%d exceeds top-n bound", tc.addresses, len(chunk))
			}
		}
	}
}

func TestComposeSourceEndpointReportTable(t *testing.T) {
	panels := []flowReportPanel{
		{ID: "endpoint", Status: "ready", Data: rawJSON(t, map[string]any{
			"table": flowTablePage{Items: []flowTableRow{
				{Path: []string{"10.0.0.1"}, Label: "10.0.0.1", Last: 100, P95: 95},
				{Path: []string{"10.0.0.2"}, Label: "10.0.0.2", Last: 50, P95: 45},
			}, Limit: 10, Total: 2, FilterOptions: map[string][]flowTableFilterOption{}},
		})},
		{ID: "endpoint_category_in", Status: "ready", Data: categoryData(
			summary("10.0.0.1", "on_net_local_city", 80),
			summary("10.0.0.1", "overseas", 20),
			summary("10.0.0.1", "unknown", 5),
			summary("10.0.0.2", "on_net_local_city", 50))},
		{ID: "endpoint_category_out", Status: "ready", Data: categoryData(
			summary("10.0.0.1", "overseas", 20))},
		{ID: "endpoint_business", Status: "ready", Data: businessData(
			[]string{"10.0.0.1", "web"}, []string{"10.0.0.1", "db"}, []string{"10.0.0.2", "web"})},
	}

	merged, err := composeFlowEndpointReportTable(panels, flowTableRequest{SortBy: "last", SortDirection: "desc", Limit: 10}, "source")
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
	first := page.Items[0]
	if first.Address != "10.0.0.1" || first.Bandwidth != 100 || first.P95 == nil || *first.P95 != 95 {
		t.Fatalf("first row = %+v", first)
	}
	if len(first.Classifications) != 3 || first.Classifications[0] != (flowEndpointClassification{Category: "on_net_local_city", Inbound: 80}) ||
		first.Classifications[1] != (flowEndpointClassification{Category: "overseas", Inbound: 20, Outbound: 20}) ||
		first.Classifications[2] != (flowEndpointClassification{Category: "unknown", Inbound: 5}) {
		t.Fatalf("classifications = %+v", first.Classifications)
	}
	if !equalStrings(first.Businesses, []string{"db", "web"}) {
		t.Fatalf("businesses = %v", first.Businesses)
	}
	if second := page.Items[1]; second.Address != "10.0.0.2" || !equalStrings(second.Businesses, []string{"web"}) {
		t.Fatalf("second row = %s biz=%v", second.Address, second.Businesses)
	}
}

func TestComposeDestinationEndpointReportTableOnlyReturnsAddressAndBandwidth(t *testing.T) {
	panels := []flowReportPanel{{ID: "endpoint", Status: "ready", Data: rawJSON(t, map[string]any{
		"table": flowTablePage{Items: []flowTableRow{{Path: []string{"203.0.113.10"}, Last: 12_500_000_000, P95: 14_000_000_000}}},
	})}}
	merged, err := composeFlowEndpointReportTable(panels, flowTableRequest{SortBy: "last", SortDirection: "desc", Limit: 10}, "destination")
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(endpointPanelData(t, merged), &envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var page struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(envelope["table"], &page); err != nil || len(page.Items) != 1 {
		t.Fatalf("page=%v err=%v", page, err)
	}
	row := page.Items[0]
	if row["address"] != "203.0.113.10" || row["bandwidth"] != float64(12_500_000_000) || len(row) != 2 {
		t.Fatalf("destination row=%v", row)
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

func summary(address, category string, bandwidth float64) endpointCategorySummaryEntry {
	return endpointCategorySummaryEntry{Address: address, Category: category, Bandwidth: bandwidth}
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
