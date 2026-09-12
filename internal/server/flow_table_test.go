// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

// TestBuildFlowTableAggregates groups points by dimension path/version, sums the
// additive total, derives last/max/average, and ranks rows by the sort field.
func TestBuildFlowTableAggregates(t *testing.T) {
	t0 := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	points := []flowTablePoint{
		{bucket: t0, path: []string{"tcp"}, dimensionSnapshotID: "s1", classificationVersion: 1, value: 100, received: 10, quality: 8},
		{bucket: t1, path: []string{"tcp"}, dimensionSnapshotID: "s1", classificationVersion: 1, value: 300, received: 10, quality: 9},
		{bucket: t0, path: []string{"udp"}, dimensionSnapshotID: "s1", classificationVersion: 1, value: 50, received: 5},
	}
	page := buildFlowTable(points, flowTablePlan{}, "bytes", flowTableRequest{Limit: 10, SortBy: "total", SortDirection: "desc"})
	if page.Total != 2 || len(page.Items) != 2 {
		t.Fatalf("page = %+v", page)
	}
	tcp := page.Items[0]
	if tcp.Label != "tcp" || tcp.Total != 400 || tcp.Maximum != 300 || tcp.Last != 300 || tcp.Average != 200 {
		t.Fatalf("tcp row = %+v", tcp)
	}
	if tcp.QualityRecordRatio != 17.0/20.0 {
		t.Fatalf("tcp quality ratio = %v", tcp.QualityRecordRatio)
	}
	if page.Items[1].Label != "udp" || page.Items[1].Total != 50 {
		t.Fatalf("udp row = %+v", page.Items[1])
	}
}

// TestMarshalFlowAggregateResultIncludesTable confirms the aggregate marshaler
// embeds the composed table (and tolerates a nil geo service).
func TestMarshalFlowAggregateResultIncludesTable(t *testing.T) {
	t0 := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	result := flowquery.Result{
		Metric:    flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBytes, Unit: "bytes"},
		Dimension: flowquery.DimensionDefinition{Kind: flowquery.DimensionProtocol},
		Points: []flowquery.Point{
			{Bucket: t0, DimensionValue: "tcp", Value: 100, ReceivedRecords: 10, DimensionSnapshotID: "s1", ClassificationVersion: 1},
			{Bucket: t0.Add(time.Hour), DimensionValue: "tcp", Value: 300, ReceivedRecords: 10, DimensionSnapshotID: "s1", ClassificationVersion: 1},
		},
	}
	req := &flowTableRequest{Limit: 10, SortBy: "total", SortDirection: "desc"}
	raw, err := marshalFlowAggregateResult(result, req, nil)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out struct {
		Table flowTablePage `json:"table"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v (%s)", err, raw)
	}
	if out.Table.Total != 1 || len(out.Table.Items) != 1 || out.Table.Items[0].Total != 400 {
		t.Fatalf("table = %+v", out.Table)
	}
}

// TestNormalizeFlowTableRequest applies defaults and rejects unsupported fields.
func TestNormalizeFlowTableRequest(t *testing.T) {
	req := &flowTableRequest{Limit: 25}
	if err := normalizeFlowTableRequest(req); err != nil || req.SortBy != "maximum" || req.SortDirection != "desc" {
		t.Fatalf("defaults: err=%v req=%+v", err, req)
	}
	if err := normalizeFlowTableRequest(&flowTableRequest{Limit: 25, SortBy: "bogus"}); err == nil {
		t.Fatal("unsupported sort field must error")
	}
	if err := normalizeFlowTableRequest(&flowTableRequest{Limit: 0}); err == nil {
		t.Fatal("limit 0 must error")
	}
}
