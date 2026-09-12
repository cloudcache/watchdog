// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

var exportBucket = time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)

// TestFlowQueryExportRows flattens both aggregate (single dimension) and joint
// (multiple dimensions) query envelopes.
func TestFlowQueryExportRows(t *testing.T) {
	aggregate, _ := json.Marshal(map[string]any{
		"points":    []flowquery.Point{{Bucket: exportBucket, DimensionValue: "cat-a", Value: 10, ReceivedRecords: 5}},
		"metric":    flowquery.MetricDefinition{Name: "estimated_bps", Unit: "bps"},
		"dimension": flowquery.DimensionDefinition{Kind: flowquery.DimensionCategory},
	})
	rows, err := flowQueryExportRows(aggregate, flowExportDecoration{ValueLayer: flowquery.ViewCustomer})
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(rows) != 1 || rows[0].Metric != "estimated_bps" || rows[0].Value != 10 ||
		len(rows[0].DimensionNames) != 1 || rows[0].DimensionNames[0] != "category" || rows[0].DimensionValues[0] != "cat-a" ||
		rows[0].ValueLayer != flowquery.ViewCustomer || rows[0].ReceivedRecords != 5 {
		t.Fatalf("aggregate rows = %+v", rows)
	}

	joint, _ := json.Marshal(map[string]any{
		"points":     []flowquery.JointPoint{{Bucket: exportBucket, DimensionValues: []string{"biz-a", "cat-a"}, Value: 20}},
		"metric":     flowquery.MetricDefinition{Name: "estimated_bps", Unit: "bps"},
		"dimensions": []flowquery.DimensionDefinition{{Kind: flowquery.DimensionBusiness}, {Kind: flowquery.DimensionCategory}},
	})
	rows, err = flowQueryExportRows(joint, flowExportDecoration{ValueLayer: flowquery.ViewCustomer})
	if err != nil {
		t.Fatalf("joint: %v", err)
	}
	if len(rows) != 1 || rows[0].Value != 20 ||
		len(rows[0].DimensionNames) != 2 || rows[0].DimensionNames[1] != "category" || rows[0].DimensionValues[0] != "biz-a" {
		t.Fatalf("joint rows = %+v", rows)
	}
}

// TestFlowReportExportRows flattens ready panels, emits a status row for panels that
// are not ready, and dispatches the vpn_findings special panel.
func TestFlowReportExportRows(t *testing.T) {
	aggregate, _ := json.Marshal(map[string]any{
		"points":    []flowquery.Point{{Bucket: exportBucket, DimensionValue: "total", Value: 100}},
		"metric":    flowquery.MetricDefinition{Name: "estimated_bytes", Unit: "bytes"},
		"dimension": flowquery.DimensionDefinition{Kind: flowquery.DimensionTotal},
	})
	summary, _ := json.Marshal(flowVPNReportSummary{
		FindingCount: 3, InboundBytes: 900, OutboundBytes: 100, TotalBytes: 1000,
		PortDistribution: []flowReportCount{{Value: "443", Count: 2, Bytes: 1100}},
	})
	panels := []flowReportPanel{
		{ID: "total", Status: "ready", Data: aggregate, Meta: gin.H{"step_seconds": uint32(60), "source": "flow_1m"}},
		{ID: "category_in", Status: "unavailable", Reason: "panel query failed"},
		{ID: "vpn_findings", Status: "ready", Data: summary, Meta: gin.H{"as_of": exportBucket, "complete_ratio": 0.9, "partial": true}},
	}
	rows, err := flowReportExportRows(panels, "vpn", flowquery.ViewCustomer)
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	// total (1) + panel_status (1) + vpn summary (8 scalars + 2 distribution rows for one port) = 12.
	if len(rows) != 12 {
		t.Fatalf("row count = %d: %+v", len(rows), rows)
	}
	byPanel := map[string]int{}
	var sawStatus, sawStep bool
	for _, row := range rows {
		byPanel[row.ReportPanel]++
		if row.ReportPanel == "category_in" && row.Metric == "panel_status" && row.ReportPanelStatus == "unavailable" {
			sawStatus = true
		}
		if row.ReportPanel == "total" && row.QueryStepSeconds == 60 && row.QuerySource == "flow_1m" {
			sawStep = true
		}
		if row.ReportKind != "vpn" {
			t.Fatalf("report kind not stamped: %+v", row)
		}
	}
	if !sawStatus || !sawStep || byPanel["total"] != 1 || byPanel["vpn_findings"] != 10 {
		t.Fatalf("panels = %v status=%v step=%v", byPanel, sawStatus, sawStep)
	}
}

// TestRenderFlowExportCSV emits the fixed wide header and neutralizes injection.
func TestRenderFlowExportCSV(t *testing.T) {
	rows := []flowExportRow{{
		Bucket: exportBucket, DimensionNames: []string{"category"}, DimensionValues: []string{"=danger"},
		Metric: "estimated_bps", Unit: "bps", Value: 1.5, ValueLayer: flowquery.ViewCustomer,
	}}
	data, err := renderFlowExportCSV(rows)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	out := string(data)
	if !strings.HasPrefix(out, strings.Join(flowExportHeader, ",")+"\n") {
		t.Fatalf("header = %q", strings.SplitN(out, "\n", 2)[0])
	}
	if !strings.Contains(out, "'=danger") {
		t.Fatalf("formula not neutralized: %q", out)
	}
}

// TestRenderFlowExportParquet renders a non-empty parquet artifact.
func TestRenderFlowExportParquet(t *testing.T) {
	rows := []flowExportRow{
		{Bucket: exportBucket, DimensionNames: []string{"category"}, DimensionValues: []string{"cat-a"}, Metric: "m", Value: 1, ValueLayer: flowquery.ViewCustomer},
		{Bucket: exportBucket, DimensionNames: []string{"category"}, DimensionValues: []string{"cat-b"}, Metric: "m", Value: 2, ValueLayer: flowquery.ViewCustomer},
	}
	data, err := renderFlowExportParquet(rows)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if len(data) < 4 || string(data[:4]) != "PAR1" {
		t.Fatalf("not a parquet file: %d bytes", len(data))
	}
}

// TestFlowPanelMeta extracts the export decoration fields from a v2 panel's gin.H.
func TestFlowPanelMeta(t *testing.T) {
	step, source, asOf, completeRatio, partial := flowPanelMeta(gin.H{
		"step_seconds": uint32(300), "source": "flow_1h", "as_of": exportBucket, "complete_ratio": 0.75, "partial": true,
	})
	if step != 300 || source != "flow_1h" || !asOf.Equal(exportBucket) || completeRatio != 0.75 || !partial {
		t.Fatalf("meta = %d/%s/%v/%v/%v", step, source, asOf, completeRatio, partial)
	}
	if step, _, _, _, _ := flowPanelMeta(nil); step != 0 {
		t.Fatalf("nil meta step = %d", step)
	}
}
