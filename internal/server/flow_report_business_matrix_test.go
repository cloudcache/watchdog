// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

// TestComposeFlowBusinessMatrixTable merges the business_category_in/out joint
// panels into a business×category matrix with category shares and residual rollup.
func TestComposeFlowBusinessMatrixTable(t *testing.T) {
	bucket := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	jp := func(business, category string, value float64) flowquery.JointPoint {
		return flowquery.JointPoint{Bucket: bucket, DimensionValues: []string{business, category}, Value: value}
	}
	panels := []flowReportPanel{
		{ID: "total", Status: "ready"},
		{ID: "business_category_in", Status: "ready", Data: matrixPanelData(t,
			jp("web", "on_net_local_city", 800), jp("web", "overseas", 200), jp("web", "unknown", 50),
			jp("db", "on_net_local_city", 500))},
		{ID: "business_category_out", Status: "ready", Data: matrixPanelData(t,
			jp("web", "overseas", 100))},
	}

	merged, err := composeFlowBusinessMatrixTable(panels, flowTableRequest{SortBy: "total", SortDirection: "desc", Limit: 10})
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	var envelope struct {
		MatrixTable flowBusinessMatrixPage `json:"matrix_table"`
	}
	for _, panel := range merged {
		if panel.ID == "business_category_in" {
			if err := json.Unmarshal(panel.Data, &envelope); err != nil {
				t.Fatalf("decode: %v", err)
			}
		}
	}
	page := envelope.MatrixTable
	if page.Total != 2 || len(page.Items) != 2 {
		t.Fatalf("page = %+v", page)
	}
	// Sorted by total (in+out) desc: web (1150) before db (500).
	web := page.Items[0]
	if web.Business != "web" {
		t.Fatalf("first business = %s", web.Business)
	}
	if web.Total.Inbound != 1050 || web.Total.Outbound != 100 {
		t.Fatalf("web total = %+v", web.Total)
	}
	if web.Categories["on_net_local_city"].Inbound != 800 || web.Categories["overseas"].Outbound != 100 {
		t.Fatalf("web categories = %+v", web.Categories)
	}
	if web.Residual.Inbound != 50 {
		t.Fatalf("web residual = %+v", web.Residual)
	}
	if page.Items[1].Business != "db" {
		t.Fatalf("second business = %s", page.Items[1].Business)
	}
}

func matrixPanelData(t *testing.T, points ...flowquery.JointPoint) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(map[string]any{"points": points})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}
