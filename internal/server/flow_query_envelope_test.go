// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

// TestFlowQueryEnvelopeDecode: the hub /flow/query wire envelope (dataset + gateway
// fields + nested parameters, incl. the special-mode fields) strict-decodes.
func TestFlowQueryEnvelopeDecode(t *testing.T) {
	from := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	body := `{
		"dataset": "flow.traffic",
		"from": "` + from.Format(time.RFC3339Nano) + `",
		"to": "` + from.Add(time.Hour).Format(time.RFC3339Nano) + `",
		"step_seconds": 0, "limit": 250000, "value_layer": "customer",
		"parameters": {
			"metric": "estimated_bps", "dimension": "category",
			"filters": {"businesses": ["b1"]}, "top_n": 20, "include_other": true,
			"target_points": 300, "timezone": "UTC",
			"operator_selection": {"operator_id": "op-1"},
			"address_set_endpoint": "either",
			"address_set_filter": {"include_any": ["set-a"], "exclude_any": ["set-b"]},
			"direction_split": true,
			"table": {"sort_by": "maximum", "sort_direction": "desc", "limit": 25, "offset": 0}
		}
	}`
	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.DisallowUnknownFields()
	var envelope flowQueryEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		t.Fatalf("strict decode of hub query envelope failed: %v", err)
	}
	if envelope.Dataset != "flow.traffic" || envelope.ValueLayer != flowquery.ViewCustomer || envelope.StepSeconds != 0 || envelope.Limit != 250000 {
		t.Fatalf("envelope = %+v", envelope)
	}
	p := envelope.Parameters
	if p.Metric != "estimated_bps" || p.Dimension != flowquery.DimensionCategory || p.TopN != 20 || !p.IncludeOther ||
		p.TargetPoints != 300 || len(p.Filters.Businesses) != 1 || p.Table == nil {
		t.Fatalf("parameters = %+v", p)
	}
	if p.Operator == nil || p.Operator.OperatorID != "op-1" || !p.DirectionSplit ||
		p.AddressSetEndpoint != "either" || p.AddressSetFilter == nil || len(p.AddressSetFilter.IncludeAny) != 1 {
		t.Fatalf("special-mode fields not decoded: %+v", p)
	}
}

// TestFlowExportCreateDispatch: the hub {query, format} export body decodes, and a
// query.parameters.report discriminates a report export from a query export.
func TestFlowExportCreateDispatch(t *testing.T) {
	from := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	base := `"query": {"dataset": "flow.traffic", "from": "` + from.Format(time.RFC3339Nano) + `",
		"to": "` + from.Add(time.Hour).Format(time.RFC3339Nano) + `", "step_seconds": 0, "limit": 250000,
		"value_layer": "customer", "parameters": {"metric": "estimated_bps", "top_n": 20, "timezone": "UTC", "target_points": 300, %s}},
		"format": "csv"`

	decode := func(inner string) createFlowExportRequest {
		decoder := json.NewDecoder(strings.NewReader("{" + strings.Replace(base, "%s", inner, 1) + "}"))
		decoder.DisallowUnknownFields()
		var req createFlowExportRequest
		if err := decoder.Decode(&req); err != nil {
			t.Fatalf("decode export body: %v", err)
		}
		return req
	}

	report := decode(`"report": {"schema_version": 1, "kind": "overview", "display_mode": "value"}`)
	if report.Query.Parameters.Report == nil {
		t.Fatalf("report export not detected")
	}
	if r := report.Query.toReportRequest(); r.Kind != flowReportOverview || r.View != flowquery.ViewCustomer || r.Metric != "estimated_bps" {
		t.Fatalf("report mapping = %+v", r)
	}

	query := decode(`"dimension": "category"`)
	if query.Query.Parameters.Report != nil {
		t.Fatalf("query export mis-detected as report")
	}
	if q := query.Query.toAggregateInput(); q.Dimension != flowquery.DimensionCategory || q.View != flowquery.ViewCustomer || q.Metric != "estimated_bps" {
		t.Fatalf("query mapping = %+v", q)
	}
}

// TestFlowQueryResultMeta exposes the hub meta fields the clients read.
func TestFlowQueryResultMeta(t *testing.T) {
	meta := flowQueryResultMeta(flowquery.ViewCustomer, "bps", "flow_1m", "UTC", 60, 0.9, true)
	for _, key := range []string{"request_id", "as_of", "source", "value_layer", "unit", "timezone", "step_seconds", "complete_ratio", "unknown_ratio", "partial"} {
		if _, ok := meta[key]; !ok {
			t.Fatalf("meta missing %q: %+v", key, meta)
		}
	}
	if meta["unit"] != "bps" || meta["value_layer"] != flowquery.ViewCustomer || meta["partial"] != true {
		t.Fatalf("meta values = %+v", meta)
	}
}
