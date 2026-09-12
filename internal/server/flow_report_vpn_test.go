// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

// TestNormalizeFlowReportVPN: vpn pins estimated_bytes, rejects other metrics, and
// validates its named distribution tables (rejecting tables elsewhere).
func TestNormalizeFlowReportVPN(t *testing.T) {
	from, to := reportWindow()

	req := flowReportRequest{Kind: flowReportVPN, From: from, To: to}
	if err := normalizeFlowReport(&req); err != nil {
		t.Fatalf("vpn: %v", err)
	}
	if req.Metric != flowquery.MetricEstimatedBytes {
		t.Fatalf("vpn metric default = %s", req.Metric)
	}

	withTable := flowReportRequest{Kind: flowReportVPN, From: from, To: to,
		Tables: map[string]*flowTableRequest{"port_distribution": {Limit: 10}}}
	if err := normalizeFlowReport(&withTable); err != nil {
		t.Fatalf("vpn table: %v", err)
	}
	if withTable.Tables["port_distribution"].SortBy != "bytes" {
		t.Fatalf("distribution default sort = %s", withTable.Tables["port_distribution"].SortBy)
	}

	for _, tc := range []struct {
		name string
		req  flowReportRequest
	}{
		{"wrong metric", flowReportRequest{Kind: flowReportVPN, Metric: flowquery.MetricEstimatedBPS, From: from, To: to}},
		{"bad table id", flowReportRequest{Kind: flowReportVPN, From: from, To: to, Tables: map[string]*flowTableRequest{"bogus": {Limit: 10}}}},
		{"tables on overview", flowReportRequest{Kind: flowReportOverview, From: from, To: to, Tables: map[string]*flowTableRequest{"port_distribution": {Limit: 10}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.req
			if err := normalizeFlowReport(&r); err == nil {
				t.Fatalf("%s: expected error", tc.name)
			}
		})
	}
}

// TestReportPanelSpecsVPN: the vpn kind resolves to just the total headline; the
// findings-backed panels are appended by the orchestrator hook, not the spec loop.
func TestReportPanelSpecsVPN(t *testing.T) {
	from, to := reportWindow()
	specs := reportPanelSpecs(flowReportRequest{Kind: flowReportVPN, TopN: 20, From: from, To: to})
	if ids := panelIDs(specs); !reflect.DeepEqual(ids, []string{"total"}) {
		t.Fatalf("vpn panels = %v", ids)
	}
}

// TestSummarizeVPNFindings aggregates hosts, ports, types, trend and versions from
// the materialized findings, including the evidence-rule type fallback.
func TestSummarizeVPNFindings(t *testing.T) {
	from, to := reportWindow()
	items := []vpnFindingDTO{
		{LocalIP: "10.0.0.1", RiskLevel: "high", LocalToRemoteBytes: 100, RemoteToLocalBytes: 900,
			PrimaryRemotePort: 443, DecisionRuleID: "rule-a", CompleteRatio: 1, RuleSetVersion: "v1", SourceGeneration: 5, WindowEnd: from.Add(10 * time.Minute)},
		{LocalIP: "10.0.0.1", RiskLevel: "low", LocalToRemoteBytes: 50, RemoteToLocalBytes: 50,
			PrimaryRemotePort: 443, DecisionRuleID: "rule-a", CompleteRatio: 0.8, RuleSetVersion: "v1", SourceGeneration: 5, WindowEnd: from.Add(10 * time.Minute)},
		{LocalIP: "10.0.0.2", RiskLevel: "critical", LocalToRemoteBytes: 200, RemoteToLocalBytes: 0,
			PrimaryRemotePort: 1194, Evidence: json.RawMessage(`[{"rule_id":"rule-b","contribution":10}]`),
			CompleteRatio: 1, RuleSetVersion: "v2", SourceGeneration: 6, WindowEnd: from.Add(20 * time.Minute)},
	}
	s := summarizeVPNFindings(items, from, to, 100)

	if s.FindingCount != 3 || s.SuspectedHosts != 2 || s.HighRiskHosts != 2 || s.ActivePorts != 2 {
		t.Fatalf("counts = %+v", s)
	}
	if s.InboundBytes != 950 || s.OutboundBytes != 350 || s.TotalBytes != 1300 {
		t.Fatalf("bytes = in=%d out=%d total=%d", s.InboundBytes, s.OutboundBytes, s.TotalBytes)
	}
	if s.MinimumCompleteRatio != 0.8 {
		t.Fatalf("min complete = %v", s.MinimumCompleteRatio)
	}
	// Ports sort by bytes desc: 443 (1100) then 1194 (200).
	if len(s.PortDistribution) != 2 || s.PortDistribution[0].Value != "443" || s.PortDistribution[0].Count != 2 || s.PortDistribution[0].Bytes != 1100 {
		t.Fatalf("ports = %+v", s.PortDistribution)
	}
	// Types: rule-a (decision rule) and rule-b (evidence fallback).
	if len(s.TypeDistribution) != 2 || s.TypeDistribution[0].Value != "rule-a" || s.TypeDistribution[1].Value != "rule-b" {
		t.Fatalf("types = %+v", s.TypeDistribution)
	}
	if !reflect.DeepEqual(s.RuleSetVersions, []string{"v1", "v2"}) || !reflect.DeepEqual(s.SourceGenerations, []uint64{5, 6}) {
		t.Fatalf("versions = %v gens = %v", s.RuleSetVersions, s.SourceGenerations)
	}
	// Trend: two buckets, oldest first, with per-direction bytes.
	if len(s.Trend) != 2 || !s.Trend[0].Bucket.Before(s.Trend[1].Bucket) {
		t.Fatalf("trend = %+v", s.Trend)
	}
	if s.Trend[0].InboundBytes != 950 || s.Trend[0].OutboundBytes != 150 || s.Trend[1].OutboundBytes != 200 {
		t.Fatalf("trend bytes = %+v", s.Trend)
	}
}

// TestOverseasVPNShareNumerator excludes unknown-Geo (counted separately) and CN
// remotes from the overseas numerator, mapping directions and tracking completeness.
func TestOverseasVPNShareNumerator(t *testing.T) {
	findings := []vpnFindingDTO{
		{RemoteCountry: "US", LocalToRemoteBytes: 100, RemoteToLocalBytes: 900, CompleteRatio: 1},
		{RemoteCountry: "cn", LocalToRemoteBytes: 500, RemoteToLocalBytes: 500, CompleteRatio: 1},      // CN excluded
		{RemoteCountry: "", LocalToRemoteBytes: 30, RemoteToLocalBytes: 70, CompleteRatio: 0.5},        // unknown geo
		{RemoteCountry: "_UNASSIGNED", LocalToRemoteBytes: 1, RemoteToLocalBytes: 2, CompleteRatio: 1}, // unknown geo
		{RemoteCountry: "JP", LocalToRemoteBytes: 10, RemoteToLocalBytes: 40, CompleteRatio: 0.9},
	}
	vpnIn, vpnOut, unknownGeo, minComplete := overseasVPNShareNumerator(findings)
	if vpnIn != 940 || vpnOut != 110 { // US + JP: in=900+40, out=100+10
		t.Fatalf("numerator = in=%d out=%d", vpnIn, vpnOut)
	}
	if unknownGeo != 103 { // (30+70) + (1+2)
		t.Fatalf("unknownGeo = %d", unknownGeo)
	}
	if minComplete != 0.5 {
		t.Fatalf("minComplete = %v", minComplete)
	}
}

// TestFilterFindingsByPeakWindows keeps only findings whose window end lands inside
// a peak time-of-day window (all-days window isolates the time-of-day test).
func TestFilterFindingsByPeakWindows(t *testing.T) {
	day := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	items := []vpnFindingDTO{
		{ID: "in", WindowEnd: day.Add(10 * time.Hour)},
		{ID: "out", WindowEnd: day.Add(20 * time.Hour)},
	}
	windows := []flowquery.LocalTimeWindow{{Days: []uint8{1, 2, 3, 4, 5, 6, 7}, StartLocal: "09:00", EndLocal: "17:00"}}
	filtered := filterFindingsByPeakWindows(items, windows, "UTC")
	if len(filtered) != 1 || filtered[0].ID != "in" {
		t.Fatalf("filtered = %+v", filtered)
	}
	if len(filterFindingsByPeakWindows(items, nil, "UTC")) != 2 {
		t.Fatalf("no-window filter must not drop rows")
	}
}

// TestBuildFlowVPNDistributionTable filters, sorts and paginates a distribution.
func TestBuildFlowVPNDistributionTable(t *testing.T) {
	source := []flowReportCount{{Value: "443", Count: 2, Bytes: 1100}, {Value: "1194", Count: 1, Bytes: 200}}

	asc := buildFlowVPNDistributionTable(source, flowTableRequest{SortBy: "bytes", SortDirection: "asc", Limit: 10})
	if len(asc.Items) != 2 || asc.Items[0].Value != "1194" || asc.Items[1].Value != "443" {
		t.Fatalf("asc sort = %+v", asc.Items)
	}
	filtered := buildFlowVPNDistributionTable(source, flowTableRequest{SortBy: "bytes", SortDirection: "desc", Limit: 10,
		Filters: map[string][]string{"value": {"443"}}})
	if len(filtered.Items) != 1 || filtered.Items[0].Value != "443" || filtered.Total != 1 {
		t.Fatalf("filtered = %+v", filtered.Items)
	}
}
