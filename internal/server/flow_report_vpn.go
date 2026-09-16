// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

// flow_report_vpn.go migrates the hub's VPN report kind to Gin. The report is
// backed by the materialized flow_vpn_findings (KISS-06 Tier-1), never by a live
// ClickHouse scan: the total panel is a normal aggregate, and vpn_findings
// summarizes the findings store over the window. rule-publication and
// probe-timeline stay faithfully unavailable (deferred Tier-2). The summarize +
// distribution-table machinery is a verbatim port, retargeted onto vpnFindingDTO.

// maxVPNFindingReportRows bounds the findings scan for a single fixed report.
const maxVPNFindingReportRows uint32 = 250_000

type flowVPNReportSummary struct {
	FindingCount         int                  `json:"finding_count"`
	SuspectedHosts       int                  `json:"suspected_hosts"`
	HighRiskHosts        int                  `json:"high_risk_hosts"`
	ActivePorts          int                  `json:"active_ports"`
	InboundBytes         uint64               `json:"inbound_bytes"`
	OutboundBytes        uint64               `json:"outbound_bytes"`
	TotalBytes           uint64               `json:"total_bytes"`
	PortDistribution     []flowReportCount    `json:"port_distribution"`
	TypeDistribution     []flowReportCount    `json:"type_distribution"`
	Trend                []flowVPNTrend       `json:"trend"`
	RuleSetVersions      []string             `json:"rule_set_versions"`
	SourceGenerations    []uint64             `json:"source_generations"`
	MinimumCompleteRatio float64              `json:"minimum_complete_ratio"`
	PortTable            *flowReportCountPage `json:"port_table,omitempty"`
	TypeTable            *flowReportCountPage `json:"type_table,omitempty"`
}

type flowReportCount struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
	Bytes uint64 `json:"bytes"`
}

type flowVPNTrend struct {
	Bucket        time.Time `json:"bucket"`
	InboundBytes  uint64    `json:"inbound_bytes"`
	OutboundBytes uint64    `json:"outbound_bytes"`
}

type flowReportCountPage struct {
	Items         []flowReportCount                  `json:"items"`
	Total         int                                `json:"total"`
	Limit         uint16                             `json:"limit"`
	Offset        uint32                             `json:"offset"`
	FilterOptions map[string][]flowTableFilterOption `json:"filter_options"`
}

var flowVPNDistributionFields = map[string]struct{}{"value": {}, "count": {}, "bytes": {}}

// normalizeFlowReportTables validates the named report tables: overview's
// business_matrix and the vpn report's port/type distributions.
func normalizeFlowReportTables(req *flowReportRequest) error {
	if len(req.Tables) == 0 {
		return nil
	}
	for id, table := range req.Tables {
		if table == nil {
			return fmt.Errorf("report table %q is required", id)
		}
		var fields map[string]struct{}
		switch req.Kind {
		case flowReportOverview:
			if id != "business_matrix" {
				return fmt.Errorf("report tables contains unsupported table %q", id)
			}
			fields = flowBusinessMatrixFields
			if table.SortBy == "" {
				table.SortBy = "total"
			}
		case flowReportVPN:
			if id != "port_distribution" && id != "type_distribution" {
				return fmt.Errorf("report tables contains unsupported table %q", id)
			}
			fields = flowVPNDistributionFields
			if table.SortBy == "" {
				table.SortBy = "bytes"
			}
		default:
			return fmt.Errorf("report tables are unsupported for report kind %q", req.Kind)
		}
		if err := normalizeFlowTableRequestWithFields(table, fields); err != nil {
			return fmt.Errorf("report table %q: %w", id, err)
		}
	}
	return nil
}

// vpnReportPanels appends the vpn report's findings-backed panels: vpn_findings
// (summarized from the store) plus the two faithfully-unavailable Tier-2 panels.
func (s *Server) vpnReportPanels(ctx context.Context, req flowReportRequest, now time.Time) ([]flowReportPanel, error) {
	items, err := s.listVPNFindingsForReport(ctx, req.From, req.To, maxVPNFindingReportRows)
	if err != nil {
		return nil, fmt.Errorf("load VPN findings for report: %w", err)
	}
	items = filterFindingsByPeakWindows(items, req.PeakWindows, req.Timezone)
	summary := summarizeVPNFindings(items, req.From, req.To, req.TargetPoints)
	if table := req.Tables["port_distribution"]; table != nil {
		summary.PortTable = buildFlowVPNDistributionTable(summary.PortDistribution, *table)
	}
	if table := req.Tables["type_distribution"]; table != nil {
		summary.TypeTable = buildFlowVPNDistributionTable(summary.TypeDistribution, *table)
	}
	data, err := json.Marshal(summary)
	if err != nil {
		return nil, fmt.Errorf("marshal VPN report: %w", err)
	}
	return []flowReportPanel{
		{ID: "vpn_findings", Status: "ready", Data: data, Meta: gin.H{
			"timezone": req.Timezone, "as_of": now,
			"versions": gin.H{
				"vpn_rule_set_versions":  strings.Join(summary.RuleSetVersions, ","),
				"vpn_source_generations": joinUint64s(summary.SourceGenerations),
			},
			"complete_ratio": summary.MinimumCompleteRatio,
			"partial":        summary.MinimumCompleteRatio < 1,
		}},
		{ID: "vpn_rule_publication", Status: "unavailable", Reason: "immutable VPN rule-set publication is not active"},
		{ID: "vpn_probe_timeline", Status: "unavailable", Reason: "authorized active-probe orchestration is not configured"},
	}, nil
}

// filterFindingsByPeakWindows keeps only findings whose window end falls inside one
// of the report's peak time-of-day windows, mirroring the aggregate/joint panels'
// TimeWindows restriction. No windows means no restriction.
func filterFindingsByPeakWindows(items []vpnFindingDTO, windows []flowquery.LocalTimeWindow, timezone string) []vpnFindingDTO {
	if len(windows) == 0 {
		return items
	}
	filtered := items[:0]
	for _, item := range items {
		if flowquery.InLocalTimeWindows(item.WindowEnd, windows, timezone) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

// listVPNFindingsForReport reads the materialized findings whose window ends within
// [from, to), oldest first, for report summarization.
func (s *Server) listVPNFindingsForReport(ctx context.Context, from, to time.Time, limit uint32) ([]vpnFindingDTO, error) {
	query := vpnFindingSelect + " WHERE window_end>=? AND window_end<? ORDER BY window_end ASC, id ASC LIMIT ?"
	rows, err := s.db.QueryContext(ctx, query, from.UTC(), to.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]vpnFindingDTO, 0, 256)
	for rows.Next() {
		item, err := scanVPNFinding(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func summarizeVPNFindings(items []vpnFindingDTO, from, to time.Time, targetPoints uint16) flowVPNReportSummary {
	hosts, highRisk, ports := map[string]struct{}{}, map[string]struct{}{}, map[uint16]*flowReportCount{}
	types := map[string]*flowReportCount{}
	ruleVersions, generations := map[string]struct{}{}, map[uint64]struct{}{}
	minimumComplete := 1.0
	interval := flowReportTrendInterval(from, to, targetPoints)
	trend := map[time.Time]*flowVPNTrend{}
	result := flowVPNReportSummary{FindingCount: len(items), MinimumCompleteRatio: minimumComplete}
	for _, item := range items {
		hosts[item.LocalIP] = struct{}{}
		if item.RiskLevel == "high" || item.RiskLevel == "critical" {
			highRisk[item.LocalIP] = struct{}{}
		}
		bytes := item.LocalToRemoteBytes + item.RemoteToLocalBytes
		if item.PrimaryRemotePort != 0 {
			entry := ports[item.PrimaryRemotePort]
			if entry == nil {
				entry = &flowReportCount{Value: strconv.FormatUint(uint64(item.PrimaryRemotePort), 10)}
				ports[item.PrimaryRemotePort] = entry
			}
			entry.Count++
			entry.Bytes += bytes
		}
		kind := classifyVPNFindingType(item)
		entry := types[kind]
		if entry == nil {
			entry = &flowReportCount{Value: kind}
			types[kind] = entry
		}
		entry.Count++
		entry.Bytes += bytes
		result.InboundBytes += item.RemoteToLocalBytes
		result.OutboundBytes += item.LocalToRemoteBytes
		if item.CompleteRatio < minimumComplete {
			minimumComplete = item.CompleteRatio
		}
		if item.RuleSetVersion != "" {
			ruleVersions[item.RuleSetVersion] = struct{}{}
		}
		generations[item.SourceGeneration] = struct{}{}
		bucket := item.WindowEnd.UTC().Truncate(interval)
		point := trend[bucket]
		if point == nil {
			point = &flowVPNTrend{Bucket: bucket}
			trend[bucket] = point
		}
		point.InboundBytes += item.RemoteToLocalBytes
		point.OutboundBytes += item.LocalToRemoteBytes
	}
	result.SuspectedHosts = len(hosts)
	result.HighRiskHosts = len(highRisk)
	result.ActivePorts = len(ports)
	result.TotalBytes = result.InboundBytes + result.OutboundBytes
	result.MinimumCompleteRatio = minimumComplete
	result.PortDistribution = sortedFlowReportCounts(ports)
	result.TypeDistribution = sortedFlowReportCounts(types)
	for _, point := range trend {
		result.Trend = append(result.Trend, *point)
	}
	sort.Slice(result.Trend, func(i, j int) bool { return result.Trend[i].Bucket.Before(result.Trend[j].Bucket) })
	for version := range ruleVersions {
		result.RuleSetVersions = append(result.RuleSetVersions, version)
	}
	sort.Strings(result.RuleSetVersions)
	for generation := range generations {
		result.SourceGenerations = append(result.SourceGenerations, generation)
	}
	sort.Slice(result.SourceGenerations, func(i, j int) bool { return result.SourceGenerations[i] < result.SourceGenerations[j] })
	return result
}

func flowReportTrendInterval(from, to time.Time, targetPoints uint16) time.Duration {
	if targetPoints == 0 {
		targetPoints = flowquery.DefaultTargetPoints
	}
	duration := to.Sub(from)
	minimum := duration / time.Duration(targetPoints)
	for _, interval := range []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, 6 * time.Hour, 24 * time.Hour} {
		if minimum <= interval {
			return interval
		}
	}
	return 24 * time.Hour
}

// classifyVPNFindingType labels a finding by its terminal decision rule, else the
// highest-contribution evidence rule, else "unclassified".
func classifyVPNFindingType(item vpnFindingDTO) string {
	if ruleID := strings.TrimSpace(item.DecisionRuleID); ruleID != "" {
		return ruleID
	}
	var evidence []struct {
		RuleID       string `json:"rule_id"`
		Contribution uint16 `json:"contribution"`
	}
	if json.Unmarshal(item.Evidence, &evidence) == nil {
		sort.SliceStable(evidence, func(i, j int) bool {
			if evidence[i].Contribution == evidence[j].Contribution {
				return evidence[i].RuleID < evidence[j].RuleID
			}
			return evidence[i].Contribution > evidence[j].Contribution
		})
		for _, signal := range evidence {
			if ruleID := strings.TrimSpace(signal.RuleID); ruleID != "" {
				return ruleID
			}
		}
	}
	return "unclassified"
}

func sortedFlowReportCounts[K comparable](source map[K]*flowReportCount) []flowReportCount {
	result := make([]flowReportCount, 0, len(source))
	for _, item := range source {
		result = append(result, *item)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Bytes == result[j].Bytes {
			return result[i].Value < result[j].Value
		}
		return result[i].Bytes > result[j].Bytes
	})
	return result
}

func buildFlowVPNDistributionTable(source []flowReportCount, request flowTableRequest) *flowReportCountPage {
	options := map[string][]flowTableFilterOption{}
	for field := range flowVPNDistributionFields {
		counts := map[string]int{}
		for _, item := range source {
			counts[flowReportCountValue(item, field)]++
		}
		for value, count := range counts {
			options[field] = append(options[field], flowTableFilterOption{Value: value, Count: count})
		}
		sort.Slice(options[field], func(i, j int) bool { return options[field][i].Value < options[field][j].Value })
	}
	filtered := make([]flowReportCount, 0, len(source))
	search := strings.ToLower(request.Search)
	for _, item := range source {
		matched := true
		for field, values := range request.Filters {
			fieldMatched := len(values) == 0
			for _, value := range values {
				fieldMatched = fieldMatched || value == flowReportCountValue(item, field)
			}
			matched = matched && fieldMatched
		}
		if matched && (search == "" || strings.Contains(strings.ToLower(item.Value), search)) {
			filtered = append(filtered, item)
		}
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		comparison := strings.Compare(filtered[i].Value, filtered[j].Value)
		if request.SortBy == "count" {
			comparison = cmpFloat64(float64(filtered[i].Count), float64(filtered[j].Count))
		} else if request.SortBy == "bytes" {
			comparison = cmpFloat64(float64(filtered[i].Bytes), float64(filtered[j].Bytes))
		}
		if comparison == 0 {
			comparison = strings.Compare(filtered[i].Value, filtered[j].Value)
		}
		if request.SortDirection == "desc" {
			return comparison > 0
		}
		return comparison < 0
	})
	total := len(filtered)
	start := min(int(request.Offset), total)
	end := min(start+int(request.Limit), total)
	return &flowReportCountPage{
		Items: append([]flowReportCount(nil), filtered[start:end]...), Total: total,
		Limit: request.Limit, Offset: request.Offset, FilterOptions: options,
	}
}

func flowReportCountValue(item flowReportCount, field string) string {
	switch field {
	case "value":
		return item.Value
	case "count":
		return strconv.FormatUint(item.Count, 10)
	case "bytes":
		return strconv.FormatUint(item.Bytes, 10)
	default:
		return ""
	}
}

func joinUint64s(values []uint64) string {
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = strconv.FormatUint(value, 10)
	}
	return strings.Join(parts, ",")
}

type flowOverseasVPNSharePoint struct {
	Direction   string    `json:"direction"`
	VPNBytes    uint64    `json:"vpn_bytes"`
	TotalBytes  float64   `json:"total_bytes"`
	Ratio       *float64  `json:"ratio,omitempty"`
	UnknownGeo  uint64    `json:"unknown_geo_bytes"`
	FindingRows uint64    `json:"finding_rows"`
	GeneratedAt time.Time `json:"generated_at"`
}

// runOverseasVPNSharePanel computes the overseas VPN traffic share: the numerator
// is materialized-findings bytes (excluding unknown-geo and CN remotes), the
// denominator the overseas total bytes per direction. v2 has no single-query
// DirectionSplit, so the denominator is two per-direction DimensionTotal queries.
//
// Faithful-migration note: the hub additionally reconciled the numerator and
// denominator against a shared immutable geo/classification version. The v2
// findings schema does not carry those version columns, so that cross-source guard
// is dropped here; the panel still surfaces completeness and unknown-geo warnings.
func (s *Server) runOverseasVPNSharePanel(ctx context.Context, scope flowquery.Scope, view flowquery.View, req flowReportRequest, now time.Time) (flowReportPanel, error) {
	totals := map[string]float64{}
	for _, direction := range []string{"in", "out"} {
		total, err := s.overseasDirectionTotalBytes(ctx, scope, view, req, direction, now)
		if err != nil {
			return flowReportPanel{}, err
		}
		totals[direction] = total
	}
	findings, err := s.listVPNFindingsForReport(ctx, req.From, req.To, maxVPNFindingReportRows)
	if err != nil {
		return flowReportPanel{}, fmt.Errorf("load overseas VPN findings: %w", err)
	}
	findings = filterFindingsByPeakWindows(findings, req.PeakWindows, req.Timezone)
	vpnIn, vpnOut, unknownGeo, minimumComplete := overseasVPNShareNumerator(findings)
	vpn := map[string]uint64{"in": vpnIn, "out": vpnOut}
	makePoint := func(direction string, numerator uint64, total float64) flowOverseasVPNSharePoint {
		point := flowOverseasVPNSharePoint{
			Direction: direction, VPNBytes: numerator, TotalBytes: total, UnknownGeo: unknownGeo,
			FindingRows: uint64(len(findings)), GeneratedAt: now,
		}
		if total > 0 {
			ratio := float64(numerator) / total
			point.Ratio = &ratio
		}
		return point
	}
	rows := []flowOverseasVPNSharePoint{
		makePoint("in", vpn["in"], totals["in"]),
		makePoint("out", vpn["out"], totals["out"]),
		makePoint("combined", vpn["in"]+vpn["out"], totals["in"]+totals["out"]),
	}
	data, err := json.Marshal(struct {
		Points []flowOverseasVPNSharePoint `json:"points"`
	}{Points: rows})
	if err != nil {
		return flowReportPanel{}, err
	}
	warnings := []string{}
	if unknownGeo > 0 {
		warnings = append(warnings, "VPN findings with unknown remote Geo are excluded from the overseas numerator")
	}
	return flowReportPanel{
		ID: "vpn_share", Status: "ready", Data: data,
		Meta: gin.H{
			"as_of": now, "complete_ratio": minimumComplete,
			"partial":  minimumComplete < 1 || unknownGeo > 0,
			"warnings": warnings,
		},
	}, nil
}

// overseasVPNShareNumerator accumulates the overseas VPN numerator from findings:
// inbound/outbound bytes for remotes with a known non-CN Geo, the excluded
// unknown-Geo byte volume, and the minimum window completeness observed.
func overseasVPNShareNumerator(findings []vpnFindingDTO) (vpnIn, vpnOut, unknownGeo uint64, minComplete float64) {
	minComplete = 1.0
	for _, finding := range findings {
		if finding.CompleteRatio < minComplete {
			minComplete = finding.CompleteRatio
		}
		country := strings.ToUpper(strings.TrimSpace(finding.RemoteCountry))
		if country == "" || country == "_UNASSIGNED" || country == "UNKNOWN" {
			unknownGeo += finding.LocalToRemoteBytes + finding.RemoteToLocalBytes
			continue
		}
		if country == "CN" {
			continue
		}
		vpnIn += finding.RemoteToLocalBytes
		vpnOut += finding.LocalToRemoteBytes
	}
	return vpnIn, vpnOut, unknownGeo, minComplete
}

// overseasDirectionTotalBytes sums the overseas total estimated_bytes for one
// direction across the window — one leg of the vpn_share denominator.
func (s *Server) overseasDirectionTotalBytes(ctx context.Context, scope flowquery.Scope, view flowquery.View, req flowReportRequest, direction string, now time.Time) (float64, error) {
	plan, err := flowquery.PlanAggregate(req.From, req.To, 0, req.TargetPoints, now)
	if err != nil {
		return 0, err
	}
	filters := req.Filters
	filters.Categories = []string{"overseas"}
	filters.Directions = []string{direction}
	queryRequest := flowquery.Request{
		From: plan.EffectiveFrom, To: plan.EffectiveTo, Bucket: plan.Source, Interval: plan.Interval,
		Metric: flowquery.MetricEstimatedBytes, Dimension: flowquery.DimensionTotal, Filters: filters, Filter: req.Filter,
		View: view, TopN: 1, IncludeOther: false, Timezone: req.Timezone, TimeWindows: req.PeakWindows,
	}
	if err := s.applyFlowStorageBoundary(ctx, &queryRequest); err != nil {
		return 0, err
	}
	compiled, err := flowquery.Compile(scope, queryRequest, now)
	if err != nil {
		return 0, err
	}
	result, err := s.flowQuery.aggregate.Run(ctx, compiled)
	if err != nil {
		return 0, err
	}
	var total float64
	for _, point := range result.Points {
		total += point.Value
	}
	return total, nil
}
