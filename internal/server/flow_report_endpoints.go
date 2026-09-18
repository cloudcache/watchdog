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

// flow_report_endpoints.go migrates the hub's multi-stage endpoint report to Gin +
// the direct runners: query the top-N endpoints, then enrich each with in/out
// direction, per-category, and business breakdowns, and merge them into one
// per-endpoint table. The table-merge composer is a faithful de-tenanted port; the
// query stages call the aggregate/joint runners directly.

type flowEndpointDirectionSummary struct {
	Last    float64 `json:"last"`
	Average float64 `json:"average"`
	P95     float64 `json:"p95"`
	Maximum float64 `json:"maximum"`
	Total   float64 `json:"total"`
}

type flowEndpointCategorySummary struct {
	Inbound       float64  `json:"inbound"`
	Outbound      float64  `json:"outbound"`
	InboundShare  *float64 `json:"inbound_share,omitempty"`
	OutboundShare *float64 `json:"outbound_share,omitempty"`
}

type flowEndpointReportRow struct {
	flowTableRow
	Inbound    flowEndpointDirectionSummary           `json:"inbound"`
	Outbound   flowEndpointDirectionSummary           `json:"outbound"`
	Categories map[string]flowEndpointCategorySummary `json:"categories"`
	Residual   flowEndpointCategorySummary            `json:"residual"`
	Businesses []string                               `json:"businesses"`
}

type flowEndpointReportPage struct {
	Items         []flowEndpointReportRow            `json:"items"`
	Total         int                                `json:"total"`
	Limit         uint16                             `json:"limit"`
	Offset        uint32                             `json:"offset"`
	FilterOptions map[string][]flowTableFilterOption `json:"filter_options"`
}

type endpointCategorySummaryEntry struct {
	Address  string  `json:"address"`
	Category string  `json:"category"`
	Total    float64 `json:"total"`
}

var flowEndpointTableFields = func() map[string]struct{} {
	fields := make(map[string]struct{}, len(flowTableFields)+3+len(flowReportCategories)+1)
	for field := range flowTableFields {
		fields[field] = struct{}{}
	}
	fields["inbound"] = struct{}{}
	fields["outbound"] = struct{}{}
	fields["business"] = struct{}{}
	for _, category := range flowReportCategories {
		fields[category] = struct{}{}
	}
	fields["residual"] = struct{}{}
	return fields
}()

func normalizeFlowEndpointTableRequest(request *flowTableRequest) error {
	return normalizeFlowTableRequestWithFields(request, flowEndpointTableFields)
}

// enrichEndpointReport runs the endpoint pipeline: top-N endpoints, then per-endpoint
// direction/category/business breakdowns, merged into one enriched endpoint table.
func (s *Server) enrichEndpointReport(ctx context.Context, scope flowquery.Scope, view flowquery.View, req flowReportRequest, now time.Time) ([]flowReportPanel, error) {
	dimension := flowquery.DimensionSourceIP
	if req.Side == "destination" {
		dimension = flowquery.DimensionDestinationIP
	}
	candidateTable := flowTableRequest{SortBy: "maximum", SortDirection: "desc", Limit: req.TopN}
	endpointRaw, plan, err := s.runEndpointAggregate(ctx, scope, view, req, dimension, req.Filters, &candidateTable, req.TargetPoints, req.TopN, now)
	if err != nil {
		return nil, err
	}
	panels := []flowReportPanel{{ID: "endpoint", Status: "ready", Data: endpointRaw, Meta: gin.H{"step_seconds": plan.StepSeconds, "source": plan.Source}}}
	addresses := flowReportEndpointAddresses(panels, "endpoint")
	if len(addresses) > 0 {
		for _, direction := range []string{"in", "out"} {
			filters := req.Filters
			filters.Directions = []string{direction}
			filters.DimensionValues = append([]string(nil), addresses...)
			table := flowTableRequest{SortBy: "dimension", SortDirection: "asc", Limit: uint16(len(addresses))}
			raw, dirPlan, err := s.runEndpointAggregate(ctx, scope, view, req, dimension, filters, &table, req.TargetPoints, uint16(len(addresses)), now)
			if err != nil {
				return nil, err
			}
			panels = append(panels, flowReportPanel{ID: "endpoint_" + direction, Status: "ready", Data: raw, Meta: gin.H{"step_seconds": dirPlan.StepSeconds, "source": dirPlan.Source}})
			catRaw, catStep, err := s.runEndpointCategoryPanel(ctx, scope, view, req, dimension, direction, addresses, now)
			if err != nil {
				return nil, err
			}
			panels = append(panels, flowReportPanel{ID: "endpoint_category_" + direction, Status: "ready", Data: catRaw, Meta: gin.H{"step_seconds": catStep}})
		}
		bizRaw, bizMeta, err := s.runEndpointBusinessPanel(ctx, scope, view, req, dimension, addresses, now)
		if err != nil {
			return nil, err
		}
		panels = append(panels, flowReportPanel{ID: "endpoint_business", Status: "ready", Data: bizRaw, Meta: bizMeta})
	}
	table := flowTableRequest{SortBy: "maximum", SortDirection: "desc", Limit: req.TopN}
	if req.Table != nil {
		table = *req.Table
	}
	return composeFlowEndpointReportTable(panels, table)
}

// runEndpointAggregate runs one single-dimension endpoint query (planned onto a
// rollup) and marshals it with a table. IncludeOther is always off for endpoints.
func (s *Server) runEndpointAggregate(ctx context.Context, scope flowquery.Scope, view flowquery.View, req flowReportRequest, dimension flowquery.Dimension, filters flowquery.Filters, table *flowTableRequest, targetPoints, topN uint16, now time.Time) (json.RawMessage, flowquery.AggregatePlan, error) {
	baseFacts, err := flowReportNeedsBaseFacts(req.Filter)
	if err != nil {
		return nil, flowquery.AggregatePlan{}, err
	}
	if baseFacts {
		compiled, err := flowquery.CompileJoint(scope, flowquery.JointRequest{
			From: req.From, To: req.To, TargetPoints: targetPoints, Metric: req.Metric,
			Dimensions: []flowquery.Dimension{dimension}, Filters: filters, Filter: req.Filter,
			View: view, TopN: topN, IncludeOther: false, Timezone: req.Timezone, TimeWindows: req.PeakWindows,
		}, now)
		if err != nil {
			return nil, flowquery.AggregatePlan{}, err
		}
		result, err := s.flowQuery.joint.Run(ctx, compiled)
		if err != nil {
			return nil, flowquery.AggregatePlan{}, err
		}
		raw, err := marshalFlowJointResult(result, table, s.flowGeo)
		if err != nil {
			return nil, flowquery.AggregatePlan{}, err
		}
		step := time.Duration(compiled.Plan.StepSeconds) * time.Second
		plan := flowquery.AggregatePlan{
			RequestedFrom: compiled.Plan.RequestedFrom,
			RequestedTo:   compiled.Plan.RequestedTo,
			EffectiveFrom: compiled.Plan.EffectiveFrom,
			EffectiveTo:   compiled.Plan.EffectiveTo,
			Source:        flowquery.BucketFlowRecords,
			SourceStep:    time.Minute,
			Interval:      step,
			SourceSeconds: 60,
			StepSeconds:   compiled.Plan.StepSeconds,
			TargetPoints:  compiled.Plan.TargetPoints,
		}
		return raw, plan, nil
	}
	plan, err := flowquery.PlanAggregate(req.From, req.To, 0, targetPoints, now)
	if err != nil {
		return nil, flowquery.AggregatePlan{}, err
	}
	queryRequest := flowquery.Request{
		From: plan.EffectiveFrom, To: plan.EffectiveTo, Bucket: plan.Source, Interval: plan.Interval,
		Metric: req.Metric, Dimension: dimension, Filters: filters, Filter: req.Filter,
		View: view, TopN: topN, IncludeOther: false, Timezone: req.Timezone, TimeWindows: req.PeakWindows,
	}
	if err := s.applyFlowStorageBoundary(ctx, &queryRequest); err != nil {
		return nil, flowquery.AggregatePlan{}, err
	}
	compiled, err := flowquery.Compile(scope, queryRequest, now)
	if err != nil {
		return nil, flowquery.AggregatePlan{}, err
	}
	result, err := s.flowQuery.aggregate.Run(ctx, compiled)
	if err != nil {
		return nil, flowquery.AggregatePlan{}, err
	}
	raw, err := marshalFlowAggregateResult(result, table, s.flowGeo)
	if err != nil {
		return nil, flowquery.AggregatePlan{}, err
	}
	return raw, plan, nil
}

// runEndpointCategoryPanel fetches endpoint/category pairs in bounded chunks.
// This avoids one ClickHouse scan per category while keeping each joint query
// within the query engine's result-cardinality limit.
func (s *Server) runEndpointCategoryPanel(ctx context.Context, scope flowquery.Scope, view flowquery.View, req flowReportRequest, dimension flowquery.Dimension, direction string, addresses []string, now time.Time) (json.RawMessage, uint32, error) {
	categories := append(append([]string(nil), flowReportCategories...), flowReportResiduals...)
	combined := make([]flowquery.JointPoint, 0, len(addresses)*len(categories))
	summaries := make([]endpointCategorySummaryEntry, 0, len(addresses)*len(categories))
	var unit string
	var step uint32
	addressFilter, err := flowEndpointAddressFilter(req.Filter, dimension, addresses)
	if err != nil {
		return nil, 0, err
	}
	for _, categoryChunk := range flowEndpointCategoryChunks(len(addresses), categories) {
		filters := req.Filters
		filters.Directions = []string{direction}
		filters.Categories = categoryChunk
		filters.DimensionValues = nil
		compiled, err := flowquery.CompileJoint(scope, flowquery.JointRequest{
			From: req.From, To: req.To, TargetPoints: flowquery.MinTargetPoints, Metric: req.Metric,
			Dimensions: []flowquery.Dimension{dimension, flowquery.DimensionCategory}, Filters: filters,
			Filter: addressFilter, View: view, TopN: flowquery.MaxTopN, IncludeOther: false,
			Timezone: req.Timezone, TimeWindows: req.PeakWindows,
		}, now)
		if err != nil {
			return nil, 0, err
		}
		result, err := s.flowQuery.joint.Run(ctx, compiled)
		if err != nil {
			return nil, 0, err
		}
		step = compiled.Plan.StepSeconds
		unit = result.Metric.Unit
		combined = append(combined, result.Points...)
		table := flowTableRequest{SortBy: "dimension", SortDirection: "asc", Limit: flowquery.MaxTopN}
		raw, err := marshalFlowJointResult(result, &table, s.flowGeo)
		if err != nil {
			return nil, 0, err
		}
		var envelope struct {
			Table flowTablePage `json:"table"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, 0, err
		}
		for _, row := range envelope.Table.Items {
			if len(row.Path) < 2 || row.Path[0] == "" || row.Path[1] == "" {
				continue
			}
			summaries = append(summaries, endpointCategorySummaryEntry{
				Address: row.Path[0], Category: row.Path[1], Total: row.Total,
			})
		}
	}
	data, err := json.Marshal(struct {
		Points     []flowquery.JointPoint          `json:"points"`
		Summaries  []endpointCategorySummaryEntry  `json:"summaries"`
		Metric     flowquery.MetricDefinition      `json:"metric"`
		Dimensions []flowquery.DimensionDefinition `json:"dimensions"`
	}{
		Points: combined, Summaries: summaries, Metric: flowquery.MetricDefinition{Name: req.Metric, Unit: unit},
		Dimensions: []flowquery.DimensionDefinition{{Kind: dimension, Additive: true}, {Kind: flowquery.DimensionCategory, Additive: true}},
	})
	if err != nil {
		return nil, 0, err
	}
	return data, step, nil
}

func flowEndpointAddressFilter(base *flowquery.FilterExpression, dimension flowquery.Dimension, addresses []string) (*flowquery.FilterExpression, error) {
	addressFilter := flowquery.FilterExpression{
		Op: flowquery.FilterPredicate, Field: string(dimension), Operator: flowquery.FilterIn,
		Values: append([]string(nil), addresses...),
	}
	combined := addressFilter
	if base != nil {
		combined = flowquery.FilterExpression{Op: flowquery.FilterAnd, Args: []flowquery.FilterExpression{*base, addressFilter}}
	}
	canonical, err := flowquery.CanonicalFilter(combined)
	if err != nil {
		return nil, err
	}
	return &canonical, nil
}

func flowEndpointCategoryChunks(addressCount int, categories []string) [][]string {
	if addressCount < 1 || len(categories) == 0 {
		return nil
	}
	chunkSize := flowquery.MaxTopN / addressCount
	if chunkSize < 1 {
		chunkSize = 1
	}
	chunks := make([][]string, 0, (len(categories)+chunkSize-1)/chunkSize)
	for start := 0; start < len(categories); start += chunkSize {
		end := min(start+chunkSize, len(categories))
		chunks = append(chunks, categories[start:end])
	}
	return chunks
}

// runEndpointBusinessPanel queries the distinct business labels per endpoint as a
// joint [endpoint, business] query, constraining to the endpoint set with a typed
// filter (composed under the caller's filter).
func (s *Server) runEndpointBusinessPanel(ctx context.Context, scope flowquery.Scope, view flowquery.View, req flowReportRequest, dimension flowquery.Dimension, addresses []string, now time.Time) (json.RawMessage, gin.H, error) {
	addressFilter, err := flowEndpointAddressFilter(req.Filter, dimension, addresses)
	if err != nil {
		return nil, nil, err
	}
	filters := req.Filters
	filters.DimensionValues = nil
	compiled, err := flowquery.CompileJoint(scope, flowquery.JointRequest{
		From: req.From, To: req.To, TargetPoints: req.TargetPoints, Metric: req.Metric,
		Dimensions: []flowquery.Dimension{dimension, flowquery.DimensionBusiness}, Filters: filters, Filter: addressFilter,
		View: view, TopN: 100, IncludeOther: false, Timezone: req.Timezone, TimeWindows: req.PeakWindows,
	}, now)
	if err != nil {
		return nil, nil, err
	}
	result, err := s.flowQuery.joint.Run(ctx, compiled)
	if err != nil {
		return nil, nil, err
	}
	raw, err := marshalFlowJointResult(result, nil, s.flowGeo)
	if err != nil {
		return nil, nil, err
	}
	return raw, gin.H{"step_seconds": compiled.Plan.StepSeconds, "source": compiled.Plan.Source}, nil
}

func flowReportDimensionPoints(data json.RawMessage) ([]flowquery.Point, error) {
	var envelope struct {
		Points []flowquery.Point `json:"points"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	return envelope.Points, nil
}

func flowReportEndpointAddresses(panels []flowReportPanel, panelID string) []string {
	for _, panel := range panels {
		if panel.ID != panelID || panel.Status != "ready" {
			continue
		}
		var envelope struct {
			Table *flowTablePage `json:"table"`
		}
		if json.Unmarshal(panel.Data, &envelope) != nil || envelope.Table == nil {
			points, err := flowReportDimensionPoints(panel.Data)
			if err != nil {
				return nil
			}
			seen := make(map[string]struct{}, len(points))
			for _, point := range points {
				if point.DimensionValue != "" && !point.Other {
					seen[point.DimensionValue] = struct{}{}
				}
			}
			addresses := make([]string, 0, len(seen))
			for address := range seen {
				addresses = append(addresses, address)
			}
			sort.Strings(addresses)
			return addresses
		}
		addresses := make([]string, 0, len(envelope.Table.Items))
		for _, item := range envelope.Table.Items {
			if len(item.Path) > 0 && item.Path[0] != "" {
				addresses = append(addresses, item.Path[0])
			}
		}
		sort.Strings(addresses)
		return addresses
	}
	return nil
}

func composeFlowEndpointReportTable(panels []flowReportPanel, request flowTableRequest) ([]flowReportPanel, error) {
	endpointIndex := -1
	var endpointEnvelope struct {
		Table flowTablePage `json:"table"`
	}
	for index, panel := range panels {
		if panel.ID != "endpoint" || panel.Status != "ready" {
			continue
		}
		if err := json.Unmarshal(panel.Data, &endpointEnvelope); err != nil {
			return nil, err
		}
		endpointIndex = index
		break
	}
	if endpointIndex < 0 {
		return panels, nil
	}
	inbound := flowEndpointDirectionRows(panels, "endpoint_in")
	outbound := flowEndpointDirectionRows(panels, "endpoint_out")
	inboundCategories, err := flowEndpointCategoryRows(panels, "endpoint_category_in")
	if err != nil {
		return nil, err
	}
	outboundCategories, err := flowEndpointCategoryRows(panels, "endpoint_category_out")
	if err != nil {
		return nil, err
	}
	businesses, err := flowEndpointBusinessRows(panels, "endpoint_business")
	if err != nil {
		return nil, err
	}

	rows := make([]flowEndpointReportRow, 0, len(endpointEnvelope.Table.Items))
	for _, base := range endpointEnvelope.Table.Items {
		address := endpointAddress(base)
		in := inbound[address]
		out := outbound[address]
		row := flowEndpointReportRow{
			flowTableRow: base,
			Inbound:      endpointDirectionSummary(in),
			Outbound:     endpointDirectionSummary(out),
			Categories:   make(map[string]flowEndpointCategorySummary, len(flowReportCategories)),
			Businesses:   append([]string(nil), businesses[address]...),
		}
		for _, category := range flowReportCategories {
			row.Categories[category] = endpointCategorySummary(
				inboundCategories[address][category], outboundCategories[address][category], in.Total, out.Total,
			)
		}
		var residualIn, residualOut float64
		for _, category := range flowReportResiduals {
			residualIn += inboundCategories[address][category]
			residualOut += outboundCategories[address][category]
		}
		row.Residual = endpointCategorySummary(residualIn, residualOut, in.Total, out.Total)
		rows = append(rows, row)
	}
	options := flowEndpointTableOptions(rows)
	filtered := rows[:0]
	search := strings.ToLower(request.Search)
	for _, row := range rows {
		if !flowEndpointTableMatchesFilters(row, request.Filters) {
			continue
		}
		if search != "" && !strings.Contains(strings.ToLower(flowEndpointTableSearchText(row)), search) {
			continue
		}
		filtered = append(filtered, row)
	}
	rows = filtered
	sort.SliceStable(rows, func(i, j int) bool {
		comparison := flowEndpointTableCompare(rows[i], rows[j], request.SortBy)
		if comparison == 0 {
			comparison = strings.Compare(endpointAddress(rows[i].flowTableRow), endpointAddress(rows[j].flowTableRow))
		}
		if request.SortDirection == "desc" {
			return comparison > 0
		}
		return comparison < 0
	})
	total := len(rows)
	start := min(int(request.Offset), total)
	end := min(start+int(request.Limit), total)
	page := flowEndpointReportPage{
		Items: append([]flowEndpointReportRow(nil), rows[start:end]...), Total: total,
		Limit: request.Limit, Offset: request.Offset, FilterOptions: options,
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(panels[endpointIndex].Data, &envelope); err != nil {
		return nil, err
	}
	table, err := json.Marshal(page)
	if err != nil {
		return nil, err
	}
	envelope["table"] = table
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	panels[endpointIndex].Data = data
	return panels, nil
}

func flowEndpointDirectionRows(panels []flowReportPanel, panelID string) map[string]flowTableRow {
	result := map[string]flowTableRow{}
	for _, panel := range panels {
		if panel.ID != panelID || panel.Status != "ready" {
			continue
		}
		var envelope struct {
			Table flowTablePage `json:"table"`
		}
		if json.Unmarshal(panel.Data, &envelope) != nil {
			return result
		}
		for _, row := range envelope.Table.Items {
			result[endpointAddress(row)] = row
		}
	}
	return result
}

func flowEndpointCategoryRows(panels []flowReportPanel, panelID string) (map[string]map[string]float64, error) {
	result := map[string]map[string]float64{}
	for _, panel := range panels {
		if panel.ID != panelID || panel.Status != "ready" {
			continue
		}
		var envelope struct {
			Summaries []endpointCategorySummaryEntry `json:"summaries"`
		}
		if err := json.Unmarshal(panel.Data, &envelope); err != nil {
			return nil, err
		}
		for _, summary := range envelope.Summaries {
			if summary.Address == "" || summary.Category == "" {
				continue
			}
			if result[summary.Address] == nil {
				result[summary.Address] = map[string]float64{}
			}
			result[summary.Address][summary.Category] += summary.Total
		}
	}
	return result, nil
}

func flowEndpointBusinessRows(panels []flowReportPanel, panelID string) (map[string][]string, error) {
	sets := map[string]map[string]struct{}{}
	for _, panel := range panels {
		if panel.ID != panelID || panel.Status != "ready" {
			continue
		}
		var envelope struct {
			Points []flowquery.JointPoint `json:"points"`
		}
		if err := json.Unmarshal(panel.Data, &envelope); err != nil {
			return nil, err
		}
		for _, point := range envelope.Points {
			if len(point.DimensionValues) < 2 || point.Other || point.DimensionValues[0] == "" || point.DimensionValues[1] == "" {
				continue
			}
			if sets[point.DimensionValues[0]] == nil {
				sets[point.DimensionValues[0]] = map[string]struct{}{}
			}
			sets[point.DimensionValues[0]][point.DimensionValues[1]] = struct{}{}
		}
	}
	result := make(map[string][]string, len(sets))
	for address, values := range sets {
		for value := range values {
			result[address] = append(result[address], value)
		}
		sort.Strings(result[address])
	}
	return result, nil
}

func endpointAddress(row flowTableRow) string {
	if len(row.Path) > 0 {
		return row.Path[0]
	}
	return row.Label
}

func endpointDirectionSummary(row flowTableRow) flowEndpointDirectionSummary {
	return flowEndpointDirectionSummary{Last: row.Last, Average: row.Average, P95: row.P95, Maximum: row.Maximum, Total: row.Total}
}

func endpointCategorySummary(inbound, outbound, inboundTotal, outboundTotal float64) flowEndpointCategorySummary {
	result := flowEndpointCategorySummary{Inbound: inbound, Outbound: outbound}
	if inboundTotal > 0 {
		share := inbound / inboundTotal
		result.InboundShare = &share
	}
	if outboundTotal > 0 {
		share := outbound / outboundTotal
		result.OutboundShare = &share
	}
	return result
}

func flowEndpointTableValue(row flowEndpointReportRow, field string) string {
	if _, base := flowTableFields[field]; base {
		return flowTableValue(row.flowTableRow, field)
	}
	switch field {
	case "business":
		return strings.Join(row.Businesses, ",")
	case "inbound":
		return strconv.FormatFloat(row.Inbound.Total, 'g', -1, 64)
	case "outbound":
		return strconv.FormatFloat(row.Outbound.Total, 'g', -1, 64)
	case "residual":
		return endpointCategoryFilterValue(row.Residual)
	default:
		if value, ok := row.Categories[field]; ok {
			return endpointCategoryFilterValue(value)
		}
	}
	return ""
}

func endpointCategoryFilterValue(value flowEndpointCategorySummary) string {
	return fmt.Sprintf("%g/%g", value.Inbound, value.Outbound)
}

func flowEndpointTableOptions(rows []flowEndpointReportRow) map[string][]flowTableFilterOption {
	result := make(map[string][]flowTableFilterOption, len(flowEndpointTableFields))
	for field := range flowEndpointTableFields {
		counts := map[string]int{}
		for _, row := range rows {
			counts[flowEndpointTableValue(row, field)]++
		}
		values := make([]flowTableFilterOption, 0, len(counts))
		for value, count := range counts {
			values = append(values, flowTableFilterOption{Value: value, Count: count})
		}
		sort.Slice(values, func(i, j int) bool { return values[i].Value < values[j].Value })
		result[field] = values
	}
	return result
}

func flowEndpointTableMatchesFilters(row flowEndpointReportRow, filters map[string][]string) bool {
	for field, values := range filters {
		if len(values) == 0 {
			continue
		}
		current := flowEndpointTableValue(row, field)
		matched := false
		for _, value := range values {
			matched = matched || value == current
		}
		if !matched {
			return false
		}
	}
	return true
}

func flowEndpointTableSearchText(row flowEndpointReportRow) string {
	values := make([]string, 0, len(flowEndpointTableFields))
	for field := range flowEndpointTableFields {
		values = append(values, flowEndpointTableValue(row, field))
	}
	return strings.Join(values, " ")
}

func flowEndpointTableCompare(left, right flowEndpointReportRow, field string) int {
	if field == "dimension" {
		return strings.Compare(left.Label, right.Label)
	}
	if field == "business" {
		return strings.Compare(strings.Join(left.Businesses, ","), strings.Join(right.Businesses, ","))
	}
	if value, ok := left.Categories[field]; ok {
		other := right.Categories[field]
		return cmpFloat64(value.Inbound+value.Outbound, other.Inbound+other.Outbound)
	}
	if field == "residual" {
		return cmpFloat64(left.Residual.Inbound+left.Residual.Outbound, right.Residual.Inbound+right.Residual.Outbound)
	}
	if field == "inbound" {
		return cmpFloat64(left.Inbound.Total, right.Inbound.Total)
	}
	if field == "outbound" {
		return cmpFloat64(left.Outbound.Total, right.Outbound.Total)
	}
	return flowTableCompare(left.flowTableRow, right.flowTableRow, field)
}
