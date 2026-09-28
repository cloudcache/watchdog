// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

type flowEndpointCategorySummary struct {
	Inbound       float64  `json:"inbound"`
	Outbound      float64  `json:"outbound"`
	InboundShare  *float64 `json:"inbound_share,omitempty"`
	OutboundShare *float64 `json:"outbound_share,omitempty"`
}

type flowEndpointReportRow struct {
	Address         string                       `json:"address"`
	Bandwidth       float64                      `json:"bandwidth"`
	P95             *float64                     `json:"p95,omitempty"`
	Classifications []flowEndpointClassification `json:"classifications,omitempty"`
	Businesses      []string                     `json:"businesses,omitempty"`
}

type flowEndpointClassification struct {
	Category string  `json:"category"`
	Inbound  float64 `json:"inbound,omitempty"`
	Outbound float64 `json:"outbound,omitempty"`
}

type flowEndpointReportPage struct {
	Items         []flowEndpointReportRow            `json:"items"`
	Total         int                                `json:"total"`
	Limit         uint16                             `json:"limit"`
	Offset        uint32                             `json:"offset"`
	FilterOptions map[string][]flowTableFilterOption `json:"filter_options"`
}

type endpointCategorySummaryEntry struct {
	Address   string  `json:"address"`
	Category  string  `json:"category"`
	Bandwidth float64 `json:"bandwidth"`
}

func flowEndpointTableFields(side string) map[string]struct{} {
	fields := map[string]struct{}{"dimension": {}, "last": {}}
	if side == "destination" {
		return fields
	}
	fields["p95"] = struct{}{}
	fields["classification"] = struct{}{}
	fields["business"] = struct{}{}
	return fields
}

func normalizeFlowEndpointTableRequest(request *flowTableRequest, side string) error {
	return normalizeFlowTableRequestWithFields(request, flowEndpointTableFields(side))
}

// enrichEndpointReport runs the endpoint pipeline: top-N endpoints, then per-endpoint
// direction/category/business breakdowns, merged into one enriched endpoint table.
func (s *Server) enrichEndpointReport(ctx context.Context, scope flowquery.Scope, view flowquery.View, req flowReportRequest, now time.Time) ([]flowReportPanel, error) {
	dimension := flowquery.DimensionSourceIP
	if req.Side == "destination" {
		dimension = flowquery.DimensionDestinationIP
	}
	candidateTable := flowTableRequest{SortBy: "last", SortDirection: "desc", Limit: req.TopN}
	endpointRaw, plan, err := s.runEndpointAggregate(ctx, scope, view, req, dimension, req.Filters, &candidateTable, req.TargetPoints, req.TopN, now)
	if err != nil {
		return nil, err
	}
	panels := []flowReportPanel{{ID: "endpoint", Status: "ready", Data: endpointRaw, Meta: gin.H{"step_seconds": plan.StepSeconds, "source": plan.Source, "approximate": plan.Approximate}}}
	addresses := flowReportEndpointAddresses(panels, "endpoint")
	if req.Side == "source" && len(addresses) > 0 {
		for _, direction := range []string{"in", "out"} {
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
	table := flowTableRequest{SortBy: "last", SortDirection: "desc", Limit: req.TopN}
	if req.Table != nil {
		table = *req.Table
	}
	return composeFlowEndpointReportTable(panels, table, req.Side)
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
			ExecutionTimeout: s.cfg.Flow.Query.ExecutionTimeout,
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
	plan, err := s.planFlowReportAggregate(req, targetPoints, now)
	if err != nil {
		return nil, flowquery.AggregatePlan{}, err
	}
	queryRequest := flowquery.Request{
		From: plan.EffectiveFrom, To: plan.EffectiveTo, Bucket: plan.Source, Interval: plan.Interval,
		Metric: req.Metric, Dimension: dimension, Filters: filters, Filter: req.Filter,
		View: view, TopN: topN, IncludeOther: false, Timezone: req.Timezone, TimeWindows: req.PeakWindows,
		ExecutionTimeout: s.cfg.Flow.Query.ExecutionTimeout,
	}
	if err := s.applyFlowStorageBoundary(ctx, &queryRequest); err != nil {
		return nil, flowquery.AggregatePlan{}, err
	}
	compiled, err := flowquery.Compile(scope, queryRequest, now)
	if err != nil {
		return nil, flowquery.AggregatePlan{}, err
	}
	// Carry the approximate-ranking signal from the candidate path so the report
	// panel can disclose that the top-N endpoint set is a heavy-hitter estimate.
	plan.Approximate = compiled.Approximate
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
	for _, categoryChunk := range flowEndpointCategoryChunks(len(addresses), categories) {
		filters := req.Filters
		filters.Directions = []string{direction}
		filters.Categories = categoryChunk
		result, err := s.runEndpointCorrelationJoint(ctx, scope, view, req, dimension,
			flowquery.DimensionCategory, filters, addresses, flowquery.MinTargetPoints, flowquery.MaxTopN, now)
		if err != nil {
			return nil, 0, err
		}
		step = result.Plan.StepSeconds
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
				Address: row.Path[0], Category: row.Path[1], Bandwidth: row.Last,
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
	filters := req.Filters
	result, err := s.runEndpointCorrelationJoint(ctx, scope, view, req, dimension,
		flowquery.DimensionBusiness, filters, addresses, req.TargetPoints, 100, now)
	if err != nil {
		return nil, nil, err
	}
	raw, err := marshalFlowJointResult(result, nil, s.flowGeo)
	if err != nil {
		return nil, nil, err
	}
	return raw, gin.H{"step_seconds": result.Plan.StepSeconds, "source": result.Plan.Source}, nil
}

// endpointCorrelationRollupPlan returns the continuous aggregate prefix. The
// caller may execute the uncovered suffix against raw facts; a marker gap never
// authorizes aggregate data beyond the returned boundary.
func (s *Server) endpointCorrelationRollupPlan(ctx context.Context, req flowReportRequest, targetPoints uint16, now time.Time) (flowquery.AggregatePlan, time.Time, error) {
	plan, err := flowquery.PlanAggregate(req.From, req.To, time.Hour, targetPoints, now)
	if err != nil {
		return flowquery.AggregatePlan{}, time.Time{}, err
	}
	if s.flowRollup == nil {
		return plan, plan.EffectiveFrom, nil
	}
	if req.Filter != nil {
		supported, filterErr := flowquery.AggregateFilterSupported(*req.Filter)
		if filterErr != nil {
			return flowquery.AggregatePlan{}, time.Time{}, filterErr
		}
		if !supported {
			return plan, plan.EffectiveFrom, nil
		}
	}
	covered, err := s.flowRollup.CoveredThroughAtLeast(
		ctx, flowch.RollupOneHour, plan.EffectiveFrom, plan.EffectiveTo, s.flowReadableGenerationFloor(),
	)
	if err != nil {
		return flowquery.AggregatePlan{}, time.Time{}, err
	}
	return plan, covered, nil
}

func (s *Server) runEndpointCorrelationJoint(ctx context.Context, scope flowquery.Scope, view flowquery.View,
	req flowReportRequest, endpoint, secondary flowquery.Dimension, filters flowquery.Filters, addresses []string,
	targetPoints, topN uint16, now time.Time) (flowquery.JointResult, error) {
	plan, archiveThrough, err := s.endpointCorrelationRollupPlan(ctx, req, targetPoints, now)
	if err != nil {
		return flowquery.JointResult{}, err
	}
	addressFilter, err := flowEndpointAddressFilter(req.Filter, endpoint, addresses)
	if err != nil {
		return flowquery.JointResult{}, err
	}
	base := flowquery.JointRequest{
		TargetPoints: targetPoints, Metric: req.Metric,
		Dimensions: []flowquery.Dimension{endpoint, secondary}, Filters: filters,
		View: view, TopN: topN, IncludeOther: false, Timezone: req.Timezone, TimeWindows: req.PeakWindows,
		ExecutionTimeout: s.cfg.Flow.Query.ExecutionTimeout,
	}
	if !archiveThrough.After(plan.EffectiveFrom) {
		base.From, base.To, base.Filter = req.From, req.To, addressFilter
		compiled, compileErr := flowquery.CompileJoint(scope, base, now)
		if compileErr != nil {
			return flowquery.JointResult{}, compileErr
		}
		return s.flowQuery.joint.Run(ctx, compiled)
	}

	interval := plan.Interval
	if archiveThrough.Before(plan.EffectiveTo) {
		// Keep the aggregate/raw split on a presentation boundary. This prevents
		// two partial rate values for the same output bucket; at most the final
		// uncovered hours use raw facts.
		interval = time.Hour
	}
	results := make([]flowquery.JointResult, 0, 2)
	aggregateRequest := base
	aggregateRequest.From, aggregateRequest.To, aggregateRequest.Interval = plan.EffectiveFrom, archiveThrough, interval
	aggregateRequest.Filter = req.Filter
	aggregateRequest.Filters.DimensionValues = append([]string(nil), addresses...)
	aggregateCompiled, err := flowquery.CompileEndpointRollupJoint(scope, aggregateRequest, now)
	if err != nil {
		return flowquery.JointResult{}, err
	}
	aggregateResult, err := s.flowQuery.joint.Run(ctx, aggregateCompiled)
	if err != nil {
		return flowquery.JointResult{}, err
	}
	results = append(results, aggregateResult)

	if archiveThrough.Before(plan.EffectiveTo) {
		rawRequest := base
		rawRequest.From, rawRequest.To, rawRequest.Interval = archiveThrough, plan.EffectiveTo, interval
		rawRequest.Filter = addressFilter
		rawRequest.Filters.DimensionValues = nil
		rawCompiled, compileErr := flowquery.CompileJoint(scope, rawRequest, now)
		if compileErr != nil {
			return flowquery.JointResult{}, compileErr
		}
		rawResult, runErr := s.flowQuery.joint.Run(ctx, rawCompiled)
		if runErr != nil {
			return flowquery.JointResult{}, runErr
		}
		results = append(results, rawResult)
	}
	return combineEndpointJointSegments(results, plan, interval), nil
}

func combineEndpointJointSegments(results []flowquery.JointResult, plan flowquery.AggregatePlan, interval time.Duration) flowquery.JointResult {
	combined := results[0]
	combined.Points = nil
	versions := make(map[string]struct{})
	for _, result := range results {
		combined.Points = append(combined.Points, result.Points...)
		for _, point := range result.Points {
			key := point.DimensionSnapshotID + "\x00" + point.GeoVersion + "\x00" + strconv.FormatUint(uint64(point.ClassificationVersion), 10)
			versions[key] = struct{}{}
		}
	}
	sort.Slice(combined.Points, func(i, j int) bool {
		if !combined.Points[i].Bucket.Equal(combined.Points[j].Bucket) {
			return combined.Points[i].Bucket.Before(combined.Points[j].Bucket)
		}
		return strings.Join(combined.Points[i].DimensionValues, "\x00") < strings.Join(combined.Points[j].DimensionValues, "\x00")
	})
	combined.MixedVersions = len(versions) > 1
	combined.VersionCount = uint64(len(versions))
	combined.Plan = flowquery.JointPlan{
		RequestedFrom: plan.RequestedFrom, RequestedTo: plan.RequestedTo,
		EffectiveFrom: plan.EffectiveFrom, EffectiveTo: plan.EffectiveTo,
		Source: "flow_aggregate_1h", StepSeconds: uint32(interval / time.Second),
		TargetPoints: plan.TargetPoints, MaxRangeSeconds: uint32((400 * 24 * time.Hour) / time.Second),
	}
	if len(results) > 1 {
		combined.Plan.Source = "flow_aggregate_1h+flow_records"
	}
	return combined
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

func composeFlowEndpointReportTable(panels []flowReportPanel, request flowTableRequest, side string) ([]flowReportPanel, error) {
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
		row := flowEndpointReportRow{
			Address:   address,
			Bandwidth: base.Last,
		}
		if side == "source" {
			p95 := base.P95
			row.P95 = &p95
			row.Businesses = append([]string(nil), businesses[address]...)
			row.Classifications = endpointClassifications(inboundCategories[address], outboundCategories[address])
		}
		rows = append(rows, row)
	}
	fields := flowEndpointTableFields(side)
	options := flowEndpointTableOptions(rows, fields)
	filtered := rows[:0]
	search := strings.ToLower(request.Search)
	for _, row := range rows {
		if !flowEndpointTableMatchesFilters(row, request.Filters) {
			continue
		}
		if search != "" && !strings.Contains(strings.ToLower(flowEndpointTableSearchText(row, fields)), search) {
			continue
		}
		filtered = append(filtered, row)
	}
	rows = filtered
	sort.SliceStable(rows, func(i, j int) bool {
		comparison := flowEndpointTableCompare(rows[i], rows[j], request.SortBy)
		if comparison == 0 {
			comparison = strings.Compare(rows[i].Address, rows[j].Address)
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
			result[summary.Address][summary.Category] += summary.Bandwidth
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

func endpointClassifications(inbound, outbound map[string]float64) []flowEndpointClassification {
	categories := append(append([]string(nil), flowReportCategories...), flowReportResiduals...)
	result := make([]flowEndpointClassification, 0, len(categories))
	for _, category := range categories {
		inValue, outValue := inbound[category], outbound[category]
		if inValue == 0 && outValue == 0 {
			continue
		}
		result = append(result, flowEndpointClassification{Category: category, Inbound: inValue, Outbound: outValue})
	}
	return result
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
	switch field {
	case "dimension":
		return row.Address
	case "last":
		return formatFlowEndpointNumber(row.Bandwidth)
	case "p95":
		if row.P95 != nil {
			return formatFlowEndpointNumber(*row.P95)
		}
	case "business":
		return strings.Join(row.Businesses, ",")
	case "classification":
		values := make([]string, 0, len(row.Classifications))
		for _, classification := range row.Classifications {
			values = append(values, classification.Category+":"+formatFlowEndpointNumber(classification.Inbound)+"/"+formatFlowEndpointNumber(classification.Outbound))
		}
		return strings.Join(values, ",")
	}
	return ""
}

func formatFlowEndpointNumber(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func endpointCategoryFilterValue(value flowEndpointCategorySummary) string {
	return formatFlowEndpointNumber(value.Inbound) + "/" + formatFlowEndpointNumber(value.Outbound)
}

func flowEndpointTableOptions(rows []flowEndpointReportRow, fields map[string]struct{}) map[string][]flowTableFilterOption {
	result := make(map[string][]flowTableFilterOption, len(fields))
	for field := range fields {
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

func flowEndpointTableSearchText(row flowEndpointReportRow, fields map[string]struct{}) string {
	values := make([]string, 0, len(fields))
	for field := range fields {
		values = append(values, flowEndpointTableValue(row, field))
	}
	return strings.Join(values, " ")
}

func flowEndpointTableCompare(left, right flowEndpointReportRow, field string) int {
	if field == "dimension" {
		return strings.Compare(left.Address, right.Address)
	}
	if field == "last" {
		return cmpFloat64(left.Bandwidth, right.Bandwidth)
	}
	if field == "p95" {
		var leftValue, rightValue float64
		if left.P95 != nil {
			leftValue = *left.P95
		}
		if right.P95 != nil {
			rightValue = *right.P95
		}
		return cmpFloat64(leftValue, rightValue)
	}
	if field == "business" {
		return strings.Compare(strings.Join(left.Businesses, ","), strings.Join(right.Businesses, ","))
	}
	if field == "classification" {
		return strings.Compare(flowEndpointTableValue(left, field), flowEndpointTableValue(right, field))
	}
	return 0
}
