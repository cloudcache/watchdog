// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

const maxFlowFilterCompletionLimit = 100

// registerFlowRoutes wires the v2 ClickHouse flow query API (KISS-06), replacing
// the retired hub /api/v1/flow/* net/http surface. Every route needs at least
// the customer value-layer view; supplier/raw are gated per-request in the
// handler (the view arrives in the request body).
func (s *Server) registerFlowRoutes(auth *gin.RouterGroup) {
	view := s.requirePermission("flow.view.customer")
	records := auth.Group("/flow/records")
	records.GET("/capabilities", view, s.flowRecordCapabilities)
	records.POST("/search", view, s.searchFlowRecords)
	records.POST("/facets", view, s.flowRecordFacets)

	auth.POST("/flow/query", view, s.queryFlow)
	auth.POST("/flow/overseas/query", view, s.queryFlowOverseas)

	filters := auth.Group("/flow/filters")
	filters.GET("/catalog", view, s.flowFilterCatalog)
	filters.POST("/validate", view, s.flowFilterValidate)
	filters.POST("/complete", view, s.flowFilterComplete)

	s.registerFlowSavedFilterRoutes(auth, view)
	s.registerFlowVPNRuleRoutes(auth)
	s.registerFlowVPNFindingRoutes(auth)
	s.registerFlowReportRoutes(auth, view)
	s.registerFlowGeoRoutes(auth, view)
	s.registerFlowExportRoutes(auth)
	s.registerFlowReclassificationRoutes(auth)
}

// flowFilterCatalog returns the typed filter-field registry (fields, operators,
// enumerable values) that drives the client's filter builder. Pure metadata.
func (s *Server) flowFilterCatalog(c *gin.Context) {
	items := flowquery.FlowFilterCatalog()
	c.JSON(http.StatusOK, gin.H{"items": items, "total": len(items)})
}

// flowFilterValidate canonicalizes and validates a client filter expression
// against the registry without touching ClickHouse.
func (s *Server) flowFilterValidate(c *gin.Context) {
	var input struct {
		Filter flowquery.FilterExpression `json:"filter"`
	}
	if !addressDecodeStrict(c, &input, 256<<10) {
		return
	}
	canonical, err := flowquery.CanonicalFilter(input.Filter)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"valid": true, "filter": canonical})
}

// flowFilterComplete offers prefix completions for filter fields, operators, or
// a field's enumerable values, from the registry catalog.
func (s *Server) flowFilterComplete(c *gin.Context) {
	var input struct {
		Kind   string `json:"kind"`
		Field  string `json:"field,omitempty"`
		Prefix string `json:"prefix,omitempty"`
		Limit  uint16 `json:"limit,omitempty"`
	}
	if !addressDecodeStrict(c, &input, 256<<10) {
		return
	}
	input.Kind = strings.TrimSpace(input.Kind)
	input.Field = strings.TrimSpace(input.Field)
	input.Prefix = strings.ToLower(strings.TrimSpace(input.Prefix))
	limit := int(input.Limit)
	if limit == 0 {
		limit = 20
	}
	if limit > maxFlowFilterCompletionLimit || len(input.Prefix) > 256 {
		fail(c, http.StatusBadRequest, "invalid_request", "completion limit or prefix is invalid")
		return
	}
	catalog := flowquery.FlowFilterCatalog()
	items := make([]string, 0, limit)
	appendMatching := func(values []string) {
		for _, value := range values {
			if len(items) >= limit {
				return
			}
			if strings.HasPrefix(strings.ToLower(value), input.Prefix) {
				items = append(items, value)
			}
		}
	}
	switch input.Kind {
	case "field":
		fields := make([]string, 0, len(catalog))
		for _, definition := range catalog {
			fields = append(fields, definition.Name)
		}
		appendMatching(fields)
	case "operator", "value":
		definition, ok := findFlowFilterField(catalog, input.Field)
		if !ok {
			fail(c, http.StatusBadRequest, "invalid_request", "completion field is not in the Flow registry")
			return
		}
		if input.Kind == "operator" {
			operators := make([]string, 0, len(definition.Operators))
			for _, operator := range definition.Operators {
				operators = append(operators, string(operator))
			}
			appendMatching(operators)
		} else {
			appendMatching(definition.Values)
		}
	default:
		fail(c, http.StatusBadRequest, "invalid_request", "completion kind must be field, operator or value")
		return
	}
	sort.Strings(items)
	c.JSON(http.StatusOK, gin.H{"items": items, "total": len(items)})
}

func findFlowFilterField(catalog []flowquery.FilterFieldDefinition, field string) (flowquery.FilterFieldDefinition, bool) {
	for _, definition := range catalog {
		if definition.Name == field {
			return definition, true
		}
	}
	return flowquery.FilterFieldDefinition{}, false
}

// flowAggregateInput is the v2 flow aggregate (Explorer) query: a business time
// window plus a display density (target_points) or explicit step, one or more
// dimensions, a metric, a value-layer view, and typed/structured filters.
type flowAggregateInput struct {
	From         time.Time                   `json:"from"`
	To           time.Time                   `json:"to"`
	StepSeconds  uint32                      `json:"step_seconds,omitempty"`
	TargetPoints uint16                      `json:"target_points,omitempty"`
	Metric       flowquery.Metric            `json:"metric"`
	View         flowquery.View              `json:"view"`
	Dimension    flowquery.Dimension         `json:"dimension,omitempty"`
	Dimensions   []flowquery.Dimension       `json:"dimensions,omitempty"`
	Filters      flowquery.Filters           `json:"filters,omitempty"`
	Filter       *flowquery.FilterExpression `json:"filter,omitempty"`
	TopN         uint16                      `json:"top_n,omitempty"`
	IncludeOther bool                        `json:"include_other,omitempty"`
	Timezone     string                      `json:"timezone,omitempty"`
	Operator     *flowOperatorSelection      `json:"operator_selection,omitempty"`
	Table        *flowTableRequest           `json:"table,omitempty"`
}

// queryFlow runs the core aggregate time-series query against ClickHouse: a
// single dimension is planned onto a rollup table and top-N ranked; multiple
// dimensions compile to a joint tuple query. Address-set combinations and the
// inbound/outbound direction split are dispatched to their own composers
// (flow_query_modes.go). Operator selection (needs the operator-classification
// binding service) and the storage-v2 recent-raw hybrid are ported in later slices.
func (s *Server) queryFlow(c *gin.Context) {
	if !s.flowQueryReady(c) {
		return
	}
	var envelope flowQueryEnvelope
	if !addressDecodeStrict(c, &envelope, 272<<10) {
		return
	}
	input := envelope.Parameters
	view, ok := s.authorizeFlowView(c, envelope.ValueLayer)
	if !ok {
		return
	}
	if !s.authorizeFlowResourceFilters(c, input.Filters.TargetIDs, input.Filters.DeviceIDs, input.Filters.ExporterIDs) {
		return
	}
	// Operator-scoped queries pin the request to the operator's customer ISP identity
	// and to the address dimension snapshots effective (and installed on every active
	// flow worker) over the range. Address-set queries filter through DetailFilters and
	// cannot carry the isp/snapshot pin, so the two modes are mutually exclusive.
	if input.Operator != nil && strings.TrimSpace(input.Operator.OperatorID) != "" {
		if input.AddressSetFilter != nil || strings.TrimSpace(input.AddressSetEndpoint) != "" {
			fail(c, http.StatusBadRequest, "invalid_request", "operator selection cannot be combined with address-set queries")
			return
		}
		if !s.applyFlowOperatorSelection(c, input.Operator, view, envelope.From, envelope.To, &input.Filters, &input.Filter) {
			return
		}
	}
	now := time.Now().UTC()

	var tableReq *flowTableRequest
	if input.Table != nil {
		if err := normalizeFlowTableRequest(input.Table); err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		input.Table.timezone = input.Timezone
		tableReq = input.Table
	}

	// Address-set combination and inbound/outbound direction split are distinct
	// compositions over the same runners (a flow_records address-set scan; two
	// per-direction total queries merged under a synthetic "direction" dimension).
	if input.AddressSetFilter != nil || strings.TrimSpace(input.AddressSetEndpoint) != "" {
		s.queryFlowAddressSet(c, envelope, input, view, now, tableReq)
		return
	}
	if input.DirectionSplit {
		s.queryFlowDirectionSplit(c, envelope, input, view, now, tableReq)
		return
	}

	step := time.Duration(envelope.StepSeconds) * time.Second
	scope := flowquery.Scope{AllowedViews: []flowquery.View{view}}

	if len(input.Dimensions) > 0 {
		compiled, err := flowquery.CompileJoint(scope, flowquery.JointRequest{
			From: envelope.From, To: envelope.To, Interval: step, TargetPoints: input.TargetPoints,
			Metric: input.Metric, Dimensions: input.Dimensions, Filters: input.Filters, Filter: input.Filter,
			View: view, TopN: input.TopN, IncludeOther: input.IncludeOther, Timezone: input.Timezone, TimeWindows: input.TimeWindows,
			ExecutionTimeout: s.cfg.Flow.Query.ExecutionTimeout,
		}, now)
		if err != nil {
			writeFlowQueryError(c, err)
			return
		}
		result, err := s.flowQuery.joint.Run(c.Request.Context(), compiled)
		if err != nil {
			writeFlowQueryError(c, err)
			return
		}
		raw, err := marshalFlowJointResult(result, tableReq, s.flowGeo)
		if err != nil {
			fail(c, http.StatusInternalServerError, "internal", "encode flow result")
			return
		}
		s.auditFlowQuery(c.Request.Context(), currentPrincipal(c).UserID, "flow.query", "flow_query", string(input.Metric), input.Operator)
		c.JSON(http.StatusOK, gin.H{"data": json.RawMessage(raw), "meta": flowQueryResultMeta(
			view, result.Metric.Unit, string(compiled.Plan.Source), input.Timezone, compiled.Plan.StepSeconds, 1, false, input.Operator)})
		return
	}

	plan, err := flowquery.PlanAggregate(envelope.From, envelope.To, step, input.TargetPoints, now)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	queryRequest := flowquery.Request{
		From: plan.EffectiveFrom, To: plan.EffectiveTo, Bucket: plan.Source, Interval: plan.Interval,
		Metric: input.Metric, Dimension: input.Dimension, Filters: input.Filters, Filter: input.Filter,
		View: view, TopN: input.TopN, IncludeOther: input.IncludeOther, Timezone: input.Timezone, TimeWindows: input.TimeWindows,
		ExecutionTimeout: s.cfg.Flow.Query.ExecutionTimeout,
	}
	if err := s.applyFlowStorageBoundary(c.Request.Context(), &queryRequest); err != nil {
		writeFlowQueryError(c, err)
		return
	}
	compiled, err := flowquery.Compile(scope, queryRequest, now)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	result, err := s.flowQuery.aggregate.Run(c.Request.Context(), compiled)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	raw, err := marshalFlowAggregateResult(result, tableReq, s.flowGeo)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", "encode flow result")
		return
	}
	s.auditFlowQuery(c.Request.Context(), currentPrincipal(c).UserID, "flow.query", "flow_query", string(input.Metric), input.Operator)
	c.JSON(http.StatusOK, gin.H{"data": json.RawMessage(raw), "meta": flowQueryResultMeta(
		view, result.Metric.Unit, string(plan.Source), input.Timezone, plan.StepSeconds,
		result.RollupCompleteness.Ratio, !result.RollupCompleteness.Complete, input.Operator)})
}

// flowQueryReady guards the ClickHouse-backed flow query handlers: when the
// server started without a ClickHouse endpoint the runners are absent, so the
// query surface answers 503 rather than nil-panicking.
func (s *Server) flowQueryReady(c *gin.Context) bool {
	if s.flowQuery == nil {
		fail(c, http.StatusServiceUnavailable, "flow_query_unavailable", "flow query requires a configured ClickHouse endpoint")
		return false
	}
	return true
}

// queryFlowOverseas runs the overseas KPI + geo breakdown against ClickHouse.
// Overseas is a customer-view-only surface. The StorageV2 recent-raw hybrid and
// human geo labels depend on the storage-lifecycle policy and geo service ported
// in later slices; until then it reads the rolled-up aggregate.
func (s *Server) queryFlowOverseas(c *gin.Context) {
	if !s.flowQueryReady(c) {
		return
	}
	var input flowquery.OverseasRequest
	if !addressDecodeStrict(c, &input, 256<<10) {
		return
	}
	view, ok := s.authorizeFlowView(c, input.View)
	if !ok {
		return
	}
	if view != flowquery.ViewCustomer {
		fail(c, http.StatusForbidden, "forbidden", "flow overseas is only available in the customer view")
		return
	}
	if !s.authorizeFlowResourceFilters(c, input.Filters.TargetIDs, input.Filters.DeviceIDs, input.Filters.ExporterIDs) {
		return
	}
	input.View = view
	input.ExecutionTimeout = s.cfg.Flow.Query.ExecutionTimeout
	if err := s.applyFlowOverseasStorageBoundary(c.Request.Context(), &input); err != nil {
		writeFlowQueryError(c, err)
		return
	}
	compiled, err := flowquery.CompileOverseas(flowquery.Scope{AllowedViews: []flowquery.View{view}}, input, time.Now().UTC())
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	result, err := s.flowQuery.overseas.Run(c.Request.Context(), compiled)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow.overseas.query", "flow_overseas", string(input.Bucket))
	c.JSON(http.StatusOK, gin.H{"data": result, "meta": gin.H{
		"source": input.Bucket, "step_seconds": uint32(compiled.BucketDuration / time.Second), "uses_raw": compiled.UsesRawFacts,
		"geo_labels": s.flowOverseasGeoLabels(result),
	}})
}

// flowRecordCapabilities returns the per-view field/filter capability catalog so
// the client knows which raw/supplier/customer columns and filters are queryable.
func (s *Server) flowRecordCapabilities(c *gin.Context) {
	capabilities := flowquery.DetailCapabilities()
	c.JSON(http.StatusOK, gin.H{"items": capabilities, "total": len(capabilities)})
}

// searchFlowRecords runs a typed flow-record detail search directly against
// ClickHouse via the flow query service.
func (s *Server) searchFlowRecords(c *gin.Context) {
	if !s.flowQueryReady(c) {
		return
	}
	var input flowquery.DetailRequest
	if !addressDecodeStrict(c, &input, 256<<10) {
		return
	}
	view, ok := s.authorizeFlowView(c, input.View)
	if !ok {
		return
	}
	targetIDs, deviceIDs, exporterIDs := flowDetailResourceFilters(input.Filters, input.ColumnFilters)
	if !s.authorizeFlowResourceFilters(c, targetIDs, deviceIDs, exporterIDs) {
		return
	}
	input.View = view
	compiled, err := flowquery.CompileDetail(flowquery.Scope{AllowedViews: []flowquery.View{view}}, input, time.Now().UTC())
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	result, err := s.flowQuery.detail.Run(c.Request.Context(), compiled)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow.records.search", "flow_records", string(view))
	c.JSON(http.StatusOK, gin.H{"data": result, "meta": gin.H{"page_size": input.Limit}})
}

// flowRecordFacets returns bounded value counts for one detail field, for the
// client's filter-builder facets.
func (s *Server) flowRecordFacets(c *gin.Context) {
	if !s.flowQueryReady(c) {
		return
	}
	var input flowquery.DetailFacetRequest
	if !addressDecodeStrict(c, &input, 256<<10) {
		return
	}
	view, ok := s.authorizeFlowView(c, input.View)
	if !ok {
		return
	}
	targetIDs, deviceIDs, exporterIDs := flowDetailResourceFilters(input.Filters, input.ColumnFilters)
	if !s.authorizeFlowResourceFilters(c, targetIDs, deviceIDs, exporterIDs) {
		return
	}
	input.View = view
	compiled, err := flowquery.CompileDetailFacet(flowquery.Scope{AllowedViews: []flowquery.View{view}}, input, time.Now().UTC())
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	result, err := s.flowQuery.detail.RunFacet(c.Request.Context(), compiled)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": result, "meta": gin.H{"limit": input.Limit}})
}
