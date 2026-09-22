// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

// flow_query_modes.go carries the two /flow/query "special modes" that compose over
// the existing ClickHouse runners rather than adding engine grammar: the
// inbound/outbound direction split (one total-row query grouped under a synthetic
// "direction" dimension) and the address-set combination (a flow_records
// membership scan via the address-set runner). Both are faithful ports of the hub
// query gateway's queryDirections / queryAddressSets, keeping the same wire shape so
// the unchanged frontend works. operator_selection is prepared before these modes
// run and therefore shares their typed filter/query path.

// flowDirectionParts is the fixed inbound/outbound split the direction mode emits,
// matching the hub's labels ("in"/"out" filter value → "Inbound"/"Outbound" label).
var flowDirectionParts = []struct{ direction, label string }{
	{direction: "in", label: "Inbound"},
	{direction: "out", label: "Outbound"},
}

// directionSplitAllowed enforces the hub gate: the split is only defined for a
// single total series with no pre-existing direction filter (the composer sets the
// direction itself and forces dimension=total, top_n=1, include_other=false).
func directionSplitAllowed(input flowQueryParameters) bool {
	if input.Dimension != "" && input.Dimension != flowquery.DimensionTotal {
		return false
	}
	return len(input.Dimensions) == 0 && input.TopN <= 1 && !input.IncludeOther && len(input.Filters.Directions) == 0
}

// queryFlowDirectionSplit runs the inbound/outbound split as one authorized
// query grouped by the business_direction carried on additive total rows. A
// non-aggregate typed filter uses the equivalent one-scan joint query.
func (s *Server) queryFlowDirectionSplit(c *gin.Context, envelope flowQueryEnvelope, input flowQueryParameters, view flowquery.View, now time.Time, tableReq *flowTableRequest) {
	if !directionSplitAllowed(input) {
		fail(c, http.StatusBadRequest, "invalid_request", "direction_split requires dimension=total, top_n=1, include_other=false, and no direction filter")
		return
	}
	if input.Filter != nil {
		supported, err := flowquery.AggregateFilterSupported(*input.Filter)
		if err != nil {
			writeFlowQueryError(c, err)
			return
		}
		if !supported {
			s.queryFlowDirectionSplitJoint(c, envelope, input, view, now, tableReq)
			return
		}
	}
	step := time.Duration(envelope.StepSeconds) * time.Second
	plan, err := flowquery.PlanAggregate(envelope.From, envelope.To, step, input.TargetPoints, now)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	scope := flowquery.Scope{AllowedViews: []flowquery.View{view}}
	filters := input.Filters
	filters.Directions = flowDirectionValues()
	queryRequest := flowquery.Request{
		From: plan.EffectiveFrom, To: plan.EffectiveTo, Bucket: plan.Source, Interval: plan.Interval,
		Metric: input.Metric, Dimension: flowquery.DimensionDirection, Filters: filters, Filter: input.Filter,
		View: view, TopN: uint16(len(flowDirectionParts)), IncludeOther: false, Timezone: input.Timezone, TimeWindows: input.TimeWindows,
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
	combined, err := s.flowQuery.aggregate.Run(c.Request.Context(), compiled)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	labelFlowDirectionResult(&combined)
	combined.Plan = &plan
	raw, err := marshalFlowAggregateResult(combined, tableReq, s.flowGeo)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", "encode flow result")
		return
	}
	s.auditFlowQuery(c.Request.Context(), currentPrincipal(c).UserID, "flow.query", "flow_query", string(input.Metric), input.Operator)
	c.JSON(http.StatusOK, gin.H{"data": json.RawMessage(raw), "meta": flowQueryResultMeta(
		view, combined.Metric.Unit, string(plan.Source), input.Timezone, plan.StepSeconds,
		combined.RollupCompleteness.Ratio, !combined.RollupCompleteness.Complete, input.Operator)})
}

func flowDirectionValues() []string {
	values := make([]string, 0, len(flowDirectionParts))
	for _, part := range flowDirectionParts {
		values = append(values, part.direction)
	}
	return values
}

func flowDirectionLabel(value string) string {
	for _, part := range flowDirectionParts {
		if value == part.direction {
			return part.label
		}
	}
	return value
}

func labelFlowDirectionResult(result *flowquery.Result) {
	if result == nil {
		return
	}
	for index := range result.Points {
		result.Points[index].DimensionValue = flowDirectionLabel(result.Points[index].DimensionValue)
	}
}

func labelFlowDirectionJointResult(result *flowquery.JointResult) {
	if result == nil {
		return
	}
	for index := range result.Points {
		if len(result.Points[index].DimensionValues) == 1 {
			result.Points[index].DimensionValues[0] = flowDirectionLabel(result.Points[index].DimensionValues[0])
		}
	}
}

// mergeFlowDirectionResults folds the per-direction total results (aligned with
// labels) into one result under a synthetic "direction" dimension: each direction's
// points are relabeled, and completeness is reduced conservatively — widest expected
// buckets, narrowest covered, minimum ratio, AND of complete. A faithful extract of
// the hub's per-direction accumulation, kept pure so the fold is unit-testable.
func mergeFlowDirectionResults(plan *flowquery.AggregatePlan, labels []string, results []flowquery.Result) flowquery.Result {
	combined := flowquery.Result{
		Dimension: flowquery.DimensionDefinition{Kind: flowquery.Dimension("direction"), Additive: true},
		Plan:      plan,
	}
	for i, result := range results {
		if i == 0 {
			combined.View = result.View
			combined.Metric = result.Metric
			combined.RollupCompleteness = result.RollupCompleteness
		} else {
			combined.RollupCompleteness.ExpectedBuckets = max(combined.RollupCompleteness.ExpectedBuckets, result.RollupCompleteness.ExpectedBuckets)
			combined.RollupCompleteness.CoveredBuckets = min(combined.RollupCompleteness.CoveredBuckets, result.RollupCompleteness.CoveredBuckets)
			combined.RollupCompleteness.Ratio = min(combined.RollupCompleteness.Ratio, result.RollupCompleteness.Ratio)
			combined.RollupCompleteness.Complete = combined.RollupCompleteness.Complete && result.RollupCompleteness.Complete
		}
		for _, point := range result.Points {
			if i < len(labels) {
				point.DimensionValue = labels[i]
			}
			combined.Points = append(combined.Points, point)
		}
	}
	return combined
}

// queryFlowDirectionSplitJoint is the flow_records fallback used when the typed
// filter is not rollup-supported.
func (s *Server) queryFlowDirectionSplitJoint(c *gin.Context, envelope flowQueryEnvelope, input flowQueryParameters, view flowquery.View, now time.Time, tableReq *flowTableRequest) {
	step := time.Duration(envelope.StepSeconds) * time.Second
	scope := flowquery.Scope{AllowedViews: []flowquery.View{view}}
	filters := input.Filters
	filters.Directions = flowDirectionValues()
	compiled, err := flowquery.CompileJoint(scope, flowquery.JointRequest{
		From: envelope.From, To: envelope.To, Interval: step, TargetPoints: input.TargetPoints,
		Metric: input.Metric, Dimensions: []flowquery.Dimension{flowquery.DimensionDirection},
		Filters: filters, Filter: input.Filter, View: view, TopN: uint16(len(flowDirectionParts)), IncludeOther: false,
		Timezone: input.Timezone, TimeWindows: input.TimeWindows,
		ExecutionTimeout: s.cfg.Flow.Query.ExecutionTimeout,
	}, now)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	combined, err := s.flowQuery.joint.Run(c.Request.Context(), compiled)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	labelFlowDirectionJointResult(&combined)
	raw, err := marshalFlowJointResult(combined, tableReq, s.flowGeo)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", "encode flow result")
		return
	}
	s.auditFlowQuery(c.Request.Context(), currentPrincipal(c).UserID, "flow.query", "flow_query", string(input.Metric), input.Operator)
	c.JSON(http.StatusOK, gin.H{"data": json.RawMessage(raw), "meta": flowQueryResultMeta(
		view, combined.Metric.Unit, string(combined.Plan.Source), input.Timezone, combined.Plan.StepSeconds, 1, true, input.Operator)})
}

// queryFlowAddressSet runs a synchronous address-set combination: a fixed 60-second
// flow_records membership scan through the address-set runner, echoing the resolved
// filter/endpoint back in the payload. A faithful port of the hub's queryAddressSets.
func (s *Server) queryFlowAddressSet(c *gin.Context, envelope flowQueryEnvelope, input flowQueryParameters, view flowquery.View, now time.Time, tableReq *flowTableRequest) {
	if s.flowQuery.addressSet == nil {
		fail(c, http.StatusServiceUnavailable, "flow_address_set_unavailable", "flow address-set queries require a configured ClickHouse endpoint")
		return
	}
	if envelope.StepSeconds != 0 && envelope.StepSeconds != 60 {
		fail(c, http.StatusBadRequest, "invalid_request", "synchronous address-set combinations use a fixed 60-second step")
		return
	}
	var sets flowdimension.AddressSetFilter
	if input.AddressSetFilter != nil {
		sets = flowdimension.AddressSetFilter{
			IncludeAny: input.AddressSetFilter.IncludeAny,
			IncludeAll: input.AddressSetFilter.IncludeAll,
			ExcludeAny: input.AddressSetFilter.ExcludeAny,
		}
	}
	compiled, err := flowquery.CompileAddressSet(flowquery.Scope{AllowedViews: []flowquery.View{view}}, flowquery.AddressSetRequest{
		From: envelope.From, To: envelope.To, Bucket: flowquery.BucketOneMinute,
		Metric: input.Metric, View: view, Endpoint: flowquery.AddressSetEndpoint(strings.TrimSpace(input.AddressSetEndpoint)),
		Sets: sets,
		Filters: flowquery.DetailFilters{
			Directions: input.Filters.Directions, Categories: input.Filters.Categories,
			Businesses: input.Filters.Businesses, TargetIDs: input.Filters.TargetIDs,
			DeviceIDs: input.Filters.DeviceIDs, ExporterIDs: input.Filters.ExporterIDs,
		},
	}, now)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	result, err := s.flowQuery.addressSet.Run(c.Request.Context(), compiled)
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	label := flowAddressSetLabel(result)
	points := make([]flowquery.Point, 0, len(result.Points))
	for _, point := range result.Points {
		points = append(points, flowquery.Point{
			Bucket: point.Bucket, DimensionValue: label, DimensionSnapshotID: point.DimensionSnapshotID,
			GeoVersion: point.GeoVersion, ClassificationVersion: point.ClassificationVersion,
			Value: point.Value, ReceivedRecords: point.ReceivedRecords,
			UnknownSamplingRecords: point.UnknownSamplingRecords, QualityRecords: point.QualityRecords,
			SamplingCompleteness: point.SamplingCompleteness, SamplingCompletenessKnown: point.SamplingCompletenessKnown,
			QualityRecordRatio: point.QualityRecordRatio, QualityRecordRatioKnown: point.QualityRecordRatioKnown,
		})
	}
	plan := &flowquery.AggregatePlan{
		RequestedFrom: envelope.From.UTC(), RequestedTo: envelope.To.UTC(),
		EffectiveFrom: compiled.From, EffectiveTo: compiled.To,
		Source: flowquery.BucketFlowRecords, SourceStep: time.Minute, Interval: time.Minute,
		SourceSeconds: 60, StepSeconds: 60, TargetPoints: input.TargetPoints,
	}
	publicResult := flowquery.Result{
		Points: points, View: view, Metric: result.Metric,
		Dimension: flowquery.DimensionDefinition{Kind: flowquery.DimensionAddressSet, Additive: false},
		Plan:      plan, MixedVersions: result.MixedVersions, VersionCount: result.VersionCount,
	}
	raw, err := marshalFlowAddressSetResult(publicResult, tableReq, result)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", "encode flow result")
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow.query", "flow_query", string(input.Metric))
	c.JSON(http.StatusOK, gin.H{"data": json.RawMessage(raw), "meta": flowQueryResultMeta(
		view, result.Metric.Unit, string(flowquery.BucketFlowRecords), "UTC", 60, 1, true, nil)})
}

// marshalFlowAddressSetResult renders an address-set combination: the standard
// aggregate shape (with an optional table page) plus the resolved filter and
// endpoint the client echoes. Ported from the hub query gateway.
func marshalFlowAddressSetResult(result flowquery.Result, request *flowTableRequest, source flowquery.AddressSetResult) ([]byte, error) {
	var table *flowTablePage
	if request != nil {
		plan := flowTablePlan{}
		if result.Plan != nil {
			plan = flowTablePlan{
				from: result.Plan.EffectiveFrom, to: result.Plan.EffectiveTo, step: time.Duration(result.Plan.StepSeconds) * time.Second,
				timeWindows: request.timeWindows, timezone: request.timezone,
			}
		}
		built := buildFlowTable(flowAggregateTablePoints(result, nil), plan, result.Metric.Unit, *request)
		table = &built
	}
	return json.Marshal(struct {
		flowquery.Result
		AddressSetFilter flowdimension.AddressSetFilter `json:"address_set_filter"`
		Endpoint         flowquery.AddressSetEndpoint   `json:"address_set_endpoint"`
		Table            *flowTablePage                 `json:"table,omitempty"`
	}{Result: result, AddressSetFilter: source.Sets, Endpoint: source.Endpoint, Table: table})
}

// flowAddressSetLabel names the single series an address-set combination produces:
// the lone included set when unambiguous, else a generic combination label.
func flowAddressSetLabel(result flowquery.AddressSetResult) string {
	if len(result.Sets.IncludeAny) == 1 && len(result.Sets.IncludeAll) == 0 && len(result.Sets.ExcludeAny) == 0 {
		return result.Sets.IncludeAny[0]
	}
	return "address-set combination"
}
