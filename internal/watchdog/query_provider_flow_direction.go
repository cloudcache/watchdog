package watchdog

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

// queryDirections keeps the overview's inbound/outbound split inside one
// authorized QueryGateway execution. Both sides still use the mature rollup
// compiler; the API no longer asks the browser to merge two independently
// admitted responses before it can paginate the result table.
func (p ClickHouseFlowQueryProvider) queryDirections(
	ctx context.Context,
	request QueryProviderRequest,
	parameters flowAggregateQueryParameters,
	view flowquery.View,
) (QueryProviderResult, error) {
	if parameters.Filter != nil {
		supported, supportErr := flowquery.AggregateFilterSupported(*parameters.Filter)
		if supportErr != nil {
			return QueryProviderResult{}, mapFlowQueryError(supportErr)
		}
		if !supported {
			return p.queryJointDirections(ctx, request, parameters, view)
		}
	}
	plan, err := flowquery.PlanAggregate(
		request.From, request.To, time.Duration(request.StepSeconds)*time.Second,
		parameters.TargetPoints, p.now(),
	)
	if err != nil {
		return QueryProviderResult{}, mapFlowQueryError(err)
	}
	combined := flowquery.Result{
		Dimension: flowquery.DimensionDefinition{Kind: flowquery.Dimension("direction"), Additive: true},
		Plan:      &plan,
	}
	first := true
	timezone := parameters.Timezone
	for _, part := range []struct {
		direction string
		label     string
	}{{direction: "in", label: "Inbound"}, {direction: "out", label: "Outbound"}} {
		filters := parameters.Filters
		filters.Directions = []string{part.direction}
		compiled, compileErr := flowquery.Compile(
			flowquery.Scope{TenantID: string(request.TenantID), AllowedViews: []flowquery.View{view}},
			flowquery.Request{
				From: plan.EffectiveFrom, To: plan.EffectiveTo, Bucket: plan.Source, Interval: plan.Interval,
				Metric: parameters.Metric, Dimension: flowquery.DimensionTotal, Filters: filters, Filter: parameters.Filter,
				View: view, TopN: 1, IncludeOther: false, Timezone: parameters.Timezone,
			},
			p.now(),
		)
		if compileErr != nil {
			return QueryProviderResult{}, mapFlowQueryError(compileErr)
		}
		timezone = compiled.Timezone
		publicRows := compiled.EstimatedRows
		if publicRows > 0 {
			publicRows--
		}
		if uint64(len(combined.Points))+publicRows > uint64(request.Limit) {
			return QueryProviderResult{}, &QueryGatewayError{
				Code: QueryErrorRowLimit, Message: "Flow direction result exceeds the query row limit",
				Details: map[string]any{"max_result_rows": request.Limit},
			}
		}
		result, runErr := p.Runner.Run(ctx, compiled)
		if runErr != nil {
			return QueryProviderResult{}, mapFlowQueryError(runErr)
		}
		if first {
			combined.Metric = result.Metric
			combined.RollupCompleteness = result.RollupCompleteness
			first = false
		} else {
			if result.Metric != combined.Metric {
				return QueryProviderResult{}, fmt.Errorf("Flow direction query returned inconsistent metrics")
			}
			combined.RollupCompleteness.ExpectedBuckets = max(combined.RollupCompleteness.ExpectedBuckets, result.RollupCompleteness.ExpectedBuckets)
			combined.RollupCompleteness.CoveredBuckets = min(combined.RollupCompleteness.CoveredBuckets, result.RollupCompleteness.CoveredBuckets)
			combined.RollupCompleteness.Ratio = min(combined.RollupCompleteness.Ratio, result.RollupCompleteness.Ratio)
			combined.RollupCompleteness.Complete = combined.RollupCompleteness.Complete && result.RollupCompleteness.Complete
		}
		for _, point := range result.Points {
			point.DimensionValue = part.label
			combined.Points = append(combined.Points, point)
		}
	}
	if uint64(len(combined.Points)) > uint64(request.Limit) {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "Flow direction result exceeds the query row limit"}
	}
	versions := make(map[string]struct{})
	for _, point := range combined.Points {
		key := point.DimensionSnapshotID + "\x00" + point.GeoVersion + "\x00" + fmt.Sprint(point.ClassificationVersion)
		versions[key] = struct{}{}
	}
	combined.VersionCount = uint64(len(versions))
	combined.MixedVersions = len(versions) > 1
	data, err := marshalFlowAggregateResult(combined, parameters.Table)
	if err != nil {
		return QueryProviderResult{}, fmt.Errorf("marshal Flow direction query result: %w", err)
	}
	from, to := plan.EffectiveFrom, plan.EffectiveTo
	completeness := QueryCompleteness{
		AvailableFrom: &from, AvailableTo: &to, CompleteRatio: combined.RollupCompleteness.Ratio,
		Partial: !combined.RollupCompleteness.Complete, UnknownRatio: flowUnknownSamplingRatio(combined.Points),
	}
	if completeness.Partial {
		completeness.Warnings = append(completeness.Warnings, "one or more closed rollup buckets are not available")
	}
	if combined.MixedVersions {
		completeness.Warnings = append(completeness.Warnings, "result contains multiple dimension or classification versions")
	}
	return QueryProviderResult{
		Data: data, Unit: combined.Metric.Unit, Timezone: timezone, StepSeconds: plan.StepSeconds,
		AsOf: flowResultAsOf(combined.Points, p.now()), Versions: flowResultVersions(combined.Points), Completeness: completeness,
	}, nil
}

func (p ClickHouseFlowQueryProvider) queryJointDirections(
	ctx context.Context,
	request QueryProviderRequest,
	parameters flowAggregateQueryParameters,
	view flowquery.View,
) (QueryProviderResult, error) {
	if p.JointRunner == nil {
		return QueryProviderResult{}, ErrQueryProviderUnavailable
	}
	combined := flowquery.JointResult{
		Dimensions: []flowquery.DimensionDefinition{{Kind: flowquery.Dimension("direction"), Additive: true}},
	}
	first := true
	var timezone string
	for _, part := range []struct {
		direction string
		label     string
	}{{direction: "in", label: "Inbound"}, {direction: "out", label: "Outbound"}} {
		filters := parameters.Filters
		filters.Directions = []string{part.direction}
		compiled, compileErr := flowquery.CompileJoint(
			flowquery.Scope{TenantID: string(request.TenantID), AllowedViews: []flowquery.View{view}},
			flowquery.JointRequest{
				From: request.From, To: request.To, Interval: time.Duration(request.StepSeconds) * time.Second,
				TargetPoints: parameters.TargetPoints, Metric: parameters.Metric, Dimensions: []flowquery.Dimension{flowquery.DimensionTotal},
				Filters: filters, Filter: parameters.Filter, View: view, TopN: 1, IncludeOther: false, Timezone: parameters.Timezone,
			},
			p.now(),
		)
		if compileErr != nil {
			return QueryProviderResult{}, mapFlowQueryError(compileErr)
		}
		if uint64(len(combined.Points))+compiled.EstimatedRows > uint64(request.Limit) {
			return QueryProviderResult{}, &QueryGatewayError{
				Code: QueryErrorRowLimit, Message: "Flow direction result exceeds the query row limit",
				Details: map[string]any{"max_result_rows": request.Limit},
			}
		}
		result, runErr := p.JointRunner.Run(ctx, compiled)
		if runErr != nil {
			return QueryProviderResult{}, mapFlowQueryError(runErr)
		}
		if first {
			combined.Metric = result.Metric
			combined.Plan = result.Plan
			timezone = compiled.Timezone
			first = false
		} else if result.Metric != combined.Metric || result.Plan != combined.Plan {
			return QueryProviderResult{}, fmt.Errorf("Flow direction query returned inconsistent joint plans")
		}
		for _, point := range result.Points {
			point.DimensionValues = []string{part.label}
			combined.Points = append(combined.Points, point)
		}
	}
	versions := make(map[string]struct{})
	for _, point := range combined.Points {
		key := point.DimensionSnapshotID + "\x00" + point.GeoVersion + "\x00" + fmt.Sprint(point.ClassificationVersion)
		versions[key] = struct{}{}
	}
	combined.VersionCount = uint64(len(versions))
	combined.MixedVersions = len(versions) > 1
	data, err := marshalFlowJointResult(combined, parameters.Table)
	if err != nil {
		return QueryProviderResult{}, fmt.Errorf("marshal Flow direction joint-query result: %w", err)
	}
	from, to := combined.Plan.EffectiveFrom, combined.Plan.EffectiveTo
	return QueryProviderResult{
		Data: data, Unit: combined.Metric.Unit, Timezone: timezone, StepSeconds: combined.Plan.StepSeconds,
		AsOf: flowJointResultAsOf(combined.Points, p.now()), Versions: flowJointResultVersions(combined.Points),
		Completeness: QueryCompleteness{
			AvailableFrom: &from, AvailableTo: &to, CompleteRatio: 1, Partial: true,
			UnknownRatio: flowJointUnknownSamplingRatio(combined.Points),
			Warnings:     []string{"joint result is a bounded flow_records scan; end-to-end ingest coverage is not independently proven"},
		},
	}, nil
}
