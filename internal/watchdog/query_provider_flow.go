package watchdog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

const FlowTrafficDataset = "flow.traffic"

type flowAggregateRunner interface {
	Run(context.Context, flowquery.Compiled) (flowquery.Result, error)
}

type flowJointRunner interface {
	Run(context.Context, flowquery.CompiledJoint) (flowquery.JointResult, error)
}

type flowQueryReadiness interface {
	Ready(context.Context) error
}

// ClickHouseFlowQueryProvider is the sole platform adapter between the
// provider-neutral QueryGateway envelope and Flow's typed compiler/runner.
// Tenant, time range and value layer always come from the authenticated
// envelope; clients cannot smuggle them through provider parameters.
type ClickHouseFlowQueryProvider struct {
	Runner           flowAggregateRunner
	JointRunner      flowJointRunner
	Readiness        flowQueryReadiness
	Network          NetworkRepository
	StorageLifecycle FlowStorageArchiveBoundaryRepository
	Now              func() time.Time
}

type flowAggregateQueryParameters struct {
	Metric         flowquery.Metric            `json:"metric"`
	Dimension      flowquery.Dimension         `json:"dimension,omitempty"`
	Dimensions     []flowquery.Dimension       `json:"dimensions,omitempty"`
	Filters        flowquery.Filters           `json:"filters,omitempty"`
	Filter         *flowquery.FilterExpression `json:"filter,omitempty"`
	TopN           uint16                      `json:"top_n"`
	IncludeOther   bool                        `json:"include_other"`
	Timezone       string                      `json:"timezone,omitempty"`
	TargetPoints   uint16                      `json:"target_points,omitempty"`
	DirectionSplit bool                        `json:"direction_split,omitempty"`
	Table          *flowTableRequest           `json:"table,omitempty"`
}

func (p ClickHouseFlowQueryProvider) Ready(ctx context.Context) error {
	if p.Runner == nil || p.Readiness == nil {
		return errors.New("ClickHouse Flow query provider is not configured")
	}
	return p.Readiness.Ready(ctx)
}

func (p ClickHouseFlowQueryProvider) Query(ctx context.Context, request QueryProviderRequest) (QueryProviderResult, error) {
	if p.Runner == nil {
		return QueryProviderResult{}, ErrQueryProviderUnavailable
	}
	if request.Dataset.Key != FlowTrafficDataset {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "unsupported Flow dataset"}
	}
	parameters, err := decodeFlowAggregateQueryParameters(request.Parameters)
	if err != nil {
		return QueryProviderResult{}, err
	}
	view, err := flowView(request.ValueLayer)
	if err != nil {
		return QueryProviderResult{}, err
	}
	if parameters.DirectionSplit {
		return p.queryDirections(ctx, request, parameters, view)
	}
	baseFilter := false
	if parameters.Filter != nil {
		aggregateSupported, supportErr := flowquery.AggregateFilterSupported(*parameters.Filter)
		if supportErr != nil {
			return QueryProviderResult{}, mapFlowQueryError(supportErr)
		}
		baseFilter = !aggregateSupported
	}
	if len(parameters.Dimensions) > 0 || baseFilter {
		return p.queryJoint(ctx, request, parameters, view)
	}
	plan, err := flowquery.PlanAggregate(
		request.From, request.To, time.Duration(request.StepSeconds)*time.Second,
		parameters.TargetPoints, p.now(),
	)
	if err != nil {
		return QueryProviderResult{}, mapFlowQueryError(err)
	}
	flowRequest := flowquery.Request{
		From: plan.EffectiveFrom, To: plan.EffectiveTo, Bucket: plan.Source, Interval: plan.Interval, Metric: parameters.Metric,
		Dimension: parameters.Dimension, Filters: parameters.Filters, View: view,
		Filter: parameters.Filter,
		TopN:   parameters.TopN, IncludeOther: parameters.IncludeOther, Timezone: parameters.Timezone,
	}
	if err := p.applyStorageV2Boundary(ctx, request.TenantID, plan, &flowRequest); err != nil {
		return QueryProviderResult{}, fmt.Errorf("resolve Flow storage boundary: %w", err)
	}
	compiled, err := flowquery.Compile(flowquery.Scope{TenantID: string(request.TenantID), AllowedViews: []flowquery.View{view}}, flowRequest, p.now())
	if err != nil {
		return QueryProviderResult{}, mapFlowQueryError(err)
	}
	publicRows := compiled.EstimatedRows
	if publicRows > 0 {
		publicRows-- // compiler includes one internal completeness sentinel
	}
	if publicRows > uint64(request.Limit) {
		return QueryProviderResult{}, &QueryGatewayError{
			Code: QueryErrorRowLimit, Message: "Flow result exceeds the query row limit",
			Details: map[string]any{"max_result_rows": request.Limit, "estimated_result_rows": publicRows},
		}
	}
	result, err := p.Runner.Run(ctx, compiled)
	if err != nil {
		return QueryProviderResult{}, mapFlowQueryError(err)
	}
	if uint64(len(result.Points)) > uint64(request.Limit) {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "Flow result exceeds the query row limit"}
	}
	result.Plan = &plan
	data, err := marshalFlowAggregateResult(result, parameters.Table)
	if err != nil {
		return QueryProviderResult{}, fmt.Errorf("marshal Flow query result: %w", err)
	}
	from, to := plan.EffectiveFrom, plan.EffectiveTo
	completeness := QueryCompleteness{
		AvailableFrom: &from, AvailableTo: &to,
		CompleteRatio: result.RollupCompleteness.Ratio,
		Partial:       !result.RollupCompleteness.Complete,
		UnknownRatio:  flowUnknownSamplingRatio(result.Points),
	}
	if completeness.Partial {
		completeness.Warnings = append(completeness.Warnings, "one or more closed rollup buckets are not available")
	}
	if result.MixedVersions {
		completeness.Warnings = append(completeness.Warnings, "result contains multiple dimension or classification versions")
	}
	if compiled.UsesRawFacts {
		completeness.Warnings = append(completeness.Warnings, "the raw portion is current data; Kafka receipt coverage is reported separately from query completeness")
	}
	return QueryProviderResult{
		Data: data, Unit: result.Metric.Unit, Timezone: compiled.Timezone,
		StepSeconds: plan.StepSeconds,
		AsOf:        flowResultAsOf(result.Points, p.now()), Versions: flowResultVersions(result.Points),
		Completeness: completeness,
	}, nil
}

func (p ClickHouseFlowQueryProvider) applyStorageV2Boundary(ctx context.Context, tenantID ID, plan flowquery.AggregatePlan, request *flowquery.Request) error {
	if p.StorageLifecycle == nil || request == nil {
		return nil
	}
	request.StorageV2 = true
	request.ArchiveThrough = plan.EffectiveFrom
	if plan.Source != flowquery.BucketOneHour {
		return nil
	}
	boundary, err := p.StorageLifecycle.FlowStorageArchiveThrough(ctx, tenantID, plan.EffectiveFrom, plan.EffectiveTo)
	if err != nil {
		return err
	}
	request.ArchiveThrough = boundary
	return nil
}

func (p ClickHouseFlowQueryProvider) queryJoint(
	ctx context.Context,
	request QueryProviderRequest,
	parameters flowAggregateQueryParameters,
	view flowquery.View,
) (QueryProviderResult, error) {
	if p.JointRunner == nil {
		return QueryProviderResult{}, ErrQueryProviderUnavailable
	}
	compiled, err := flowquery.CompileJoint(flowquery.Scope{
		TenantID: string(request.TenantID), AllowedViews: []flowquery.View{view},
	}, flowquery.JointRequest{
		From: request.From, To: request.To, Interval: time.Duration(request.StepSeconds) * time.Second,
		TargetPoints: parameters.TargetPoints, Metric: parameters.Metric, Dimensions: flowBaseDimensions(parameters),
		Filters: parameters.Filters, Filter: parameters.Filter, View: view, TopN: parameters.TopN,
		IncludeOther: parameters.IncludeOther, Timezone: parameters.Timezone,
	}, p.now())
	if err != nil {
		return QueryProviderResult{}, mapFlowQueryError(err)
	}
	if compiled.EstimatedRows > uint64(request.Limit) {
		return QueryProviderResult{}, &QueryGatewayError{
			Code: QueryErrorRowLimit, Message: "Flow joint result exceeds the query row limit",
			Details: map[string]any{"max_result_rows": request.Limit, "estimated_result_rows": compiled.EstimatedRows},
		}
	}
	result, err := p.JointRunner.Run(ctx, compiled)
	if err != nil {
		return QueryProviderResult{}, mapFlowQueryError(err)
	}
	if uint64(len(result.Points)) > uint64(request.Limit) {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "Flow joint result exceeds the query row limit"}
	}
	data, err := marshalFlowJointResult(result, parameters.Table)
	if err != nil {
		return QueryProviderResult{}, fmt.Errorf("marshal Flow joint-query result: %w", err)
	}
	from, to := compiled.From, compiled.To
	return QueryProviderResult{
		Data: data, Unit: result.Metric.Unit, Timezone: compiled.Timezone, StepSeconds: result.Plan.StepSeconds,
		AsOf: flowJointResultAsOf(result.Points, p.now()), Versions: flowJointResultVersions(result.Points),
		Completeness: QueryCompleteness{
			AvailableFrom: &from, AvailableTo: &to, CompleteRatio: 1, Partial: true,
			UnknownRatio: flowJointUnknownSamplingRatio(result.Points),
			Warnings:     []string{"joint result is a bounded flow_records scan; end-to-end ingest coverage is not independently proven"},
		},
	}, nil
}

func flowBaseDimensions(parameters flowAggregateQueryParameters) []flowquery.Dimension {
	if len(parameters.Dimensions) > 0 {
		return parameters.Dimensions
	}
	return []flowquery.Dimension{parameters.Dimension}
}

func (p ClickHouseFlowQueryProvider) AuthorizeQuery(ctx context.Context, auth AuthContext, request QueryProviderRequest) error {
	if auth.IsAdmin {
		return nil
	}
	parameters, err := decodeFlowAggregateQueryParameters(request.Parameters)
	if err != nil {
		return err
	}
	if err := authorizeFlowResourceFilters(ctx, auth, p.Network, parameters.Filters.TargetIDs, parameters.Filters.DeviceIDs, parameters.Filters.ExporterIDs); err != nil {
		return err
	}
	if parameters.Filter != nil {
		fields, fieldErr := flowquery.FilterFields(*parameters.Filter)
		if fieldErr != nil {
			return mapFlowQueryError(fieldErr)
		}
		for _, field := range fields {
			switch field {
			case "target", "device", "exporter":
				return &QueryGatewayError{
					Code:    QueryErrorPermissionDenied,
					Message: "resource fields in typed filters require administrator access; use the authorized resource selector",
				}
			}
		}
	}
	return nil
}

func authorizeFlowResourceFilters(ctx context.Context, auth AuthContext, network NetworkRepository, targetIDs, deviceIDs, exporterIDs []string) error {
	if auth.IsAdmin {
		return nil
	}
	for _, targetID := range targetIDs {
		if !canAccessMetrics(auth, MetricsQueryRequest{TenantID: auth.TenantID, TargetID: ID(targetID)}) {
			return &QueryGatewayError{Code: QueryErrorPermissionDenied, Message: "query resource permission denied"}
		}
	}
	for _, deviceID := range deviceIDs {
		if network == nil {
			return &QueryGatewayError{Code: QueryErrorPermissionDenied, Message: "query resource permission denied"}
		}
		device, err := network.GetDevice(ctx, auth.TenantID, ID(deviceID))
		if err != nil || !canAccessMetrics(auth, MetricsQueryRequest{TenantID: auth.TenantID, TargetID: device.TargetID, DeviceID: device.ID}) {
			return &QueryGatewayError{Code: QueryErrorPermissionDenied, Message: "query resource permission denied", Cause: err}
		}
	}
	if len(exporterIDs) > 0 {
		return &QueryGatewayError{Code: QueryErrorPermissionDenied, Message: "exporter-scoped queries require administrator access"}
	}
	return nil
}

func decodeFlowAggregateQueryParameters(raw json.RawMessage) (flowAggregateQueryParameters, error) {
	var parameters flowAggregateQueryParameters
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parameters); err != nil {
		return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "invalid Flow query parameters", Cause: err}
	}
	if err := ensureDashboardJSONEOF(decoder); err != nil {
		return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "invalid Flow query parameters", Cause: err}
	}
	if (parameters.Dimension == "") == (len(parameters.Dimensions) == 0) {
		return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "exactly one of dimension or dimensions is required"}
	}
	if parameters.DirectionSplit && (parameters.Dimension != flowquery.DimensionTotal || len(parameters.Dimensions) != 0 ||
		parameters.TopN != 1 || parameters.IncludeOther || len(parameters.Filters.Directions) != 0) {
		return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "direction_split requires dimension=total, top_n=1, include_other=false, and no direction filter"}
	}
	if err := normalizeFlowTableRequest(parameters.Table); err != nil {
		return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: err.Error(), Cause: err}
	}
	if parameters.Filter != nil {
		canonical, err := flowquery.CanonicalFilter(*parameters.Filter)
		if err != nil {
			return parameters, mapFlowQueryError(err)
		}
		if !reflect.DeepEqual(canonical, *parameters.Filter) {
			return parameters, &QueryGatewayError{
				Code:    QueryErrorInvalidRequest,
				Message: "filter must use the canonical AST returned by POST /api/v1/flow/filters/validate",
			}
		}
	}
	return parameters, nil
}

func flowView(layer QueryValueLayer) (flowquery.View, error) {
	switch layer {
	case QueryValueRaw:
		return flowquery.ViewRaw, nil
	case QueryValueSupplier:
		return flowquery.ViewSupplier, nil
	case QueryValueCustomer:
		return flowquery.ViewCustomer, nil
	default:
		return "", &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "unsupported Flow value layer"}
	}
}

func mapFlowQueryError(err error) error {
	var requestErr *flowquery.RequestError
	if !errors.As(err, &requestErr) {
		return err
	}
	code := QueryErrorInvalidRequest
	switch requestErr.Code {
	case flowquery.ErrorLimitExceeded:
		code = QueryErrorRangeLimit
	case flowquery.ErrorIncompleteRange:
		code = QueryErrorIncomplete
	case flowquery.ErrorPermissionDenied:
		code = QueryErrorPermissionDenied
	}
	return &QueryGatewayError{
		Code: code, Message: requestErr.Message,
		Details: map[string]any{"field": requestErr.Field, "flow_code": requestErr.Code}, Cause: err,
	}
}

func flowUnknownSamplingRatio(points []flowquery.Point) float64 {
	var received, unknown uint64
	for _, point := range points {
		received += point.ReceivedRecords
		unknown += point.UnknownSamplingRecords
	}
	if received == 0 {
		return 0
	}
	return float64(unknown) / float64(received)
}

func flowJointUnknownSamplingRatio(points []flowquery.JointPoint) float64 {
	var received, unknown uint64
	for _, point := range points {
		received += point.ReceivedRecords
		unknown += point.UnknownSamplingRecords
	}
	if received == 0 {
		return 0
	}
	return float64(unknown) / float64(received)
}

func flowResultAsOf(points []flowquery.Point, fallback time.Time) time.Time {
	asOf := time.Time{}
	for _, point := range points {
		if point.GeneratedAt.After(asOf) {
			asOf = point.GeneratedAt
		}
	}
	if asOf.IsZero() {
		asOf = fallback
	}
	return asOf.UTC()
}

func flowJointResultAsOf(points []flowquery.JointPoint, fallback time.Time) time.Time {
	asOf := time.Time{}
	for _, point := range points {
		if point.ObservedAt.After(asOf) {
			asOf = point.ObservedAt
		}
	}
	if asOf.IsZero() {
		asOf = fallback
	}
	return asOf.UTC()
}

func flowResultVersions(points []flowquery.Point) map[string]string {
	versions := map[string]map[string]struct{}{
		"dimension_snapshot_id": {}, "geo_version": {}, "classification_version": {},
	}
	for _, point := range points {
		versions["dimension_snapshot_id"][point.DimensionSnapshotID] = struct{}{}
		versions["geo_version"][point.GeoVersion] = struct{}{}
		versions["classification_version"][strconv.FormatUint(uint64(point.ClassificationVersion), 10)] = struct{}{}
	}
	result := make(map[string]string, len(versions))
	for key, values := range versions {
		if len(values) != 1 {
			continue
		}
		for value := range values {
			result[key] = value
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func flowJointResultVersions(points []flowquery.JointPoint) map[string]string {
	versions := map[string]map[string]struct{}{
		"dimension_snapshot_id": {}, "geo_version": {}, "classification_version": {},
	}
	for _, point := range points {
		versions["dimension_snapshot_id"][point.DimensionSnapshotID] = struct{}{}
		versions["geo_version"][point.GeoVersion] = struct{}{}
		versions["classification_version"][strconv.FormatUint(uint64(point.ClassificationVersion), 10)] = struct{}{}
	}
	result := make(map[string]string, len(versions))
	for key, values := range versions {
		if len(values) != 1 {
			continue
		}
		for value := range values {
			result[key] = value
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func (p ClickHouseFlowQueryProvider) now() time.Time {
	if p.Now != nil {
		return p.Now().UTC()
	}
	return time.Now().UTC()
}
