package watchdog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowquery"
)

const FlowTrafficDataset = "flow.traffic"

type flowAggregateRunner interface {
	Run(context.Context, flowquery.Compiled) (flowquery.Result, error)
}

type flowJointRunner interface {
	Run(context.Context, flowquery.CompiledJoint) (flowquery.JointResult, error)
}

type flowAddressSetRunner interface {
	Run(context.Context, flowquery.CompiledAddressSet) (flowquery.AddressSetResult, error)
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
	AddressSetRunner flowAddressSetRunner
	OverseasRunner   flowOverseasRunner
	VPNFindings      VPNFindingExportRepository
	Readiness        flowQueryReadiness
	Network          NetworkRepository
	StorageLifecycle FlowStorageArchiveBoundaryRepository
	FlowGeo          *FlowGeoService
	OperatorBindings FlowOperatorQueryBindingReader
	Now              func() time.Time
}

type flowOperatorQuerySelection struct {
	SchemaVersion          uint16   `json:"schema_version,omitempty"`
	OperatorID             ID       `json:"operator_id"`
	FlowISPID              uint16   `json:"flow_isp_id,omitempty"`
	PublicationIDs         []ID     `json:"publication_ids,omitempty"`
	DimensionSnapshotIDs   []string `json:"dimension_snapshot_ids,omitempty"`
	ClassificationVersions []uint32 `json:"classification_versions,omitempty"`
}

type flowAggregateQueryParameters struct {
	Metric             flowquery.Metric               `json:"metric"`
	Dimension          flowquery.Dimension            `json:"dimension,omitempty"`
	Dimensions         []flowquery.Dimension          `json:"dimensions,omitempty"`
	Filters            flowquery.Filters              `json:"filters,omitempty"`
	Filter             *flowquery.FilterExpression    `json:"filter,omitempty"`
	TopN               uint16                         `json:"top_n"`
	IncludeOther       bool                           `json:"include_other"`
	Timezone           string                         `json:"timezone,omitempty"`
	TargetPoints       uint16                         `json:"target_points,omitempty"`
	TimeWindows        []flowquery.LocalTimeWindow    `json:"time_windows,omitempty"`
	DirectionSplit     bool                           `json:"direction_split,omitempty"`
	AddressSetFilter   flowdimension.AddressSetFilter `json:"address_set_filter,omitempty"`
	AddressSetEndpoint flowquery.AddressSetEndpoint   `json:"address_set_endpoint,omitempty"`
	OperatorSelection  *flowOperatorQuerySelection    `json:"operator_selection,omitempty"`
	Table              *flowTableRequest              `json:"table,omitempty"`
	Report             *flowReportSpec                `json:"report,omitempty"`
}

func (p ClickHouseFlowQueryProvider) Ready(ctx context.Context) error {
	if p.Runner == nil || p.Readiness == nil {
		return errors.New("ClickHouse Flow query provider is not configured")
	}
	return p.Readiness.Ready(ctx)
}

func (p ClickHouseFlowQueryProvider) PrepareQuery(ctx context.Context, request QueryProviderRequest) (json.RawMessage, error) {
	parameters, err := decodeFlowAggregateQueryParameters(request.Parameters)
	if err != nil || parameters.OperatorSelection == nil {
		return request.Parameters, err
	}
	if request.ValueLayer != QueryValueCustomer {
		return nil, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "operator selection requires the customer value layer"}
	}
	if p.OperatorBindings == nil {
		return nil, &QueryGatewayError{
			Code: QueryErrorProviderUnavailable, Message: "Flow operator classification readiness is unavailable", Retryable: true,
		}
	}
	selection := parameters.OperatorSelection
	prepared := selection.SchemaVersion == flowOperatorQueryBindingSchemaVersion
	if selection.SchemaVersion != 0 && !prepared {
		return nil, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "unsupported Flow operator selection schema"}
	}
	if !prepared && (selection.FlowISPID != 0 || len(selection.PublicationIDs) != 0 || len(selection.DimensionSnapshotIDs) != 0 || len(selection.ClassificationVersions) != 0) {
		return nil, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "unprepared Flow operator selection may only contain operator_id"}
	}
	binding, err := p.OperatorBindings.ResolveFlowOperatorQueryBinding(ctx, request.TenantID, selection.OperatorID, request.From, request.To)
	if err != nil {
		switch {
		case errors.Is(err, ErrFlowOperatorQueryInvalid):
			return nil, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "Flow operator selection is invalid", Cause: err}
		case errors.Is(err, ErrFlowOperatorQueryUnavailable):
			return nil, &QueryGatewayError{
				Code: QueryErrorIncomplete, Message: "Flow operator classification is not installed on every active worker for the requested range",
				Retryable: true, Cause: err,
			}
		default:
			return nil, &QueryGatewayError{Code: QueryErrorProviderUnavailable, Message: "Flow operator classification readiness is unavailable", Retryable: true, Cause: err}
		}
	}
	expected := flowOperatorQuerySelection{
		SchemaVersion: flowOperatorQueryBindingSchemaVersion, OperatorID: binding.OperatorID, FlowISPID: binding.FlowISPID,
		PublicationIDs:         append([]ID(nil), binding.PublicationIDs...),
		DimensionSnapshotIDs:   append([]string(nil), binding.DimensionSnapshotIDs...),
		ClassificationVersions: append([]uint32(nil), binding.ClassificationVersions...),
	}
	if prepared && !reflect.DeepEqual(*selection, expected) {
		return nil, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "prepared Flow operator selection does not match the immutable publication timeline"}
	}
	if prepared {
		if !reflect.DeepEqual(parameters.Filters.DimensionSnapshotIDs, expected.DimensionSnapshotIDs) ||
			!reflect.DeepEqual(parameters.Filters.ClassificationVersions, expected.ClassificationVersions) ||
			!flowFilterHasOnlyExpectedISP(parameters.Filter, expected.FlowISPID) {
			return nil, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "prepared Flow operator query constraints were changed"}
		}
	} else {
		if len(parameters.Filters.DimensionSnapshotIDs) != 0 || len(parameters.Filters.ClassificationVersions) != 0 || flowFilterReferencesISP(parameters.Filter) {
			return nil, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "operator selection owns ISP and enrichment-version constraints"}
		}
		parameters.Filters.DimensionSnapshotIDs = append([]string(nil), expected.DimensionSnapshotIDs...)
		parameters.Filters.ClassificationVersions = append([]uint32(nil), expected.ClassificationVersions...)
		isp := flowquery.FilterExpression{
			Op: flowquery.FilterPredicate, Field: "isp", Operator: flowquery.FilterEqual,
			Values: []string{strconv.FormatUint(uint64(expected.FlowISPID), 10)},
		}
		if parameters.Filter == nil {
			parameters.Filter = &isp
		} else {
			combined, canonicalErr := flowquery.CanonicalFilter(flowquery.FilterExpression{
				Op: flowquery.FilterAnd, Args: []flowquery.FilterExpression{*parameters.Filter, isp},
			})
			if canonicalErr != nil {
				return nil, mapFlowQueryError(canonicalErr)
			}
			parameters.Filter = &combined
		}
	}
	parameters.OperatorSelection = &expected
	canonical, err := json.Marshal(parameters)
	if err != nil {
		return nil, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "encode prepared Flow operator query", Cause: err}
	}
	return canonical, nil
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
	if parameters.OperatorSelection != nil && parameters.OperatorSelection.SchemaVersion != flowOperatorQueryBindingSchemaVersion {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "Flow operator selection was not prepared by the query gateway"}
	}
	if parameters.Table != nil && len(parameters.TimeWindows) != 0 {
		table := *parameters.Table
		table.timeWindows = append([]flowquery.LocalTimeWindow(nil), parameters.TimeWindows...)
		table.timezone = parameters.Timezone
		parameters.Table = &table
	}
	view, err := flowView(request.ValueLayer)
	if err != nil {
		return QueryProviderResult{}, err
	}
	if parameters.Report != nil {
		result, queryErr := p.queryReport(ctx, request, parameters)
		result.Versions = withFlowOperatorQueryVersions(result.Versions, parameters.OperatorSelection)
		return result, queryErr
	}
	if flowAddressSetFilterSelected(parameters.AddressSetFilter) {
		if parameters.OperatorSelection != nil {
			return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "operator selection cannot be combined with address-set combinations"}
		}
		return p.queryAddressSets(ctx, request, parameters, view)
	}
	if parameters.DirectionSplit {
		result, queryErr := p.queryDirections(ctx, request, parameters, view)
		result.Versions = withFlowOperatorQueryVersions(result.Versions, parameters.OperatorSelection)
		return result, queryErr
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
		result, queryErr := p.queryJoint(ctx, request, parameters, view)
		result.Versions = withFlowOperatorQueryVersions(result.Versions, parameters.OperatorSelection)
		return result, queryErr
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
		TimeWindows: parameters.TimeWindows,
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
	data, err := marshalFlowAggregateResult(result, parameters.Table, p.FlowGeo)
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
	providerResult := QueryProviderResult{
		Data: data, Unit: result.Metric.Unit, Timezone: compiled.Timezone,
		StepSeconds: plan.StepSeconds,
		AsOf:        flowResultAsOf(result.Points, p.now()), Versions: flowResultVersions(result.Points),
		Completeness: completeness,
	}
	providerResult.Versions = withFlowOperatorQueryVersions(providerResult.Versions, parameters.OperatorSelection)
	return providerResult, nil
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
		IncludeOther: parameters.IncludeOther, Timezone: parameters.Timezone, TimeWindows: parameters.TimeWindows,
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
	data, err := marshalFlowJointResult(result, parameters.Table, p.FlowGeo)
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
	needsVPNAccess := parameters.Report != nil && (parameters.Report.Kind == flowReportVPN ||
		(parameters.Report.Kind == flowReportOverseas && flowReportPanelSelected(*parameters.Report, "vpn_share")))
	if needsVPNAccess && !HasPermission(AccessRequest{
		TenantID: auth.TenantID, UserID: auth.UserID, RoleIDs: auth.RoleIDs, Action: ActionVPNView,
		Resource: ResourceRef{Type: ResourceTenant, ID: auth.TenantID},
	}, auth.Grants) {
		return &QueryGatewayError{Code: QueryErrorPermissionDenied, Message: "VPN reports require vpn_view permission"}
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
	if parameters.Report != nil {
		if parameters.Dimension != "" || len(parameters.Dimensions) != 0 || parameters.DirectionSplit ||
			flowAddressSetFilterSelected(parameters.AddressSetFilter) || parameters.AddressSetEndpoint != "" || len(parameters.TimeWindows) != 0 {
			return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "report cannot be combined with dimension, dimensions, direction_split, or address-set mode"}
		}
		if err := normalizeFlowReportSpec(parameters.Report, &parameters); err != nil {
			return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: err.Error(), Cause: err}
		}
	} else if (parameters.Dimension == "") == (len(parameters.Dimensions) == 0) {
		return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "exactly one of dimension or dimensions is required"}
	}
	if parameters.OperatorSelection != nil {
		parameters.OperatorSelection.OperatorID = ID(strings.TrimSpace(string(parameters.OperatorSelection.OperatorID)))
		if parameters.OperatorSelection.OperatorID == "" || len(parameters.OperatorSelection.OperatorID) > 64 {
			return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "operator_selection.operator_id is required"}
		}
	}
	if parameters.Report == nil && parameters.DirectionSplit && (parameters.Dimension != flowquery.DimensionTotal || len(parameters.Dimensions) != 0 ||
		parameters.TopN != 1 || parameters.IncludeOther || len(parameters.Filters.Directions) != 0) {
		return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "direction_split requires dimension=total, top_n=1, include_other=false, and no direction filter"}
	}
	addressSetSelected := flowAddressSetFilterSelected(parameters.AddressSetFilter)
	if parameters.Report == nil && addressSetSelected {
		if parameters.Dimension != flowquery.DimensionAddressSet || len(parameters.Dimensions) != 0 || parameters.DirectionSplit ||
			parameters.TopN != 1 || parameters.IncludeOther || parameters.Filter != nil ||
			parameters.AddressSetEndpoint == "" {
			return parameters, &QueryGatewayError{
				Code:    QueryErrorInvalidRequest,
				Message: "address_set_filter requires dimension=address_set, address_set_endpoint, top_n=1, include_other=false, and no joint/direction/typed filter",
			}
		}
		if len(parameters.Filters.DimensionValues) != 0 || len(parameters.Filters.DimensionSnapshotIDs) != 0 ||
			len(parameters.Filters.GeoVersions) != 0 || len(parameters.Filters.ClassificationVersions) != 0 {
			return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "address-set combinations do not accept aggregate dimension/version filters"}
		}
	} else if parameters.Report == nil && parameters.AddressSetEndpoint != "" {
		return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "address_set_endpoint requires address_set_filter"}
	}
	var tableErr error
	if parameters.Report != nil && parameters.Report.Kind == flowReportEndpoints {
		tableErr = normalizeFlowEndpointTableRequest(parameters.Table)
	} else {
		tableErr = normalizeFlowTableRequest(parameters.Table)
	}
	if tableErr != nil {
		return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: tableErr.Error(), Cause: tableErr}
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

func flowFilterReferencesISP(filter *flowquery.FilterExpression) bool {
	if filter == nil {
		return false
	}
	if filter.Op == flowquery.FilterPredicate {
		return filter.Field == "isp"
	}
	for index := range filter.Args {
		if flowFilterReferencesISP(&filter.Args[index]) {
			return true
		}
	}
	return false
}

func flowFilterHasOnlyExpectedISP(filter *flowquery.FilterExpression, flowISPID uint16) bool {
	if filter == nil || flowISPID == 0 {
		return false
	}
	want := strconv.FormatUint(uint64(flowISPID), 10)
	count := 0
	valid := true
	var visit func(flowquery.FilterExpression)
	visit = func(node flowquery.FilterExpression) {
		if node.Op == flowquery.FilterPredicate {
			if node.Field == "isp" {
				count++
				if node.Operator != flowquery.FilterEqual || len(node.Values) != 1 || node.Values[0] != want {
					valid = false
				}
			}
			return
		}
		for _, child := range node.Args {
			visit(child)
		}
	}
	visit(*filter)
	return valid && count == 1
}

func withFlowOperatorQueryVersions(versions map[string]string, selection *flowOperatorQuerySelection) map[string]string {
	if selection == nil {
		return versions
	}
	if versions == nil {
		versions = make(map[string]string, 5)
	}
	publicationIDs := make([]string, len(selection.PublicationIDs))
	for index, id := range selection.PublicationIDs {
		publicationIDs[index] = string(id)
	}
	classificationVersions := make([]string, len(selection.ClassificationVersions))
	for index, version := range selection.ClassificationVersions {
		classificationVersions[index] = strconv.FormatUint(uint64(version), 10)
	}
	versions["operator_id"] = string(selection.OperatorID)
	versions["operator_flow_isp_id"] = strconv.FormatUint(uint64(selection.FlowISPID), 10)
	versions["operator_publication_ids"] = strings.Join(publicationIDs, ",")
	versions["operator_dimension_snapshot_ids"] = strings.Join(selection.DimensionSnapshotIDs, ",")
	versions["operator_classification_versions"] = strings.Join(classificationVersions, ",")
	return versions
}

func flowAddressSetFilterSelected(filter flowdimension.AddressSetFilter) bool {
	return len(filter.IncludeAny)+len(filter.IncludeAll)+len(filter.ExcludeAny) > 0
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
