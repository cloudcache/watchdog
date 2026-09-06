package watchdog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// VictoriaMetricsQueryProvider is the platform adapter for the existing SNMP
// interface dataset. Provider parameters are deliberately small and typed;
// callers cannot submit raw MetricsQL or a tenant label.
type VictoriaMetricsQueryProvider struct {
	Client  MetricsQueryClient
	Network NetworkRepository
}

type victoriaMetricsQueryParameters struct {
	Metric      string          `json:"metric"`
	TargetID    ID              `json:"target_id,omitempty"`
	DeviceID    ID              `json:"device_id,omitempty"`
	PortID      ID              `json:"port_id,omitempty"`
	TargetIDs   []ID            `json:"target_ids,omitempty"`
	PortIDs     []ID            `json:"port_ids,omitempty"`
	Function    string          `json:"function,omitempty"`
	Aggregation string          `json:"aggregation,omitempty"`
	ValueMode   MetricValueMode `json:"value_mode,omitempty"`
	TrafficView TrafficViewMode `json:"traffic_view,omitempty"`
	PerPort     bool            `json:"per_port,omitempty"`
}

func (p VictoriaMetricsQueryProvider) Ready(ctx context.Context) error {
	if p.Client == nil {
		return errors.New("VictoriaMetrics query client is not configured")
	}
	if ready, ok := p.Client.(interface{ Ready(context.Context) error }); ok {
		return ready.Ready(ctx)
	}
	return nil
}

func (p VictoriaMetricsQueryProvider) Query(ctx context.Context, request QueryProviderRequest) (QueryProviderResult, error) {
	if p.Client == nil {
		return QueryProviderResult{}, ErrQueryProviderUnavailable
	}
	parameters, err := decodeVictoriaMetricsQueryParameters(request.Parameters)
	if err != nil {
		return QueryProviderResult{}, err
	}
	if !slices.Contains(request.Dataset.Metrics, parameters.Metric) {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "metric is not registered for this dataset"}
	}
	if parameters.Function != "" && parameters.Function != "rate" {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "function must be empty or rate"}
	}
	valueMode, trafficView, err := normalizeVictoriaMetricsValueSelection(request.ValueLayer, parameters)
	if err != nil {
		return QueryProviderResult{}, err
	}
	metricRequest := MetricsQueryRequest{
		TenantID: request.TenantID, TargetID: parameters.TargetID, DeviceID: parameters.DeviceID, PortID: parameters.PortID,
		Metric: parameters.Metric, Start: request.From, End: request.To, Step: time.Duration(request.StepSeconds) * time.Second,
		TimeMode: TimeModeCustom, MaxDataPoints: int(request.Limit), Func: parameters.Function,
		ValueMode: valueMode, TrafficView: trafficView,
	}
	metricRequest = normalizeMetricsQueryTime(metricRequest)
	if err := ValidateMetricsQuery(metricRequest, true); err != nil {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: err.Error(), Cause: err}
	}

	service := MetricsService{Client: p.Client}
	var response VictoriaMetricsResponse
	if parameters.Aggregation != "" {
		if parameters.PerPort || parameters.TargetID != "" || parameters.DeviceID != "" || parameters.PortID != "" ||
			(len(parameters.TargetIDs) == 0 && len(parameters.PortIDs) == 0) || !isSupportedPromAggregate(parameters.Aggregation) {
			return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "invalid aggregate query parameters"}
		}
		aggregateRequest := MetricsAggregateRequest{
			Query: metricRequest, TargetIDs: parameters.TargetIDs, PortIDs: parameters.PortIDs, Method: parameters.Aggregation,
		}
		response, err = aggregatePortSeries(ctx, p.Network, service, aggregateRequest)
		if err == nil {
			response = applyTrafficViewResponse(metricRequest, response)
		}
	} else {
		if len(parameters.TargetIDs) != 0 || len(parameters.PortIDs) != 0 ||
			(parameters.TargetID == "" && parameters.DeviceID == "" && parameters.PortID == "") {
			return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "target_id, device_id or port_id is required"}
		}
		resolved, resolveErr := (metricsAPI{network: p.Network}).resolveMetricsResource(ctx, AuthContext{TenantID: request.TenantID}, metricRequest)
		if resolveErr != nil {
			return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "metric resource is invalid", Cause: resolveErr}
		}
		metricRequest = resolved
		if isSNMPTrafficBpsMetric(metricRequest.Metric) && metricRequest.PortID == "" && !parameters.PerPort &&
			(metricRequest.TrafficView == TrafficViewSupplier || metricRequest.TrafficView == TrafficViewCustomer) {
			response, err = p.queryTrafficSide(ctx, service, metricRequest)
		} else {
			if isSNMPTrafficBpsMetric(metricRequest.Metric) && metricRequest.ValueMode != MetricValueRaw && metricRequest.PortID == "" &&
				!(parameters.PerPort && metricRequest.DeviceID != "") {
				return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "corrected traffic values require port_id or aggregate graph scope"}
			}
			response, err = service.QueryRange(ctx, metricRequest, metricsSelector(metricRequest), true)
		}
		if err == nil && isSNMPTrafficBpsMetric(metricRequest.Metric) &&
			!(metricRequest.PortID == "" && !parameters.PerPort && (metricRequest.TrafficView == TrafficViewSupplier || metricRequest.TrafficView == TrafficViewCustomer)) {
			response = applyCounterRateToVMResponse(response)
			if metricRequest.PortID != "" && metricRequest.ValueMode != MetricValueRaw {
				policy := loadPortPolicy(ctx, p.Network, request.TenantID, metricRequest.PortID)
				response, _ = TransformVMRangeValues(response, metricRequest.ValueMode, policy, nil)
			} else if parameters.PerPort && metricRequest.DeviceID != "" && metricRequest.ValueMode != MetricValueRaw {
				response = transformVMRangePerPort(ctx, p.Network, request.TenantID, metricRequest.DeviceID, response)
			}
			response = applyTrafficViewResponse(metricRequest, response)
		}
	}
	if err != nil {
		if errors.Is(err, ErrVictoriaMetricsUnavailable) {
			return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorProviderUnavailable, Message: "VictoriaMetrics is unavailable", Retryable: true, Cause: err}
		}
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorProviderFailure, Message: "VictoriaMetrics query failed", Retryable: true, Cause: err}
	}
	if points := victoriaMetricsPointCount(response); points > uint64(request.Limit) {
		return QueryProviderResult{}, &QueryGatewayError{
			Code: QueryErrorRowLimit, Message: "VictoriaMetrics result exceeds the query row limit",
			Details: map[string]any{"max_result_rows": request.Limit, "result_rows": points},
		}
	}
	data, err := json.Marshal(response)
	if err != nil {
		return QueryProviderResult{}, fmt.Errorf("marshal VictoriaMetrics result: %w", err)
	}
	unit := ""
	for _, definition := range MetricCatalog {
		if definition.Name == metricRequest.Metric {
			unit = definition.Unit
			break
		}
	}
	from, to := request.From.UTC(), request.To.UTC()
	return QueryProviderResult{
		Data: data, Unit: unit, Timezone: "UTC", AsOf: time.Now().UTC(),
		Completeness: QueryCompleteness{
			AvailableFrom: &from, AvailableTo: &to, UnknownRatio: 1,
			Warnings: []string{"VictoriaMetrics does not expose expected-sample completeness for this dataset"},
		},
	}, nil
}

func decodeVictoriaMetricsQueryParameters(raw json.RawMessage) (victoriaMetricsQueryParameters, error) {
	var parameters victoriaMetricsQueryParameters
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parameters); err != nil {
		return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "invalid VictoriaMetrics query parameters", Cause: err}
	}
	if err := ensureDashboardJSONEOF(decoder); err != nil {
		return parameters, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "invalid VictoriaMetrics query parameters", Cause: err}
	}
	return parameters, nil
}

func normalizeVictoriaMetricsValueSelection(layer QueryValueLayer, parameters victoriaMetricsQueryParameters) (MetricValueMode, TrafficViewMode, error) {
	mode, view := parameters.ValueMode, parameters.TrafficView
	switch layer {
	case QueryValueRaw:
		if mode == "" {
			mode = MetricValueRaw
		}
		if view == "" {
			view = TrafficViewRaw
		}
	case QueryValueSupplier:
		if mode == "" {
			mode = MetricValueCorrected
		}
		if view == "" {
			view = TrafficViewSupplier
		}
		if mode != MetricValueCorrected || view != TrafficViewSupplier {
			return "", "", &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "supplier layer requires corrected supplier values"}
		}
	case QueryValueCustomer:
		if mode == "" {
			mode = MetricValueCorrected
		}
		if view == "" {
			view = TrafficViewCustomer
		}
		if mode != MetricValueCorrected || view == TrafficViewSupplier {
			return "", "", &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "customer layer cannot request raw or supplier values"}
		}
	default:
		return "", "", &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "unsupported value layer"}
	}
	if mode != MetricValueRaw && mode != MetricValueCorrected && mode != MetricValueBoth {
		return "", "", &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "unsupported metric value mode"}
	}
	if normalized := normalizeTrafficView(view); normalized == "" {
		return "", "", &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "unsupported traffic view"}
	}
	return mode, view, nil
}

// AuthorizeQuery is the VM dataset's third admission gate: after tenant
// dataset policy and value-layer RBAC, every typed target/port selector must
// also be visible to the principal.
func (p VictoriaMetricsQueryProvider) AuthorizeQuery(ctx context.Context, auth AuthContext, request QueryProviderRequest) error {
	if auth.IsAdmin {
		return nil
	}
	parameters, err := decodeVictoriaMetricsQueryParameters(request.Parameters)
	if err != nil {
		return err
	}
	api := metricsAPI{network: p.Network}
	if parameters.Aggregation != "" {
		aggregate := MetricsAggregateRequest{
			Query: MetricsQueryRequest{TenantID: auth.TenantID}, TargetIDs: parameters.TargetIDs, PortIDs: parameters.PortIDs,
		}
		if err := api.authorizeAggregate(ctx, auth, aggregate); err != nil {
			return &QueryGatewayError{Code: QueryErrorPermissionDenied, Message: "query resource permission denied", Cause: err}
		}
		return nil
	}
	query := MetricsQueryRequest{
		TenantID: auth.TenantID, TargetID: parameters.TargetID, DeviceID: parameters.DeviceID, PortID: parameters.PortID,
	}
	resolved, err := api.resolveMetricsResource(ctx, auth, query)
	if err != nil || !canAccessMetrics(auth, resolved) {
		return &QueryGatewayError{Code: QueryErrorPermissionDenied, Message: "query resource permission denied", Cause: err}
	}
	return nil
}

func (p VictoriaMetricsQueryProvider) queryTrafficSide(ctx context.Context, service MetricsService, request MetricsQueryRequest) (VictoriaMetricsResponse, error) {
	if p.Network == nil || request.DeviceID == "" {
		return VictoriaMetricsResponse{}, errors.New("supplier/customer traffic view requires device_id or port_id")
	}
	ports, err := p.Network.ListPorts(ctx, request.TenantID, request.DeviceID)
	if err != nil {
		return VictoriaMetricsResponse{}, err
	}
	ports = aggregatablePorts(ports)
	side := PortSideCustomer
	if request.TrafficView == TrafficViewSupplier {
		side = PortSideProvider
	}
	portIDs := make([]ID, 0, len(ports))
	for _, port := range ports {
		policy, policyErr := p.Network.GetPortPolicy(ctx, request.TenantID, port.ID)
		if policyErr == nil && policy.SideType == side {
			portIDs = append(portIDs, port.ID)
		}
	}
	if len(portIDs) == 0 {
		return VictoriaMetricsResponse{Status: "success"}, nil
	}
	policies, _ := loadPortPolicies(ctx, p.Network, request.TenantID, portIDs)
	response, err := aggregateResolvedPortSeries(ctx, p.Network, service, request.TenantID, portIDs, request.Metric, "sum", request.ValueMode, request.Start, request.End, request.Step, policies)
	if err != nil {
		return VictoriaMetricsResponse{}, err
	}
	return applyTrafficViewResponse(request, response), nil
}

func victoriaMetricsPointCount(response VictoriaMetricsResponse) uint64 {
	var count uint64
	for _, series := range response.Data.Result {
		count += uint64(len(series.Values))
	}
	return count
}
