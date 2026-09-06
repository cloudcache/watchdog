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
	Metric   string `json:"metric"`
	TargetID ID     `json:"target_id,omitempty"`
	DeviceID ID     `json:"device_id,omitempty"`
	PortID   ID     `json:"port_id,omitempty"`
	Function string `json:"function,omitempty"`
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
	var parameters victoriaMetricsQueryParameters
	decoder := json.NewDecoder(bytes.NewReader(request.Parameters))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parameters); err != nil {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "invalid VictoriaMetrics query parameters", Cause: err}
	}
	if err := ensureDashboardJSONEOF(decoder); err != nil {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "invalid VictoriaMetrics query parameters", Cause: err}
	}
	if !slices.Contains(request.Dataset.Metrics, parameters.Metric) {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "metric is not registered for this dataset"}
	}
	if parameters.TargetID == "" && parameters.DeviceID == "" && parameters.PortID == "" {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "target_id, device_id or port_id is required"}
	}
	if parameters.Function != "" && parameters.Function != "rate" {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "function must be empty or rate"}
	}
	metricRequest := MetricsQueryRequest{
		TenantID: request.TenantID, TargetID: parameters.TargetID, DeviceID: parameters.DeviceID, PortID: parameters.PortID,
		Metric: parameters.Metric, Start: request.From, End: request.To, Step: time.Duration(request.StepSeconds) * time.Second,
		TimeMode: TimeModeCustom, MaxDataPoints: int(request.Limit), Func: parameters.Function,
	}
	switch request.ValueLayer {
	case QueryValueRaw:
		metricRequest.ValueMode, metricRequest.TrafficView = MetricValueRaw, TrafficViewRaw
	case QueryValueSupplier:
		metricRequest.ValueMode, metricRequest.TrafficView = MetricValueCorrected, TrafficViewSupplier
	case QueryValueCustomer:
		metricRequest.ValueMode, metricRequest.TrafficView = MetricValueCorrected, TrafficViewCustomer
	default:
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "unsupported value layer"}
	}
	resolved, err := (metricsAPI{network: p.Network}).resolveMetricsResource(ctx, AuthContext{TenantID: request.TenantID}, metricRequest)
	if err != nil {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: "metric resource is invalid", Cause: err}
	}
	resolved = normalizeMetricsQueryTime(resolved)
	if err := ValidateMetricsQuery(resolved, true); err != nil {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorInvalidRequest, Message: err.Error(), Cause: err}
	}

	service := MetricsService{Client: p.Client}
	var response VictoriaMetricsResponse
	if isSNMPTrafficBpsMetric(resolved.Metric) && resolved.PortID == "" &&
		(resolved.TrafficView == TrafficViewSupplier || resolved.TrafficView == TrafficViewCustomer) {
		response, err = p.queryTrafficSide(ctx, service, resolved)
	} else {
		response, err = service.QueryRange(ctx, resolved, metricsSelector(resolved), true)
		if err == nil && isSNMPTrafficBpsMetric(resolved.Metric) {
			response = applyCounterRateToVMResponse(response)
			if resolved.PortID != "" && resolved.ValueMode != MetricValueRaw {
				policy := loadPortPolicy(ctx, p.Network, request.TenantID, resolved.PortID)
				response, _ = TransformVMRangeValues(response, resolved.ValueMode, policy, nil)
			}
			response = applyTrafficViewResponse(resolved, response)
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
		if definition.Name == resolved.Metric {
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
