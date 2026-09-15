package watchdog

import (
	"context"
	"sort"
	"time"
)

// loadPortPolicy returns the stored policy for a port (normalized), or a zero
// policy when the port has none. Correction is only meaningful on port-scoped
// traffic metrics, so callers decide whether to apply it.
func loadPortPolicy(ctx context.Context, network NetworkRepository, tenantID, portID ID) PortPolicy {
	if network == nil || portID == "" {
		return PortPolicy{}
	}
	policy, err := network.GetPortPolicy(ctx, tenantID, portID)
	if err != nil {
		return PortPolicy{}
	}
	return policy.Normalize()
}

// correctionActive reports whether a policy would actually change a value.
func correctionActive(policy PortPolicy) bool {
	return policy.Enabled && policy.CorrectionDirection != CorrectionNone && policy.CorrectionMax > 0
}

// loadPortPolicies fetches every port policy and reports whether at least one
// has an active correction. When none are active, the corrected view equals the
// raw view and callers can use the fast PromQL aggregate path.
func loadPortPolicies(ctx context.Context, network NetworkRepository, tenantID ID, portIDs []ID) (map[ID]PortPolicy, bool) {
	policies := make(map[ID]PortPolicy, len(portIDs))
	any := false
	for _, portID := range portIDs {
		policy := loadPortPolicy(ctx, network, tenantID, portID)
		policies[portID] = policy
		if correctionActive(policy) {
			any = true
		}
	}
	return policies, any
}

// transformVMRangePerPort applies each port's own correction policy to its
// series (matched by the port_id label) — the per-port flavor of
// TransformVMRangeValues for device-wide multi-series responses.
func transformVMRangePerPort(ctx context.Context, network NetworkRepository, tenantID, deviceID ID, response VictoriaMetricsResponse) VictoriaMetricsResponse {
	if network == nil {
		return response
	}
	ports, err := network.ListPorts(ctx, tenantID, deviceID)
	if err != nil {
		return response
	}
	portIDs := make([]ID, 0, len(ports))
	for _, port := range ports {
		portIDs = append(portIDs, port.ID)
	}
	policies, any := loadPortPolicies(ctx, network, tenantID, portIDs)
	if !any {
		return response
	}
	response = cloneVMResponse(response)
	for i := range response.Data.Result {
		portID := ID(response.Data.Result[i].Metric["port_id"])
		policy := policies[portID]
		if !correctionActive(policy) {
			continue
		}
		for j := range response.Data.Result[i].Values {
			value := response.Data.Result[i].Values[j]
			response.Data.Result[i].Values[j].Value = ApplyCorrectionFloat(value.Value, policy, DeterministicCorrectionRNG(string(portID), value.Time))
		}
	}
	return response
}

// aggregatePortSeries produces an aggregate series across ports honoring the
// requested value mode. Raw mode (or corrected when no port has an active
// correction) uses a single PromQL sum. Corrected/both with active corrections
// queries each port's raw samples and applies per-port correction before
// summing, since a post-hoc single-policy transform on a summed series would
// be incorrect.
func aggregatePortSeries(
	ctx context.Context,
	network NetworkRepository,
	service MetricsService,
	req MetricsAggregateRequest,
) (VictoriaMetricsResponse, error) {
	mode := req.Query.ValueMode
	if mode == "" {
		mode = MetricValueCorrected
	}

	if len(req.PortIDs) > 0 && isSNMPTrafficMetric(req.Query.Metric) {
		policies, _ := loadPortPolicies(ctx, network, req.Query.TenantID, req.PortIDs)
		return aggregateResolvedPortSeries(ctx, network, service, req.Query.TenantID, req.PortIDs, req.Query.Metric, req.Method, mode, req.Query.Start, req.Query.End, req.Query.Step, policies)
	}

	if mode == MetricValueRaw {
		return service.QueryRange(ctx, req.Query, aggregateMetricsSelector(req), true)
	}

	policies, anyCorrection := loadPortPolicies(ctx, network, req.Query.TenantID, req.PortIDs)
	if !anyCorrection {
		// No port has an active correction, so corrected == raw. Use the fast
		// PromQL path. For "both", return a single raw series (identical to
		// corrected) rather than a noisy duplicate.
		return service.QueryRange(ctx, req.Query, aggregateMetricsSelector(req), true)
	}

	return aggregateResolvedPortSeries(ctx, network, service, req.Query.TenantID, req.PortIDs, req.Query.Metric, req.Method, mode, req.Query.Start, req.Query.End, req.Query.Step, policies)
}

func aggregateResolvedPortSeries(
	ctx context.Context,
	network NetworkRepository,
	service MetricsService,
	tenantID ID,
	portIDs []ID,
	metric string,
	method string,
	mode MetricValueMode,
	start, end time.Time,
	step time.Duration,
	policies map[ID]PortPolicy,
) (VictoriaMetricsResponse, error) {
	if isSNMPTrafficMetric(metric) && step > 0 && step < time.Minute {
		step = time.Minute
	}
	rawValues := map[time.Time][]float64{}
	corrValues := map[time.Time][]float64{}
	for _, portID := range portIDs {
		port, err := network.GetPort(ctx, tenantID, portID)
		if err != nil {
			continue
		}
		device, err := network.GetDevice(ctx, tenantID, port.DeviceID)
		if err != nil {
			continue
		}
		portReq := MetricsQueryRequest{
			TenantID:    tenantID,
			TargetID:    device.TargetID,
			DeviceID:    device.ID,
			PortID:      port.ID,
			PortIfIndex: port.IfIndex,
			Metric:      metric,
			TimeMode:    TimeModeCustom,
			ValueMode:   MetricValueRaw,
			Start:       start,
			End:         end,
			Step:        step,
		}
		resp, err := service.QueryRange(ctx, portReq, metricsSelector(portReq), true)
		if err != nil {
			return VictoriaMetricsResponse{}, err
		}
		if isSNMPTrafficMetric(metric) {
			resp = applyCounterRateToVMResponse(resp)
		}
		policy := policies[portID]
		active := correctionActive(policy)
		for _, item := range resp.Data.Result {
			for _, v := range item.Values {
				rawValues[v.Time] = append(rawValues[v.Time], v.Value)
				if mode == MetricValueCorrected || mode == MetricValueBoth {
					value := v.Value
					if active {
						value = ApplyCorrectionFloat(v.Value, policy, DeterministicCorrectionRNG(string(portID), v.Time))
					}
					corrValues[v.Time] = append(corrValues[v.Time], value)
				}
			}
		}
	}

	var result []VMRangeQueryItem
	if mode == MetricValueBoth {
		result = append(result, aggregatedSeries(rawValues, method, map[string]string{"value_mode": "raw"}))
		result = append(result, aggregatedSeries(corrValues, method, map[string]string{"value_mode": "corrected"}))
	} else if mode == MetricValueRaw {
		result = append(result, aggregatedSeries(rawValues, method, map[string]string{"value_mode": "raw"}))
	} else {
		result = append(result, aggregatedSeries(corrValues, method, map[string]string{"value_mode": "corrected"}))
	}
	return victoriaMetricsResponse(result), nil
}

func isSNMPTrafficMetric(metric string) bool {
	return metric == MetricSNMPIfInBps || metric == MetricSNMPIfOutBps
}

func aggregatedSeries(values map[time.Time][]float64, method string, metric map[string]string) VMRangeQueryItem {
	sums := make(map[time.Time]float64, len(values))
	for t, samples := range values {
		sums[t] = aggregateSamples(samples, method)
	}
	return summedSeries(sums, metric)
}

func aggregateSamples(samples []float64, method string) float64 {
	if len(samples) == 0 {
		return 0
	}
	switch method {
	case "avg":
		var sum float64
		for _, value := range samples {
			sum += value
		}
		return sum / float64(len(samples))
	case "max":
		max := samples[0]
		for _, value := range samples[1:] {
			if value > max {
				max = value
			}
		}
		return max
	case "min":
		min := samples[0]
		for _, value := range samples[1:] {
			if value < min {
				min = value
			}
		}
		return min
	case "count":
		return float64(len(samples))
	default:
		var sum float64
		for _, value := range samples {
			sum += value
		}
		return sum
	}
}

func summedSeries(sums map[time.Time]float64, metric map[string]string) VMRangeQueryItem {
	values := make([]VMValue, 0, len(sums))
	for t, value := range sums {
		values = append(values, VMValue{Time: t, Value: value})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Time.Before(values[j].Time) })
	return VMRangeQueryItem{Metric: metric, Values: values}
}

func valueModeOrDefault(mode MetricValueMode) MetricValueMode {
	if mode == "" {
		return MetricValueCorrected
	}
	return mode
}

func victoriaMetricsResponse(result []VMRangeQueryItem) VictoriaMetricsResponse {
	var resp VictoriaMetricsResponse
	resp.Status = "success"
	resp.Data.ResultType = "matrix"
	resp.Data.Result = result
	return resp
}
