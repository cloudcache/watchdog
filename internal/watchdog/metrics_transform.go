package watchdog

import "errors"

func TransformVMRangeValues(response VictoriaMetricsResponse, mode MetricValueMode, policy PortPolicy, rng CorrectionRandom) (VictoriaMetricsResponse, error) {
	if mode == "" {
		mode = MetricValueCorrected
	}
	switch mode {
	case MetricValueRaw:
		return response, nil
	case MetricValueCorrected:
		return transformVMRangeCorrected(cloneVMResponse(response), policy, rng), nil
	case MetricValueBoth:
		raw := cloneVMResponseWithMode(cloneVMResponse(response), "raw")
		corrected := transformVMRangeCorrected(cloneVMResponse(response), policy, rng)
		corrected = cloneVMResponseWithMode(corrected, "corrected")
		raw.Data.Result = append(raw.Data.Result, corrected.Data.Result...)
		return raw, nil
	default:
		return VictoriaMetricsResponse{}, errors.New("unsupported metric value mode")
	}
}

func cloneVMResponse(response VictoriaMetricsResponse) VictoriaMetricsResponse {
	result := make([]VMRangeQueryItem, len(response.Data.Result))
	for i, item := range response.Data.Result {
		metric := make(map[string]string, len(item.Metric))
		for key, value := range item.Metric {
			metric[key] = value
		}
		values := make([]VMValue, len(item.Values))
		copy(values, item.Values)
		result[i] = VMRangeQueryItem{Metric: metric, Values: values}
	}
	response.Data.Result = result
	return response
}

func transformVMRangeCorrected(response VictoriaMetricsResponse, policy PortPolicy, rng CorrectionRandom) VictoriaMetricsResponse {
	for i := range response.Data.Result {
		portID := response.Data.Result[i].Metric["port_id"]
		for j := range response.Data.Result[i].Values {
			value := response.Data.Result[i].Values[j].Value
			sampleRNG := rng
			if sampleRNG == nil {
				sampleRNG = DeterministicCorrectionRNG(portID, response.Data.Result[i].Values[j].Time)
			}
			response.Data.Result[i].Values[j].Value = ApplyCorrectionFloat(value, policy, sampleRNG)
		}
	}
	return response
}

func cloneVMResponseWithMode(response VictoriaMetricsResponse, mode string) VictoriaMetricsResponse {
	for i := range response.Data.Result {
		metric := make(map[string]string, len(response.Data.Result[i].Metric)+1)
		for key, value := range response.Data.Result[i].Metric {
			metric[key] = value
		}
		metric["value_mode"] = mode
		response.Data.Result[i].Metric = metric
	}
	return response
}
