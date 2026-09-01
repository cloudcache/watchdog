package watchdog

import (
	"context"
	"errors"
	"slices"
	"time"
)

type BillingMetricsLoader struct {
	Billing BillingRepository
	Network NetworkRepository
	Metrics MetricsService
}

func (l BillingMetricsLoader) LoadSamples(ctx context.Context, tenantID ID, account BillingAccount, period BillingPeriod) ([]Sample, error) {
	if l.Billing == nil {
		return nil, errors.New("billing repository is required")
	}
	if l.Network == nil {
		return nil, errors.New("network repository is required")
	}
	if l.Metrics.Client == nil {
		return nil, errors.New("victoriametrics client is required")
	}
	ports, err := l.Billing.ListBillingAccountPorts(ctx, tenantID, account.ID)
	if err != nil {
		return nil, err
	}
	if len(ports) == 0 {
		return nil, errors.New("billing account has no ports")
	}
	step := billingStep(period)
	totalByTime := map[int64]float64{}
	for _, accountPort := range ports {
		portSamples, err := l.loadPortSamples(ctx, tenantID, account, period, accountPort, step)
		if err != nil {
			return nil, err
		}
		for _, sample := range portSamples {
			totalByTime[sample.Time.Unix()] += sample.Value
		}
	}
	samples := make([]Sample, 0, len(totalByTime))
	for ts, value := range totalByTime {
		samples = append(samples, Sample{Time: time.Unix(ts, 0).UTC(), Value: value})
	}
	sortSamples(samples)
	if len(samples) == 0 {
		return nil, errors.New("no billing samples returned from victoriametrics")
	}
	return samples, nil
}

func (l BillingMetricsLoader) loadPortSamples(ctx context.Context, tenantID ID, account BillingAccount, period BillingPeriod, accountPort BillingAccountPort, step time.Duration) ([]Sample, error) {
	port, err := l.Network.GetPort(ctx, tenantID, accountPort.PortID)
	if err != nil {
		return nil, err
	}
	device, err := l.Network.GetDevice(ctx, tenantID, port.DeviceID)
	if err != nil {
		return nil, err
	}
	policy, _ := l.Network.GetPortPolicy(ctx, tenantID, port.ID)
	policy = policy.Normalize()
	inSamples, err := l.queryPortDirection(ctx, tenantID, device, port, MetricSNMPIfInBps, account.ValueMode, policy, period, step)
	if err != nil {
		return nil, err
	}
	outSamples, err := l.queryPortDirection(ctx, tenantID, device, port, MetricSNMPIfOutBps, account.ValueMode, policy, period, step)
	if err != nil {
		return nil, err
	}
	return mergeBillingDirection(inSamples, outSamples, normalizeBillingDirection(accountPort.Direction)), nil
}

func (l BillingMetricsLoader) queryPortDirection(ctx context.Context, tenantID ID, device NetworkDevice, port NetworkPort, metric string, valueMode ExportValueMode, policy PortPolicy, period BillingPeriod, step time.Duration) ([]Sample, error) {
	req := MetricsQueryRequest{
		TenantID:    tenantID,
		TargetID:    device.TargetID,
		DeviceID:    device.ID,
		PortID:      port.ID,
		PortIfIndex: port.IfIndex,
		Metric:      metric,
		TimeMode:    TimeModeCustom,
		ValueMode:   exportValueModeToMetricMode(valueMode),
		Start:       period.RangeStart,
		End:         period.RangeEnd,
		Step:        step,
	}
	response, err := l.Metrics.QueryRange(ctx, req, metricsSelector(req), true)
	if err != nil {
		return nil, err
	}
	// The selector returns raw octet counters; billing aggregates rates.
	response = applyCounterRateToVMResponse(response)
	transformed, err := TransformVMRangeValues(response, req.ValueMode, policy, nil)
	if err != nil {
		return nil, err
	}
	return samplesFromVMResponse(transformed), nil
}

func mergeBillingDirection(inSamples, outSamples []Sample, direction BillingDirection) []Sample {
	inByTime := samplesByUnix(inSamples)
	outByTime := samplesByUnix(outSamples)
	keys := map[int64]struct{}{}
	for key := range inByTime {
		keys[key] = struct{}{}
	}
	for key := range outByTime {
		keys[key] = struct{}{}
	}
	samples := make([]Sample, 0, len(keys))
	for key := range keys {
		inValue := inByTime[key]
		outValue := outByTime[key]
		value := inValue
		switch direction {
		case BillingDirectionOut:
			value = outValue
		case BillingDirectionSum:
			value = inValue + outValue
		case BillingDirectionMax:
			if outValue > inValue {
				value = outValue
			}
		}
		samples = append(samples, Sample{Time: time.Unix(key, 0).UTC(), Value: value})
	}
	sortSamples(samples)
	return samples
}

func samplesByUnix(samples []Sample) map[int64]float64 {
	result := make(map[int64]float64, len(samples))
	for _, sample := range samples {
		result[sample.Time.Unix()] = sample.Value
	}
	return result
}

func normalizeBillingDirection(direction BillingDirection) BillingDirection {
	switch direction {
	case BillingDirectionIn, BillingDirectionOut, BillingDirectionSum, BillingDirectionMax:
		return direction
	default:
		return BillingDirectionMax
	}
}

func exportValueModeToMetricMode(mode ExportValueMode) MetricValueMode {
	switch mode {
	case ExportValueRaw:
		return MetricValueRaw
	default:
		return MetricValueCorrected
	}
}

func billingStep(period BillingPeriod) time.Duration {
	step := time.Minute
	window := period.RangeEnd.Sub(period.RangeStart)
	if window > 45*24*time.Hour {
		step = 5 * time.Minute
	}
	if min := minStepForRange(window, maxVMPointsPerSeries); min > step {
		step = min
	}
	return step
}

// estimateBillingBytes integrates bps samples into bytes using the actual
// spacing between consecutive samples, clamped to twice the nominal step so
// data gaps are not billed at the last observed rate.
func estimateBillingBytes(samples []Sample, step time.Duration) uint64 {
	if step <= 0 {
		step = time.Minute
	}
	sorted := make([]Sample, len(samples))
	copy(sorted, samples)
	sortSamples(sorted)
	var total float64
	var prev time.Time
	for _, sample := range sorted {
		dt := step
		if !prev.IsZero() {
			if gap := sample.Time.Sub(prev); gap > 0 {
				dt = gap
				if max := 2 * step; dt > max {
					dt = max
				}
			}
		}
		prev = sample.Time
		if sample.Value > 0 {
			total += sample.Value * dt.Seconds() / 8
		}
	}
	if total <= 0 {
		return 0
	}
	return uint64(total)
}

func sortSamples(samples []Sample) {
	slices.SortFunc(samples, func(a, b Sample) int {
		return a.Time.Compare(b.Time)
	})
}
