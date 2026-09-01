package watchdog

import (
	"context"
	"errors"
	"time"
)

type VictoriaMetricsExportDataProvider struct {
	Client         MetricsQueryClient
	Metric         string
	Network        NetworkRepository
	CounterBits    int
	CollectionStep time.Duration
}

func (p VictoriaMetricsExportDataProvider) LoadSamples(ctx context.Context, task ExportTask) ([]Sample, error) {
	if p.Client == nil {
		return nil, errors.New("victoriametrics export client is required")
	}
	task = normalizeExportTask(task)
	metric := p.Metric
	if metric == "" {
		metric = MetricSNMPIfInBps
	}
	counterBits := p.CounterBits
	if counterBits == 0 {
		counterBits = 64
	}
	rawMetric := exportRawCounterMetric(metric)
	queryMetric := metric
	applyRate := false
	if rawMetric != "" {
		queryMetric = rawMetric
		applyRate = true
	}
	queryStep := ExportQueryStep(task, p.CollectionStep)
	response, err := p.Client.QueryRange(ctx, RangeQuery{
		Query: exportMetricSelector(queryMetric, task),
		Start: task.RangeStart,
		End:   task.RangeEnd,
		Step:  queryStep,
	})
	if err != nil {
		return nil, err
	}
	samples := samplesFromVMResponse(response)
	if applyRate {
		samples = exportCounterRateSamples(response, counterBits)
	}
	if len(samples) == 0 {
		return nil, errors.New("no export samples returned from victoriametrics")
	}
	return samples, nil
}

// ExportQueryStep is the effective sample step an export task is loaded at:
// the collection step (or the task step as fallback), widened when the range
// would exceed VictoriaMetrics' per-series point limit. The completeness
// policy must use the same step so month-scale exports stay verifiable.
func ExportQueryStep(task ExportTask, collectionStep time.Duration) time.Duration {
	task = normalizeExportTask(task)
	step := collectionStep
	if step <= 0 {
		step = task.Step
	}
	if step <= 0 {
		step = time.Minute
	}
	if min := minStepForRange(task.RangeEnd.Sub(task.RangeStart), maxVMPointsPerSeries); min > step {
		step = min
	}
	return step
}

func exportRawCounterMetric(metric string) string {
	switch metric {
	case MetricSNMPIfInBps:
		return MetricSNMPIfInOctetsTotal
	case MetricSNMPIfOutBps:
		return MetricSNMPIfOutOctetsTotal
	default:
		return ""
	}
}

func exportCounterRateSamples(response VictoriaMetricsResponse, counterBits int) []Sample {
	var samples []Sample
	for _, result := range response.Data.Result {
		rate := ComputeCounterRate(result.Values, CounterRateOptions{CounterBits: counterBits, ToBits: true})
		for _, v := range rate {
			samples = append(samples, Sample{Time: v.Time, Value: v.Value})
		}
	}
	return samples
}

func exportMetricSelector(metric string, task ExportTask) string {
	labels := []string{labelMatcher("tenant_id", string(task.TenantID))}
	if task.TargetID != "" {
		labels = append(labels, labelMatcher("target_id", string(task.TargetID)))
	}
	if task.PortID != "" {
		labels = append(labels, labelMatcher("port_id", string(task.PortID)))
	}
	return metric + "{" + joinLabelMatchers(labels) + "}"
}

func joinLabelMatchers(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	result := labels[0]
	for _, label := range labels[1:] {
		result += "," + label
	}
	return result
}

func samplesFromVMResponse(response VictoriaMetricsResponse) []Sample {
	var samples []Sample
	for _, result := range response.Data.Result {
		for _, value := range result.Values {
			samples = append(samples, Sample{Time: value.Time, Value: value.Value})
		}
	}
	return samples
}

func aggregateExportSamples(task ExportTask, samples []Sample) ([]Sample, error) {
	if task.Aggregation == "" {
		return samples, nil
	}
	switch task.Aggregation {
	case AggregationDailyP95, AggregationDailyAverage:
		values, err := AggregateDaily(samples, task.Aggregation, time.UTC)
		if err != nil {
			return nil, err
		}
		result := make([]Sample, 0, len(values))
		for _, value := range values {
			result = append(result, Sample{Time: value.Day, Value: value.Value})
		}
		return result, nil
	case AggregationTotalBytes:
		// Samples are bps rates; volume is their time integral, not their sum.
		if len(sampleValues(samples)) == 0 {
			return nil, errors.New("no samples")
		}
		return []Sample{{Time: task.RangeEnd, Value: float64(estimateBillingBytes(samples, task.Step))}}, nil
	default:
		value, err := Aggregate(samples, task.Aggregation)
		if err != nil {
			return nil, err
		}
		return []Sample{{Time: task.RangeEnd, Value: value}}, nil
	}
}

var _ ExportDataProvider = (*VictoriaMetricsExportDataProvider)(nil)
