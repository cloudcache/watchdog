package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

type VictoriaMetricsExportDataProvider struct {
	Client         MetricsQueryClient
	Metric         string
	Network        NetworkRepository
	CounterBits    int
	CollectionStep time.Duration
}

// QueryGatewayExportDataProvider executes contract-v1 exports through the
// same admission, tenant scoping, provider limits and typed compiler as the
// interactive query API. The immutable query is read from the task snapshot;
// no request is reconstructed from mutable UI fields at execution time.
type QueryGatewayExportDataProvider struct {
	Gateway *QueryGateway
}

func (p QueryGatewayExportDataProvider) LoadSamples(ctx context.Context, task ExportTask) ([]Sample, error) {
	if p.Gateway == nil {
		return nil, errors.New("export query gateway is required")
	}
	if err := validateExportExecutionTask(task); err != nil {
		return nil, err
	}
	var snapshot exportQuerySnapshot
	if err := decodeStrictJSON(task.QueryJSON, &snapshot); err != nil {
		return nil, fmt.Errorf("decode export query: %w", err)
	}
	auth, ok := AuthFromContext(ctx)
	if !ok || auth.TenantID != task.TenantID || auth.UserID != task.CreatedBy {
		return nil, errors.New("current export authorization is required")
	}
	result, err := p.Gateway.Execute(ctx, auth, "export:"+string(task.ID), snapshot.Query)
	if err != nil {
		return nil, err
	}
	var response VictoriaMetricsResponse
	if err := json.Unmarshal(result.Data, &response); err != nil {
		return nil, fmt.Errorf("decode VictoriaMetrics export result: %w", err)
	}
	var parameters victoriaMetricsQueryParameters
	if err := decodeStrictJSON(snapshot.Query.Parameters, &parameters); err != nil {
		return nil, fmt.Errorf("decode export query parameters: %w", err)
	}
	var samples []Sample
	if parameters.PortID == "" {
		samples = samplesFromVMResponseSummed(response)
	} else {
		samples = samplesFromVMResponse(response)
	}
	if len(samples) == 0 {
		return nil, errors.New("no export samples returned from query gateway")
	}
	return samples, nil
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

// samplesFromVMResponseSummed collapses a target/device multi-series result
// to one value per timestamp before P95/average/volume aggregation. Flattening
// the series would calculate a percentile across individual ports, which is
// not the target's traffic total.
func samplesFromVMResponseSummed(response VictoriaMetricsResponse) []Sample {
	values := make(map[time.Time]float64)
	for _, result := range response.Data.Result {
		for _, value := range result.Values {
			values[value.Time] += value.Value
		}
	}
	times := make([]time.Time, 0, len(values))
	for timestamp := range values {
		times = append(times, timestamp)
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	samples := make([]Sample, 0, len(times))
	for _, timestamp := range times {
		samples = append(samples, Sample{Time: timestamp, Value: values[timestamp]})
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
var _ ExportDataProvider = QueryGatewayExportDataProvider{}
