// SPDX-License-Identifier: AGPL-3.0-only

package billing

import (
	"encoding/json"
	"math"
	"slices"
)

type LayerStats struct {
	Layer                  Layer
	InBytes                uint64
	OutBytes               uint64
	SelectedBytes          uint64
	SelectedRates          []float64
	Coverage               float64
	ExpectedBuckets        uint32
	ObservedBuckets        uint32
	ResetBuckets           uint32
	GapBuckets             uint32
	UnknownSamplingRecords uint64
	SourceGenerationMin    uint64
	SourceGenerationMax    uint64
	Provenance             any
}

func BuildValue(periodID string, generation uint64, algorithm Algorithm, stats LayerStats) Value {
	p95, average := RateStatistics(stats.SelectedRates)
	missing := uint32(0)
	if stats.ExpectedBuckets > stats.ObservedBuckets {
		missing = stats.ExpectedBuckets - stats.ObservedBuckets
	}
	unit := "bps"
	algorithmValue := p95
	switch algorithm {
	case AlgorithmAverage:
		algorithmValue = average
	case AlgorithmTotal:
		unit = "bytes"
		algorithmValue = stats.SelectedBytes
	}
	provenance, _ := json.Marshal(stats.Provenance)
	if len(provenance) == 0 || string(provenance) == "null" {
		provenance = json.RawMessage(`{}`)
	}
	return Value{
		PeriodID: periodID, CalculationVersion: generation, Layer: stats.Layer,
		Algorithm: algorithm, Unit: unit, InBytes: stats.InBytes, OutBytes: stats.OutBytes,
		SelectedBytes: stats.SelectedBytes, Rate95thBPS: p95, RateAverageBPS: average,
		AlgorithmValue: algorithmValue, Coverage: clamp01(stats.Coverage),
		ExpectedBuckets: stats.ExpectedBuckets, ObservedBuckets: stats.ObservedBuckets,
		MissingBuckets: missing, ResetBuckets: stats.ResetBuckets, GapBuckets: stats.GapBuckets,
		UnknownSamplingRecords: stats.UnknownSamplingRecords,
		SourceGenerationMin:    stats.SourceGenerationMin, SourceGenerationMax: stats.SourceGenerationMax,
		Provenance: provenance,
	}
}

func RateStatistics(input []float64) (p95 uint64, average uint64) {
	values := make([]float64, 0, len(input))
	var sum float64
	for _, value := range input {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		values = append(values, value)
		sum += value
	}
	if len(values) == 0 {
		return 0, 0
	}
	slices.Sort(values)
	index := int(math.Ceil(.95*float64(len(values)))) - 1
	return roundUint64(values[index]), roundUint64(sum / float64(len(values)))
}

func Reconcile(values []Value, absolute uint64, percent float64) []ReconciliationIssue {
	byLayer := make(map[Layer]Value, len(values))
	for _, value := range values {
		byLayer[value.Layer] = value
	}
	issues := make([]ReconciliationIssue, 0)
	for _, value := range values {
		if value.Coverage < 1 {
			issues = append(issues, evidenceIssue("coverage", "warning", value.Layer, "coverage", int64(value.ExpectedBuckets), int64(value.ObservedBuckets), 0))
		}
		if value.MissingBuckets > 0 {
			issues = append(issues, evidenceIssue("missing_bucket", "warning", value.Layer, "buckets", int64(value.ExpectedBuckets), int64(value.ObservedBuckets), 0))
		}
		if value.ResetBuckets > 0 {
			issues = append(issues, evidenceIssue("counter_reset", "warning", value.Layer, "buckets", 0, int64(value.ResetBuckets), 0))
		}
		if value.GapBuckets > 0 {
			issues = append(issues, evidenceIssue("counter_gap", "warning", value.Layer, "buckets", 0, int64(value.GapBuckets), 0))
		}
		if value.UnknownSamplingRecords > 0 {
			issues = append(issues, evidenceIssue("missing_sampling", "critical", value.Layer, "records", 0, saturatingInt64(value.UnknownSamplingRecords), 0))
		}
	}
	for _, pair := range [][2]Layer{{LayerSNMP, LayerRaw}, {LayerRaw, LayerSupplier}, {LayerSupplier, LayerCustomer}, {LayerCustomer, LayerExternal}} {
		left, leftOK := byLayer[pair[0]]
		right, rightOK := byLayer[pair[1]]
		if !leftOK || !rightOK {
			continue
		}
		expected, actual := saturatingInt64(left.AlgorithmValue), saturatingInt64(right.AlgorithmValue)
		delta := subtractInt64(actual, expected)
		threshold := absolute
		percentage := roundUint64(float64(left.AlgorithmValue) * clampPercent(percent) / 100)
		if percentage > threshold {
			threshold = percentage
		}
		if absInt64(delta) > threshold {
			issue := evidenceIssue("threshold", "warning", pair[0], "algorithm_value", expected, actual, threshold)
			issue.RightLayer = pair[1]
			issues = append(issues, issue)
		}
	}
	return issues
}

func evidenceIssue(kind, severity string, layer Layer, metric string, expected, actual int64, threshold uint64) ReconciliationIssue {
	detail, _ := json.Marshal(map[string]any{"automatic_adjustment": false})
	return ReconciliationIssue{
		Kind: kind, Severity: severity, LeftLayer: layer, Metric: metric,
		ExpectedValue: expected, ActualValue: actual, DeltaValue: subtractInt64(actual, expected),
		ThresholdValue: threshold, Status: "open", Detail: detail, RowVersion: 1,
	}
}

func EffectiveAdjustment(adjustments []Adjustment, layer Layer, unit string) int64 {
	var total int64
	for _, item := range adjustments {
		if item.Status == "approved" && item.Layer == layer && item.Unit == unit {
			total = addInt64(total, item.Amount)
		}
	}
	return total
}

func ApplyAdjustment(base uint64, adjustment int64) uint64 {
	if adjustment < 0 {
		magnitude := uint64(-(adjustment + 1)) + 1
		if magnitude >= base {
			return 0
		}
		return base - magnitude
	}
	if uint64(adjustment) > math.MaxUint64-base {
		return math.MaxUint64
	}
	return base + uint64(adjustment)
}

func roundUint64(value float64) uint64 {
	if value <= 0 || math.IsNaN(value) {
		return 0
	}
	if value >= math.MaxUint64 {
		return math.MaxUint64
	}
	return uint64(math.Round(value))
}

func clamp01(value float64) float64      { return math.Max(0, math.Min(1, value)) }
func clampPercent(value float64) float64 { return math.Max(0, math.Min(100, value)) }

func saturatingInt64(value uint64) int64 {
	if value > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(value)
}

func subtractInt64(left, right int64) int64 {
	if right > 0 && left < math.MinInt64+right {
		return math.MinInt64
	}
	if right < 0 && left > math.MaxInt64+right {
		return math.MaxInt64
	}
	return left - right
}

func addInt64(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}
	if right < 0 && left < math.MinInt64-right {
		return math.MinInt64
	}
	return left + right
}

func absInt64(value int64) uint64 {
	if value < 0 {
		return uint64(-(value + 1)) + 1
	}
	return uint64(value)
}
