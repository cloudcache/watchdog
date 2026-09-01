package watchdog

import (
	"errors"
	"math"
	"slices"
	"time"
)

type Aggregation string

const (
	AggregationP95FiveMinute        Aggregation = "p95_5m"
	AggregationAverageFiveMinute    Aggregation = "avg_5m"
	AggregationFourthPeakFiveMinute Aggregation = "fourth_peak_5m"
	AggregationDailyP95             Aggregation = "daily_p95"
	AggregationDailyAverage         Aggregation = "daily_avg"
	AggregationTotalBytes           Aggregation = "total_bytes"
)

type Sample struct {
	Time  time.Time
	Value float64
}

type DailyValue struct {
	Day   time.Time
	Value float64
}

func Aggregate(samples []Sample, aggregation Aggregation) (float64, error) {
	values := sampleValues(samples)
	if len(values) == 0 {
		return 0, errors.New("no samples")
	}
	switch aggregation {
	case AggregationP95FiveMinute:
		return Percentile(values, 95), nil
	case AggregationAverageFiveMinute:
		return Average(values), nil
	case AggregationFourthPeakFiveMinute:
		return NthPeak(values, 4), nil
	case AggregationTotalBytes:
		return Sum(values), nil
	default:
		return 0, errors.New("unsupported scalar aggregation")
	}
}

func AggregateDaily(samples []Sample, aggregation Aggregation, loc *time.Location) ([]DailyValue, error) {
	if loc == nil {
		loc = time.UTC
	}
	grouped := make(map[time.Time][]float64)
	for _, sample := range samples {
		day := sample.Time.In(loc)
		key := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, loc)
		grouped[key] = append(grouped[key], sample.Value)
	}
	days := make([]time.Time, 0, len(grouped))
	for day := range grouped {
		days = append(days, day)
	}
	slices.SortFunc(days, func(a, b time.Time) int {
		return a.Compare(b)
	})
	result := make([]DailyValue, 0, len(days))
	for _, day := range days {
		var value float64
		switch aggregation {
		case AggregationDailyP95:
			value = Percentile(grouped[day], 95)
		case AggregationDailyAverage:
			value = Average(grouped[day])
		default:
			return nil, errors.New("unsupported daily aggregation")
		}
		result = append(result, DailyValue{Day: day, Value: value})
	}
	return result, nil
}

func sampleValues(samples []Sample) []float64 {
	values := make([]float64, 0, len(samples))
	for _, sample := range samples {
		if !math.IsNaN(sample.Value) && !math.IsInf(sample.Value, 0) {
			values = append(values, sample.Value)
		}
	}
	return values
}

func Average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

func Sum(values []float64) float64 {
	var sum float64
	for _, value := range values {
		sum += value
	}
	return sum
}

func Percentile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	if percentile <= 0 {
		return sorted[0]
	}
	if percentile >= 100 {
		return sorted[len(sorted)-1]
	}
	rank := int(math.Ceil(percentile/100*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	return sorted[rank]
}

func NthPeak(values []float64, n int) float64 {
	if len(values) == 0 || n <= 0 {
		return 0
	}
	sorted := slices.Clone(values)
	slices.SortFunc(sorted, func(a, b float64) int {
		return cmpFloatDesc(a, b)
	})
	if n > len(sorted) {
		return sorted[len(sorted)-1]
	}
	return sorted[n-1]
}

func cmpFloatDesc(a, b float64) int {
	switch {
	case a > b:
		return -1
	case a < b:
		return 1
	default:
		return 0
	}
}
