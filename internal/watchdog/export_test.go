package watchdog

import (
	"testing"
	"time"
)

func TestAggregateFiveMinuteValues(t *testing.T) {
	samples := []Sample{
		{Value: 10},
		{Value: 20},
		{Value: 30},
		{Value: 40},
		{Value: 50},
	}
	avg, err := Aggregate(samples, AggregationAverageFiveMinute)
	if err != nil {
		t.Fatal(err)
	}
	if avg != 30 {
		t.Fatalf("average = %v", avg)
	}
	p95, err := Aggregate(samples, AggregationP95FiveMinute)
	if err != nil {
		t.Fatal(err)
	}
	if p95 != 50 {
		t.Fatalf("p95 = %v", p95)
	}
	fourth, err := Aggregate(samples, AggregationFourthPeakFiveMinute)
	if err != nil {
		t.Fatal(err)
	}
	if fourth != 20 {
		t.Fatalf("fourth peak = %v", fourth)
	}
	total, err := Aggregate(samples, AggregationTotalBytes)
	if err != nil {
		t.Fatal(err)
	}
	if total != 150 {
		t.Fatalf("total = %v", total)
	}
}

func TestAggregateDaily(t *testing.T) {
	loc := time.UTC
	samples := []Sample{
		{Time: time.Date(2026, 6, 1, 0, 0, 0, 0, loc), Value: 10},
		{Time: time.Date(2026, 6, 1, 0, 5, 0, 0, loc), Value: 20},
		{Time: time.Date(2026, 6, 2, 0, 0, 0, 0, loc), Value: 30},
		{Time: time.Date(2026, 6, 2, 0, 5, 0, 0, loc), Value: 50},
	}
	values, err := AggregateDaily(samples, AggregationDailyAverage, loc)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 {
		t.Fatalf("daily values len = %d", len(values))
	}
	if values[0].Value != 15 || values[1].Value != 40 {
		t.Fatalf("daily average values = %#v", values)
	}
}
