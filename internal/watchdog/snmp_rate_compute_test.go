package watchdog

import (
	"math"
	"testing"
	"time"
)

func TestComputeCounterRateNormalAscending(t *testing.T) {
	base := time.Unix(1000, 0).UTC()
	samples := []VMValue{
		{Time: base, Value: 1000},
		{Time: base.Add(60 * time.Second), Value: 7000},
	}
	rate := ComputeCounterRate(samples, CounterRateOptions{CounterBits: 64, ToBits: true})
	if len(rate) != 1 {
		t.Fatalf("len = %d, want 1", len(rate))
	}
	want := (7000.0 - 1000.0) / 60.0 * 8
	if math.Abs(rate[0].Value-want) > 0.01 {
		t.Fatalf("value = %v, want %v", rate[0].Value, want)
	}
	if !rate[0].Time.Equal(base.Add(60 * time.Second)) {
		t.Fatalf("time = %v", rate[0].Time)
	}
}

func TestComputeCounterRateCounter64WrapSkipsSegment(t *testing.T) {
	base := time.Unix(2000, 0).UTC()
	samples := []VMValue{
		{Time: base, Value: 100},
		{Time: base.Add(60 * time.Second), Value: 50},
		{Time: base.Add(120 * time.Second), Value: 200},
	}
	rate := ComputeCounterRate(samples, CounterRateOptions{CounterBits: 64, ToBits: false})
	if len(rate) != 1 {
		t.Fatalf("len = %d, want 1 (wrap segment skipped)", len(rate))
	}
	want := (200.0 - 50.0) / 60
	if math.Abs(rate[0].Value-want) > 0.01 {
		t.Fatalf("value = %v, want %v", rate[0].Value, want)
	}
}

func TestComputeCounterRateCounter32WrapRepair(t *testing.T) {
	base := time.Unix(3000, 0).UTC()
	prev := float64(math.Pow(2, 32) - 100)
	curr := float64(50)
	samples := []VMValue{
		{Time: base, Value: prev},
		{Time: base.Add(60 * time.Second), Value: curr},
	}
	rate := ComputeCounterRate(samples, CounterRateOptions{CounterBits: 32, ToBits: false})
	if len(rate) != 1 {
		t.Fatalf("len = %d, want 1", len(rate))
	}
	want := (curr + math.Pow(2, 32) - prev) / 60
	if math.Abs(rate[0].Value-want) > 0.01 {
		t.Fatalf("value = %v, want %v", rate[0].Value, want)
	}
}

func TestComputeCounterRateSkipsNonPositiveDt(t *testing.T) {
	base := time.Unix(4000, 0).UTC()
	samples := []VMValue{
		{Time: base, Value: 100},
		{Time: base, Value: 200},
		{Time: base.Add(60 * time.Second), Value: 800},
	}
	rate := ComputeCounterRate(samples, CounterRateOptions{CounterBits: 64, ToBits: false})
	if len(rate) != 1 {
		t.Fatalf("len = %d, want 1 (zero-dt skipped)", len(rate))
	}
	want := (800.0 - 200.0) / 60
	if math.Abs(rate[0].Value-want) > 0.01 {
		t.Fatalf("value = %v, want %v", rate[0].Value, want)
	}
}

func TestComputeCounterRateEmptyAndSingle(t *testing.T) {
	if got := ComputeCounterRate(nil, CounterRateOptions{}); got != nil {
		t.Fatalf("nil input = %v", got)
	}
	if got := ComputeCounterRate([]VMValue{{Time: time.Now(), Value: 1}}, CounterRateOptions{}); got != nil {
		t.Fatalf("single input = %v", got)
	}
}

func TestComputeCounterRateToBitsFalse(t *testing.T) {
	base := time.Unix(5000, 0).UTC()
	samples := []VMValue{
		{Time: base, Value: 0},
		{Time: base.Add(60 * time.Second), Value: 600},
	}
	rate := ComputeCounterRate(samples, CounterRateOptions{CounterBits: 64, ToBits: false})
	if len(rate) != 1 {
		t.Fatalf("len = %d", len(rate))
	}
	want := 600.0 / 60
	if math.Abs(rate[0].Value-want) > 0.01 {
		t.Fatalf("value = %v, want %v", rate[0].Value, want)
	}
}

func TestComputeCounterRateEmitsZeroForIdleIntervals(t *testing.T) {
	base := time.Unix(6000, 0).UTC()
	samples := []VMValue{
		{Time: base, Value: 1000},
		{Time: base.Add(60 * time.Second), Value: 2000},
		{Time: base.Add(120 * time.Second), Value: 2000},
		{Time: base.Add(180 * time.Second), Value: 4000},
	}
	rate := ComputeCounterRate(samples, CounterRateOptions{CounterBits: 64, ToBits: false})
	if len(rate) != 3 {
		t.Fatalf("len = %d, want 3 (idle interval must render as 0, not a gap)", len(rate))
	}
	// The increase after the stale step divides by the accumulated 120s, so a
	// poller/grid beat cannot double the rate.
	want := []float64{(2000.0 - 1000.0) / 60, 0, (4000.0 - 2000.0) / 120}
	for i, value := range want {
		if math.Abs(rate[i].Value-value) > 0.01 {
			t.Fatalf("rate[%d] = %v, want %v", i, rate[i].Value, value)
		}
	}
}

// Regression for the 0 / 2x sawtooth: when the poller lands every other query
// step, peak-hold bucketing must not see doubled rates.
func TestComputeCounterRateDoesNotDoubleOnGridBeat(t *testing.T) {
	base := time.Unix(6000, 0).UTC()
	samples := []VMValue{
		{Time: base, Value: 0},
		{Time: base.Add(60 * time.Second), Value: 0},
		{Time: base.Add(120 * time.Second), Value: 12000},
		{Time: base.Add(180 * time.Second), Value: 12000},
		{Time: base.Add(240 * time.Second), Value: 24000},
	}
	rate := ComputeCounterRate(samples, CounterRateOptions{CounterBits: 64, ToBits: false})
	for _, point := range rate {
		if point.Value > 12000.0/120+0.01 {
			t.Fatalf("rate spiked to %v, want <= %v (true rate)", point.Value, 12000.0/120)
		}
	}
}

// Trailing zeros at the query edge are staleness fill, not idleness; they
// must not drag the live edge of the chart down to 0.
func TestComputeCounterRateTrimsStalenessTail(t *testing.T) {
	base := time.Unix(6000, 0).UTC()
	samples := []VMValue{
		{Time: base, Value: 1000},
		{Time: base.Add(60 * time.Second), Value: 2000},
		{Time: base.Add(120 * time.Second), Value: 2000},
		{Time: base.Add(180 * time.Second), Value: 2000},
	}
	rate := ComputeCounterRate(samples, CounterRateOptions{CounterBits: 64, ToBits: false})
	if len(rate) != 1 || rate[0].Value == 0 {
		t.Fatalf("rate = %+v, want single non-zero point (stale tail trimmed)", rate)
	}
}
