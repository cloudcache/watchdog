package watchdog

import (
	"testing"
	"time"
)

func TestBillingStepWidensForLongPeriods(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	month := BillingPeriod{RangeStart: start, RangeEnd: start.AddDate(0, 1, 0)}
	if step := billingStep(month); step != 2*time.Minute {
		t.Fatalf("month step = %s, want 2m (31d at 1m would exceed the VM point limit)", step)
	}
	week := BillingPeriod{RangeStart: start, RangeEnd: start.AddDate(0, 0, 7)}
	if step := billingStep(week); step != time.Minute {
		t.Fatalf("week step = %s, want 1m", step)
	}
	quarter := BillingPeriod{RangeStart: start, RangeEnd: start.AddDate(0, 3, 0)}
	if step := billingStep(quarter); int(quarter.RangeEnd.Sub(quarter.RangeStart)/step) > maxVMPointsPerSeries {
		t.Fatalf("quarter step %s still exceeds the VM point limit", step)
	}
}

func TestEstimateBillingBytesIntegratesWithGapClamp(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	samples := []Sample{
		{Time: start, Value: 800},                       // first sample: nominal step
		{Time: start.Add(time.Minute), Value: 800},      // 60s gap
		{Time: start.Add(31 * time.Minute), Value: 800}, // 30m outage: clamped to 2x step
	}
	got := estimateBillingBytes(samples, time.Minute)
	// 800bps x 60s/8 + 800 x 60/8 + 800 x 120(clamped)/8 = 6000+6000+12000
	if got != 24000 {
		t.Fatalf("bytes = %d, want 24000", got)
	}
}
