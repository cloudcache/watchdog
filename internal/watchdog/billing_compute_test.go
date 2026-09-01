package watchdog

import (
	"testing"
	"time"
)

func TestComputeBillingPeriodP95(t *testing.T) {
	result, err := ComputeBillingPeriod(
		BillingAccount{ID: "billing-a", Aggregation: AggregationP95FiveMinute},
		BillingPeriod{BillingAccountID: "billing-a"},
		[]Sample{{Value: 1}, {Value: 2}, {Value: 100}},
	)
	if err != nil {
		t.Fatalf("ComputeBillingPeriod() error = %v", err)
	}
	if result.ComputedValue != 100 {
		t.Fatalf("ComputedValue = %v, want 100", result.ComputedValue)
	}
}

func TestComputeBillingPeriodTotalBytesIntegratesRates(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	result, err := ComputeBillingPeriod(
		BillingAccount{ID: "billing-a", Aggregation: AggregationTotalBytes, QuotaBytes: 10},
		BillingPeriod{BillingAccountID: "billing-a", RangeStart: start, RangeEnd: start.Add(2 * time.Minute)},
		[]Sample{{Time: start, Value: 4}, {Time: start.Add(time.Minute), Value: 8}},
	)
	if err != nil {
		t.Fatalf("ComputeBillingPeriod() error = %v", err)
	}
	// bytes = 4 bps x 60s / 8 + 8 bps x 60s / 8 — the time integral, never the
	// bare sum of bps samples.
	if result.TotalBytes != 90 || result.ComputedValue != 90 || !result.QuotaExceeded {
		t.Fatalf("result = %#v", result)
	}
}

func TestComputeBillingPeriodRejectsWrongAccount(t *testing.T) {
	_, err := ComputeBillingPeriod(BillingAccount{ID: "billing-a"}, BillingPeriod{BillingAccountID: "billing-b"}, []Sample{{Value: 1}})
	if err == nil {
		t.Fatal("expected wrong account error")
	}
}
