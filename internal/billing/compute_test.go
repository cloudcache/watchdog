package billing

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNearestRank95thAverageAndTotal(t *testing.T) {
	rates := make([]float64, 20)
	for i := range rates {
		rates[i] = float64(i + 1)
	}
	rates[19] = 100
	p95, average := RateStatistics(rates)
	if p95 != 19 || average != 15 {
		t.Fatalf("p95=%d average=%d", p95, average)
	}
	value := BuildValue("period", 3, AlgorithmTotal, LayerStats{
		Layer: LayerRaw, InBytes: 100, OutBytes: 200, SelectedBytes: 300,
		SelectedRates: rates, Coverage: 1, ExpectedBuckets: 20, ObservedBuckets: 20,
	})
	if value.AlgorithmValue != 300 || value.Unit != "bytes" || value.Rate95thBPS != 19 {
		t.Fatalf("value=%+v", value)
	}
}

func TestExternalValueValidation(t *testing.T) {
	valid := Value{Layer: LayerExternal, Algorithm: Algorithm95th, Unit: "bps", Rate95thBPS: 900, AlgorithmValue: 900,
		Coverage: .95, ExpectedBuckets: 20, ObservedBuckets: 19, MissingBuckets: 1, Provenance: json.RawMessage(`{"invoice":"x"}`)}
	if err := ValidateExternalValue(valid); err != nil {
		t.Fatal(err)
	}
	tests := []Value{
		func() Value { value := valid; value.Coverage = 1.1; return value }(),
		func() Value { value := valid; value.ObservedBuckets = 21; return value }(),
		func() Value { value := valid; value.MissingBuckets = 0; return value }(),
		func() Value { value := valid; value.AlgorithmValue++; return value }(),
	}
	for index, value := range tests {
		if err := ValidateExternalValue(value); err == nil {
			t.Fatalf("invalid external evidence %d was accepted: %+v", index, value)
		}
	}
}

func TestPeriodWindowUsesUTCClosedFiveMinuteBuckets(t *testing.T) {
	from := time.Date(2026, 3, 8, 6, 55, 0, 0, time.UTC)
	to := from.Add(10 * time.Minute)
	if err := ValidatePeriodWindow(from, to, "America/New_York", to.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePeriodWindow(from.Add(time.Minute), to, "UTC", to.Add(time.Hour)); err == nil {
		t.Fatal("expected non-aligned range rejection")
	}
	if err := ValidatePeriodWindow(from, to.Add(24*time.Hour), "UTC", to); err == nil {
		t.Fatal("expected open/future range rejection")
	}
}

func TestSuggestedPeriodWindowUsesTimezoneBillingDayAndShortMonth(t *testing.T) {
	account := Account{BillingDay: 31, Timezone: "Asia/Singapore"}
	from, to, err := SuggestedPeriodWindow(account, time.Date(2026, time.May, 15, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	location, _ := time.LoadLocation(account.Timezone)
	fromLocal, toLocal := from.In(location), to.In(location)
	if fromLocal.Year() != 2026 || fromLocal.Month() != time.March || fromLocal.Day() != 31 || fromLocal.Hour() != 0 {
		t.Fatalf("from=%s", fromLocal)
	}
	if toLocal.Year() != 2026 || toLocal.Month() != time.April || toLocal.Day() != 30 || toLocal.Hour() != 0 {
		t.Fatalf("to=%s", toLocal)
	}

	account.BillingDay = 20
	from, to, err = SuggestedPeriodWindow(account, time.Date(2026, time.May, 15, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if from.In(location).Day() != 20 || from.In(location).Month() != time.March || to.In(location).Day() != 20 || to.In(location).Month() != time.April {
		t.Fatalf("cycle before current boundary=[%s,%s)", from.In(location), to.In(location))
	}

	account = Account{BillingDay: 5, Timezone: "America/New_York"}
	from, to, err = SuggestedPeriodWindow(account, time.Date(2026, time.April, 20, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !from.Equal(time.Date(2026, time.March, 5, 5, 0, 0, 0, time.UTC)) || !to.Equal(time.Date(2026, time.April, 5, 4, 0, 0, 0, time.UTC)) {
		t.Fatalf("DST cycle=[%s,%s)", from, to)
	}
}

func TestBillingPricingContractValidation(t *testing.T) {
	contract := uint64(10_000_000_000)
	valid := Account{
		Name: "customer-a", Status: "active", MeasurementType: MeasurementBandwidth, BillingMethod: BillingMonthly95th, Algorithm: Algorithm95th,
		BillingDay: 1, Timezone: "UTC", Direction: DirectionAgg, DefaultLayer: LayerCustomer,
		PriceCurrency: "CNY", UnitPrice: "12.345600", MinimumPercent: 30, ContractBandwidthBPS: &contract,
	}
	if err := ValidateAccount(valid); err != nil {
		t.Fatalf("valid pricing contract: %v", err)
	}
	tests := []Account{
		func() Account { item := valid; item.PriceCurrency = "cny"; return item }(),
		func() Account { item := valid; item.UnitPrice = "1.0000001"; return item }(),
		func() Account { item := valid; item.UnitPrice = "-1"; return item }(),
		func() Account { item := valid; item.Algorithm = AlgorithmAverage; return item }(),
		func() Account { item := valid; item.MinimumPercent = 101; return item }(),
		func() Account {
			item := valid
			item.BillingMethod, item.Algorithm = BillingPackagePort, AlgorithmAverage
			return item
		}(),
	}
	for index, account := range tests {
		if err := ValidateAccount(account); err == nil {
			t.Fatalf("invalid pricing contract %d was accepted: %+v", index, account)
		}
	}
	quota := uint64(1_000_000)
	flat := valid
	flat.MeasurementType, flat.BillingMethod, flat.Algorithm, flat.TrafficAllowance = MeasurementTraffic, BillingPackagePort, AlgorithmTotal, &quota
	flat.ContractBandwidthBPS = nil
	flat.MinimumPercent = 0
	if err := ValidateAccount(flat); err != nil {
		t.Fatalf("valid flat-port pricing contract: %v", err)
	}
}

func TestCapacityCheckIsEvidenceNotBillingAuthority(t *testing.T) {
	contract := uint64(8_000_000_000)
	check := BuildCapacityCheck(&contract, []AccountPort{{CapacityBPS: 10_000_000_000}})
	if check.Status != "excess" || check.ContractBPS != contract || check.SelectedBPS != 10_000_000_000 || check.DifferenceBPS != 2_000_000_000 {
		t.Fatalf("capacity check=%+v", check)
	}
	unknown := BuildCapacityCheck(&contract, []AccountPort{{CapacityBPS: 10_000_000_000}, {}})
	if unknown.Status != "unknown" || unknown.UnknownPortCount != 1 {
		t.Fatalf("unknown capacity check=%+v", unknown)
	}
	insufficient := BuildCapacityCheck(&contract, []AccountPort{{CapacityBPS: 5_000_000_000}})
	if insufficient.Status != "insufficient" || insufficient.DifferenceBPS != 3_000_000_000 {
		t.Fatalf("insufficient capacity check=%+v", insufficient)
	}
	match := BuildCapacityCheck(&contract, []AccountPort{{CapacityBPS: contract}})
	if match.Status != "match" || match.DifferenceBPS != 0 {
		t.Fatalf("matching capacity check=%+v", match)
	}
}

func TestDaily95thAveragesLocalCalendarDays(t *testing.T) {
	location, err := time.LoadLocation("Asia/Singapore")
	if err != nil {
		t.Fatal(err)
	}
	// Both local-day groups fall on the same UTC date. Grouping by UTC would
	// produce P95=38; grouping by the frozen account timezone produces
	// per-day P95 values 19 and 39, then their rounded average 29.
	starts := []time.Time{
		time.Date(2026, time.September, 1, 22, 20, 0, 0, location),
		time.Date(2026, time.September, 2, 0, 0, 0, 0, location),
	}
	samples := make([]RateSample, 0, 40)
	for day, start := range starts {
		for index := 0; index < 20; index++ {
			samples = append(samples, RateSample{Time: start.Add(time.Duration(index) * 5 * time.Minute).UTC(), Value: float64(index + 1 + day*20)})
		}
	}
	value, err := Daily95thAverage(samples, "Asia/Singapore")
	if err != nil || value != 29 { // daily P95 values 19 and 39, averaged and rounded.
		t.Fatalf("daily95=%d err=%v", value, err)
	}
}

func TestReconciliationReportsEvidenceAndNeverAdjusts(t *testing.T) {
	values := []Value{
		{Layer: LayerSNMP, AlgorithmValue: 1000, Coverage: .9, ExpectedBuckets: 20, ObservedBuckets: 18, MissingBuckets: 2, ResetBuckets: 1, GapBuckets: 1},
		{Layer: LayerRaw, AlgorithmValue: 1000, Coverage: 1, ExpectedBuckets: 20, ObservedBuckets: 20, UnknownSamplingRecords: 2},
		{Layer: LayerSupplier, AlgorithmValue: 900, Coverage: 1, ExpectedBuckets: 20, ObservedBuckets: 20},
		{Layer: LayerCustomer, AlgorithmValue: 800, Coverage: 1, ExpectedBuckets: 20, ObservedBuckets: 20},
		{Layer: LayerExternal, AlgorithmValue: 750, Coverage: 1, ExpectedBuckets: 20, ObservedBuckets: 20},
	}
	issues := Reconcile(values, 20, 5)
	counts := map[string]int{}
	for _, issue := range issues {
		counts[issue.Kind]++
		if string(issue.Detail) != `{"automatic_adjustment":false}` {
			t.Fatalf("issue unexpectedly adjusts values: %s", issue.Detail)
		}
	}
	if counts["coverage"] != 1 || counts["missing_bucket"] != 1 || counts["counter_reset"] != 1 || counts["counter_gap"] != 1 || counts["missing_sampling"] != 1 || counts["threshold"] != 3 {
		t.Fatalf("issue counts=%v issues=%+v", counts, issues)
	}
}

func TestAdjustmentAndReversalSumToZero(t *testing.T) {
	items := []Adjustment{
		{Layer: LayerCustomer, Unit: "bps", Amount: 100, Status: "approved"},
		{Layer: LayerCustomer, Unit: "bps", Amount: -100, Status: "approved", ReversesAdjustmentID: "a"},
	}
	if got := EffectiveAdjustment(items, LayerCustomer, "bps"); got != 0 {
		t.Fatalf("adjustment=%d", got)
	}
	if got := ApplyAdjustment(50, -100); got != 0 {
		t.Fatalf("underflow result=%d", got)
	}
}
