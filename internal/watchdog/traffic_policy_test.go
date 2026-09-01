package watchdog

import (
	"testing"
	"time"
)

type fixedRand int64

func (f fixedRand) Int64N(n int64) int64 {
	return int64(f) % n
}

func TestDefaultPortPolicyUsesSideSpecificBillingBase(t *testing.T) {
	provider := DefaultPortPolicy("tenant-a", "port-a", PortSideProvider)
	if provider.BillingBaseBps != ProviderBillingBaseBps {
		t.Fatalf("provider base = %d", provider.BillingBaseBps)
	}
	customer := DefaultPortPolicy("tenant-a", "port-b", PortSideCustomer)
	if customer.BillingBaseBps != CustomerBillingBaseBps {
		t.Fatalf("customer base = %d", customer.BillingBaseBps)
	}
	if customer.SampleStep != 5*time.Minute {
		t.Fatalf("default sample step = %s", customer.SampleStep)
	}
}

func TestDefaultPortPolicyWithDefaultsUsesAdminManagedValues(t *testing.T) {
	defaults := TrafficPolicyDefaults{
		Provider: TrafficPolicyDefault{
			BillingBaseBps:      2000,
			SampleStep:          time.Minute,
			CorrectionDirection: CorrectionUp,
			CorrectionMin:       1,
			CorrectionMax:       10,
		},
		Customer: TrafficPolicyDefault{
			BillingBaseBps: 3000,
			SampleStep:     5 * time.Minute,
		},
	}
	policy := DefaultPortPolicyWithDefaults("tenant-a", "port-a", PortSideProvider, defaults)
	if policy.BillingBaseBps != 2000 {
		t.Fatalf("billing base = %d", policy.BillingBaseBps)
	}
	if policy.SampleStep != time.Minute {
		t.Fatalf("sample step = %s", policy.SampleStep)
	}
	if policy.CorrectionDirection != CorrectionUp || policy.CorrectionMin != 1 || policy.CorrectionMax != 10 {
		t.Fatalf("correction = %s %d %d", policy.CorrectionDirection, policy.CorrectionMin, policy.CorrectionMax)
	}
}

func TestApplyCorrectionUpAndDown(t *testing.T) {
	up := PortPolicy{
		Enabled:             true,
		CorrectionDirection: CorrectionUp,
		CorrectionMin:       10,
		CorrectionMax:       20,
	}
	if got := ApplyCorrectionWithRand(100, up, fixedRand(5)); got != 115 {
		t.Fatalf("up correction = %d", got)
	}
	down := up
	down.CorrectionDirection = CorrectionDown
	if got := ApplyCorrectionWithRand(100, down, fixedRand(5)); got != 85 {
		t.Fatalf("down correction = %d", got)
	}
}

func TestApplyCorrectionDoesNotGoBelowZero(t *testing.T) {
	policy := PortPolicy{
		Enabled:             true,
		CorrectionDirection: CorrectionDown,
		CorrectionMin:       50,
		CorrectionMax:       50,
	}
	if got := ApplyCorrectionWithRand(10, policy, fixedRand(0)); got != 0 {
		t.Fatalf("down correction below zero = %d", got)
	}
}

func TestApplyCorrectionFloatPreservesFraction(t *testing.T) {
	policy := PortPolicy{Enabled: true, CorrectionDirection: CorrectionUp, CorrectionMin: 10, CorrectionMax: 10}
	if got := ApplyCorrectionFloat(100.75, policy, fixedRand(0)); got != 110.75 {
		t.Fatalf("corrected = %v, want 110.75", got)
	}
	inactive := PortPolicy{Enabled: true, CorrectionDirection: CorrectionNone}
	if got := ApplyCorrectionFloat(-3.7, inactive, fixedRand(0)); got != -3.7 {
		t.Fatalf("inactive policy must not alter values, got %v", got)
	}
}
