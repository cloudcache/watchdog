package snmpdomain

import (
	"testing"
	"time"
)

type fixedRand int64

func (f fixedRand) Int64N(n int64) int64 { return int64(f) % n }

func TestDefaultPortPolicyUsesSideSpecificBillingBase(t *testing.T) {
	provider := DefaultPortPolicy("port-a", PortSideProvider)
	if provider.BillingBaseBps != ProviderBillingBaseBps {
		t.Fatalf("provider base = %d", provider.BillingBaseBps)
	}
	customer := DefaultPortPolicy("port-b", PortSideCustomer)
	if customer.BillingBaseBps != CustomerBillingBaseBps || customer.SampleStep != 5*time.Minute {
		t.Fatalf("customer policy = %+v", customer)
	}
}

func TestDefaultPortPolicyUsesManagedDefaults(t *testing.T) {
	defaults := TrafficPolicyDefaults{Provider: TrafficPolicyDefault{
		BillingBaseBps: 2000, SampleStep: time.Minute,
		CorrectionDirection: CorrectionUp, CorrectionMin: 1, CorrectionMax: 10,
	}}
	policy := DefaultPortPolicyWithDefaults("port-a", PortSideProvider, defaults)
	if policy.BillingBaseBps != 2000 || policy.SampleStep != time.Minute || policy.CorrectionDirection != CorrectionUp {
		t.Fatalf("provider policy = %+v", policy)
	}
}

func TestCorrectionIsStableAndPreservesFloat(t *testing.T) {
	policy := PortPolicy{Enabled: true, CorrectionDirection: CorrectionUp, CorrectionMin: 10, CorrectionMax: 20}
	if got := ApplyCorrectionWithRand(100, policy, fixedRand(5)); got != 115 {
		t.Fatalf("up correction = %d", got)
	}
	if got := ApplyCorrectionFloat(100.75, PortPolicy{Enabled: true, CorrectionDirection: CorrectionUp, CorrectionMin: 10, CorrectionMax: 10}, fixedRand(0)); got != 110.75 {
		t.Fatalf("float correction = %v", got)
	}
	first := DeterministicCorrectionRNG("port-a", time.Unix(100, 200)).Int64N(1000)
	second := DeterministicCorrectionRNG("port-a", time.Unix(100, 200)).Int64N(1000)
	if first != second {
		t.Fatalf("deterministic correction changed: %d != %d", first, second)
	}
}

func TestCorrectionDoesNotGoBelowZero(t *testing.T) {
	policy := PortPolicy{Enabled: true, CorrectionDirection: CorrectionDown, CorrectionMin: 50, CorrectionMax: 50}
	if got := ApplyCorrectionWithRand(10, policy, fixedRand(0)); got != 0 {
		t.Fatalf("down correction below zero = %d", got)
	}
}
