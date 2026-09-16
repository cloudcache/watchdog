package server

import (
	"errors"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/cloudcache/watchdog/internal/snmpdomain"
)

func TestSNMPValueModesEnforceRawAdminBoundary(t *testing.T) {
	if modes, err := snmpValueModes("", false); err != nil || len(modes) != 1 || modes[0] != "corrected" {
		t.Fatalf("default modes=%v err=%v", modes, err)
	}
	if _, err := snmpValueModes("raw", false); !errors.Is(err, errSNMPRawForbidden) {
		t.Fatalf("non-admin raw error=%v", err)
	}
	if modes, err := snmpValueModes("both", true); err != nil || len(modes) != 2 || modes[0] != "corrected" || modes[1] != "raw" {
		t.Fatalf("admin both modes=%v err=%v", modes, err)
	}
}

func TestSNMPPolicyIsAppliedBeforeAggregation(t *testing.T) {
	observed := time.Date(2026, 9, 12, 1, 0, 0, 0, time.UTC)
	series := []snmpch.Series{
		{EntityKind: "port", EntityID: "port-a", Points: []snmpch.Point{{Time: observed, Value: 100}}},
		{EntityKind: "port", EntityID: "port-b", Points: []snmpch.Point{{Time: observed, Value: 200}}},
	}
	supplierPolicies := map[string]snmpdomain.PortPolicy{
		"port-a": {PortID: "port-a", SideType: snmpdomain.PortSideProvider, Enabled: true, CorrectionDirection: snmpdomain.CorrectionUp, CorrectionMin: 10, CorrectionMax: 10},
		"port-b": {PortID: "port-b", SideType: snmpdomain.PortSideProvider, Enabled: true, CorrectionDirection: snmpdomain.CorrectionUp, CorrectionMin: 10, CorrectionMax: 10},
	}
	customerPolicies := map[string]snmpdomain.PortPolicy{
		"port-a": {PortID: "port-a", SideType: snmpdomain.PortSideCustomer, Enabled: true, CorrectionDirection: snmpdomain.CorrectionDown, CorrectionMin: 20, CorrectionMax: 20},
		"port-b": {PortID: "port-b", SideType: snmpdomain.PortSideCustomer, Enabled: true, CorrectionDirection: snmpdomain.CorrectionDown, CorrectionMin: 20, CorrectionMax: 20},
	}
	supplier := aggregateSNMPSeries(correctedSNMPSeries(series, supplierPolicies, true), "sum")
	if len(supplier) != 1 || supplier[0].Value != 320 {
		t.Fatalf("supplier corrected total=%+v", supplier)
	}
	customer := aggregateSNMPSeries(correctedSNMPSeries(series, customerPolicies, true), "sum")
	if len(customer) != 1 || customer[0].Value != 260 {
		t.Fatalf("customer corrected total=%+v", customer)
	}
	raw := aggregateSNMPSeries(correctedSNMPSeries(series, supplierPolicies, false), "sum")
	if raw[0].Value != 300 {
		t.Fatalf("raw total=%+v", raw)
	}
}

func TestSNMPDefaultPolicyKeepsSideSpecificBases(t *testing.T) {
	provider := snmpdomain.DefaultPortPolicyWithDefaults("p", snmpdomain.PortSideProvider, snmpdomain.BuiltinTrafficPolicyDefaults)
	customer := snmpdomain.DefaultPortPolicyWithDefaults("c", snmpdomain.PortSideCustomer, snmpdomain.BuiltinTrafficPolicyDefaults)
	if provider.BillingBaseBps != snmpdomain.ProviderBillingBaseBps || customer.BillingBaseBps != snmpdomain.CustomerBillingBaseBps {
		t.Fatalf("provider=%d customer=%d", provider.BillingBaseBps, customer.BillingBaseBps)
	}
}

func TestEffectiveViewPoliciesKeepTheSamePorts(t *testing.T) {
	server := (*Server)(nil)
	portIDs := []string{"port-a", "port-b"}
	supplier, err := server.readPortPoliciesForViewContext(t.Context(), portIDs, snmpdomain.PortSideProvider)
	if err != nil {
		t.Fatal(err)
	}
	customer, err := server.readPortPoliciesForViewContext(t.Context(), portIDs, snmpdomain.PortSideCustomer)
	if err != nil {
		t.Fatal(err)
	}
	for _, portID := range portIDs {
		if supplier[portID].SideType != snmpdomain.PortSideProvider || customer[portID].SideType != snmpdomain.PortSideCustomer {
			t.Fatalf("port %s supplier=%+v customer=%+v", portID, supplier[portID], customer[portID])
		}
	}
	if len(supplier) != len(portIDs) || len(customer) != len(portIDs) {
		t.Fatalf("supplier ports=%d customer ports=%d", len(supplier), len(customer))
	}
	if supplier["port-a"].SampleStep != 5*time.Minute || customer["port-a"].SampleStep != 5*time.Minute {
		t.Fatalf("supplier step=%v customer step=%v", supplier["port-a"].SampleStep, customer["port-a"].SampleStep)
	}
}
