package server

import (
	"errors"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/cloudcache/watchdog/internal/watchdog"
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
		{EntityKind: "port", EntityID: "provider", Points: []snmpch.Point{{Time: observed, Value: 100}}},
		{EntityKind: "port", EntityID: "customer", Points: []snmpch.Point{{Time: observed, Value: 200}}},
	}
	policies := map[string]watchdog.PortPolicy{
		"provider": {PortID: "provider", SideType: watchdog.PortSideProvider, Enabled: true, CorrectionDirection: watchdog.CorrectionUp, CorrectionMin: 10, CorrectionMax: 10},
		"customer": {PortID: "customer", SideType: watchdog.PortSideCustomer, Enabled: true, CorrectionDirection: watchdog.CorrectionDown, CorrectionMin: 20, CorrectionMax: 20},
	}
	provider := filterSNMPSeriesBySide(series, policies, watchdog.PortSideProvider)
	corrected := correctedSNMPSeries(provider, policies, true)
	points := aggregateSNMPSeries(corrected, "sum")
	if len(points) != 1 || points[0].Value != 110 {
		t.Fatalf("provider corrected total=%+v", points)
	}
	all := aggregateSNMPSeries(correctedSNMPSeries(series, policies, true), "sum")
	if len(all) != 1 || all[0].Value != 290 {
		t.Fatalf("all corrected total=%+v", all)
	}
	raw := aggregateSNMPSeries(correctedSNMPSeries(series, policies, false), "sum")
	if raw[0].Value != 300 {
		t.Fatalf("raw total=%+v", raw)
	}
}

func TestSNMPDefaultPolicyKeepsSideSpecificBases(t *testing.T) {
	provider := watchdog.DefaultPortPolicyWithDefaults("", "p", watchdog.PortSideProvider, watchdog.BuiltinTrafficPolicyDefaults)
	customer := watchdog.DefaultPortPolicyWithDefaults("", "c", watchdog.PortSideCustomer, watchdog.BuiltinTrafficPolicyDefaults)
	if provider.BillingBaseBps != watchdog.ProviderBillingBaseBps || customer.BillingBaseBps != watchdog.CustomerBillingBaseBps {
		t.Fatalf("provider=%d customer=%d", provider.BillingBaseBps, customer.BillingBaseBps)
	}
}

func TestExplicitSNMPScopeSideFiltering(t *testing.T) {
	scopes := []snmpch.Scope{
		{DeviceID: "device-a", PortID: "provider"},
		{DeviceID: "device-a", PortID: "customer"},
	}
	portIDs, explicit := explicitSNMPScopePortIDs(scopes)
	if !explicit || len(portIDs) != 2 {
		t.Fatalf("explicit scopes=%v portIDs=%v", explicit, portIDs)
	}
	policies := map[string]watchdog.PortPolicy{
		"provider": {PortID: "provider", SideType: watchdog.PortSideProvider},
		"customer": {PortID: "customer", SideType: watchdog.PortSideCustomer},
	}
	filtered := filterSNMPScopesBySide(scopes, policies, watchdog.PortSideProvider)
	if len(filtered) != 1 || filtered[0].PortID != "provider" {
		t.Fatalf("provider scopes=%+v", filtered)
	}
	if _, explicit := explicitSNMPScopePortIDs([]snmpch.Scope{{DeviceID: "device-a"}}); explicit {
		t.Fatal("device-wide scope was treated as an explicit port set")
	}
}
