package snmpdomain

import (
	"context"
	"testing"
)

func TestSNMPMemoryDiscoveryHuaweiUsesMIBUsageAndSize(t *testing.T) {
	query := fakeQueryEngine{
		walks: map[string]QueryResponse{
			snmpOIDHwEntityMemUsage: {
				VarBinds: []VarBind{{OID: snmpOIDHwEntityMemUsage + ".9", Value: "43"}},
			},
			snmpOIDHwEntityMemSize: {
				VarBinds: []VarBind{{OID: snmpOIDHwEntityMemSize + ".9", Value: "2147483648"}},
			},
		},
	}
	module := SNMPMemoryDiscoveryModule{}
	result, err := module.Discover(context.Background(), DiscoveryContext{
		Device: Device{ID: "dev1"},
		OS:     OSMatch{OSName: "vrp", Vendor: "huawei"},
		Query:  query,
	})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(result.Recipes) != 3 {
		t.Fatalf("expected 3 recipes (used/total/percent), got %d", len(result.Recipes))
	}
	byMetric := map[string]Recipe{}
	for _, recipe := range result.Recipes {
		byMetric[recipe.MetricName] = recipe
	}
	percent := byMetric[MetricSNMPDeviceMemPercent]
	if percent.NumericOID != snmpOIDHwEntityMemUsage+".9" || percent.Unit != "percent" || percent.HasMultiplier {
		t.Fatalf("percent recipe = %+v", percent)
	}
	used := byMetric[MetricSNMPMemoryUsed]
	if used.NumericOID != snmpOIDHwEntityMemUsage+".9" {
		t.Fatalf("used recipe polls %s, want hwEntityMemUsage.9", used.NumericOID)
	}
	if !used.HasMultiplier || used.Multiplier != 2147483648.0/100 {
		t.Fatalf("used recipe multiplier = %v (has=%v), want size/100", used.Multiplier, used.HasMultiplier)
	}
	if used.OID != "HUAWEI-ENTITY-EXTENT-MIB::hwEntityMemUsage.9" {
		t.Fatalf("used recipe textual OID = %q", used.OID)
	}
	total := byMetric[MetricSNMPMemoryTotal]
	if total.NumericOID != snmpOIDHwEntityMemSize+".9" {
		t.Fatalf("total recipe polls %s, want hwEntityMemSize.9", total.NumericOID)
	}
	if total.HasMultiplier {
		t.Fatalf("total recipe is already bytes, no multiplier expected")
	}
}

func TestSNMPMemoryDiscoveryHostResourcesAppliesAllocationUnits(t *testing.T) {
	query := fakeQueryEngine{
		walks: map[string]QueryResponse{
			snmpOIDHrStorageDescr: {
				VarBinds: []VarBind{{OID: snmpOIDHrStorageDescr + ".4", Value: "Physical memory"}},
			},
			snmpOIDHrStorageAllocUnits: {
				VarBinds: []VarBind{{OID: snmpOIDHrStorageAllocUnits + ".4", Value: "4096"}},
			},
		},
	}
	module := SNMPMemoryDiscoveryModule{}
	result, err := module.Discover(context.Background(), DiscoveryContext{
		Device: Device{ID: "dev1"},
		Query:  query,
	})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(result.Recipes) != 2 {
		t.Fatalf("expected 2 recipes, got %d", len(result.Recipes))
	}
	for _, recipe := range result.Recipes {
		if !recipe.HasMultiplier || recipe.Multiplier != 4096 {
			t.Fatalf("recipe %s multiplier = %v (has=%v), want allocation units 4096", recipe.MetricName, recipe.Multiplier, recipe.HasMultiplier)
		}
		if recipe.Options["alloc_units"] != "4096" {
			t.Fatalf("recipe %s alloc_units option = %q", recipe.MetricName, recipe.Options["alloc_units"])
		}
	}
	used := result.Recipes[0]
	if used.OID != "HOST-RESOURCES-MIB::hrStorageUsed.4" {
		t.Fatalf("textual OID = %q, want HOST-RESOURCES-MIB::hrStorageUsed.4", used.OID)
	}
}

func TestExtractOSVersionKeepsFullParenthetical(t *testing.T) {
	huawei := "S5720-56C-EI-AC \r\nHuawei Versatile Routing Platform Software \r\n VRP (R) software,Version 5.170 (S5720 V200R010C00SPC600) \r\n Copyright (C) 2007 Huawei"
	if got := extractOSVersion(huawei); got != "5.170 (S5720 V200R010C00SPC600)" {
		t.Fatalf("huawei version = %q", got)
	}
	cisco := "Cisco IOS Software, C2960X Software, Version 15.2(2)E3, RELEASE SOFTWARE (fc2)"
	if got := extractOSVersion(cisco); got != "15.2(2)E3" {
		t.Fatalf("cisco version = %q", got)
	}
	if got := extractOSVersion("no version marker here"); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}
