package snmpdomain

import (
	"reflect"
	"testing"
)

func TestDefaultSNMPCollectorModuleRegistry(t *testing.T) {
	registry := DefaultSNMPCollectorModuleRegistry()
	want := []string{"bgp", "entity-physical", "lags", "memory", "ports", "processors", "sensors", "storage", "vlans"}
	if got := registry.DiscoveryModuleNames(); !reflect.DeepEqual(got, want) {
		t.Fatalf("module names = %v, want %v", got, want)
	}

	selected := registry.DiscoveryModulesForDefinitions([]ModuleDefinition{
		{ModuleName: "ports", ModuleType: ModuleDiscovery},
		{ModuleName: "ports", ModuleType: ModuleDiscovery},
		{ModuleName: "bgp", ModuleType: ModulePoller},
		{ModuleName: "missing", ModuleType: ModuleDiscovery},
		{ModuleName: "sensors", ModuleType: ModuleDiscovery},
	})
	if len(selected) != 2 || selected[0].Name() != "ports" || selected[1].Name() != "sensors" {
		t.Fatalf("selected modules = %#v", selected)
	}
}

func TestDiscoveryStableIDKeepsPreExtractionIdentity(t *testing.T) {
	// The removed tenant argument was always empty in the live single-domain
	// server. Keep its separator in the seed so existing inventory IDs do not
	// churn during package extraction.
	if got, want := collectorStableID("port", "", "device-a", "101"), "c_7859e6379e2b8f5cb50b598d"; got != want {
		t.Fatalf("stable ID = %q, want %q", got, want)
	}
}
