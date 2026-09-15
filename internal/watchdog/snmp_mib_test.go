package watchdog

import (
	"strings"
	"testing"
)

func TestEmbeddedSNMPMIBModulesDescribeCollectorBundle(t *testing.T) {
	modules, err := EmbeddedSNMPMIBModules()
	if err != nil {
		t.Fatal(err)
	}
	if len(modules) != len(snmpMIBEmbeddedModules) {
		t.Fatalf("module count = %d, want %d", len(modules), len(snmpMIBEmbeddedModules))
	}
	want := map[string]bool{"SNMPv2-MIB": false, "IF-MIB": false, "BGP4-MIB": false}
	for _, module := range modules {
		if !module.Builtin || !module.Enabled || !IsEmbeddedSNMPMIBSource(module.Source) {
			t.Fatalf("embedded module metadata is inconsistent: %+v", module)
		}
		if !strings.HasPrefix(module.Checksum, "sha256:") || len(module.Checksum) != len("sha256:")+64 {
			t.Fatalf("embedded module %s checksum = %q", module.Name, module.Checksum)
		}
		if _, ok := want[module.Name]; ok {
			want[module.Name] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("collector module %s is missing from management inventory", name)
		}
	}
}

// Locks the MIB-resolved OIDs of every object the collector uses to their
// canonical numeric values, so the embedded bundle and resolver mistakes are
// caught at test time rather than against live devices.
func TestSNMPMIBRegistryResolvesCollectorObjects(t *testing.T) {
	expected := map[string]string{
		// core fingerprint
		"SNMPv2-MIB::sysDescr.0":             "1.3.6.1.2.1.1.1.0",
		"SNMPv2-MIB::sysObjectID.0":          "1.3.6.1.2.1.1.2.0",
		"SNMPv2-MIB::sysUpTime.0":            "1.3.6.1.2.1.1.3.0",
		"SNMPv2-MIB::sysName.0":              "1.3.6.1.2.1.1.5.0",
		"SNMP-FRAMEWORK-MIB::snmpEngineID.0": "1.3.6.1.6.3.10.2.1.1.0",
		// ports (IF-MIB)
		"IF-MIB::ifDescr":            "1.3.6.1.2.1.2.2.1.2",
		"IF-MIB::ifType":             "1.3.6.1.2.1.2.2.1.3",
		"IF-MIB::ifSpeed":            "1.3.6.1.2.1.2.2.1.5",
		"IF-MIB::ifAdminStatus":      "1.3.6.1.2.1.2.2.1.7",
		"IF-MIB::ifOperStatus":       "1.3.6.1.2.1.2.2.1.8",
		"IF-MIB::ifInOctets":         "1.3.6.1.2.1.2.2.1.10",
		"IF-MIB::ifInDiscards":       "1.3.6.1.2.1.2.2.1.13",
		"IF-MIB::ifInErrors":         "1.3.6.1.2.1.2.2.1.14",
		"IF-MIB::ifOutOctets":        "1.3.6.1.2.1.2.2.1.16",
		"IF-MIB::ifOutDiscards":      "1.3.6.1.2.1.2.2.1.19",
		"IF-MIB::ifOutErrors":        "1.3.6.1.2.1.2.2.1.20",
		"IF-MIB::ifName":             "1.3.6.1.2.1.31.1.1.1.1",
		"IF-MIB::ifHCInOctets":       "1.3.6.1.2.1.31.1.1.1.6",
		"IF-MIB::ifHCOutOctets":      "1.3.6.1.2.1.31.1.1.1.10",
		"IF-MIB::ifHighSpeed":        "1.3.6.1.2.1.31.1.1.1.15",
		"IF-MIB::ifConnectorPresent": "1.3.6.1.2.1.31.1.1.1.17",
		"IF-MIB::ifAlias":            "1.3.6.1.2.1.31.1.1.1.18",
		// bgp (BGP4-MIB + CISCO-BGP4-MIB)
		"BGP4-MIB::bgpLocalAs.0":                      "1.3.6.1.2.1.15.2.0",
		"BGP4-MIB::bgpPeerState":                      "1.3.6.1.2.1.15.3.1.2",
		"BGP4-MIB::bgpPeerRemoteAddr":                 "1.3.6.1.2.1.15.3.1.7",
		"BGP4-MIB::bgpPeerRemoteAs":                   "1.3.6.1.2.1.15.3.1.9",
		"BGP4-MIB::bgpPeerInUpdates":                  "1.3.6.1.2.1.15.3.1.10",
		"BGP4-MIB::bgpPeerOutUpdates":                 "1.3.6.1.2.1.15.3.1.11",
		"BGP4-MIB::bgpPeerInTotalMessages":            "1.3.6.1.2.1.15.3.1.12",
		"BGP4-MIB::bgpPeerOutTotalMessages":           "1.3.6.1.2.1.15.3.1.13",
		"BGP4-MIB::bgpPeerFsmEstablishedTime":         "1.3.6.1.2.1.15.3.1.16",
		"CISCO-BGP4-MIB::cbgpPeer2RemoteAddr":         "1.3.6.1.4.1.9.9.187.1.2.5.1.2",
		"CISCO-BGP4-MIB::cbgpPeer2State":              "1.3.6.1.4.1.9.9.187.1.2.5.1.3",
		"CISCO-BGP4-MIB::cbgpPeer2RemoteAs":           "1.3.6.1.4.1.9.9.187.1.2.5.1.11",
		"CISCO-BGP4-MIB::cbgpPeer2InUpdates":          "1.3.6.1.4.1.9.9.187.1.2.5.1.13",
		"CISCO-BGP4-MIB::cbgpPeer2OutUpdates":         "1.3.6.1.4.1.9.9.187.1.2.5.1.14",
		"CISCO-BGP4-MIB::cbgpPeer2InTotalMessages":    "1.3.6.1.4.1.9.9.187.1.2.5.1.15",
		"CISCO-BGP4-MIB::cbgpPeer2OutTotalMessages":   "1.3.6.1.4.1.9.9.187.1.2.5.1.16",
		"CISCO-BGP4-MIB::cbgpPeer2FsmEstablishedTime": "1.3.6.1.4.1.9.9.187.1.2.5.1.19",
		// host resources
		"HOST-RESOURCES-MIB::hrStorageDescr":           "1.3.6.1.2.1.25.2.3.1.3",
		"HOST-RESOURCES-MIB::hrStorageAllocationUnits": "1.3.6.1.2.1.25.2.3.1.4",
		"HOST-RESOURCES-MIB::hrStorageSize":            "1.3.6.1.2.1.25.2.3.1.5",
		"HOST-RESOURCES-MIB::hrStorageUsed":            "1.3.6.1.2.1.25.2.3.1.6",
		"HOST-RESOURCES-MIB::hrProcessorLoad":          "1.3.6.1.2.1.25.3.3.1.2",
		"HOST-RESOURCES-MIB::hrDeviceDescr":            "1.3.6.1.2.1.25.3.2.1.3",
		// entity + entity-sensor
		"ENTITY-MIB::entPhysicalDescr":                "1.3.6.1.2.1.47.1.1.1.1.2",
		"ENTITY-MIB::entPhysicalVendorType":           "1.3.6.1.2.1.47.1.1.1.1.3",
		"ENTITY-MIB::entPhysicalContainedIn":          "1.3.6.1.2.1.47.1.1.1.1.4",
		"ENTITY-MIB::entPhysicalClass":                "1.3.6.1.2.1.47.1.1.1.1.5",
		"ENTITY-MIB::entPhysicalName":                 "1.3.6.1.2.1.47.1.1.1.1.7",
		"ENTITY-MIB::entPhysicalHardwareRev":          "1.3.6.1.2.1.47.1.1.1.1.8",
		"ENTITY-MIB::entPhysicalSerialNum":            "1.3.6.1.2.1.47.1.1.1.1.11",
		"ENTITY-MIB::entPhysicalMfgName":              "1.3.6.1.2.1.47.1.1.1.1.12",
		"ENTITY-MIB::entPhysicalModelName":            "1.3.6.1.2.1.47.1.1.1.1.13",
		"ENTITY-MIB::entPhysicalIsFRU":                "1.3.6.1.2.1.47.1.1.1.1.16",
		"ENTITY-SENSOR-MIB::entPhySensorType":         "1.3.6.1.2.1.99.1.1.1.1",
		"ENTITY-SENSOR-MIB::entPhySensorScale":        "1.3.6.1.2.1.99.1.1.1.2",
		"ENTITY-SENSOR-MIB::entPhySensorPrecision":    "1.3.6.1.2.1.99.1.1.1.3",
		"ENTITY-SENSOR-MIB::entPhySensorValue":        "1.3.6.1.2.1.99.1.1.1.4",
		"ENTITY-SENSOR-MIB::entPhySensorOperStatus":   "1.3.6.1.2.1.99.1.1.1.5",
		"ENTITY-SENSOR-MIB::entPhySensorUnitsDisplay": "1.3.6.1.2.1.99.1.1.1.6",
		// vlans / lags
		"Q-BRIDGE-MIB::dot1qVlanStaticName":        "1.3.6.1.2.1.17.7.1.4.3.1.1",
		"Q-BRIDGE-MIB::dot1qVlanStaticRowStatus":   "1.3.6.1.2.1.17.7.1.4.3.1.5",
		"IEEE8023-LAG-MIB::dot3adAggMACAddress":    "1.2.840.10006.300.43.1.1.1.1.2",
		"IEEE8023-LAG-MIB::dot3adAggActorAdminKey": "1.2.840.10006.300.43.1.1.1.1.6",
		// huawei entity extensions
		"HUAWEI-ENTITY-EXTENT-MIB::hwEntityCpuUsage": "1.3.6.1.4.1.2011.5.25.31.1.1.1.1.5",
		"HUAWEI-ENTITY-EXTENT-MIB::hwEntityMemUsage": "1.3.6.1.4.1.2011.5.25.31.1.1.1.1.7",
		"HUAWEI-ENTITY-EXTENT-MIB::hwEntityMemSize":  "1.3.6.1.4.1.2011.5.25.31.1.1.1.1.9",
		// traps
		"SNMPv2-MIB::coldStart":             "1.3.6.1.6.3.1.1.5.1",
		"SNMPv2-MIB::warmStart":             "1.3.6.1.6.3.1.1.5.2",
		"IF-MIB::linkDown":                  "1.3.6.1.6.3.1.1.5.3",
		"IF-MIB::linkUp":                    "1.3.6.1.6.3.1.1.5.4",
		"SNMPv2-MIB::authenticationFailure": "1.3.6.1.6.3.1.1.5.5",
		"BGP4-MIB::bgpBackwardTransition":   "1.3.6.1.2.1.15.7.2",
	}
	registry := DefaultSNMPMIBRegistry()
	for ref, want := range expected {
		got, err := registry.OID(ref)
		if err != nil {
			t.Errorf("OID(%q): %v", ref, err)
			continue
		}
		if got != want {
			t.Errorf("OID(%q) = %s, want %s", ref, got, want)
		}
	}
}

func TestSNMPMIBRegistryAcceptsBareAndNumericReferences(t *testing.T) {
	registry := DefaultSNMPMIBRegistry()
	if got, err := registry.OID("ifHCInOctets"); err != nil || got != "1.3.6.1.2.1.31.1.1.1.6" {
		t.Fatalf("bare name resolution = %q, %v", got, err)
	}
	if got, err := registry.OID(".1.3.6.1.2.1.2.2.1.2"); err != nil || got != "1.3.6.1.2.1.2.2.1.2" {
		t.Fatalf("numeric passthrough = %q, %v", got, err)
	}
	if _, err := registry.OID("IF-MIB::doesNotExist"); err == nil {
		t.Fatal("expected error for unknown object")
	}
}

func TestSNMPMIBRegistryReverseLookupAndDisplay(t *testing.T) {
	registry := DefaultSNMPMIBRegistry()
	name, err := registry.Name("1.3.6.1.2.1.31.1.1.1.6")
	if err != nil {
		t.Fatalf("Name: %v", err)
	}
	if name != "IF-MIB::ifHCInOctets" {
		t.Fatalf("Name = %q, want IF-MIB::ifHCInOctets", name)
	}
	if got := snmpMIBDisplayOID("1.3.6.1.2.1.2.2.1.8"); got != "IF-MIB::ifOperStatus" {
		t.Fatalf("snmpMIBDisplayOID = %q", got)
	}
	if got := snmpMIBDisplayOID("1.3.6.1.4.1.99999.1.2.3"); got != "1.3.6.1.4.1.99999.1.2.3" {
		t.Fatalf("unknown oid should render unchanged, got %q", got)
	}
}

func TestSNMPMIBRegistryStateValues(t *testing.T) {
	states := DefaultSNMPMIBRegistry().StateValues("IF-MIB::ifOperStatus")
	labels := map[int]string{}
	for _, state := range states {
		labels[state.Value] = state.Label
	}
	if labels[1] != "up" || labels[2] != "down" || labels[3] != "testing" {
		t.Fatalf("unexpected ifOperStatus enum: %v", labels)
	}
}
