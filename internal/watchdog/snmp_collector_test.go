package watchdog

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gosnmp/gosnmp"
)

func TestSNMPCollectorSchemaDefinesDeviceScopedRecipes(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "007_snmp_collector.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	sql := string(data)
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS snmp_collection_recipes",
		"id CHAR(26) PRIMARY KEY",
		"device_id CHAR(26) NOT NULL",
		"UNIQUE KEY uq_snmp_collection_recipe",
		"tenant_id, device_id, module_name, entity_type, entity_id, metric_name, oid_index, context_name",
		"KEY idx_snmp_collection_device_module (tenant_id, device_id, module_name)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("schema missing %q", want)
		}
	}
}

func TestSNMPCollectorDetectOSBySysObjectIDAndRegex(t *testing.T) {
	match, ok := DetectSNMPCollectorOS(SNMPCollectorOSFingerprint{
		SysObjectID: ".1.3.6.1.4.1.9.1.1208",
		SysDescr:    "Cisco IOS Software",
	}, []SNMPCollectorOSDefinition{
		{
			OSName: "generic",
			Definition: map[string]any{
				"sysDescr_regex": "Cisco",
			},
		},
		{
			OSName:  "ios",
			OSGroup: "cisco",
			Vendor:  "cisco",
			Class:   "network",
			Definition: map[string]any{
				"discovery": []any{
					map[string]any{"sysObjectID": []any{".1.3.6.1.4.1.9.1."}},
				},
			},
		},
	})
	if !ok {
		t.Fatal("expected OS match")
	}
	if match.OSName != "ios" || match.Vendor != "cisco" || match.Reason != "sysObjectID" {
		t.Fatalf("unexpected match: %#v", match)
	}
}

func TestSNMPCollectorDetectOSHonorsNegativeRules(t *testing.T) {
	_, ok := DetectSNMPCollectorOS(SNMPCollectorOSFingerprint{
		SysObjectID: ".1.3.6.1.4.1.2011.2.23",
		SysDescr:    "Huawei test device",
	}, []SNMPCollectorOSDefinition{
		{
			OSName: "vrp",
			Definition: map[string]any{
				"sysDescr_regex":        "Huawei",
				"sysObjectID_except":    ".1.3.6.1.4.1.2011.2.",
				"sysName_regex_except":  "ignored",
				"sysDescr_regex_except": "not-present",
			},
		},
	})
	if ok {
		t.Fatal("negative sysObjectID rule should reject match")
	}
}

func TestSNMPCollectorDetectOSRequiresEveryLibreNMSCondition(t *testing.T) {
	match, ok := DetectSNMPCollectorOS(SNMPCollectorOSFingerprint{
		SysObjectID: ".1.3.6.1.4.1.2636.1.1.1.2.25",
		SysDescr:    "Juniper Networks, Inc. mx480 internet router, kernel JUNOS 21.2R3-S8.5",
	}, []SNMPCollectorOSDefinition{
		{
			OSName: "edgeswitch",
			Vendor: "ubiquiti",
			Definition: map[string]any{"discovery": []any{map[string]any{
				"sysObjectID":    ".1.3.6.1.4.1",
				"sysDescr_regex": "/^EdgeSwitch/",
			}}},
		},
		{
			OSName: "junos",
			Vendor: "juniper",
			Definition: map[string]any{"discovery": []any{map[string]any{
				"sysObjectID": ".1.3.6.1.4.1.2636",
			}}},
		},
	})
	if !ok || match.OSName != "junos" || match.Vendor != "juniper" {
		t.Fatalf("unexpected match: %#v, ok=%v", match, ok)
	}
}

func TestSNMPCollectorDiscoveryCorrectsVendorAndJunosVersion(t *testing.T) {
	query := fakeSNMPCollectorQueryEngine{gets: map[string]SNMPCollectorResponse{
		"": {VarBinds: []SNMPCollectorVarBind{
			{OID: snmpOIDSysObjectID, Value: ".1.3.6.1.4.1.2636.1.1.1.2.25"},
			{OID: snmpOIDSysDescr, Value: "Juniper Networks, Inc. mx480 internet router, kernel JUNOS 21.2R3-S8.5, Build date"},
			{OID: snmpOIDSysName, Value: "core-mx480"},
			{OID: snmpOIDSysUpTime, Value: uint32(12300)},
		}},
	}}
	engine := SNMPDiscoveryEngine{
		Query: query,
		OSDefinitions: []SNMPCollectorOSDefinition{{
			OSName: "junos", Vendor: "juniper",
			Definition: map[string]any{"sysObjectID": ".1.3.6.1.4.1.2636"},
		}},
	}
	result, err := engine.Discover(context.Background(), SNMPDiscoveryEngineRequest{
		TenantID: "tenant-a", TargetID: "target-a", Target: SNMPCollectorTarget{Host: "192.0.2.1"},
		Device:  NetworkDevice{ID: "device-a", Vendor: "ubiquiti", OSName: "edgeswitch"},
		Profile: SNMPProfile{Version: SNMPVersion2c, Security: map[string]string{"community": "public"}},
	})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if got := result.DeviceUpdates; got.Vendor != "juniper" || got.OSName != "junos" || got.OSVersion != "21.2R3-S8.5" {
		t.Fatalf("device updates = %#v", got)
	}
}

func TestInterfaceAddressDiscoveryUsesIPMIBIndexesForBothFamilies(t *testing.T) {
	provider := snmpInterfaceAddressProvider{
		name: "test-ip-mib", ifIndex: ".1", prefix: ".2", origin: ".3",
	}
	query := fakeSNMPCollectorQueryEngine{walks: map[string]SNMPCollectorResponse{
		"1": {VarBinds: []SNMPCollectorVarBind{
			{OID: "1.1.4.192.0.2.10", Value: uint32(7)},
			{OID: "1.2.16.32.1.13.184.0.0.0.0.0.0.0.0.0.0.0.16", Value: uint32(8)},
		}},
		"2": {VarBinds: []SNMPCollectorVarBind{
			{OID: "2.1.4.192.0.2.10", Value: ".1.3.6.1.2.1.4.32.1.5.7.1.4.192.0.2.0.24"},
			{OID: "2.2.16.32.1.13.184.0.0.0.0.0.0.0.0.0.0.0.16", Value: ".1.3.6.1.2.1.4.32.1.5.8.2.16.32.1.13.184.0.0.0.0.0.0.0.0.0.0.0.0.64"},
		}},
		"3": {VarBinds: []SNMPCollectorVarBind{
			{OID: "3.1.4.192.0.2.10", Value: uint32(2)},
			{OID: "3.2.16.32.1.13.184.0.0.0.0.0.0.0.0.0.0.0.16", Value: uint32(5)},
		}},
	}}
	addresses := discoverSNMPInterfaceAddressesWithProvider(context.Background(), SNMPCollectorDiscoveryContext{
		TenantID: "tenant-a", Device: NetworkDevice{ID: "device-a"}, Query: query,
	}, provider)
	if len(addresses) != 2 {
		t.Fatalf("addresses = %#v", addresses)
	}
	byFamily := map[string]NetworkInterfaceAddress{}
	for _, address := range addresses {
		byFamily[address.Family] = address
	}
	if address := byFamily["ipv4"]; address.Address != "192.0.2.10" || address.PrefixLength != 24 || address.Origin != "manual" {
		t.Fatalf("unexpected IPv4 address: %#v", address)
	}
	if address := byFamily["ipv6"]; address.Address != "2001:db8::10" || address.PrefixLength != 64 || address.Origin != "linklayer" {
		t.Fatalf("unexpected IPv6 address: %#v", address)
	}
}

func TestBGPDiscoverySelectsConfiguredMIBProviderByTableCapability(t *testing.T) {
	provider := map[string]any{
		"name": "TEST-BGP-V2", "state": ".1", "local_as": ".2", "remote_as": ".3",
		"remote_address_type": ".4", "remote_address": ".5", "peer_index": ".6",
		"established_seconds": ".7", "prefix_afi": ".8", "prefix_safi": ".9",
		"accepted_prefixes": ".10", "denied_prefixes": ".11", "advertised_prefixes": ".12",
	}
	peerIndex := "1.2.16.32.1.13.184.0.0.0.0.0.0.0.0.0.0.0.1"
	query := fakeSNMPCollectorQueryEngine{walks: map[string]SNMPCollectorResponse{
		"1":  {VarBinds: []SNMPCollectorVarBind{{OID: "1." + peerIndex, Value: uint32(6)}}},
		"2":  {VarBinds: []SNMPCollectorVarBind{{OID: "2." + peerIndex, Value: uint32(64512)}}},
		"3":  {VarBinds: []SNMPCollectorVarBind{{OID: "3." + peerIndex, Value: uint32(64513)}}},
		"4":  {VarBinds: []SNMPCollectorVarBind{{OID: "4." + peerIndex, Value: uint32(2)}}},
		"5":  {VarBinds: []SNMPCollectorVarBind{{OID: "5." + peerIndex, Value: []byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}}}},
		"6":  {VarBinds: []SNMPCollectorVarBind{{OID: "6." + peerIndex, Value: uint32(42)}}},
		"7":  {VarBinds: []SNMPCollectorVarBind{{OID: "7." + peerIndex, Value: uint32(3600)}}},
		"8":  {VarBinds: []SNMPCollectorVarBind{{OID: "8.42.2.1", Value: uint32(2)}}},
		"9":  {VarBinds: []SNMPCollectorVarBind{{OID: "9.42.2.1", Value: uint32(1)}}},
		"10": {VarBinds: []SNMPCollectorVarBind{{OID: "10.42.2.1", Value: uint32(123)}}},
		"11": {VarBinds: []SNMPCollectorVarBind{{OID: "11.42.2.1", Value: uint32(4)}}},
		"12": {VarBinds: []SNMPCollectorVarBind{{OID: "12.42.2.1", Value: uint32(55)}}},
	}}
	result, err := (SNMPBGPDiscoveryModule{}).Discover(context.Background(), SNMPCollectorDiscoveryContext{
		TenantID: "tenant-a", TargetID: "target-a", Device: NetworkDevice{ID: "device-a"}, Query: query,
		OS:         SNMPCollectorOSMatch{OSName: "deliberately-unrelated"},
		Definition: SNMPCollectorModuleDefinition{Definition: map[string]any{"providers": []any{provider}}},
	})
	if err != nil || len(result.BGPSessions) != 1 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	session := result.BGPSessions[0]
	if session.PeerAddr != "2001:db8::1" || session.AFI != "ipv6" || session.SAFI != "unicast" || session.AcceptedPrefixes != 123 || session.PeerAS != 64513 {
		t.Fatalf("unexpected session: %#v", session)
	}
	if session.Metadata["source"] != "TEST-BGP-V2" {
		t.Fatalf("source = %#v", session.Metadata)
	}
}

func TestSNMPCollectorSensorIdentityIncludesOID(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "015_network_sensor_identity.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	sql := string(data)
	for _, want := range []string{"DROP INDEX uq_network_device_sensors_index", "sensor_class", "sensor_index", "oid(191)"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("schema missing %q", want)
		}
	}
}

func TestSNMPCollectorDiscoveryEngineRunsCoreOSAndModules(t *testing.T) {
	query := fakeSNMPCollectorQueryEngine{
		gets: map[string]SNMPCollectorResponse{
			"": {
				VarBinds: []SNMPCollectorVarBind{
					{OID: snmpOIDSysObjectID, Value: ".1.3.6.1.4.1.9.1.1208"},
					{OID: snmpOIDSysDescr, Value: "Cisco IOS Software"},
					{OID: snmpOIDSysName, Value: "sw1"},
					{OID: snmpOIDSysUpTime, Value: uint32(12300)},
				},
			},
		},
	}
	module := &fakeSNMPDiscoveryModule{}
	engine := SNMPDiscoveryEngine{
		Query: query,
		OSDefinitions: []SNMPCollectorOSDefinition{{
			OSName: "ios",
			Vendor: "cisco",
			Definition: map[string]any{
				"sysObjectID": ".1.3.6.1.4.1.9.1.",
			},
		}},
		Modules: []SNMPCollectorDiscoveryModule{module},
	}
	result, err := engine.Discover(context.Background(), SNMPDiscoveryEngineRequest{
		TenantID: "tenant-a",
		TargetID: "target-a",
		Target:   SNMPCollectorTarget{Host: "192.0.2.1"},
		Device:   NetworkDevice{ID: "device-a"},
		Profile:  SNMPProfile{Version: SNMPVersion2c, Security: map[string]string{"community": "public"}},
	})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if result.DeviceUpdates.OSName != "ios" || result.DeviceUpdates.Vendor != "cisco" || result.DeviceUpdates.SysName != "sw1" {
		t.Fatalf("unexpected device updates: %#v", result.DeviceUpdates)
	}
	if module.seenOS != "ios" {
		t.Fatalf("module saw OS %q, want ios", module.seenOS)
	}
	if len(result.Recipes) != 1 || result.Recipes[0].DeviceID != "device-a" {
		t.Fatalf("unexpected recipes: %#v", result.Recipes)
	}
}

func TestSNMPCollectorPortsDiscoveryCreatesRawDeviceScopedRecipes(t *testing.T) {
	query := fakeSNMPCollectorQueryEngine{
		walks: map[string]SNMPCollectorResponse{
			snmpOIDIfDescr: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDIfDescr + ".101", Value: "Ethernet1/1"}},
			},
			snmpOIDIfName: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDIfName + ".101", Value: "Eth1/1"}},
			},
			snmpOIDIfAlias: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDIfAlias + ".101", Value: "customer-a"}},
			},
			snmpOIDIfType: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDIfType + ".101", Value: "6"}},
			},
			snmpOIDIfSpeed: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDIfSpeed + ".101", Value: "1000000000"}},
			},
			snmpOIDIfHighSpeed: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDIfHighSpeed + ".101", Value: "1000"}},
			},
			snmpOIDIfAdminStatus: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDIfAdminStatus + ".101", Value: "1"}},
			},
			snmpOIDIfOperStatus: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDIfOperStatus + ".101", Value: "1"}},
			},
			snmpOIDConnectorPresent: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDConnectorPresent + ".101", Value: "1"}},
			},
			snmpOIDIfHCInOctets: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDIfHCInOctets + ".101", Value: uint64(1)}},
			},
			snmpOIDIfHCOutOctets: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDIfHCOutOctets + ".101", Value: uint64(2)}},
			},
		},
	}
	module := SNMPPortsDiscoveryModule{}
	result, err := module.Discover(context.Background(), SNMPCollectorDiscoveryContext{
		TenantID: "tenant-a",
		TargetID: "target-a",
		Target:   SNMPCollectorTarget{Host: "192.0.2.1"},
		Device:   NetworkDevice{ID: "device-a", TenantID: "tenant-a"},
		Profile:  SNMPProfile{Version: SNMPVersion2c, Security: map[string]string{"community": "public"}},
		Query:    query,
	})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(result.Ports) != 1 {
		t.Fatalf("ports = %d, want 1", len(result.Ports))
	}
	if result.Ports[0].DeviceID != "device-a" || result.Ports[0].IfIndex != 101 || result.Ports[0].IfName != "Eth1/1" {
		t.Fatalf("unexpected port: %#v", result.Ports[0])
	}
	if len(result.Recipes) != 8 {
		t.Fatalf("recipes = %d, want 8", len(result.Recipes))
	}
	for _, recipe := range result.Recipes {
		if recipe.DeviceID != "device-a" || recipe.ID == "" || recipe.EntityID != result.Ports[0].ID {
			t.Fatalf("recipe lost device/entity identity: %#v", recipe)
		}
		if strings.Contains(recipe.MetricName, "_bps") {
			t.Fatalf("ports discovery created derived bps recipe: %#v", recipe)
		}
	}
}

func TestSNMPCollectorSensorsDiscoveryCreatesScaledRawRecipes(t *testing.T) {
	query := fakeSNMPCollectorQueryEngine{
		walks: map[string]SNMPCollectorResponse{
			oidEntPhySensorType: {
				VarBinds: []SNMPCollectorVarBind{{OID: oidEntPhySensorType + ".501", Value: "8"}},
			},
			oidEntPhySensorScale: {
				VarBinds: []SNMPCollectorVarBind{{OID: oidEntPhySensorScale + ".501", Value: "8"}},
			},
			oidEntPhySensorPrecision: {
				VarBinds: []SNMPCollectorVarBind{{OID: oidEntPhySensorPrecision + ".501", Value: "1"}},
			},
			oidEntPhySensorValue: {
				VarBinds: []SNMPCollectorVarBind{{OID: oidEntPhySensorValue + ".501", Value: "38500"}},
			},
			oidEntPhySensorOper: {
				VarBinds: []SNMPCollectorVarBind{{OID: oidEntPhySensorOper + ".501", Value: "1"}},
			},
			oidEntPhySensorUnits: {
				VarBinds: []SNMPCollectorVarBind{{OID: oidEntPhySensorUnits + ".501", Value: "C"}},
			},
			oidEntPhysicalName: {
				VarBinds: []SNMPCollectorVarBind{{OID: oidEntPhysicalName + ".501", Value: "Temp sensor"}},
			},
			oidEntPhysicalDescr: {
				VarBinds: []SNMPCollectorVarBind{{OID: oidEntPhysicalDescr + ".501", Value: "Linecard temperature"}},
			},
		},
	}
	module := SNMPSensorsDiscoveryModule{}
	result, err := module.Discover(context.Background(), SNMPCollectorDiscoveryContext{
		TenantID: "tenant-a",
		TargetID: "target-a",
		Target:   SNMPCollectorTarget{Host: "192.0.2.1"},
		Device:   NetworkDevice{ID: "device-a", TenantID: "tenant-a"},
		Profile:  SNMPProfile{Version: SNMPVersion2c, Security: map[string]string{"community": "public"}},
		Query:    query,
	})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(result.Sensors) != 1 {
		t.Fatalf("sensors = %d, want 1", len(result.Sensors))
	}
	sensor := result.Sensors[0]
	if sensor.DeviceID != "device-a" || sensor.SensorIndex != 501 || sensor.Class != "temperature" || sensor.Unit != "C" || sensor.Status != "ok" {
		t.Fatalf("unexpected sensor: %#v", sensor)
	}
	if len(result.Recipes) != 2 {
		t.Fatalf("recipes = %d, want 2", len(result.Recipes))
	}
	var valueRecipe SNMPCollectionRecipe
	for _, recipe := range result.Recipes {
		if recipe.DeviceID != "device-a" || recipe.EntityType != SNMPCollectorEntitySensor || recipe.EntityID != sensor.ID {
			t.Fatalf("recipe lost device/entity identity: %#v", recipe)
		}
		if strings.Contains(recipe.MetricName, "_bps") {
			t.Fatalf("sensors discovery created derived bps recipe: %#v", recipe)
		}
		if recipe.MetricName == MetricSNMPSensorValue {
			valueRecipe = recipe
		}
	}
	if valueRecipe.ID == "" || !valueRecipe.HasMultiplier || valueRecipe.Multiplier != 0.0001 {
		t.Fatalf("value recipe missing scale metadata: %#v", valueRecipe)
	}
}

func TestSNMPCollectorBGPDiscoveryCreatesContextScopedRecipes(t *testing.T) {
	getRequests := []SNMPCollectorGetRequest{}
	walkRequests := []SNMPCollectorWalkRequest{}
	query := fakeSNMPCollectorQueryEngine{
		getRequests:  &getRequests,
		walkRequests: &walkRequests,
		gets: map[string]SNMPCollectorResponse{
			"vrf-a": {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDBGPLocalAS, Value: "64501"}},
			},
		},
		walks: map[string]SNMPCollectorResponse{
			snmpOIDBGPPeerState: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDBGPPeerState + ".192.0.2.2", Value: "6"}},
			},
			snmpOIDBGPPeerRemoteAddr: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDBGPPeerRemoteAddr + ".192.0.2.2", Value: "192.0.2.2"}},
			},
			snmpOIDBGPPeerRemoteAS: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDBGPPeerRemoteAS + ".192.0.2.2", Value: "64500"}},
			},
			snmpOIDBGPPeerInUpdates: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDBGPPeerInUpdates + ".192.0.2.2", Value: "10"}},
			},
			snmpOIDBGPPeerOutUpdates: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDBGPPeerOutUpdates + ".192.0.2.2", Value: "11"}},
			},
			snmpOIDBGPPeerInTotalMessages: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDBGPPeerInTotalMessages + ".192.0.2.2", Value: "12"}},
			},
			snmpOIDBGPPeerOutTotalMessages: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDBGPPeerOutTotalMessages + ".192.0.2.2", Value: "13"}},
			},
			snmpOIDBGPPeerEstablishedSeconds: {
				VarBinds: []SNMPCollectorVarBind{{OID: snmpOIDBGPPeerEstablishedSeconds + ".192.0.2.2", Value: "3600"}},
			},
		},
	}
	module := SNMPBGPDiscoveryModule{}
	result, err := module.Discover(context.Background(), SNMPCollectorDiscoveryContext{
		TenantID: "tenant-a",
		TargetID: "target-a",
		Target:   SNMPCollectorTarget{Host: "192.0.2.1"},
		Device:   NetworkDevice{ID: "device-a", TenantID: "tenant-a"},
		Profile:  SNMPProfile{Version: SNMPVersion2c, Security: map[string]string{"community": "public"}},
		Query:    query,
		Definition: SNMPCollectorModuleDefinition{Definition: map[string]any{
			"context_name": "vrf-a",
		}},
	})
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(result.BGPSessions) != 1 {
		t.Fatalf("sessions = %d, want 1", len(result.BGPSessions))
	}
	session := result.BGPSessions[0]
	if session.DeviceID != "device-a" || session.PeerAddr != "192.0.2.2" || session.PeerAS != 64500 || session.LocalAS != 64501 || session.State != "established" {
		t.Fatalf("unexpected session: %#v", session)
	}
	if len(result.Recipes) != 6 {
		t.Fatalf("recipes = %d, want 6", len(result.Recipes))
	}
	for _, recipe := range result.Recipes {
		if recipe.DeviceID != "device-a" || recipe.EntityType != SNMPCollectorEntityBGPPeer || recipe.EntityID != session.ID {
			t.Fatalf("recipe lost device/entity identity: %#v", recipe)
		}
		if recipe.ContextName != "vrf-a" {
			t.Fatalf("recipe context = %q, want vrf-a: %#v", recipe.ContextName, recipe)
		}
		if strings.Contains(recipe.MetricName, "_bps") {
			t.Fatalf("bgp discovery created derived bps recipe: %#v", recipe)
		}
	}
	if len(getRequests) != 1 || getRequests[0].Context != "vrf-a" {
		t.Fatalf("GET context not used: %#v", getRequests)
	}
	for _, request := range walkRequests {
		if request.Context != "vrf-a" {
			t.Fatalf("walk context not used: %#v", walkRequests)
		}
	}
}

func TestSNMPCollectorRawSampleCarriesDeviceAndRecipeIdentity(t *testing.T) {
	sample := SNMPRawSample{
		TenantID:   "tenant-a",
		DeviceID:   "device-a",
		EntityType: SNMPCollectorEntityPort,
		EntityID:   "port-a",
		RecipeID:   "recipe-a",
		MetricName: "watchdog_snmp_if_in_octets_total",
		ValueType:  SNMPCollectorValueCounter64,
	}
	if sample.TenantID == "" || sample.DeviceID == "" || sample.RecipeID == "" {
		t.Fatalf("raw sample lost required series identity: %#v", sample)
	}
}

func TestSNMPCollectorPollerWritesRawSamplesOnly(t *testing.T) {
	getRequests := []SNMPCollectorGetRequest{}
	query := fakeSNMPCollectorQueryEngine{
		getRequests: &getRequests,
		gets: map[string]SNMPCollectorResponse{
			"": {
				VarBinds: []SNMPCollectorVarBind{
					{OID: "1.3.6.1.2.1.31.1.1.1.6.101", Value: uint64(1000), ValueType: SNMPCollectorValueCounter64},
				},
			},
		},
	}
	writer := &fakeSNMPRawSampleWriter{}
	poller := SNMPPoller{Query: query, Writer: writer, MaxOids: 17}
	recipe := SNMPCollectionRecipe{
		ID:                    "recipe-a",
		TenantID:              "tenant-a",
		DeviceID:              "device-a",
		EntityType:            SNMPCollectorEntityPort,
		EntityID:              "port-a",
		ModuleName:            snmpCollectorModulePorts,
		MetricName:            MetricSNMPIfInOctetsTotal,
		ValueType:             SNMPCollectorValueCounter64,
		NumericOID:            "1.3.6.1.2.1.31.1.1.1.6.101",
		SampleIntervalSeconds: 60,
		Labels: map[string]string{
			"port_id": "port-a",
		},
		Enabled: true,
	}
	result, err := poller.Poll(context.Background(), SNMPPollJob{
		TenantID: "tenant-a",
		TargetID: "target-a",
		DeviceID: "device-a",
		Target:   SNMPCollectorTarget{Host: "192.0.2.1"},
		Profile:  SNMPProfile{Version: SNMPVersion2c, Security: map[string]string{"community": "public"}},
		Recipes:  []SNMPCollectionRecipe{recipe},
	})
	if err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if result.SampleCount != 1 {
		t.Fatalf("sample count = %d, want 1", result.SampleCount)
	}
	if len(getRequests) != 1 {
		t.Fatalf("GET requests = %d, want 1", len(getRequests))
	}
	if getRequests[0].Flags.MaxOids != 17 {
		t.Fatalf("MaxOids = %d, want 17", getRequests[0].Flags.MaxOids)
	}
	if len(writer.samples) != 1 {
		t.Fatalf("written samples = %d, want 1", len(writer.samples))
	}
	sample := writer.samples[0]
	if sample.DeviceID != "device-a" || sample.RecipeID != "recipe-a" || sample.Labels["device_id"] != "device-a" || sample.Labels["recipe_id"] != "recipe-a" {
		t.Fatalf("sample lost device/recipe identity: %#v", sample)
	}
	if sample.FloatValue != 1000 {
		t.Fatalf("sample value = %v, want 1000", sample.FloatValue)
	}
	if strings.Contains(sample.MetricName, "_bps") {
		t.Fatalf("poller wrote derived bps metric: %#v", sample)
	}
}

func TestSNMPCollectorRawWriterRendersDeviceScopedLabels(t *testing.T) {
	payload, err := RenderSNMPRawSamplesPrometheus([]SNMPRawSample{{
		TenantID:   "tenant-a",
		TargetID:   "target-a",
		DeviceID:   "device-a",
		EntityType: SNMPCollectorEntityPort,
		EntityID:   "port-a",
		RecipeID:   "recipe-a",
		MetricName: MetricSNMPIfInOctetsTotal,
		ValueType:  SNMPCollectorValueCounter64,
		FloatValue: 42,
		Labels: map[string]string{
			"module":      snmpCollectorModulePorts,
			"port_id":     "port-a",
			"if_index":    "101",
			"numeric_oid": "1.2.3",
		},
	}})
	if err != nil {
		t.Fatalf("RenderSNMPRawSamplesPrometheus() error = %v", err)
	}
	text := string(payload)
	for _, want := range []string{
		MetricSNMPIfInOctetsTotal,
		`tenant_id="tenant-a"`,
		`target_id="target-a"`,
		`device_id="device-a"`,
		`recipe_id="recipe-a"`,
		`entity_type="port"`,
		`entity_id="port-a"`,
		`module="ports"`,
		`port_id="port-a"`,
		`if_index="101"`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("payload missing %s: %s", want, text)
		}
	}
	if strings.Contains(text, "numeric_oid") {
		t.Fatalf("payload leaked forbidden label: %s", text)
	}
}

func TestSNMPCollectorRawWriterRejectsDerivedBps(t *testing.T) {
	_, err := RenderSNMPRawSamplesPrometheus([]SNMPRawSample{{
		TenantID:   "tenant-a",
		DeviceID:   "device-a",
		EntityType: SNMPCollectorEntityPort,
		EntityID:   "port-a",
		RecipeID:   "recipe-a",
		MetricName: MetricSNMPIfInBps,
		ValueType:  SNMPCollectorValueGauge,
		FloatValue: 1,
	}})
	if err == nil {
		t.Fatal("expected derived bps metric to be rejected")
	}
}

func TestSNMPCollectorTrapLinkDownUpdatesPort(t *testing.T) {
	device := NetworkDevice{ID: "device-a", TenantID: "tenant-a"}
	port := NetworkPort{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 101, IfName: "Eth1/1", OperStatus: "up"}
	dispatcher := NewSNMPTrapDispatcher(DefaultSNMPTrapHandlers(
		map[uint64]NetworkPort{101: port},
		map[ID][]ID{"port-a": {"recipe-a"}},
		nil,
		nil,
	))
	result, err := dispatcher.Dispatch(context.Background(), device, SNMPTrap{
		TrapOID: SNMPTrapOIDLinkDown,
		VarBinds: []SNMPTrapVarBind{
			{OID: "IF-MIB::ifIndex.101", Value: "101"},
		},
	})
	if err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	if len(result.PortUpdates) != 1 || result.PortUpdates[0].OperStatus != "down" {
		t.Fatalf("unexpected port updates: %#v", result.PortUpdates)
	}
	if len(result.ImmediatePollRecipe) != 1 || result.ImmediatePollRecipe[0] != "recipe-a" {
		t.Fatalf("unexpected immediate poll recipes: %#v", result.ImmediatePollRecipe)
	}
	if len(result.Events) != 1 || result.Events[0].DeviceID != "device-a" || result.Events[0].EntityID != "port-a" {
		t.Fatalf("unexpected events: %#v", result.Events)
	}
}

func TestSNMPCollectorTrapUnknownInterfaceRequestsRediscovery(t *testing.T) {
	device := NetworkDevice{ID: "device-a", TenantID: "tenant-a"}
	dispatcher := NewSNMPTrapDispatcher(DefaultSNMPTrapHandlers(nil, nil, nil, nil))
	result, err := dispatcher.Dispatch(context.Background(), device, SNMPTrap{
		TrapOID: SNMPTrapOIDLinkDown,
		VarBinds: []SNMPTrapVarBind{
			{OID: "IF-MIB::ifIndex.999", Value: "999"},
		},
	})
	if err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	if !result.RediscoverDevice {
		t.Fatalf("expected rediscovery request: %#v", result)
	}
}

func TestSNMPCollectorTrapUnhandledCreatesEvent(t *testing.T) {
	device := NetworkDevice{ID: "device-a", TenantID: "tenant-a"}
	dispatcher := NewSNMPTrapDispatcher(nil)
	result, err := dispatcher.Dispatch(context.Background(), device, SNMPTrap{TrapOID: "1.2.3.4"})
	if err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	if len(result.Events) != 1 || result.Events[0].EventType != "unhandled_trap" {
		t.Fatalf("unexpected unhandled event: %#v", result.Events)
	}
}

func TestSNMPCollectorTrapBGPBackwardTransitionUpdatesPeer(t *testing.T) {
	device := NetworkDevice{ID: "device-a", TenantID: "tenant-a"}
	session := BGPSession{ID: "bgp-a", TenantID: "tenant-a", DeviceID: "device-a", PeerAddr: "192.0.2.2", State: "established"}
	dispatcher := NewSNMPTrapDispatcher(DefaultSNMPTrapHandlers(
		nil,
		nil,
		map[string]BGPSession{"192.0.2.2": session},
		map[ID][]ID{"bgp-a": {"recipe-bgp"}},
	))
	result, err := dispatcher.Dispatch(context.Background(), device, SNMPTrap{
		TrapOID: SNMPTrapOIDBGPBackwardTransition,
		VarBinds: []SNMPTrapVarBind{
			{OID: "BGP4-MIB::bgpPeerRemoteAddr", Value: "192.0.2.2"},
		},
	})
	if err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	if len(result.BGPUpdates) != 1 || result.BGPUpdates[0].State != "idle" {
		t.Fatalf("unexpected bgp updates: %#v", result.BGPUpdates)
	}
	if len(result.ImmediatePollRecipe) != 1 || result.ImmediatePollRecipe[0] != "recipe-bgp" {
		t.Fatalf("unexpected immediate poll recipes: %#v", result.ImmediatePollRecipe)
	}
}

func TestSNMPCollectorTrafficRateProjectionUsesRawOctets(t *testing.T) {
	query, err := SNMPTrafficRateQuery(SNMPTrafficRateProjectionRequest{
		TenantID:  "tenant-a",
		DeviceID:  "device-a",
		EntityID:  "port-a",
		RecipeID:  "recipe-a",
		Direction: SNMPTrafficIn,
		Window:    "2m",
	})
	if err != nil {
		t.Fatalf("SNMPTrafficRateQuery() error = %v", err)
	}
	for _, want := range []string{
		"rate(" + MetricSNMPIfInOctetsTotal,
		`tenant_id="tenant-a"`,
		`device_id="device-a"`,
		`entity_type="port"`,
		`entity_id="port-a"`,
		`recipe_id="recipe-a"`,
		"[2m]) * 8",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("query missing %s: %s", want, query)
		}
	}
	if strings.Contains(query, MetricSNMPIfInBps) || strings.Contains(query, MetricSNMPIfOutBps) {
		t.Fatalf("projection used derived metric: %s", query)
	}
}

func TestSNMPCollectorTrafficRateProjectionRequiresDeviceScope(t *testing.T) {
	if _, err := SNMPTrafficRateQuery(SNMPTrafficRateProjectionRequest{TenantID: "tenant-a", Direction: SNMPTrafficIn}); err == nil {
		t.Fatal("expected device_id validation error")
	}
	if _, err := SNMPTrafficRateQuery(SNMPTrafficRateProjectionRequest{DeviceID: "device-a", Direction: SNMPTrafficIn}); err == nil {
		t.Fatal("expected tenant_id validation error")
	}
}

func TestSNMPCollectorV3FlagsDeriveFromProtocols(t *testing.T) {
	if got := snmpV3MsgFlags("", gosnmp.NoAuth, gosnmp.NoPriv); got != gosnmp.NoAuthNoPriv {
		t.Fatalf("no auth/no priv flags = %v", got)
	}
	if got := snmpV3MsgFlags("", gosnmp.SHA256, gosnmp.NoPriv); got != gosnmp.AuthNoPriv {
		t.Fatalf("auth/no priv flags = %v", got)
	}
	if got := snmpV3MsgFlags("", gosnmp.SHA256, gosnmp.AES); got != gosnmp.AuthPriv {
		t.Fatalf("auth/priv flags = %v", got)
	}
	if got := snmpV3MsgFlags("noAuthNoPriv", gosnmp.SHA256, gosnmp.AES); got != gosnmp.NoAuthNoPriv {
		t.Fatalf("explicit security level should win, got %v", got)
	}
}

func TestSNMPCollectorPDUValueTypes(t *testing.T) {
	tests := []struct {
		name string
		pdu  gosnmp.SnmpPDU
		want SNMPCollectorValueType
	}{
		{name: "counter32", pdu: gosnmp.SnmpPDU{Type: gosnmp.Counter32}, want: SNMPCollectorValueCounter32},
		{name: "counter64", pdu: gosnmp.SnmpPDU{Type: gosnmp.Counter64}, want: SNMPCollectorValueCounter64},
		{name: "gauge", pdu: gosnmp.SnmpPDU{Type: gosnmp.Gauge32}, want: SNMPCollectorValueGauge},
		{name: "state", pdu: gosnmp.SnmpPDU{Type: gosnmp.Integer}, want: SNMPCollectorValueGauge},
		{name: "timeticks", pdu: gosnmp.SnmpPDU{Type: gosnmp.TimeTicks}, want: SNMPCollectorValueTimeTicks},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := snmpCollectorValueTypeFromPDU(tt.pdu); got != tt.want {
				t.Fatalf("value type = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSNMPCollectorDefinitionImporterWritesCollectorDefinitions(t *testing.T) {
	repository := &fakeSNMPCollectorRepository{}
	importer := SNMPDefinitionImporter{Repository: repository}
	report, err := importer.Import(context.Background(), SNMPDefinitionImport{
		OSDefinitions: []SNMPCollectorOSDefinition{{
			OSName: "ios",
			Vendor: "cisco",
			Definition: map[string]any{
				"sysObjectID": ".1.3.6.1.4.1.9.1.",
			},
			Source: "librenms-os",
		}},
		ModuleDefinitions: []SNMPCollectorModuleDefinition{{
			ModuleName: "ports",
			ModuleType: SNMPCollectorModuleDiscovery,
			Definition: map[string]any{
				"mib": "IF-MIB",
			},
			Source: "librenms-discovery",
		}},
		StateTranslations: []SNMPStateTranslation{{
			Name:   "ifOperStatus",
			Source: "librenms-state",
			States: []SNMPStateValue{
				{Value: 1, Generic: 0, Label: "up"},
				{Value: 2, Generic: 2, Label: "down"},
			},
		}},
		TrapHandlers: []SNMPTrapHandlerDefinition{{
			TrapOID:    SNMPTrapOIDLinkDown,
			HandlerKey: "linkDown",
			Enabled:    true,
		}},
	})
	if err != nil {
		t.Fatalf("Import() error = %v", err)
	}
	if report.OSDefinitions != 1 || report.ModuleDefinitions != 1 || report.StateTranslations != 1 || report.TrapHandlers != 1 {
		t.Fatalf("unexpected import report: %#v", report)
	}
	if len(repository.osDefinitions) != 1 || len(repository.moduleDefinitions) != 1 || len(repository.stateTranslations) != 1 || len(repository.trapHandlers) != 1 {
		t.Fatalf("repository writes did not match report: %#v", repository)
	}
	if repository.moduleDefinitions[0].ModuleName != "ports" || repository.trapHandlers[0].TrapOID != SNMPTrapOIDLinkDown {
		t.Fatalf("unexpected imported records: %#v %#v", repository.moduleDefinitions[0], repository.trapHandlers[0])
	}
}

func TestSNMPCollectorDiscoveryImporterPersistsAssetsRecipesAndEvents(t *testing.T) {
	network := &fakeNetworkRepository{}
	collector := &fakeSNMPCollectorRepository{}
	eventTime := time.Unix(100, 0).UTC()
	report, err := ImportSNMPCollectorDiscoveryResult(context.Background(), network, collector, "tenant-a", NetworkDevice{
		ID:       "device-a",
		TargetID: "target-a",
	}, SNMPCollectorDiscoveryResult{
		DeviceUpdates: NetworkDevice{
			SysName:     "sw1",
			SysDescr:    "Cisco IOS",
			SysObjectID: ".1.3.6.1.4.1.9.1.1208",
			Vendor:      "cisco",
			OSName:      "ios",
		},
		Ports: []NetworkPort{{
			ID:      "port-a",
			IfIndex: 101,
			IfName:  "Eth1/1",
		}},
		InterfaceAddresses: []NetworkInterfaceAddress{{
			ID: "address-a", PortID: "port-a", IfIndex: 101, Address: "2001:db8::10", Family: "ipv6", PrefixLength: 64,
		}},
		Sensors: []NetworkDeviceSensor{{
			ID:          "sensor-a",
			SensorIndex: 501,
			Class:       "temperature",
			Name:        "Temp sensor",
		}},
		PhysicalEntities: []PhysicalEntity{{
			Index: 1,
			Name:  "Chassis",
		}},
		BGPSessions: []BGPSession{{
			ID:       "bgp-a",
			PeerAddr: "192.0.2.2",
			PeerAS:   64500,
			AFI:      "ipv4",
			SAFI:     "unicast",
		}},
		VLANs: []DeviceVLAN{{
			VLANID: 10,
			Name:   "users",
		}},
		LAGs: []DeviceLAGGroup{{
			AggregateIndex: 200,
			Mode:           "dynamic",
		}},
		Recipes: []SNMPCollectionRecipe{{
			EntityType: SNMPCollectorEntityPort,
			EntityID:   "port-a",
			ModuleName: snmpCollectorModulePorts,
			MetricName: MetricSNMPIfInOctetsTotal,
			ValueType:  SNMPCollectorValueCounter64,
			NumericOID: snmpOIDIfHCInOctets + ".101",
			OIDIndex:   "101",
		}},
		Events: []SNMPEvent{{
			Source:     "trap",
			EventType:  "link_down",
			Message:    "Eth1/1 down",
			OccurredAt: eventTime,
		}},
	})
	if err != nil {
		t.Fatalf("ImportSNMPCollectorDiscoveryResult() error = %v", err)
	}
	if report.Device.ID != "device-a" || report.Device.TenantID != "tenant-a" || report.Device.SysName != "sw1" {
		t.Fatalf("unexpected device report: %#v", report.Device)
	}
	if report.Ports != 1 || report.InterfaceAddresses != 1 || report.Sensors != 1 || report.PhysicalEntities != 1 || report.BGPSessions != 1 || report.VLANs != 1 || report.LAGs != 1 || report.Recipes != 1 || report.Events != 1 || report.DeviceModules != 1 {
		t.Fatalf("unexpected import report: %#v", report)
	}
	if len(network.devices) != 1 || len(network.ports) != 1 || len(network.addresses) != 1 || len(network.sensors) != 1 || len(network.physical) != 1 || len(network.bgp) != 1 || len(network.vlans) != 1 || len(network.lags) != 1 {
		t.Fatalf("network writes missing: %#v", network)
	}
	if len(collector.recipes) != 1 || collector.recipes[0].TenantID != "tenant-a" || collector.recipes[0].DeviceID != "device-a" || collector.recipes[0].ID == "" || !collector.recipes[0].Enabled {
		t.Fatalf("recipe write missing identity: %#v", collector.recipes)
	}
	if len(collector.deviceModules) != 1 || collector.deviceModules[0].ModuleName != snmpCollectorModulePorts || collector.deviceModules[0].DiscoveryStatus != SNMPCollectorModuleOK {
		t.Fatalf("device module write missing: %#v", collector.deviceModules)
	}
	if len(collector.events) != 1 || collector.events[0].TenantID != "tenant-a" || collector.events[0].DeviceID != "device-a" || collector.events[0].OccurredAt != eventTime {
		t.Fatalf("event write missing identity: %#v", collector.events)
	}
}

func TestSNMPCollectorDiscoveryImporterClearsCompletedEmptySnapshots(t *testing.T) {
	network := &fakeNetworkRepository{
		addresses: []NetworkInterfaceAddress{{ID: "old-address", TenantID: "tenant-a", DeviceID: "device-a", PortID: "port-a"}},
		bgp:       []BGPSession{{ID: "old-bgp", TenantID: "tenant-a", DeviceID: "device-a", PeerAddr: "192.0.2.1"}},
	}
	_, err := ImportSNMPCollectorDiscoveryResult(context.Background(), network, &fakeSNMPCollectorRepository{}, "tenant-a", NetworkDevice{
		ID: "device-a", TargetID: "target-a",
	}, SNMPCollectorDiscoveryResult{CompletedModules: []string{snmpCollectorModulePorts, snmpCollectorModuleBGP}})
	if err != nil {
		t.Fatalf("ImportSNMPCollectorDiscoveryResult() error = %v", err)
	}
	if len(network.addresses) != 0 || len(network.bgp) != 0 {
		t.Fatalf("completed empty snapshots were not cleared: addresses=%#v bgp=%#v", network.addresses, network.bgp)
	}
}

func TestSNMPCollectorRegistryBuildsDiscoveryEngineFromDefinitions(t *testing.T) {
	repository := &fakeSNMPCollectorRepository{
		osDefinitions: []SNMPCollectorOSDefinition{{
			OSName: "ios",
			Vendor: "cisco",
		}},
		moduleDefinitions: []SNMPCollectorModuleDefinition{
			{
				ModuleName: snmpCollectorModulePorts,
				ModuleType: SNMPCollectorModuleDiscovery,
			},
			{
				ModuleName: snmpCollectorModuleBGP,
				ModuleType: SNMPCollectorModuleDiscovery,
				Definition: map[string]any{"context_name": "vrf-a"},
			},
		},
	}
	engine, err := NewSNMPDiscoveryEngineFromRepository(context.Background(), repository, fakeSNMPCollectorQueryEngine{}, DefaultSNMPCollectorModuleRegistry())
	if err != nil {
		t.Fatalf("NewSNMPDiscoveryEngineFromRepository() error = %v", err)
	}
	if len(engine.OSDefinitions) != 1 || engine.OSDefinitions[0].OSName != "ios" {
		t.Fatalf("unexpected OS definitions: %#v", engine.OSDefinitions)
	}
	if len(engine.Modules) != 2 || engine.Modules[0].Name() != snmpCollectorModulePorts || engine.Modules[1].Name() != snmpCollectorModuleBGP {
		t.Fatalf("unexpected modules: %#v", engine.Modules)
	}
	if engine.ModuleDefinitions[snmpCollectorModuleBGP].Definition["context_name"] != "vrf-a" {
		t.Fatalf("module definition not attached: %#v", engine.ModuleDefinitions)
	}
	if _, ok := DefaultSNMPCollectorModuleRegistry().DiscoveryModule("unknown-vendor-metric"); ok {
		t.Fatal("registry returned an unknown module")
	}
}

func TestSNMPCollectorPollRunnerExecutesDueRecipesByDevice(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	getRequests := []SNMPCollectorGetRequest{}
	writer := &fakeSNMPRawSampleWriter{}
	collector := &fakeSNMPCollectorRepository{
		recipes: []SNMPCollectionRecipe{{
			ID:         "recipe-a",
			TenantID:   "tenant-a",
			DeviceID:   "device-a",
			EntityType: SNMPCollectorEntityPort,
			EntityID:   "port-a",
			ModuleName: snmpCollectorModulePorts,
			MetricName: MetricSNMPIfInOctetsTotal,
			ValueType:  SNMPCollectorValueCounter64,
			NumericOID: snmpOIDIfHCInOctets + ".101",
			OIDIndex:   "101",
			Enabled:    true,
		}},
	}
	runner := SNMPPollRunner{
		Collector: collector,
		Network: &fakeNetworkRepository{devices: []NetworkDevice{{
			ID:            "device-a",
			TenantID:      "tenant-a",
			TargetID:      "target-a",
			SNMPProfileID: "profile-a",
			SNMPPort:      1161,
			SNMPSecurity:  map[string]string{"community": "private-a"},
		}}},
		Targets: &fakeTargetRepository{targets: []Target{{
			ID:       "target-a",
			TenantID: "tenant-a",
			Host:     "192.0.2.1",
		}}},
		SNMP: &fakeSNMPRepository{profiles: []SNMPProfile{{
			ID:       "profile-a",
			TenantID: "tenant-a",
			Version:  SNMPVersion2c,
			Security: map[string]string{
				"community": "public",
			},
		}}},
		Poller: SNMPPoller{
			Query: fakeSNMPCollectorQueryEngine{
				getRequests: &getRequests,
				gets: map[string]SNMPCollectorResponse{
					"": {
						VarBinds: []SNMPCollectorVarBind{{
							OID:       snmpOIDIfHCInOctets + ".101",
							Value:     uint64(9000),
							ValueType: SNMPCollectorValueCounter64,
						}},
					},
				},
			},
			Writer: writer,
		},
		Now: func() time.Time { return now },
	}
	result, err := runner.RunDue(context.Background(), "tenant-a", 10)
	if err != nil {
		t.Fatalf("RunDue() error = %v", err)
	}
	if result.RecipeCount != 1 || result.DeviceCount != 1 || result.SampleCount != 1 || result.FailedCount != 0 {
		t.Fatalf("unexpected runner result: %#v", result)
	}
	if len(getRequests) != 1 || getRequests[0].Target.Host != "192.0.2.1" || getRequests[0].Target.Port != 1161 || getRequests[0].Profile.Security["community"] != "private-a" {
		t.Fatalf("unexpected poll job request: %#v", getRequests)
	}
	if len(writer.samples) != 1 || writer.samples[0].DeviceID != "device-a" || writer.samples[0].FloatValue != 9000 {
		t.Fatalf("unexpected raw samples: %#v", writer.samples)
	}
	if collector.recipes[0].LastPolledAt != now || collector.recipes[0].LastError != "" {
		t.Fatalf("recipe poll result not marked: %#v", collector.recipes[0])
	}
}

type fakeSNMPCollectorQueryEngine struct {
	walks        map[string]SNMPCollectorResponse
	gets         map[string]SNMPCollectorResponse
	getRequests  *[]SNMPCollectorGetRequest
	walkRequests *[]SNMPCollectorWalkRequest
}

type fakeSNMPDiscoveryModule struct {
	seenOS string
}

func (m *fakeSNMPDiscoveryModule) Name() string {
	return "fake"
}

func (m *fakeSNMPDiscoveryModule) Discover(_ context.Context, req SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error) {
	m.seenOS = req.OS.OSName
	return SNMPCollectorDiscoveryResult{
		Recipes: []SNMPCollectionRecipe{{
			ID:         "recipe-a",
			TenantID:   req.TenantID,
			DeviceID:   req.Device.ID,
			EntityType: SNMPCollectorEntityDevice,
			EntityID:   req.Device.ID,
			ModuleName: m.Name(),
			MetricName: "watchdog_snmp_fake_raw",
			ValueType:  SNMPCollectorValueGauge,
			NumericOID: "1.2.3.0",
			Enabled:    true,
		}},
	}, nil
}

func (f fakeSNMPCollectorQueryEngine) Get(_ context.Context, req SNMPCollectorGetRequest) (SNMPCollectorResponse, error) {
	if f.getRequests != nil {
		*f.getRequests = append(*f.getRequests, req)
	}
	return f.gets[req.Context], nil
}

func (f fakeSNMPCollectorQueryEngine) Walk(_ context.Context, req SNMPCollectorWalkRequest) (SNMPCollectorResponse, error) {
	if f.walkRequests != nil {
		*f.walkRequests = append(*f.walkRequests, req)
	}
	return f.walks[req.BaseOID], nil
}

type fakeSNMPRawSampleWriter struct {
	samples []SNMPRawSample
}

func (w *fakeSNMPRawSampleWriter) WriteSNMPRawSamples(_ context.Context, samples []SNMPRawSample) error {
	w.samples = append(w.samples, samples...)
	return nil
}

type fakeSNMPCollectorRepository struct {
	osDefinitions     []SNMPCollectorOSDefinition
	moduleDefinitions []SNMPCollectorModuleDefinition
	deviceModules     []SNMPCollectorDeviceModule
	stateTranslations []SNMPStateTranslation
	recipes           []SNMPCollectionRecipe
	trapHandlers      []SNMPTrapHandlerDefinition
	events            []SNMPEvent
}

func (r *fakeSNMPCollectorRepository) ListSNMPOSDefinitions(_ context.Context) ([]SNMPCollectorOSDefinition, error) {
	return append([]SNMPCollectorOSDefinition(nil), r.osDefinitions...), nil
}

func (r *fakeSNMPCollectorRepository) UpsertSNMPOSDefinition(_ context.Context, definition SNMPCollectorOSDefinition) (SNMPCollectorOSDefinition, error) {
	r.osDefinitions = append(r.osDefinitions, definition)
	return definition, nil
}

func (r *fakeSNMPCollectorRepository) ListSNMPModuleDefinitions(_ context.Context, moduleType SNMPCollectorModuleType) ([]SNMPCollectorModuleDefinition, error) {
	definitions := make([]SNMPCollectorModuleDefinition, 0, len(r.moduleDefinitions))
	for _, definition := range r.moduleDefinitions {
		if moduleType == "" || definition.ModuleType == moduleType {
			definitions = append(definitions, definition)
		}
	}
	return definitions, nil
}

func (r *fakeSNMPCollectorRepository) UpsertSNMPModuleDefinition(_ context.Context, definition SNMPCollectorModuleDefinition) (SNMPCollectorModuleDefinition, error) {
	r.moduleDefinitions = append(r.moduleDefinitions, definition)
	return definition, nil
}

func (r *fakeSNMPCollectorRepository) UpsertSNMPDeviceModule(_ context.Context, module SNMPCollectorDeviceModule) (SNMPCollectorDeviceModule, error) {
	r.deviceModules = append(r.deviceModules, module)
	return module, nil
}

func (r *fakeSNMPCollectorRepository) UpsertSNMPStateTranslation(_ context.Context, translation SNMPStateTranslation) (SNMPStateTranslation, error) {
	r.stateTranslations = append(r.stateTranslations, translation)
	return translation, nil
}

func (r *fakeSNMPCollectorRepository) PruneSNMPCollectionRecipes(_ context.Context, _ ID, deviceID ID, keep []ID) (int64, error) {
	keepSet := make(map[ID]bool, len(keep))
	for _, id := range keep {
		keepSet[id] = true
	}
	var kept []SNMPCollectionRecipe
	var pruned int64
	for _, recipe := range r.recipes {
		if recipe.DeviceID == deviceID && !keepSet[recipe.ID] {
			pruned++
			continue
		}
		kept = append(kept, recipe)
	}
	r.recipes = kept
	return pruned, nil
}

func (r *fakeSNMPCollectorRepository) UpsertSNMPCollectionRecipes(_ context.Context, recipes []SNMPCollectionRecipe) error {
	r.recipes = append(r.recipes, recipes...)
	return nil
}

func (r *fakeSNMPCollectorRepository) ListDueSNMPCollectionRecipes(_ context.Context, _ ID, limit int, _ time.Time) ([]SNMPCollectionRecipe, error) {
	recipes := append([]SNMPCollectionRecipe(nil), r.recipes...)
	if limit > 0 && len(recipes) > limit {
		recipes = recipes[:limit]
	}
	return recipes, nil
}

func (r *fakeSNMPCollectorRepository) MarkSNMPRecipePollResult(_ context.Context, recipeID ID, polledAt time.Time, lastError string) error {
	for idx := range r.recipes {
		if r.recipes[idx].ID == recipeID {
			r.recipes[idx].LastPolledAt = polledAt
			r.recipes[idx].LastError = lastError
		}
	}
	return nil
}

func (r *fakeSNMPCollectorRepository) ListDueSNMPDevices(_ context.Context, _ ID, limit int, _ time.Time) ([]ID, error) {
	seen := map[ID]bool{}
	var deviceIDs []ID
	for _, recipe := range r.recipes {
		if !seen[recipe.DeviceID] {
			seen[recipe.DeviceID] = true
			deviceIDs = append(deviceIDs, recipe.DeviceID)
			if len(deviceIDs) >= limit {
				break
			}
		}
	}
	return deviceIDs, nil
}

func (r *fakeSNMPCollectorRepository) ListSNMPCollectionRecipesByDevice(_ context.Context, _ ID, deviceID ID) ([]SNMPCollectionRecipe, error) {
	var result []SNMPCollectionRecipe
	for _, recipe := range r.recipes {
		if recipe.DeviceID == deviceID && recipe.Enabled {
			result = append(result, recipe)
		}
	}
	return result, nil
}

func (r *fakeSNMPCollectorRepository) GetSNMPDeviceLastPolledAt(_ context.Context, _ ID, deviceID ID) (time.Time, error) {
	var latest time.Time
	for _, recipe := range r.recipes {
		if recipe.DeviceID == deviceID && recipe.LastError == "" && recipe.LastPolledAt.After(latest) {
			latest = recipe.LastPolledAt
		}
	}
	if latest.IsZero() {
		return time.Time{}, sql.ErrNoRows
	}
	return latest, nil
}

func (r *fakeSNMPCollectorRepository) ListSNMPTrapHandlers(_ context.Context) ([]SNMPTrapHandlerDefinition, error) {
	return append([]SNMPTrapHandlerDefinition(nil), r.trapHandlers...), nil
}

func (r *fakeSNMPCollectorRepository) UpsertSNMPTrapHandler(_ context.Context, handler SNMPTrapHandlerDefinition) (SNMPTrapHandlerDefinition, error) {
	r.trapHandlers = append(r.trapHandlers, handler)
	return handler, nil
}

func (r *fakeSNMPCollectorRepository) ListSNMPEvents(_ context.Context, _ ID, _ ID, _ int) ([]SNMPEvent, error) {
	return r.events, nil
}

func (r *fakeSNMPCollectorRepository) ListSNMPEventsPaged(_ context.Context, _ ID, _ ID, _ SNMPEventFilter) ([]SNMPEvent, string, error) {
	return r.events, "", nil
}

func (r *fakeSNMPCollectorRepository) CreateSNMPEvent(_ context.Context, event SNMPEvent) error {
	r.events = append(r.events, event)
	return nil
}
