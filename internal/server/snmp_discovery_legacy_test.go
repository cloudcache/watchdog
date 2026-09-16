package server

import (
	"reflect"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpdomain"
	"github.com/cloudcache/watchdog/internal/watchdog"
)

// TestSingleDomainDiscoveryResultParity prevents the temporary legacy adapter
// from silently dropping mature discovery semantics while tenant ownership is
// removed. Every non-tenant field in every discovery DTO must have a same-name
// field carrying the same value in the single-domain contract.
func TestSingleDomainDiscoveryResultParity(t *testing.T) {
	timestamp := time.Date(2026, 9, 16, 7, 8, 9, 10, time.UTC)
	legacy := watchdog.SNMPCollectorDiscoveryResult{
		DeviceUpdates: watchdog.NetworkDevice{
			ID: "device", TenantID: "removed-tenant", TargetID: "target", Vendor: "vendor", Model: "model",
			Platform: "platform", OSName: "os", OSVersion: "version", SysObjectID: ".1.3.6.1",
			SysName: "name", SysDescr: "description", SysLocation: "location", Uptime: 7 * time.Hour,
			SNMPProfileID: "profile", SNMPPort: 1161, SNMPSecurity: map[string]string{"community": "secret"}, UpdatedAt: timestamp,
		},
		CompletedModules: []string{"ports", "bgp"},
		Ports: []watchdog.NetworkPort{{
			ID: "port", TenantID: "removed-tenant", DeviceID: "device", IfIndex: 7, IfName: "xe-0/0/0",
			IfAlias: "transit", IfDescr: "uplink", AdminStatus: "up", OperStatus: "down", SpeedBps: 100,
			Metadata: map[string]string{"if_type": "ethernet"}, UpdatedAt: timestamp,
		}},
		InterfaceAddresses: []watchdog.NetworkInterfaceAddress{{
			ID: "address", TenantID: "removed-tenant", DeviceID: "device", PortID: "port", IfIndex: 7,
			Address: "2001:db8::1", Family: "ipv6", PrefixLength: 127, Origin: "manual", ContextName: "blue", UpdatedAt: timestamp,
		}},
		Sensors: []watchdog.NetworkDeviceSensor{{
			ID: "sensor", TenantID: "removed-tenant", DeviceID: "device", PortID: "port", SensorIndex: 8,
			Class: "temperature", Name: "inlet", OID: ".1.3.6.1.8", Unit: "C", Value: 31.5,
			WarnLimit: 70, CritLimit: 80, Status: "ok", Metadata: map[string]string{"scale": "1"}, UpdatedAt: timestamp,
		}},
		PhysicalEntities: []watchdog.PhysicalEntity{{
			Index: 9, Name: "chassis", Description: "main", Class: "chassis", VendorType: "type", ContainedIn: 1,
			ParentRelPos: 2, HardwareRevision: "h", FirmwareRevision: "f", SoftwareRevision: "s", SerialNumber: "serial",
			ManufacturerName: "maker", ModelName: "model", Alias: "alias", AssetID: "asset", IsFRU: true,
		}},
		BGPSessions: []watchdog.BGPSession{{
			ID: "bgp", TenantID: "removed-tenant", DeviceID: "device", PeerAddr: "2001:db8::2", PeerAS: 64501,
			LocalAS: 64500, AFI: "ipv6", SAFI: "unicast", State: "established", AcceptedPrefixes: 10,
			DeniedPrefixes: 2, AdvertisedPrefixes: 8, Uptime: time.Hour, Metadata: map[string]string{"vrf": "blue"}, UpdatedAt: timestamp,
		}},
		VLANs: []watchdog.DeviceVLAN{{VLANID: 100, Name: "users", Status: "active"}},
		LAGs:  []watchdog.DeviceLAGGroup{{AggregateIndex: 10, MACAddress: "00:11:22:33:44:55", Mode: "lacp"}},
		Recipes: []watchdog.SNMPCollectionRecipe{{
			ID: "recipe", TenantID: "removed-tenant", DeviceID: "device", EntityType: "port", EntityID: "port",
			ModuleName: "ports", MetricName: "if.in.bps", ValueType: "counter64", OID: ".1.3.6.1.2",
			NumericOID: ".1.3.6.1.2.7", OIDIndex: "7", MIB: "IF-MIB", ContextName: "blue", PollerType: "snmp",
			Divisor: 2, HasDivisor: true, Multiplier: 8, HasMultiplier: true, UserFunc: "normalize",
			StateMapID: "state-map", Unit: "bps", SampleIntervalSeconds: 30, Labels: map[string]string{"side": "raw"},
			Options: map[string]string{"flag": "value"}, Enabled: true, DiscoveredAt: timestamp, LastSeenAt: timestamp,
			LastPolledAt: timestamp, LastError: "previous", CreatedAt: timestamp, UpdatedAt: timestamp,
		}},
		Events: []watchdog.SNMPEvent{{
			ID: "event", TenantID: "removed-tenant", DeviceID: "device", EntityType: "port", EntityID: "port",
			Source: "discovery", Severity: "warning", EventType: "changed", Message: "changed",
			Raw: map[string]any{"value": "old"}, OccurredAt: timestamp, CreatedAt: timestamp,
		}},
	}

	got := singleDomainDiscoveryResult(legacy)
	if !reflect.DeepEqual(got.CompletedModules, legacy.CompletedModules) {
		t.Fatalf("completed modules mismatch: got=%v want=%v", got.CompletedModules, legacy.CompletedModules)
	}
	assertMappedFields(t, legacy.DeviceUpdates, got.DeviceUpdates, "TenantID")
	assertMappedFields(t, legacy.Ports[0], got.Ports[0], "TenantID")
	assertMappedFields(t, legacy.InterfaceAddresses[0], got.InterfaceAddresses[0], "TenantID")
	assertMappedFields(t, legacy.Sensors[0], got.Sensors[0], "TenantID")
	assertMappedFields(t, legacy.PhysicalEntities[0], got.PhysicalEntities[0])
	assertMappedFields(t, legacy.BGPSessions[0], got.BGPSessions[0], "TenantID")
	assertMappedFields(t, legacy.VLANs[0], got.VLANs[0])
	assertMappedFields(t, legacy.LAGs[0], got.LAGs[0])
	assertMappedFields(t, legacy.Recipes[0], got.Recipes[0], "TenantID")
	assertMappedFields(t, legacy.Events[0], got.Events[0], "TenantID")

	assertMappedFields(t, got.DeviceUpdates, legacySNMPDevice(got.DeviceUpdates))
	profile := snmpdomain.Profile{ID: "profile", Name: "v3", Version: snmpdomain.Version3,
		Security: map[string]string{"username": "collector"}, Timeout: 4 * time.Second, Retries: 3,
		CreatedAt: timestamp, UpdatedAt: timestamp}
	assertMappedFields(t, profile, legacySNMPProfile(profile))
}

func assertMappedFields(t *testing.T, source, target any, ignored ...string) {
	t.Helper()
	ignoredFields := make(map[string]struct{}, len(ignored))
	for _, name := range ignored {
		ignoredFields[name] = struct{}{}
	}
	sourceValue := reflect.ValueOf(source)
	targetValue := reflect.ValueOf(target)
	sourceType := sourceValue.Type()
	for index := 0; index < sourceValue.NumField(); index++ {
		field := sourceType.Field(index)
		if _, ok := ignoredFields[field.Name]; ok {
			continue
		}
		want := sourceValue.Field(index)
		got := targetValue.FieldByName(field.Name)
		if !got.IsValid() {
			t.Errorf("%T.%s has no mapping in %T", source, field.Name, target)
			continue
		}
		if want.Type() != got.Type() && want.Type().ConvertibleTo(got.Type()) {
			want = want.Convert(got.Type())
		}
		if want.Type() != got.Type() || !reflect.DeepEqual(want.Interface(), got.Interface()) {
			t.Errorf("%T.%s mismatch: got=%v (%s), want=%v (%s)", source, field.Name,
				got.Interface(), got.Type(), want.Interface(), want.Type())
		}
	}
}
