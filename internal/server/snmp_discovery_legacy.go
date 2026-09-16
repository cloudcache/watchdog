package server

import (
	"context"
	"strings"

	"github.com/cloudcache/watchdog/internal/snmpdomain"
	"github.com/cloudcache/watchdog/internal/watchdog"
)

// legacySNMPDiscoveryRunner is the only remaining production boundary to the
// historical package. G3d2 keeps the mature MIB discovery algorithms intact
// while the Gin runtime and persistence layer use single-domain contracts.
type legacySNMPDiscoveryRunner struct{ engine watchdog.SNMPDiscoveryEngine }

func newSNMPDiscoveryRunner(cfg SNMPConfig) (snmpDiscoveryRunner, error) {
	if len(cfg.MIBDirs) > 0 || strings.TrimSpace(cfg.MIBLoad) != "" {
		if err := watchdog.ConfigureSNMPMIBRegistry(watchdog.SNMPConfig{MIBDirs: cfg.MIBDirs, MIBLoad: cfg.MIBLoad}); err != nil {
			return nil, err
		}
	}
	var definitions []snmpdomain.OSDefinition
	if dir := strings.TrimSpace(cfg.DefinitionsDir); dir != "" {
		parsed, err := snmpdomain.ParseLibrenmsDefinitions(dir, cfg.DefinitionsVersion)
		if err != nil {
			return nil, err
		}
		definitions = parsed.OSDefinitions
	}
	registry := watchdog.DefaultSNMPCollectorModuleRegistry()
	return legacySNMPDiscoveryRunner{engine: watchdog.SNMPDiscoveryEngine{
		Query: watchdog.NewGoSNMPCollectorQueryEngine(), OSDefinitions: definitions,
		ModuleDefinitions: map[string]watchdog.SNMPCollectorModuleDefinition{},
		Modules:           registry.DiscoveryModulesForDefinitions(nil),
	}}, nil
}

func (r legacySNMPDiscoveryRunner) Discover(ctx context.Context, request snmpdomain.DiscoveryRequest) (snmpdomain.DiscoveryResult, error) {
	result, err := r.engine.Discover(ctx, watchdog.SNMPDiscoveryEngineRequest{
		TargetID: watchdog.ID(request.TargetID),
		Target:   watchdog.SNMPCollectorTarget{Host: request.Target.Host, Port: request.Target.Port},
		Device:   legacySNMPDevice(request.Device), Profile: legacySNMPProfile(request.Profile),
	})
	if err != nil {
		return snmpdomain.DiscoveryResult{}, err
	}
	return singleDomainDiscoveryResult(result), nil
}

func singleDomainDiscoveryResult(result watchdog.SNMPCollectorDiscoveryResult) snmpdomain.DiscoveryResult {
	out := snmpdomain.DiscoveryResult{
		DeviceUpdates:    singleDomainSNMPDevice(result.DeviceUpdates),
		CompletedModules: append([]string(nil), result.CompletedModules...),
		PhysicalEntities: make([]snmpdomain.PhysicalEntity, 0, len(result.PhysicalEntities)),
		VLANs:            make([]snmpdomain.VLAN, 0, len(result.VLANs)), LAGs: make([]snmpdomain.LAGGroup, 0, len(result.LAGs)),
		Events: make([]snmpdomain.Event, 0, len(result.Events)),
	}
	for _, value := range result.Ports {
		out.Ports = append(out.Ports, snmpdomain.Port{ID: string(value.ID), DeviceID: string(value.DeviceID), IfIndex: value.IfIndex, IfName: value.IfName, IfAlias: value.IfAlias, IfDescr: value.IfDescr, AdminStatus: value.AdminStatus, OperStatus: value.OperStatus, SpeedBps: value.SpeedBps, Metadata: value.Metadata, UpdatedAt: value.UpdatedAt})
	}
	for _, value := range result.InterfaceAddresses {
		out.InterfaceAddresses = append(out.InterfaceAddresses, snmpdomain.InterfaceAddress{ID: string(value.ID), DeviceID: string(value.DeviceID), PortID: string(value.PortID), IfIndex: value.IfIndex, Address: value.Address, Family: value.Family, PrefixLength: value.PrefixLength, Origin: value.Origin, ContextName: value.ContextName, UpdatedAt: value.UpdatedAt})
	}
	for _, value := range result.Sensors {
		out.Sensors = append(out.Sensors, snmpdomain.Sensor{ID: string(value.ID), DeviceID: string(value.DeviceID), PortID: string(value.PortID), SensorIndex: value.SensorIndex, Class: value.Class, Name: value.Name, OID: value.OID, Unit: value.Unit, Value: value.Value, WarnLimit: value.WarnLimit, CritLimit: value.CritLimit, Status: value.Status, Metadata: value.Metadata, UpdatedAt: value.UpdatedAt})
	}
	for _, value := range result.PhysicalEntities {
		out.PhysicalEntities = append(out.PhysicalEntities, snmpdomain.PhysicalEntity{Index: value.Index, Name: value.Name, Description: value.Description, Class: value.Class, VendorType: value.VendorType, ContainedIn: value.ContainedIn, ParentRelPos: value.ParentRelPos, HardwareRevision: value.HardwareRevision, FirmwareRevision: value.FirmwareRevision, SoftwareRevision: value.SoftwareRevision, SerialNumber: value.SerialNumber, ManufacturerName: value.ManufacturerName, ModelName: value.ModelName, Alias: value.Alias, AssetID: value.AssetID, IsFRU: value.IsFRU})
	}
	for _, value := range result.BGPSessions {
		out.BGPSessions = append(out.BGPSessions, snmpdomain.BGPSession{ID: string(value.ID), DeviceID: string(value.DeviceID), PeerAddr: value.PeerAddr, PeerAS: value.PeerAS, LocalAS: value.LocalAS, AFI: value.AFI, SAFI: value.SAFI, State: value.State, AcceptedPrefixes: value.AcceptedPrefixes, DeniedPrefixes: value.DeniedPrefixes, AdvertisedPrefixes: value.AdvertisedPrefixes, Uptime: value.Uptime, Metadata: value.Metadata, UpdatedAt: value.UpdatedAt})
	}
	for _, value := range result.VLANs {
		out.VLANs = append(out.VLANs, snmpdomain.VLAN{VLANID: value.VLANID, Name: value.Name, Status: value.Status})
	}
	for _, value := range result.LAGs {
		out.LAGs = append(out.LAGs, snmpdomain.LAGGroup{AggregateIndex: value.AggregateIndex, MACAddress: value.MACAddress, Mode: value.Mode})
	}
	for _, value := range result.Recipes {
		out.Recipes = append(out.Recipes, singleDomainRecipe(value))
	}
	for _, value := range result.Events {
		out.Events = append(out.Events, snmpdomain.Event{ID: string(value.ID), DeviceID: string(value.DeviceID), EntityType: snmpdomain.EntityType(value.EntityType), EntityID: string(value.EntityID), Source: value.Source, Severity: value.Severity, EventType: value.EventType, Message: value.Message, Raw: value.Raw, OccurredAt: value.OccurredAt, CreatedAt: value.CreatedAt})
	}
	return out
}

func singleDomainSNMPDevice(value watchdog.NetworkDevice) snmpdomain.Device {
	return snmpdomain.Device{ID: string(value.ID), TargetID: string(value.TargetID), Vendor: value.Vendor, Model: value.Model, Platform: value.Platform, OSName: value.OSName, OSVersion: value.OSVersion, SysObjectID: value.SysObjectID, SysName: value.SysName, SysDescr: value.SysDescr, SysLocation: value.SysLocation, Uptime: value.Uptime, SNMPProfileID: string(value.SNMPProfileID), SNMPPort: value.SNMPPort, SNMPSecurity: value.SNMPSecurity, UpdatedAt: value.UpdatedAt}
}

func singleDomainRecipe(value watchdog.SNMPCollectionRecipe) snmpdomain.Recipe {
	return snmpdomain.Recipe{
		ID: string(value.ID), DeviceID: string(value.DeviceID), EntityType: snmpdomain.EntityType(value.EntityType),
		EntityID: string(value.EntityID), ModuleName: value.ModuleName, MetricName: value.MetricName,
		ValueType: snmpdomain.ValueType(value.ValueType), OID: value.OID, NumericOID: value.NumericOID,
		OIDIndex: value.OIDIndex, MIB: value.MIB, ContextName: value.ContextName, PollerType: value.PollerType,
		Divisor: value.Divisor, HasDivisor: value.HasDivisor, Multiplier: value.Multiplier,
		HasMultiplier: value.HasMultiplier, UserFunc: value.UserFunc, StateMapID: string(value.StateMapID),
		Unit: value.Unit, SampleIntervalSeconds: value.SampleIntervalSeconds, Labels: value.Labels,
		Options: value.Options, Enabled: value.Enabled, DiscoveredAt: value.DiscoveredAt,
		LastSeenAt: value.LastSeenAt, LastPolledAt: value.LastPolledAt, LastError: value.LastError,
		CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
	}
}

func legacySNMPDevice(device snmpdomain.Device) watchdog.NetworkDevice {
	return watchdog.NetworkDevice{
		ID: watchdog.ID(device.ID), TargetID: watchdog.ID(device.TargetID), Vendor: device.Vendor,
		Model: device.Model, Platform: device.Platform, OSName: device.OSName, OSVersion: device.OSVersion,
		SysObjectID: device.SysObjectID, SysName: device.SysName, SysDescr: device.SysDescr,
		SysLocation: device.SysLocation, Uptime: device.Uptime, SNMPProfileID: watchdog.ID(device.SNMPProfileID),
		SNMPPort: device.SNMPPort, SNMPSecurity: device.SNMPSecurity, UpdatedAt: device.UpdatedAt,
	}
}

func legacySNMPProfile(profile snmpdomain.Profile) watchdog.SNMPProfile {
	return watchdog.SNMPProfile{
		ID: watchdog.ID(profile.ID), Name: profile.Name, Version: watchdog.SNMPVersion(profile.Version),
		Security: profile.Security, Timeout: profile.Timeout, Retries: profile.Retries,
		CreatedAt: profile.CreatedAt, UpdatedAt: profile.UpdatedAt,
	}
}
