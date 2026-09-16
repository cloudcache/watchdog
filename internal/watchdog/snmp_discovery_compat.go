package watchdog

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cloudcache/watchdog/internal/snmpdomain"
)

// These names remain package-private compatibility symbols while the old
// watchdog tests and import path are retired. The live server and collector
// use snmpdomain directly.
var (
	snmpOIDSysDescr                  = snmpMIBOID("SNMPv2-MIB::sysDescr.0")
	snmpOIDSysObjectID               = snmpMIBOID("SNMPv2-MIB::sysObjectID.0")
	snmpOIDSysUpTime                 = snmpMIBOID("SNMPv2-MIB::sysUpTime.0")
	snmpOIDSysName                   = snmpMIBOID("SNMPv2-MIB::sysName.0")
	snmpOIDIfDescr                   = snmpMIBOID("IF-MIB::ifDescr")
	snmpOIDIfType                    = snmpMIBOID("IF-MIB::ifType")
	snmpOIDIfSpeed                   = snmpMIBOID("IF-MIB::ifSpeed")
	snmpOIDIfAdminStatus             = snmpMIBOID("IF-MIB::ifAdminStatus")
	snmpOIDIfOperStatus              = snmpMIBOID("IF-MIB::ifOperStatus")
	snmpOIDIfName                    = snmpMIBOID("IF-MIB::ifName")
	snmpOIDIfHighSpeed               = snmpMIBOID("IF-MIB::ifHighSpeed")
	snmpOIDIfAlias                   = snmpMIBOID("IF-MIB::ifAlias")
	snmpOIDIfHCInOctets              = snmpMIBOID("IF-MIB::ifHCInOctets")
	snmpOIDIfHCOutOctets             = snmpMIBOID("IF-MIB::ifHCOutOctets")
	snmpOIDConnectorPresent          = snmpMIBOID("IF-MIB::ifConnectorPresent")
	snmpOIDBGPLocalAS                = snmpMIBOID("BGP4-MIB::bgpLocalAs.0")
	snmpOIDBGPPeerState              = snmpMIBOID("BGP4-MIB::bgpPeerState")
	snmpOIDBGPPeerRemoteAddr         = snmpMIBOID("BGP4-MIB::bgpPeerRemoteAddr")
	snmpOIDBGPPeerRemoteAS           = snmpMIBOID("BGP4-MIB::bgpPeerRemoteAs")
	snmpOIDBGPPeerInUpdates          = snmpMIBOID("BGP4-MIB::bgpPeerInUpdates")
	snmpOIDBGPPeerOutUpdates         = snmpMIBOID("BGP4-MIB::bgpPeerOutUpdates")
	snmpOIDBGPPeerInTotalMessages    = snmpMIBOID("BGP4-MIB::bgpPeerInTotalMessages")
	snmpOIDBGPPeerOutTotalMessages   = snmpMIBOID("BGP4-MIB::bgpPeerOutTotalMessages")
	snmpOIDBGPPeerEstablishedSeconds = snmpMIBOID("BGP4-MIB::bgpPeerFsmEstablishedTime")
	oidEntPhysicalDescr              = snmpMIBOID("ENTITY-MIB::entPhysicalDescr")
	oidEntPhysicalName               = snmpMIBOID("ENTITY-MIB::entPhysicalName")
	oidEntPhySensorType              = snmpMIBOID("ENTITY-SENSOR-MIB::entPhySensorType")
	oidEntPhySensorScale             = snmpMIBOID("ENTITY-SENSOR-MIB::entPhySensorScale")
	oidEntPhySensorPrecision         = snmpMIBOID("ENTITY-SENSOR-MIB::entPhySensorPrecision")
	oidEntPhySensorValue             = snmpMIBOID("ENTITY-SENSOR-MIB::entPhySensorValue")
	oidEntPhySensorOper              = snmpMIBOID("ENTITY-SENSOR-MIB::entPhySensorOperStatus")
	oidEntPhySensorUnits             = snmpMIBOID("ENTITY-SENSOR-MIB::entPhySensorUnitsDisplay")
)

const (
	snmpCollectorModulePorts     = "ports"
	snmpCollectorModuleBGP       = "bgp"
	snmpCollectorModuleSensors   = "sensors"
	snmpCollectorModuleMemory    = "memory"
	snmpCollectorModuleStorage   = "storage"
	snmpCollectorModuleProcessor = "processors"
	MetricSNMPSensorValue        = "watchdog_snmp_sensor_value"
	MetricSNMPSensorState        = "watchdog_snmp_sensor_state"
)

type SNMPDiscoveryEngineRequest struct {
	TenantID ID
	TargetID ID
	Target   SNMPCollectorTarget
	Device   NetworkDevice
	Profile  SNMPProfile
}

type SNMPDiscoveryEngine struct {
	Query             SNMPCollectorQueryEngine
	OSDefinitions     []SNMPCollectorOSDefinition
	ModuleDefinitions map[string]SNMPCollectorModuleDefinition
	Modules           []SNMPCollectorDiscoveryModule
}

func (e SNMPDiscoveryEngine) Discover(ctx context.Context, request SNMPDiscoveryEngineRequest) (SNMPCollectorDiscoveryResult, error) {
	modules := make([]snmpdomain.DiscoveryModule, 0, len(e.Modules))
	for _, module := range e.Modules {
		modules = append(modules, legacyDiscoveryModuleAdapter{module: module, tenantID: request.TenantID, query: e.Query})
	}
	result, err := (snmpdomain.DiscoveryEngine{
		Query: legacyQueryAdapter{query: e.Query}, OSDefinitions: e.OSDefinitions,
		ModuleDefinitions: e.ModuleDefinitions, Modules: modules,
	}).Discover(ctx, snmpdomain.DiscoveryRequest{
		TargetID: string(request.TargetID),
		Target:   snmpdomain.QueryTarget{Host: request.Target.Host, Port: request.Target.Port},
		Device:   singleDomainLegacyDevice(request.Device),
		Profile:  singleDomainLegacyProfile(request.Profile),
	})
	if err != nil {
		return SNMPCollectorDiscoveryResult{}, err
	}
	return legacyDiscoveryResult(result, request.TenantID), nil
}

type legacyDiscoveryModuleAdapter struct {
	module   SNMPCollectorDiscoveryModule
	tenantID ID
	query    SNMPCollectorQueryEngine
}

func (a legacyDiscoveryModuleAdapter) Name() string { return a.module.Name() }

func (a legacyDiscoveryModuleAdapter) Discover(ctx context.Context, request snmpdomain.DiscoveryContext) (snmpdomain.DiscoveryResult, error) {
	result, err := a.module.Discover(ctx, SNMPCollectorDiscoveryContext{
		TenantID: a.tenantID, TargetID: ID(request.TargetID),
		Target: SNMPCollectorTarget{Host: request.Target.Host, Port: request.Target.Port},
		Device: legacyDiscoveryDevice(request.Device), Profile: legacyDiscoveryProfile(request.Profile),
		OS: request.OS, Query: a.query, Definition: request.Definition, OSDiscovery: request.OSDiscovery,
	})
	return singleDomainLegacyResult(result), err
}

type legacyQueryAdapter struct{ query SNMPCollectorQueryEngine }

func (a legacyQueryAdapter) Get(ctx context.Context, request snmpdomain.GetRequest) (snmpdomain.QueryResponse, error) {
	response, err := a.query.Get(ctx, SNMPCollectorGetRequest{
		Target:  SNMPCollectorTarget{Host: request.Target.Host, Port: request.Target.Port},
		Profile: legacyDiscoveryProfile(request.Profile), Context: request.Context, OIDs: request.OIDs,
		Flags: SNMPCollectorQueryFlags(request.Flags),
	})
	return singleDomainQueryResponse(response), err
}

func (a legacyQueryAdapter) Walk(ctx context.Context, request snmpdomain.WalkRequest) (snmpdomain.QueryResponse, error) {
	response, err := a.query.Walk(ctx, SNMPCollectorWalkRequest{
		Target:  SNMPCollectorTarget{Host: request.Target.Host, Port: request.Target.Port},
		Profile: legacyDiscoveryProfile(request.Profile), Context: request.Context, BaseOID: request.BaseOID,
		Flags: SNMPCollectorQueryFlags(request.Flags),
	})
	return singleDomainQueryResponse(response), err
}

func singleDomainQueryResponse(response SNMPCollectorResponse) snmpdomain.QueryResponse {
	result := snmpdomain.QueryResponse{VarBinds: make([]snmpdomain.VarBind, 0, len(response.VarBinds))}
	for _, value := range response.VarBinds {
		result.VarBinds = append(result.VarBinds, snmpdomain.VarBind{OID: value.OID, Value: value.Value, ValueType: snmpdomain.ValueType(value.ValueType)})
	}
	return result
}

func discoverWithSingleDomainModule(ctx context.Context, module snmpdomain.DiscoveryModule, request SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error) {
	result, err := module.Discover(ctx, snmpdomain.DiscoveryContext{
		TargetID: string(request.TargetID), Target: snmpdomain.QueryTarget{Host: request.Target.Host, Port: request.Target.Port},
		Device: singleDomainLegacyDevice(request.Device), Profile: singleDomainLegacyProfile(request.Profile),
		OS: request.OS, Query: legacyQueryAdapter{query: request.Query}, Definition: request.Definition,
		OSDiscovery: request.OSDiscovery,
	})
	return legacyDiscoveryResult(result, request.TenantID), err
}

type SNMPEntityPhysicalDiscoveryModule struct{}
type SNMPPortsDiscoveryModule struct{}
type SNMPSensorsDiscoveryModule struct{}
type SNMPProcessorsDiscoveryModule struct{}
type SNMPMemoryDiscoveryModule struct{}
type SNMPStorageDiscoveryModule struct{}
type SNMPVLANsDiscoveryModule struct{}
type SNMPLAGsDiscoveryModule struct{}
type SNMPBGPDiscoveryModule struct{}

func (SNMPEntityPhysicalDiscoveryModule) Name() string { return "entity-physical" }
func (SNMPPortsDiscoveryModule) Name() string          { return snmpCollectorModulePorts }
func (SNMPSensorsDiscoveryModule) Name() string        { return snmpCollectorModuleSensors }
func (SNMPProcessorsDiscoveryModule) Name() string     { return snmpCollectorModuleProcessor }
func (SNMPMemoryDiscoveryModule) Name() string         { return snmpCollectorModuleMemory }
func (SNMPStorageDiscoveryModule) Name() string        { return snmpCollectorModuleStorage }
func (SNMPVLANsDiscoveryModule) Name() string          { return "vlans" }
func (SNMPLAGsDiscoveryModule) Name() string           { return "lags" }
func (SNMPBGPDiscoveryModule) Name() string            { return snmpCollectorModuleBGP }

func (SNMPEntityPhysicalDiscoveryModule) Discover(ctx context.Context, request SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error) {
	return discoverWithSingleDomainModule(ctx, snmpdomain.SNMPEntityPhysicalDiscoveryModule{}, request)
}
func (SNMPPortsDiscoveryModule) Discover(ctx context.Context, request SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error) {
	return discoverWithSingleDomainModule(ctx, snmpdomain.SNMPPortsDiscoveryModule{}, request)
}
func (SNMPSensorsDiscoveryModule) Discover(ctx context.Context, request SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error) {
	return discoverWithSingleDomainModule(ctx, snmpdomain.SNMPSensorsDiscoveryModule{}, request)
}
func (SNMPProcessorsDiscoveryModule) Discover(ctx context.Context, request SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error) {
	return discoverWithSingleDomainModule(ctx, snmpdomain.SNMPProcessorsDiscoveryModule{}, request)
}
func (SNMPMemoryDiscoveryModule) Discover(ctx context.Context, request SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error) {
	return discoverWithSingleDomainModule(ctx, snmpdomain.SNMPMemoryDiscoveryModule{}, request)
}
func (SNMPStorageDiscoveryModule) Discover(ctx context.Context, request SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error) {
	return discoverWithSingleDomainModule(ctx, snmpdomain.SNMPStorageDiscoveryModule{}, request)
}
func (SNMPVLANsDiscoveryModule) Discover(ctx context.Context, request SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error) {
	return discoverWithSingleDomainModule(ctx, snmpdomain.SNMPVLANsDiscoveryModule{}, request)
}
func (SNMPLAGsDiscoveryModule) Discover(ctx context.Context, request SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error) {
	return discoverWithSingleDomainModule(ctx, snmpdomain.SNMPLAGsDiscoveryModule{}, request)
}
func (SNMPBGPDiscoveryModule) Discover(ctx context.Context, request SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error) {
	return discoverWithSingleDomainModule(ctx, snmpdomain.SNMPBGPDiscoveryModule{}, request)
}

func legacyDefaultDiscoveryModules() []SNMPCollectorDiscoveryModule {
	return []SNMPCollectorDiscoveryModule{
		SNMPEntityPhysicalDiscoveryModule{},
		SNMPPortsDiscoveryModule{},
		SNMPSensorsDiscoveryModule{},
		SNMPProcessorsDiscoveryModule{},
		SNMPMemoryDiscoveryModule{},
		SNMPStorageDiscoveryModule{},
		SNMPVLANsDiscoveryModule{},
		SNMPLAGsDiscoveryModule{},
		SNMPBGPDiscoveryModule{},
	}
}

type SNMPCollectorModuleRegistry struct {
	discoveryModules map[string]SNMPCollectorDiscoveryModule
}

func DefaultSNMPCollectorModuleRegistry() SNMPCollectorModuleRegistry {
	return NewSNMPCollectorModuleRegistry(legacyDefaultDiscoveryModules())
}

func NewSNMPCollectorModuleRegistry(modules []SNMPCollectorDiscoveryModule) SNMPCollectorModuleRegistry {
	registry := SNMPCollectorModuleRegistry{discoveryModules: map[string]SNMPCollectorDiscoveryModule{}}
	for _, module := range modules {
		if module != nil && module.Name() != "" {
			registry.discoveryModules[module.Name()] = module
		}
	}
	return registry
}

func (r SNMPCollectorModuleRegistry) DiscoveryModule(name string) (SNMPCollectorDiscoveryModule, bool) {
	module, ok := r.discoveryModules[name]
	return module, ok
}

func (r SNMPCollectorModuleRegistry) DiscoveryModuleNames() []string {
	names := make([]string, 0, len(r.discoveryModules))
	for name := range r.discoveryModules {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r SNMPCollectorModuleRegistry) DiscoveryModulesForDefinitions(definitions []SNMPCollectorModuleDefinition) []SNMPCollectorDiscoveryModule {
	if len(definitions) == 0 {
		modules := make([]SNMPCollectorDiscoveryModule, 0, len(r.discoveryModules))
		for _, name := range r.DiscoveryModuleNames() {
			modules = append(modules, r.discoveryModules[name])
		}
		return modules
	}
	modules := make([]SNMPCollectorDiscoveryModule, 0, len(definitions))
	seen := map[string]struct{}{}
	for _, definition := range definitions {
		if definition.ModuleName == "" || definition.ModuleType != SNMPCollectorModuleDiscovery {
			continue
		}
		if _, ok := seen[definition.ModuleName]; ok {
			continue
		}
		if module, ok := r.discoveryModules[definition.ModuleName]; ok {
			modules = append(modules, module)
			seen[definition.ModuleName] = struct{}{}
		}
	}
	return modules
}

func NewSNMPDiscoveryEngineFromRepository(ctx context.Context, repository SNMPCollectorRepository, query SNMPCollectorQueryEngine, registry SNMPCollectorModuleRegistry) (SNMPDiscoveryEngine, error) {
	if registry.discoveryModules == nil {
		registry = DefaultSNMPCollectorModuleRegistry()
	}
	osDefinitions, err := repository.ListSNMPOSDefinitions(ctx)
	if err != nil {
		return SNMPDiscoveryEngine{}, err
	}
	moduleDefinitions, err := repository.ListSNMPModuleDefinitions(ctx, SNMPCollectorModuleDiscovery)
	if err != nil {
		return SNMPDiscoveryEngine{}, err
	}
	byName := make(map[string]SNMPCollectorModuleDefinition, len(moduleDefinitions))
	for _, definition := range moduleDefinitions {
		if definition.ModuleName != "" {
			byName[definition.ModuleName] = definition
		}
	}
	return SNMPDiscoveryEngine{Query: query, OSDefinitions: osDefinitions, ModuleDefinitions: byName,
		Modules: registry.DiscoveryModulesForDefinitions(moduleDefinitions)}, nil
}

func firstID(values ...ID) ID {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func mergeStringMap(base map[string]string, updates map[string]string) map[string]string {
	if len(base) == 0 && len(updates) == 0 {
		return nil
	}
	merged := make(map[string]string, len(base)+len(updates))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range updates {
		merged[key] = value
	}
	return merged
}

func mergeDiscoveredDevice(current NetworkDevice, updates NetworkDevice) NetworkDevice {
	merged := current
	if updates.ID != "" {
		merged.ID = updates.ID
	}
	if updates.TenantID != "" {
		merged.TenantID = updates.TenantID
	}
	if updates.TargetID != "" {
		merged.TargetID = updates.TargetID
	}
	if updates.Vendor != "" {
		merged.Vendor = updates.Vendor
	}
	if updates.Model != "" {
		merged.Model = updates.Model
	}
	if updates.Platform != "" {
		merged.Platform = updates.Platform
	}
	if updates.OSName != "" {
		merged.OSName = updates.OSName
	}
	if updates.OSVersion != "" {
		merged.OSVersion = updates.OSVersion
	}
	if updates.SysObjectID != "" {
		merged.SysObjectID = updates.SysObjectID
	}
	if updates.SysName != "" {
		merged.SysName = updates.SysName
	}
	if updates.SysDescr != "" {
		merged.SysDescr = updates.SysDescr
	}
	if updates.SysLocation != "" {
		merged.SysLocation = updates.SysLocation
	}
	if updates.Uptime != 0 {
		merged.Uptime = updates.Uptime
	}
	if updates.SNMPProfileID != "" {
		merged.SNMPProfileID = updates.SNMPProfileID
	}
	if updates.SNMPPort != 0 {
		merged.SNMPPort = updates.SNMPPort
	}
	if updates.SNMPSecurity != nil {
		merged.SNMPSecurity = updates.SNMPSecurity
	}
	return merged
}

func snmpCollectorStringValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	case int:
		return strconv.Itoa(typed)
	case uint:
		return strconv.FormatUint(uint64(typed), 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case uint32:
		return strconv.FormatUint(uint64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", typed))
	}
}

func firstNonEmptySNMPString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func singleDomainLegacyDevice(value NetworkDevice) snmpdomain.Device {
	return snmpdomain.Device{ID: string(value.ID), TargetID: string(value.TargetID), Vendor: value.Vendor, Model: value.Model,
		Platform: value.Platform, OSName: value.OSName, OSVersion: value.OSVersion, SysObjectID: value.SysObjectID,
		SysName: value.SysName, SysDescr: value.SysDescr, SysLocation: value.SysLocation, Uptime: value.Uptime,
		SNMPProfileID: string(value.SNMPProfileID), SNMPPort: value.SNMPPort, SNMPSecurity: value.SNMPSecurity, UpdatedAt: value.UpdatedAt}
}

func legacyDiscoveryDevice(value snmpdomain.Device) NetworkDevice {
	return NetworkDevice{ID: ID(value.ID), TargetID: ID(value.TargetID), Vendor: value.Vendor, Model: value.Model,
		Platform: value.Platform, OSName: value.OSName, OSVersion: value.OSVersion, SysObjectID: value.SysObjectID,
		SysName: value.SysName, SysDescr: value.SysDescr, SysLocation: value.SysLocation, Uptime: value.Uptime,
		SNMPProfileID: ID(value.SNMPProfileID), SNMPPort: value.SNMPPort, SNMPSecurity: value.SNMPSecurity, UpdatedAt: value.UpdatedAt}
}

func singleDomainLegacyProfile(value SNMPProfile) snmpdomain.Profile {
	return snmpdomain.Profile{ID: string(value.ID), Name: value.Name, Version: snmpdomain.Version(value.Version),
		Security: value.Security, Timeout: value.Timeout, Retries: value.Retries, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt}
}

func legacyDiscoveryProfile(value snmpdomain.Profile) SNMPProfile {
	return SNMPProfile{ID: ID(value.ID), Name: value.Name, Version: SNMPVersion(value.Version),
		Security: value.Security, Timeout: value.Timeout, Retries: value.Retries, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt}
}

func singleDomainLegacyResult(result SNMPCollectorDiscoveryResult) snmpdomain.DiscoveryResult {
	out := snmpdomain.DiscoveryResult{DeviceUpdates: singleDomainLegacyDevice(result.DeviceUpdates), CompletedModules: append([]string(nil), result.CompletedModules...)}
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
		out.PhysicalEntities = append(out.PhysicalEntities, snmpdomain.PhysicalEntity{
			Index: value.Index, Name: value.Name, Description: value.Description, Class: value.Class,
			VendorType: value.VendorType, ContainedIn: value.ContainedIn, ParentRelPos: value.ParentRelPos,
			HardwareRevision: value.HardwareRevision, FirmwareRevision: value.FirmwareRevision,
			SoftwareRevision: value.SoftwareRevision, SerialNumber: value.SerialNumber,
			ManufacturerName: value.ManufacturerName, ModelName: value.ModelName, Alias: value.Alias,
			AssetID: value.AssetID, IsFRU: value.IsFRU,
		})
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
		out.Recipes = append(out.Recipes, singleDomainLegacyRecipe(value))
	}
	for _, value := range result.Events {
		out.Events = append(out.Events, snmpdomain.Event{ID: string(value.ID), DeviceID: string(value.DeviceID), EntityType: snmpdomain.EntityType(value.EntityType), EntityID: string(value.EntityID), Source: value.Source, Severity: value.Severity, EventType: value.EventType, Message: value.Message, Raw: value.Raw, OccurredAt: value.OccurredAt, CreatedAt: value.CreatedAt})
	}
	return out
}

func legacyDiscoveryResult(result snmpdomain.DiscoveryResult, tenantID ID) SNMPCollectorDiscoveryResult {
	out := SNMPCollectorDiscoveryResult{DeviceUpdates: legacyDiscoveryDevice(result.DeviceUpdates), CompletedModules: append([]string(nil), result.CompletedModules...)}
	out.DeviceUpdates.TenantID = tenantID
	for _, value := range result.Ports {
		out.Ports = append(out.Ports, NetworkPort{ID: ID(value.ID), TenantID: tenantID, DeviceID: ID(value.DeviceID), IfIndex: value.IfIndex, IfName: value.IfName, IfAlias: value.IfAlias, IfDescr: value.IfDescr, AdminStatus: value.AdminStatus, OperStatus: value.OperStatus, SpeedBps: value.SpeedBps, Metadata: value.Metadata, UpdatedAt: value.UpdatedAt})
	}
	for _, value := range result.InterfaceAddresses {
		out.InterfaceAddresses = append(out.InterfaceAddresses, NetworkInterfaceAddress{ID: ID(value.ID), TenantID: tenantID, DeviceID: ID(value.DeviceID), PortID: ID(value.PortID), IfIndex: value.IfIndex, Address: value.Address, Family: value.Family, PrefixLength: value.PrefixLength, Origin: value.Origin, ContextName: value.ContextName, UpdatedAt: value.UpdatedAt})
	}
	for _, value := range result.Sensors {
		out.Sensors = append(out.Sensors, NetworkDeviceSensor{ID: ID(value.ID), TenantID: tenantID, DeviceID: ID(value.DeviceID), PortID: ID(value.PortID), SensorIndex: value.SensorIndex, Class: value.Class, Name: value.Name, OID: value.OID, Unit: value.Unit, Value: value.Value, WarnLimit: value.WarnLimit, CritLimit: value.CritLimit, Status: value.Status, Metadata: value.Metadata, UpdatedAt: value.UpdatedAt})
	}
	for _, value := range result.PhysicalEntities {
		out.PhysicalEntities = append(out.PhysicalEntities, PhysicalEntity{
			Index: value.Index, Name: value.Name, Description: value.Description, Class: value.Class,
			VendorType: value.VendorType, ContainedIn: value.ContainedIn, ParentRelPos: value.ParentRelPos,
			HardwareRevision: value.HardwareRevision, FirmwareRevision: value.FirmwareRevision,
			SoftwareRevision: value.SoftwareRevision, SerialNumber: value.SerialNumber,
			ManufacturerName: value.ManufacturerName, ModelName: value.ModelName, Alias: value.Alias,
			AssetID: value.AssetID, IsFRU: value.IsFRU,
		})
	}
	for _, value := range result.BGPSessions {
		out.BGPSessions = append(out.BGPSessions, BGPSession{ID: ID(value.ID), TenantID: tenantID, DeviceID: ID(value.DeviceID), PeerAddr: value.PeerAddr, PeerAS: value.PeerAS, LocalAS: value.LocalAS, AFI: value.AFI, SAFI: value.SAFI, State: value.State, AcceptedPrefixes: value.AcceptedPrefixes, DeniedPrefixes: value.DeniedPrefixes, AdvertisedPrefixes: value.AdvertisedPrefixes, Uptime: value.Uptime, Metadata: value.Metadata, UpdatedAt: value.UpdatedAt})
	}
	for _, value := range result.VLANs {
		out.VLANs = append(out.VLANs, DeviceVLAN{VLANID: value.VLANID, Name: value.Name, Status: value.Status})
	}
	for _, value := range result.LAGs {
		out.LAGs = append(out.LAGs, DeviceLAGGroup{AggregateIndex: value.AggregateIndex, MACAddress: value.MACAddress, Mode: value.Mode})
	}
	for _, value := range result.Recipes {
		out.Recipes = append(out.Recipes, legacyDiscoveryRecipe(value, tenantID))
	}
	for _, value := range result.Events {
		out.Events = append(out.Events, SNMPEvent{ID: ID(value.ID), TenantID: tenantID, DeviceID: ID(value.DeviceID), EntityType: SNMPCollectorEntityType(value.EntityType), EntityID: ID(value.EntityID), Source: value.Source, Severity: value.Severity, EventType: value.EventType, Message: value.Message, Raw: value.Raw, OccurredAt: value.OccurredAt, CreatedAt: value.CreatedAt})
	}
	return out
}

func singleDomainLegacyRecipe(value SNMPCollectionRecipe) snmpdomain.Recipe {
	return snmpdomain.Recipe{ID: string(value.ID), DeviceID: string(value.DeviceID), EntityType: snmpdomain.EntityType(value.EntityType), EntityID: string(value.EntityID), ModuleName: value.ModuleName, MetricName: value.MetricName, ValueType: snmpdomain.ValueType(value.ValueType), OID: value.OID, NumericOID: value.NumericOID, OIDIndex: value.OIDIndex, MIB: value.MIB, ContextName: value.ContextName, PollerType: value.PollerType, Divisor: value.Divisor, HasDivisor: value.HasDivisor, Multiplier: value.Multiplier, HasMultiplier: value.HasMultiplier, UserFunc: value.UserFunc, StateMapID: string(value.StateMapID), Unit: value.Unit, SampleIntervalSeconds: value.SampleIntervalSeconds, Labels: value.Labels, Options: value.Options, Enabled: value.Enabled, DiscoveredAt: value.DiscoveredAt, LastSeenAt: value.LastSeenAt, LastPolledAt: value.LastPolledAt, LastError: value.LastError, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt}
}

func legacyDiscoveryRecipe(value snmpdomain.Recipe, tenantID ID) SNMPCollectionRecipe {
	return SNMPCollectionRecipe{ID: ID(value.ID), TenantID: tenantID, DeviceID: ID(value.DeviceID), EntityType: SNMPCollectorEntityType(value.EntityType), EntityID: ID(value.EntityID), ModuleName: value.ModuleName, MetricName: value.MetricName, ValueType: SNMPCollectorValueType(value.ValueType), OID: value.OID, NumericOID: value.NumericOID, OIDIndex: value.OIDIndex, MIB: value.MIB, ContextName: value.ContextName, PollerType: value.PollerType, Divisor: value.Divisor, HasDivisor: value.HasDivisor, Multiplier: value.Multiplier, HasMultiplier: value.HasMultiplier, UserFunc: value.UserFunc, StateMapID: ID(value.StateMapID), Unit: value.Unit, SampleIntervalSeconds: value.SampleIntervalSeconds, Labels: value.Labels, Options: value.Options, Enabled: value.Enabled, DiscoveredAt: value.DiscoveredAt, LastSeenAt: value.LastSeenAt, LastPolledAt: value.LastPolledAt, LastError: value.LastError, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt}
}
