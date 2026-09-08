package watchdog

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	snmpOIDSysDescr     = snmpMIBOID("SNMPv2-MIB::sysDescr.0")
	snmpOIDSysObjectID  = snmpMIBOID("SNMPv2-MIB::sysObjectID.0")
	snmpOIDSysUpTime    = snmpMIBOID("SNMPv2-MIB::sysUpTime.0")
	snmpOIDSysName      = snmpMIBOID("SNMPv2-MIB::sysName.0")
	snmpOIDSysLocation  = snmpMIBOID("SNMPv2-MIB::sysLocation.0")
	snmpOIDSNMPEngineID = snmpMIBOID("SNMP-FRAMEWORK-MIB::snmpEngineID.0")
)

type SNMPDiscoveryEngine struct {
	Query             SNMPCollectorQueryEngine
	OSDefinitions     []SNMPCollectorOSDefinition
	ModuleDefinitions map[string]SNMPCollectorModuleDefinition
	Modules           []SNMPCollectorDiscoveryModule
}

type SNMPDiscoveryEngineRequest struct {
	TenantID ID
	TargetID ID
	Target   SNMPCollectorTarget
	Device   NetworkDevice
	Profile  SNMPProfile
}

func (e SNMPDiscoveryEngine) Discover(ctx context.Context, req SNMPDiscoveryEngineRequest) (SNMPCollectorDiscoveryResult, error) {
	if e.Query == nil {
		return SNMPCollectorDiscoveryResult{}, errSNMPCollectorQueryRequired
	}
	fingerprint, err := e.coreFingerprint(ctx, req)
	if err != nil {
		return SNMPCollectorDiscoveryResult{}, err
	}
	osMatch, osDef, _ := DetectSNMPCollectorOSWithDefinition(fingerprint, e.OSDefinitions)
	device := req.Device
	device.TenantID = req.TenantID
	device.TargetID = req.TargetID
	device.SysObjectID = fingerprint.SysObjectID
	device.SysDescr = fingerprint.SysDescr
	device.SysName = fingerprint.SysName
	device.SysLocation = fingerprint.SysLocation
	device.Uptime = time.Duration(fingerprint.SysUpTime/100) * time.Second
	device.OSName = osMatch.OSName
	if osMatch.Vendor != "" {
		device.Vendor = osMatch.Vendor
	}
	if osMatch.Model != "" {
		device.Model = osMatch.Model
	}
	device.OSVersion = extractOSVersion(fingerprint.SysDescr)

	osDiscovery := osDiscoveryDefinition(osDef.Definition)
	// LibreNMS discovery_modules semantics: modules run by default and the
	// list only toggles — an explicit `false` disables a module, `true`
	// enables optional ones. Their "mempools" is our "memory" module.
	moduleToggles := osDiscoveryModuleNames(osDef.Definition)
	moduleDisabled := func(name string) bool {
		if moduleToggles == nil {
			return false
		}
		lookup := name
		if name == snmpCollectorModuleMemory {
			lookup = "mempools"
		}
		enabled, listed := moduleToggles[lookup]
		return listed && !enabled
	}
	result := SNMPCollectorDiscoveryResult{DeviceUpdates: device}
	for _, module := range e.Modules {
		if moduleDisabled(module.Name()) {
			continue
		}
		moduleResult, err := module.Discover(ctx, SNMPCollectorDiscoveryContext{
			TenantID:    req.TenantID,
			TargetID:    req.TargetID,
			Target:      req.Target,
			Device:      device,
			Profile:     req.Profile,
			OS:          osMatch,
			Query:       e.Query,
			Definition:  e.ModuleDefinitions[module.Name()],
			OSDiscovery: osDiscovery,
		})
		if err != nil {
			appendDiscoveryResult(&result, moduleResult)
			continue
		}
		appendDiscoveryResult(&result, moduleResult)
		result.CompletedModules = append(result.CompletedModules, module.Name())
	}
	return result, nil
}

func osDiscoveryDefinition(definition map[string]any) map[string]any {
	if definition == nil {
		return nil
	}
	raw, ok := definition["os_discovery"]
	if !ok {
		return nil
	}
	modules, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	return modules
}

func osDiscoveryModuleNames(definition map[string]any) map[string]bool {
	if definition == nil {
		return nil
	}
	raw, ok := definition["discovery_modules"]
	if !ok {
		return nil
	}
	m, ok := raw.(map[string]bool)
	if ok {
		return m
	}
	// JSON unmarshal produces map[string]any
	m2, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	result := make(map[string]bool, len(m2))
	for name, val := range m2 {
		enabled, _ := val.(bool)
		result[name] = enabled
	}
	return result
}

func (e SNMPDiscoveryEngine) coreFingerprint(ctx context.Context, req SNMPDiscoveryEngineRequest) (SNMPCollectorOSFingerprint, error) {
	response, err := e.Query.Get(ctx, SNMPCollectorGetRequest{
		Target:  req.Target,
		Profile: req.Profile,
		OIDs: []string{
			snmpOIDSysDescr,
			snmpOIDSysObjectID,
			snmpOIDSysUpTime,
			snmpOIDSysName,
			snmpOIDSysLocation,
			snmpOIDSNMPEngineID,
		},
		Flags: SNMPCollectorQueryFlags{MaxOids: 10},
	})
	if err != nil {
		return SNMPCollectorOSFingerprint{}, err
	}
	values := make(map[string]SNMPCollectorVarBind, len(response.VarBinds))
	for _, vb := range response.VarBinds {
		values[vb.OID] = vb
	}
	if _, ok := values[snmpOIDSysObjectID]; !ok {
		return SNMPCollectorOSFingerprint{}, errors.New("core discovery missing sysObjectID")
	}
	sysUpTime, _ := snmpCollectorFloatValue(values[snmpOIDSysUpTime].Value)
	return SNMPCollectorOSFingerprint{
		SysObjectID:  snmpCollectorStringValue(values[snmpOIDSysObjectID].Value),
		SysDescr:     snmpCollectorStringValue(values[snmpOIDSysDescr].Value),
		SysName:      snmpCollectorStringValue(values[snmpOIDSysName].Value),
		SysLocation:  snmpCollectorStringValue(values[snmpOIDSysLocation].Value),
		SysUpTime:    uint64(sysUpTime),
		SNMPEngineID: snmpCollectorStringValue(values[snmpOIDSNMPEngineID].Value),
	}, nil
}

func appendDiscoveryResult(dst *SNMPCollectorDiscoveryResult, src SNMPCollectorDiscoveryResult) {
	dst.Ports = append(dst.Ports, src.Ports...)
	dst.InterfaceAddresses = append(dst.InterfaceAddresses, src.InterfaceAddresses...)
	dst.Sensors = append(dst.Sensors, src.Sensors...)
	dst.PhysicalEntities = append(dst.PhysicalEntities, src.PhysicalEntities...)
	dst.BGPSessions = append(dst.BGPSessions, src.BGPSessions...)
	dst.VLANs = append(dst.VLANs, src.VLANs...)
	dst.LAGs = append(dst.LAGs, src.LAGs...)
	dst.Recipes = append(dst.Recipes, src.Recipes...)
	dst.Events = append(dst.Events, src.Events...)
}

func parseSNMPTimeticks(value string) uint64 {
	parsed, _ := strconv.ParseUint(value, 10, 64)
	return parsed
}

// osVersionPattern captures everything after "Version" up to the end of the
// sysDescr line (or a comma, which Cisco uses to separate trailing clauses).
// A character whitelist used to truncate Huawei strings mid-parenthesis
// ("5.170 (S5720 V200R010" instead of "... V200R010C00SPC600)").
var osVersionPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)version[\s:,]+([vV]?\d[^\r\n,]*)`),
	regexp.MustCompile(`(?i)kernel\s+junos\s+([^\s,]+)`),
}

func extractOSVersion(sysDescr string) string {
	for _, pattern := range osVersionPatterns {
		if match := pattern.FindStringSubmatch(sysDescr); match != nil {
			return strings.TrimSpace(match[1])
		}
	}
	return ""
}
