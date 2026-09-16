package snmpdomain

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

type DiscoveryEngine struct {
	Query             QueryEngine
	OSDefinitions     []OSDefinition
	ModuleDefinitions map[string]ModuleDefinition
	Modules           []DiscoveryModule
}

func (e DiscoveryEngine) Discover(ctx context.Context, req DiscoveryRequest) (DiscoveryResult, error) {
	if e.Query == nil {
		return DiscoveryResult{}, errSNMPCollectorQueryRequired
	}
	fingerprint, err := e.coreFingerprint(ctx, req)
	if err != nil {
		return DiscoveryResult{}, err
	}
	osMatch, osDef, _ := DetectOSWithDefinition(fingerprint, e.OSDefinitions)
	device := req.Device
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
	result := DiscoveryResult{DeviceUpdates: device}
	for _, module := range e.Modules {
		if moduleDisabled(module.Name()) {
			continue
		}
		moduleResult, err := module.Discover(ctx, DiscoveryContext{
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

func (e DiscoveryEngine) coreFingerprint(ctx context.Context, req DiscoveryRequest) (OSFingerprint, error) {
	response, err := e.Query.Get(ctx, GetRequest{
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
		Flags: QueryFlags{MaxOids: 10},
	})
	if err != nil {
		return OSFingerprint{}, err
	}
	values := make(map[string]VarBind, len(response.VarBinds))
	for _, vb := range response.VarBinds {
		values[vb.OID] = vb
	}
	if _, ok := values[snmpOIDSysObjectID]; !ok {
		return OSFingerprint{}, errors.New("core discovery missing sysObjectID")
	}
	sysUpTime, _ := floatValue(values[snmpOIDSysUpTime].Value)
	return OSFingerprint{
		SysObjectID:  stringValue(values[snmpOIDSysObjectID].Value),
		SysDescr:     stringValue(values[snmpOIDSysDescr].Value),
		SysName:      stringValue(values[snmpOIDSysName].Value),
		SysLocation:  stringValue(values[snmpOIDSysLocation].Value),
		SysUpTime:    uint64(sysUpTime),
		SNMPEngineID: stringValue(values[snmpOIDSNMPEngineID].Value),
	}, nil
}

func appendDiscoveryResult(dst *DiscoveryResult, src DiscoveryResult) {
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
