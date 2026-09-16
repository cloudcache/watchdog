package snmpdomain

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"unicode"
)

var (
	oidEntPhysicalDescr      = snmpMIBOID("ENTITY-MIB::entPhysicalDescr")
	oidEntPhysicalName       = snmpMIBOID("ENTITY-MIB::entPhysicalName")
	oidEntPhySensorType      = snmpMIBOID("ENTITY-SENSOR-MIB::entPhySensorType")
	oidEntPhySensorScale     = snmpMIBOID("ENTITY-SENSOR-MIB::entPhySensorScale")
	oidEntPhySensorPrecision = snmpMIBOID("ENTITY-SENSOR-MIB::entPhySensorPrecision")
	oidEntPhySensorValue     = snmpMIBOID("ENTITY-SENSOR-MIB::entPhySensorValue")
	oidEntPhySensorOper      = snmpMIBOID("ENTITY-SENSOR-MIB::entPhySensorOperStatus")
	oidEntPhySensorUnits     = snmpMIBOID("ENTITY-SENSOR-MIB::entPhySensorUnitsDisplay")
)

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

func mergeDiscoveredDevice(current Device, updates Device) Device {
	merged := current
	if updates.ID != "" {
		merged.ID = updates.ID
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

func cleanSNMPValue(value string) string {
	value = strings.TrimSpace(value)
	if before, after, ok := strings.Cut(value, ":"); ok && !strings.Contains(before, ".") {
		value = after
	}
	value = strings.TrimSpace(value)
	value = strings.Trim(value, `"`)
	return strings.TrimSpace(value)
}

func parseUintValue(value string) uint64 {
	cleaned := cleanSNMPValue(value)
	if start := strings.LastIndex(cleaned, "("); start >= 0 && strings.HasSuffix(cleaned, ")") {
		if parsed, err := strconv.ParseUint(strings.TrimSpace(cleaned[start+1:len(cleaned)-1]), 10, 64); err == nil {
			return parsed
		}
	}
	for _, token := range numericTokens(cleaned, false) {
		parsed, err := strconv.ParseUint(token, 10, 64)
		if err == nil {
			return parsed
		}
	}
	return 0
}

func parseFloatValue(value string) (float64, error) {
	for _, token := range numericTokens(cleanSNMPValue(value), true) {
		parsed, err := strconv.ParseFloat(token, 64)
		if err == nil {
			return parsed, nil
		}
	}
	return 0, errors.New("snmp value does not contain a number")
}

func numericTokens(value string, allowFloat bool) []string {
	tokens := make([]string, 0, 1)
	start := -1
	seenDigit := false
	for index, r := range value {
		isSign := r == '-' || r == '+'
		isDecimal := allowFloat && r == '.'
		if unicode.IsDigit(r) || isDecimal || (isSign && start == -1) {
			if start == -1 {
				start = index
			}
			if unicode.IsDigit(r) {
				seenDigit = true
			}
			continue
		}
		if start >= 0 && seenDigit {
			tokens = append(tokens, value[start:index])
		}
		start = -1
		seenDigit = false
	}
	if start >= 0 && seenDigit {
		tokens = append(tokens, value[start:])
	}
	return tokens
}

func bgpPeerAddrFromIndex(index string) string {
	index = strings.TrimSpace(index)
	if net.ParseIP(index) != nil {
		return index
	}
	parts := strings.Split(index, ".")
	if len(parts) < 4 {
		return ""
	}
	last := strings.Join(parts[len(parts)-4:], ".")
	if net.ParseIP(last) != nil {
		return last
	}
	return ""
}

func bgpPeerState(value string) string {
	cleaned := cleanSNMPValue(value)
	if start := strings.LastIndex(cleaned, "("); start > 0 && strings.HasSuffix(cleaned, ")") {
		return strings.TrimSpace(cleaned[:start])
	}
	switch parseUintValue(cleaned) {
	case 1:
		return "idle"
	case 2:
		return "connect"
	case 3:
		return "active"
	case 4:
		return "opensent"
	case 5:
		return "openconfirm"
	case 6:
		return "established"
	default:
		return cleaned
	}
}

func entitySensorClassAndUnit(sensorType string, units string) (string, string) {
	unit := cleanSNMPValue(units)
	switch parseUintValue(sensorType) {
	case 3:
		return "voltage", firstNonEmptySNMPString(unit, "V")
	case 4:
		return "voltage", firstNonEmptySNMPString(unit, "V")
	case 5:
		return "current", firstNonEmptySNMPString(unit, "A")
	case 6:
		return "power", firstNonEmptySNMPString(unit, "W")
	case 7:
		return "frequency", firstNonEmptySNMPString(unit, "Hz")
	case 8:
		return "temperature", firstNonEmptySNMPString(unit, "C")
	case 9:
		return "humidity", firstNonEmptySNMPString(unit, "%")
	case 10:
		return "fanspeed", firstNonEmptySNMPString(unit, "rpm")
	case 11:
		return "airflow", firstNonEmptySNMPString(unit, "m3/min")
	case 12:
		return "state", unit
	default:
		return "other", unit
	}
}

func applyEntitySensorScale(value float64, scale string, precision string) float64 {
	return value * entitySensorScaleMultiplier(scale, precision)
}

func entitySensorStatus(value string) string {
	cleaned := cleanSNMPValue(value)
	if start := strings.LastIndex(cleaned, "("); start > 0 && strings.HasSuffix(cleaned, ")") {
		return strings.TrimSpace(cleaned[:start])
	}
	switch parseUintValue(cleaned) {
	case 1:
		return "ok"
	case 2:
		return "unavailable"
	case 3:
		return "nonoperational"
	default:
		return cleaned
	}
}

func walkColumn(ctx context.Context, req DiscoveryContext, oid string) map[uint64]string {
	resp, err := req.Query.Walk(ctx, WalkRequest{
		Target: req.Target, Profile: req.Profile, BaseOID: oid,
		Flags: QueryFlags{UseBulk: true, MaxRepetitions: 25},
	})
	if err != nil {
		return map[uint64]string{}
	}
	return valuesByNumericSuffix(oid, resp)
}
