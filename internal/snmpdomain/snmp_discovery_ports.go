package snmpdomain

import (
	"context"
	"errors"
	"strconv"
	"strings"
)

const snmpCollectorModulePorts = "ports"

var (
	snmpOIDIfDescr          = snmpMIBOID("IF-MIB::ifDescr")
	snmpOIDIfType           = snmpMIBOID("IF-MIB::ifType")
	snmpOIDIfSpeed          = snmpMIBOID("IF-MIB::ifSpeed")
	snmpOIDIfAdminStatus    = snmpMIBOID("IF-MIB::ifAdminStatus")
	snmpOIDIfOperStatus     = snmpMIBOID("IF-MIB::ifOperStatus")
	snmpOIDIfInOctets       = snmpMIBOID("IF-MIB::ifInOctets")
	snmpOIDIfInDiscards     = snmpMIBOID("IF-MIB::ifInDiscards")
	snmpOIDIfInErrors       = snmpMIBOID("IF-MIB::ifInErrors")
	snmpOIDIfOutOctets      = snmpMIBOID("IF-MIB::ifOutOctets")
	snmpOIDIfOutDiscards    = snmpMIBOID("IF-MIB::ifOutDiscards")
	snmpOIDIfOutErrors      = snmpMIBOID("IF-MIB::ifOutErrors")
	snmpOIDIfName           = snmpMIBOID("IF-MIB::ifName")
	snmpOIDIfHighSpeed      = snmpMIBOID("IF-MIB::ifHighSpeed")
	snmpOIDIfAlias          = snmpMIBOID("IF-MIB::ifAlias")
	snmpOIDIfHCInOctets     = snmpMIBOID("IF-MIB::ifHCInOctets")
	snmpOIDIfHCOutOctets    = snmpMIBOID("IF-MIB::ifHCOutOctets")
	snmpOIDConnectorPresent = snmpMIBOID("IF-MIB::ifConnectorPresent")
)

type SNMPPortsDiscoveryModule struct{}

func (SNMPPortsDiscoveryModule) Name() string {
	return snmpCollectorModulePorts
}

func (m SNMPPortsDiscoveryModule) Discover(ctx context.Context, req DiscoveryContext) (DiscoveryResult, error) {
	if req.Query == nil {
		return DiscoveryResult{}, errSNMPCollectorQueryRequired
	}
	columns, err := m.walkPortColumns(ctx, req)
	if err != nil {
		return DiscoveryResult{}, err
	}
	ports := make([]Port, 0, len(columns[snmpOIDIfDescr]))
	recipes := make([]Recipe, 0, len(columns[snmpOIDIfDescr])*8)
	for ifIndex, descr := range columns[snmpOIDIfDescr] {
		if ifIndex == 0 {
			continue
		}
		port := Port{
			ID:          collectorStableID("port", "", string(req.Device.ID), strconv.FormatUint(ifIndex, 10)),
			DeviceID:    req.Device.ID,
			IfIndex:     ifIndex,
			IfDescr:     descr,
			IfName:      firstNonEmptySNMPString(columns[snmpOIDIfName][ifIndex], descr),
			IfAlias:     columns[snmpOIDIfAlias][ifIndex],
			AdminStatus: NormalizeIfStatus(columns[snmpOIDIfAdminStatus][ifIndex]),
			OperStatus:  NormalizeIfStatus(columns[snmpOIDIfOperStatus][ifIndex]),
			SpeedBps:    portSpeedBps(columns[snmpOIDIfHighSpeed][ifIndex], columns[snmpOIDIfSpeed][ifIndex]),
			Metadata: map[string]string{
				"if_type":           columns[snmpOIDIfType][ifIndex],
				"connector_present": columns[snmpOIDConnectorPresent][ifIndex],
			},
		}
		ports = append(ports, port)
		recipes = append(recipes, portRecipes(req, port, hasColumnIndex(columns[snmpOIDIfHCInOctets], ifIndex), hasColumnIndex(columns[snmpOIDIfHCOutOctets], ifIndex))...)
	}
	return DiscoveryResult{
		Ports:              ports,
		InterfaceAddresses: discoverSNMPInterfaceAddresses(ctx, req),
		Recipes:            recipes,
	}, nil
}

func (m SNMPPortsDiscoveryModule) walkPortColumns(ctx context.Context, req DiscoveryContext) (map[string]map[uint64]string, error) {
	oids := []string{
		snmpOIDIfDescr,
		snmpOIDIfType,
		snmpOIDIfSpeed,
		snmpOIDIfAdminStatus,
		snmpOIDIfOperStatus,
		snmpOIDIfName,
		snmpOIDIfHighSpeed,
		snmpOIDIfAlias,
		snmpOIDConnectorPresent,
		snmpOIDIfHCInOctets,
		snmpOIDIfHCOutOctets,
	}
	columns := make(map[string]map[uint64]string, len(oids))
	for _, oid := range oids {
		response, err := req.Query.Walk(ctx, WalkRequest{
			Target:  req.Target,
			Profile: req.Profile,
			Context: "",
			BaseOID: oid,
			Flags: QueryFlags{
				UseBulk:        true,
				MaxRepetitions: 25,
			},
		})
		if err != nil {
			return nil, err
		}
		columns[oid] = valuesByNumericSuffix(oid, response)
	}
	return columns, nil
}

func valuesByNumericSuffix(baseOID string, response QueryResponse) map[uint64]string {
	values := make(map[uint64]string, len(response.VarBinds))
	prefix := strings.TrimPrefix(baseOID, ".") + "."
	for _, vb := range response.VarBinds {
		oid := strings.TrimPrefix(vb.OID, ".")
		suffix := strings.TrimPrefix(oid, prefix)
		if suffix == oid {
			continue
		}
		ifIndex, err := strconv.ParseUint(suffix, 10, 64)
		if err != nil {
			continue
		}
		values[ifIndex] = stringValue(vb.Value)
	}
	return values
}

func portRecipes(req DiscoveryContext, port Port, hasHCIn bool, hasHCOut bool) []Recipe {
	idx := strconv.FormatUint(port.IfIndex, 10)
	inOctetsOID := snmpOIDIfHCInOctets
	inOctetsType := ValueCounter64
	if !hasHCIn {
		inOctetsOID = snmpOIDIfInOctets
		inOctetsType = ValueCounter32
	}
	outOctetsOID := snmpOIDIfHCOutOctets
	outOctetsType := ValueCounter64
	if !hasHCOut {
		outOctetsOID = snmpOIDIfOutOctets
		outOctetsType = ValueCounter32
	}
	definitions := []struct {
		metric      string
		oid         string
		fallbackOID string
		valueType   ValueType
	}{
		{MetricSNMPIfInOctetsTotal, inOctetsOID, snmpOIDIfInOctets, inOctetsType},
		{MetricSNMPIfOutOctetsTotal, outOctetsOID, snmpOIDIfOutOctets, outOctetsType},
		{MetricSNMPIfInErrorsTotal, snmpOIDIfInErrors, "", ValueCounter32},
		{MetricSNMPIfOutErrorsTotal, snmpOIDIfOutErrors, "", ValueCounter32},
		{MetricSNMPIfInDiscardsTotal, snmpOIDIfInDiscards, "", ValueCounter32},
		{MetricSNMPIfOutDiscardsTotal, snmpOIDIfOutDiscards, "", ValueCounter32},
		{MetricSNMPIfAdminStatus, snmpOIDIfAdminStatus, "", ValueState},
		{MetricSNMPIfOperStatus, snmpOIDIfOperStatus, "", ValueState},
	}
	recipes := make([]Recipe, 0, len(definitions))
	for _, definition := range definitions {
		recipeID := collectorStableID("snmp-recipe", "", string(req.Device.ID), snmpCollectorModulePorts, string(EntityPort), string(port.ID), definition.metric, idx, "")
		options := map[string]string{
			"counter_bits": counterBits(definition.valueType),
		}
		if definition.valueType == ValueCounter64 && definition.fallbackOID != "" {
			options["fallback_numeric_oid"] = definition.fallbackOID + "." + idx
			options["fallback_counter_bits"] = "32"
		}
		recipes = append(recipes, Recipe{
			ID:                    recipeID,
			DeviceID:              req.Device.ID,
			EntityType:            EntityPort,
			EntityID:              port.ID,
			ModuleName:            snmpCollectorModulePorts,
			MetricName:            definition.metric,
			ValueType:             definition.valueType,
			OID:                   snmpMIBDisplayOID(definition.oid + "." + idx),
			NumericOID:            definition.oid + "." + idx,
			OIDIndex:              idx,
			MIB:                   "IF-MIB",
			PollerType:            "snmp",
			SampleIntervalSeconds: 60,
			Labels: map[string]string{
				"target_id": string(req.TargetID),
				"port_id":   string(port.ID),
				"if_index":  idx,
				"if_name":   port.IfName,
				"module":    snmpCollectorModulePorts,
			},
			Options: options,
			Enabled: true,
		})
	}
	return recipes
}

func hasColumnIndex(column map[uint64]string, ifIndex uint64) bool {
	_, ok := column[ifIndex]
	return ok
}

func portSpeedBps(ifHighSpeed string, ifSpeed string) uint64 {
	if highSpeed, err := strconv.ParseUint(strings.TrimSpace(ifHighSpeed), 10, 64); err == nil && highSpeed > 0 {
		return highSpeed * 1_000_000
	}
	speed, _ := strconv.ParseUint(strings.TrimSpace(ifSpeed), 10, 64)
	return speed
}

func counterBits(valueType ValueType) string {
	if valueType == ValueCounter64 {
		return "64"
	}
	if valueType == ValueCounter32 {
		return "32"
	}
	return ""
}

func firstNonEmptySNMPString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

var errSNMPCollectorQueryRequired = errors.New("snmp collector query engine is required")
