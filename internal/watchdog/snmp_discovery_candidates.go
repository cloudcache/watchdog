package watchdog

import (
	"context"
	"strings"
)

// Standard metric families are collected by probing candidate sources: every
// candidate the device answers contributes recipes. Nothing is gated on
// vendor or model — support varies per unit and firmware, so capability is
// established by asking the device, not by branching on its brand. The
// tables below are declarative MIB references; a candidate whose module is
// not present in the configured MIB tree simply does not resolve and is
// skipped, and a resolved candidate the device does not answer walks empty.

type snmpValueCandidate struct {
	value string // MIB ref of the value column
	descr string // optional MIB ref of a same-index description column
}

var snmpCPUCandidates = []snmpValueCandidate{
	{value: "HOST-RESOURCES-MIB::hrProcessorLoad", descr: "HOST-RESOURCES-MIB::hrDeviceDescr"},
	{value: "HUAWEI-ENTITY-EXTENT-MIB::hwEntityCpuUsage", descr: "ENTITY-MIB::entPhysicalName"},
	{value: "HH3C-ENTITY-EXT-MIB::hh3cEntityExtCpuUsage", descr: "ENTITY-MIB::entPhysicalName"},
	{value: "CISCO-PROCESS-MIB::cpmCPUTotal5minRev"},
	{value: "JUNIPER-MIB::jnxOperatingCPU", descr: "JUNIPER-MIB::jnxOperatingDescr"},
}

var snmpMemPercentCandidates = []snmpValueCandidate{
	{value: "HUAWEI-ENTITY-EXTENT-MIB::hwEntityMemUsage", descr: "ENTITY-MIB::entPhysicalName"},
	{value: "HH3C-ENTITY-EXT-MIB::hh3cEntityExtMemUsage", descr: "ENTITY-MIB::entPhysicalName"},
	{value: "JUNIPER-MIB::jnxOperatingBuffer", descr: "JUNIPER-MIB::jnxOperatingDescr"},
}

// usage% x size-bytes pairs, yielding used/total byte metrics.
type snmpMemPairCandidate struct {
	usage string
	size  string
	descr string
}

var snmpMemPairCandidates = []snmpMemPairCandidate{
	{usage: "HUAWEI-ENTITY-EXTENT-MIB::hwEntityMemUsage", size: "HUAWEI-ENTITY-EXTENT-MIB::hwEntityMemSize", descr: "ENTITY-MIB::entPhysicalName"},
	{usage: "HH3C-ENTITY-EXT-MIB::hh3cEntityExtMemUsage", size: "HH3C-ENTITY-EXT-MIB::hh3cEntityExtMemSize", descr: "ENTITY-MIB::entPhysicalName"},
}

var snmpMemUsedCandidates = []snmpValueCandidate{
	{value: "CISCO-MEMORY-POOL-MIB::ciscoMemoryPoolUsed", descr: "CISCO-MEMORY-POOL-MIB::ciscoMemoryPoolName"},
}

// transceiver DDM rx/tx power pairs; values are dBm scaled by divisor.
type snmpDBMPairCandidate struct {
	rx      string
	tx      string
	descr   string
	divisor float64
	invalid float64
}

var snmpDBMPairCandidates = []snmpDBMPairCandidate{
	{rx: "HUAWEI-ENTITY-EXTENT-MIB::hwEntityOpticalRxPower", tx: "HUAWEI-ENTITY-EXTENT-MIB::hwEntityOpticalTxPower", descr: "ENTITY-MIB::entPhysicalName", divisor: 100, invalid: 2147483647},
	{rx: "JUNIPER-DOM-MIB::jnxDomCurrentRxLaserPower", tx: "JUNIPER-DOM-MIB::jnxDomCurrentTxLaserOutputPower", descr: "IF-MIB::ifDescr", divisor: 100},
	{rx: "HH3C-TRANSCEIVER-INFO-MIB::hh3cTransceiverCurRXPower", tx: "HH3C-TRANSCEIVER-INFO-MIB::hh3cTransceiverCurTXPower", descr: "IF-MIB::ifDescr", divisor: 100, invalid: 2147483647},
}

type snmpTempCandidate struct {
	value   string
	descr   string
	invalid float64
}

var snmpTempCandidates = []snmpTempCandidate{
	{value: "HUAWEI-ENTITY-EXTENT-MIB::hwEntityTemperature", descr: "ENTITY-MIB::entPhysicalName", invalid: 2147483646},
	{value: "HH3C-ENTITY-EXT-MIB::hh3cEntityExtTemperature", descr: "ENTITY-MIB::entPhysicalName", invalid: 2147483646},
	{value: "JUNIPER-MIB::jnxOperatingTemp", descr: "JUNIPER-MIB::jnxOperatingDescr"},
}

// candidateOID resolves a candidate reference; "" means not resolvable with
// the loaded MIB tree, so the candidate is skipped rather than guessed.
func candidateOID(ref string) string {
	oid, err := DefaultSNMPMIBRegistry().OID(ref)
	if err != nil {
		return ""
	}
	return oid
}

func candidateWalk(ctx context.Context, req SNMPCollectorDiscoveryContext, oid string) map[string]string {
	if oid == "" {
		return nil
	}
	resp, err := req.Query.Walk(ctx, SNMPCollectorWalkRequest{
		Target: req.Target, Profile: req.Profile, BaseOID: oid,
		Flags: SNMPCollectorQueryFlags{UseBulk: true, MaxRepetitions: 25},
	})
	if err != nil {
		return nil
	}
	return valuesBySNMPIndex(oid, resp)
}

func candidateDescr(descrValues map[string]string, index, fallback string) string {
	if name := cleanSNMPValue(descrValues[index]); name != "" {
		return name
	}
	return fallback + " " + index
}

// claimedOIDSet records the numeric base OIDs already produced by the OS
// discovery definition so candidate probing does not duplicate them.
func claimedOIDSet(recipes []SNMPCollectionRecipe) map[string]bool {
	claimed := make(map[string]bool, len(recipes))
	for _, recipe := range recipes {
		oid := recipe.NumericOID
		if idx := strings.LastIndexByte(oid, '.'); idx > 0 && recipe.OIDIndex != "" {
			oid = strings.TrimSuffix(oid, "."+recipe.OIDIndex)
		}
		claimed[oid] = true
	}
	return claimed
}

func probeCPUCandidates(ctx context.Context, req SNMPCollectorDiscoveryContext, claimed map[string]bool) []SNMPCollectionRecipe {
	var recipes []SNMPCollectionRecipe
	for _, candidate := range snmpCPUCandidates {
		valueOID := candidateOID(candidate.value)
		if valueOID == "" || claimed[valueOID] {
			continue
		}
		values := candidateWalk(ctx, req, valueOID)
		if len(values) == 0 {
			continue
		}
		descrValues := candidateWalk(ctx, req, candidateOID(candidate.descr))
		mib := candidateMIBLabel(candidate.value)
		for index := range values {
			entityID := collectorStableID("processor", string(req.TenantID), string(req.Device.ID), "cand", valueOID, index)
			descr := candidateDescr(descrValues, index, "Processor")
			recipes = append(recipes, processorRecipe(req, entityID, index, valueOID+"."+index, mib, descr))
			recipes = append(recipes, deviceCPUPercentRecipe(req, entityID, index, valueOID+"."+index, mib, descr))
		}
	}
	return recipes
}

func probeMemoryCandidates(ctx context.Context, req SNMPCollectorDiscoveryContext, claimed map[string]bool) []SNMPCollectionRecipe {
	var recipes []SNMPCollectionRecipe
	for _, candidate := range snmpMemPercentCandidates {
		valueOID := candidateOID(candidate.value)
		if valueOID == "" || claimed[valueOID] {
			continue
		}
		values := candidateWalk(ctx, req, valueOID)
		if len(values) == 0 {
			continue
		}
		// Entity-state tables answer for every chassis entity (fans, PSUs,
		// ports) with a 0% usage; when a sibling size column exists, keep
		// only rows that actually have memory.
		var sizes map[string]string
		for _, pair := range snmpMemPairCandidates {
			if pair.usage == candidate.value {
				sizes = candidateWalk(ctx, req, candidateOID(pair.size))
				break
			}
		}
		descrValues := candidateWalk(ctx, req, candidateOID(candidate.descr))
		mib := candidateMIBLabel(candidate.value)
		for index := range values {
			if sizes != nil && parseUintValue(sizes[index]) == 0 {
				continue
			}
			entityID := collectorStableID("memory", string(req.TenantID), string(req.Device.ID), "cand", valueOID, index)
			recipe := memoryRecipe(req, entityID, MetricSNMPDeviceMemPercent, index, valueOID+"."+index, mib, candidateDescr(descrValues, index, "Memory"), 1, nil)
			recipe.Unit = "percent"
			recipes = append(recipes, recipe)
		}
	}
	for _, candidate := range snmpMemPairCandidates {
		usageOID := candidateOID(candidate.usage)
		sizeOID := candidateOID(candidate.size)
		if usageOID == "" || sizeOID == "" {
			continue
		}
		usages := candidateWalk(ctx, req, usageOID)
		if len(usages) == 0 {
			continue
		}
		sizes := candidateWalk(ctx, req, sizeOID)
		descrValues := candidateWalk(ctx, req, candidateOID(candidate.descr))
		mib := candidateMIBLabel(candidate.usage)
		for index := range usages {
			sizeBytes := parseUintValue(sizes[index])
			if sizeBytes == 0 {
				continue
			}
			entityID := collectorStableID("memory", string(req.TenantID), string(req.Device.ID), "cand", usageOID, index)
			descr := candidateDescr(descrValues, index, "Memory")
			// used = usage% x size; total = size.
			recipes = append(recipes, memoryRecipe(req, entityID, MetricSNMPMemoryUsed, index, usageOID+"."+index, mib, descr, float64(sizeBytes)/100, nil))
			recipes = append(recipes, memoryRecipe(req, entityID, MetricSNMPMemoryTotal, index, sizeOID+"."+index, mib, descr, 1, nil))
		}
	}
	for _, candidate := range snmpMemUsedCandidates {
		valueOID := candidateOID(candidate.value)
		if valueOID == "" || claimed[valueOID] {
			continue
		}
		values := candidateWalk(ctx, req, valueOID)
		if len(values) == 0 {
			continue
		}
		descrValues := candidateWalk(ctx, req, candidateOID(candidate.descr))
		mib := candidateMIBLabel(candidate.value)
		for index := range values {
			entityID := collectorStableID("memory", string(req.TenantID), string(req.Device.ID), "cand", valueOID, index)
			recipes = append(recipes, memoryRecipe(req, entityID, MetricSNMPMemoryUsed, index, valueOID+"."+index, mib, candidateDescr(descrValues, index, "Memory pool"), 1, nil))
		}
	}
	return recipes
}

func probeSensorCandidates(ctx context.Context, req SNMPCollectorDiscoveryContext, claimed map[string]bool) ([]NetworkDeviceSensor, []SNMPCollectionRecipe) {
	var sensors []NetworkDeviceSensor
	var recipes []SNMPCollectionRecipe
	for _, candidate := range snmpDBMPairCandidates {
		rxOID := candidateOID(candidate.rx)
		txOID := candidateOID(candidate.tx)
		if rxOID == "" || txOID == "" || claimed[rxOID] {
			continue
		}
		rx := candidateWalk(ctx, req, rxOID)
		if len(rx) == 0 {
			continue
		}
		tx := candidateWalk(ctx, req, txOID)
		descrValues := candidateWalk(ctx, req, candidateOID(candidate.descr))
		directions := []struct {
			metric string
			oid    string
			label  string
			values map[string]string
		}{
			{MetricSNMPOpticalRxDBM, rxOID, "Rx", rx},
			{MetricSNMPOpticalTxDBM, txOID, "Tx", tx},
		}
		for index := range rx {
			name := candidateDescr(descrValues, index, "optical")
			for _, direction := range directions {
				raw, ok := direction.values[index]
				if !ok || !candidateValueValid(raw, candidate.invalid) {
					continue
				}
				value, _ := parseFloatValue(raw)
				if candidate.divisor > 1 {
					value /= candidate.divisor
				}
				sensor := NetworkDeviceSensor{
					ID:       collectorStableID("sensor", string(req.TenantID), string(req.Device.ID), "cand-dbm", strings.ToLower(direction.label), direction.oid, index),
					TenantID: req.TenantID,
					DeviceID: req.Device.ID,
					Class:    "dbm",
					Name:     name + " " + direction.label + " power",
					OID:      direction.oid + "." + index,
					Unit:     "dBm",
					Value:    value,
					Status:   "ok",
					Metadata: map[string]string{"sensor_type": "optical_" + strings.ToLower(direction.label)},
				}
				sensors = append(sensors, sensor)
				recipes = append(recipes, candidateSensorRecipe(req, sensor, direction.metric, "dBm", candidate.divisor))
			}
		}
	}
	for _, candidate := range snmpTempCandidates {
		valueOID := candidateOID(candidate.value)
		if valueOID == "" || claimed[valueOID] {
			continue
		}
		values := candidateWalk(ctx, req, valueOID)
		if len(values) == 0 {
			continue
		}
		descrValues := candidateWalk(ctx, req, candidateOID(candidate.descr))
		for index, raw := range values {
			if !candidateValueValid(raw, candidate.invalid) {
				continue
			}
			value, err := parseFloatValue(raw)
			if err != nil || value <= 0 {
				continue
			}
			sensor := NetworkDeviceSensor{
				ID:       collectorStableID("sensor", string(req.TenantID), string(req.Device.ID), "cand-temp", valueOID, index),
				TenantID: req.TenantID,
				DeviceID: req.Device.ID,
				Class:    "temperature",
				Name:     candidateDescr(descrValues, index, "Temperature"),
				OID:      valueOID + "." + index,
				Unit:     "C",
				Value:    value,
				Status:   "ok",
				Metadata: map[string]string{"sensor_type": "candidate:temperature"},
			}
			sensors = append(sensors, sensor)
			recipes = append(recipes, candidateSensorRecipe(req, sensor, MetricSNMPSensorValue, "C", 0))
		}
	}
	return sensors, recipes
}

func candidateSensorRecipe(req SNMPCollectorDiscoveryContext, sensor NetworkDeviceSensor, metric, unit string, divisor float64) SNMPCollectionRecipe {
	recipe := SNMPCollectionRecipe{
		ID:                    collectorStableID("snmp-recipe", string(req.TenantID), string(req.Device.ID), snmpCollectorModuleSensors, string(SNMPCollectorEntitySensor), string(sensor.ID), metric, sensor.OID, ""),
		TenantID:              req.TenantID,
		DeviceID:              req.Device.ID,
		EntityType:            SNMPCollectorEntitySensor,
		EntityID:              sensor.ID,
		ModuleName:            snmpCollectorModuleSensors,
		MetricName:            metric,
		ValueType:             SNMPCollectorValueGauge,
		OID:                   snmpMIBDisplayOID(sensor.OID),
		NumericOID:            sensor.OID,
		OIDIndex:              sensorOIDIndex(sensor.OID),
		MIB:                   snmpMIBDisplayOID(sensor.OID),
		PollerType:            "snmp",
		Unit:                  unit,
		SampleIntervalSeconds: 60,
		Labels: map[string]string{
			"target_id":    string(req.TargetID),
			"sensor_id":    string(sensor.ID),
			"sensor_class": sensor.Class,
			"sensor_name":  sensor.Name,
			"module":       snmpCollectorModuleSensors,
		},
		Enabled: true,
	}
	if module, _, ok := strings.Cut(recipe.MIB, "::"); ok {
		recipe.MIB = module
	}
	if divisor > 1 {
		recipe.Divisor = divisor
		recipe.HasDivisor = true
	}
	return recipe
}

func sensorOIDIndex(oid string) string {
	if idx := strings.LastIndexByte(oid, '.'); idx > 0 {
		return oid[idx+1:]
	}
	return ""
}

func candidateValueValid(raw string, invalid float64) bool {
	if strings.TrimSpace(raw) == "" {
		return false
	}
	value, err := parseFloatValue(raw)
	if err != nil {
		return false
	}
	if invalid > 0 && (value >= invalid || value <= -invalid) {
		return false
	}
	return value != -1 || invalid == 0
}

func candidateMIBLabel(ref string) string {
	if module, _, ok := strings.Cut(ref, "::"); ok {
		return module
	}
	return ref
}
