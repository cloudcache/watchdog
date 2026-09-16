package snmpdomain

import (
	"context"
	"strconv"
)

const (
	snmpCollectorModuleMemory = "memory"

	MetricSNMPMemoryUsed  = MetricSNMPDeviceMemUsed
	MetricSNMPMemoryTotal = MetricSNMPDeviceMemTotal
)

var (
	snmpOIDHrStorageDescr      = snmpMIBOID("HOST-RESOURCES-MIB::hrStorageDescr")
	snmpOIDHrStorageSize       = snmpMIBOID("HOST-RESOURCES-MIB::hrStorageSize")
	snmpOIDHrStorageUsed       = snmpMIBOID("HOST-RESOURCES-MIB::hrStorageUsed")
	snmpOIDHrStorageAllocUnits = snmpMIBOID("HOST-RESOURCES-MIB::hrStorageAllocationUnits")

	// hwEntityMemUsage is a percentage (0..100); hwEntityMemSize is bytes.
	snmpOIDHwEntityMemUsage = snmpMIBOID("HUAWEI-ENTITY-EXTENT-MIB::hwEntityMemUsage")
	snmpOIDHwEntityMemSize  = snmpMIBOID("HUAWEI-ENTITY-EXTENT-MIB::hwEntityMemSize")
)

type SNMPMemoryDiscoveryModule struct{}

func (SNMPMemoryDiscoveryModule) Name() string { return snmpCollectorModuleMemory }

func (m SNMPMemoryDiscoveryModule) Discover(ctx context.Context, req DiscoveryContext) (DiscoveryResult, error) {
	if req.Query == nil {
		return DiscoveryResult{}, errSNMPCollectorQueryRequired
	}
	// Definition-driven mempools first, then every generic candidate the
	// device answers (probed, never vendor-gated), then RAM rows from
	// HOST-RESOURCES for hosts that expose it.
	recipes := discoverDefinitionMempools(ctx, req)
	recipes = append(recipes, probeMemoryCandidates(ctx, req, claimedOIDSet(recipes))...)
	if len(recipes) == 0 {
		recipes = m.discoverHostResources(ctx, req)
	}
	return DiscoveryResult{Recipes: recipes}, nil
}

func (m SNMPMemoryDiscoveryModule) discoverHostResources(ctx context.Context, req DiscoveryContext) []Recipe {
	descrs := walkColumn(ctx, req, snmpOIDHrStorageDescr)
	units := walkColumn(ctx, req, snmpOIDHrStorageAllocUnits)
	recipes := make([]Recipe, 0, len(descrs)*2)
	for index, descr := range descrs {
		if index == 0 {
			continue
		}
		name := cleanSNMPValue(descr)
		if name == "" {
			continue
		}
		idx := strconv.FormatUint(index, 10)
		entityID := collectorStableID("memory", "", string(req.Device.ID), idx)
		allocUnits := parseUintValue(units[index])
		multiplier := float64(allocUnits)
		if multiplier == 0 {
			multiplier = 1
		}
		for _, def := range []struct {
			metric, oid string
		}{
			{MetricSNMPMemoryUsed, snmpOIDHrStorageUsed},
			{MetricSNMPMemoryTotal, snmpOIDHrStorageSize},
		} {
			recipes = append(recipes, memoryRecipe(req, entityID, def.metric, idx, def.oid+"."+idx, "HOST-RESOURCES-MIB", name, multiplier, map[string]string{
				"alloc_units": strconv.FormatUint(allocUnits, 10),
			}))
		}
	}
	return recipes
}

func memoryRecipe(req DiscoveryContext, entityID string, metric, idx, oid, mib, name string, multiplier float64, options map[string]string) Recipe {
	recipe := Recipe{
		ID:                    collectorStableID("snmp-recipe", "", string(req.Device.ID), snmpCollectorModuleMemory, string(EntityMemory), string(entityID), metric, idx, ""),
		DeviceID:              req.Device.ID,
		EntityType:            EntityMemory,
		EntityID:              entityID,
		ModuleName:            snmpCollectorModuleMemory,
		MetricName:            metric,
		ValueType:             ValueGauge,
		OID:                   snmpMIBDisplayOID(oid),
		NumericOID:            oid,
		OIDIndex:              idx,
		MIB:                   mib,
		PollerType:            "snmp",
		Unit:                  "bytes",
		SampleIntervalSeconds: 60,
		Labels: map[string]string{
			"target_id":   string(req.TargetID),
			"module":      snmpCollectorModuleMemory,
			"description": name,
		},
		Options: options,
		Enabled: true,
	}
	if multiplier != 1 {
		recipe.Multiplier = multiplier
		recipe.HasMultiplier = true
	}
	return recipe
}
