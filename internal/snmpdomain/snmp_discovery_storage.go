package snmpdomain

import (
	"context"
	"strconv"
)

const (
	snmpCollectorModuleStorage = "storage"

	MetricSNMPStorageUsed  = "watchdog_snmp_storage_used_bytes"
	MetricSNMPStorageTotal = "watchdog_snmp_storage_total_bytes"
)

type SNMPStorageDiscoveryModule struct{}

func (SNMPStorageDiscoveryModule) Name() string { return snmpCollectorModuleStorage }

func (m SNMPStorageDiscoveryModule) Discover(ctx context.Context, req DiscoveryContext) (DiscoveryResult, error) {
	if req.Query == nil {
		return DiscoveryResult{}, errSNMPCollectorQueryRequired
	}
	if recipes := discoverDefinitionStorage(ctx, req); len(recipes) > 0 {
		return DiscoveryResult{Recipes: recipes}, nil
	}
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
		entityID := collectorStableID("storage", "", string(req.Device.ID), idx)
		allocUnits := parseUintValue(units[index])
		multiplier := float64(allocUnits)
		if multiplier == 0 {
			multiplier = 1
		}
		for _, def := range []struct {
			metric string
			oid    string
		}{
			{MetricSNMPStorageUsed, snmpOIDHrStorageUsed},
			{MetricSNMPStorageTotal, snmpOIDHrStorageSize},
		} {
			recipe := Recipe{
				ID:                    collectorStableID("snmp-recipe", "", string(req.Device.ID), snmpCollectorModuleStorage, string(EntityStorage), string(entityID), def.metric, idx, ""),
				DeviceID:              req.Device.ID,
				EntityType:            EntityStorage,
				EntityID:              entityID,
				ModuleName:            snmpCollectorModuleStorage,
				MetricName:            def.metric,
				ValueType:             ValueGauge,
				OID:                   snmpMIBDisplayOID(def.oid + "." + idx),
				NumericOID:            def.oid + "." + idx,
				OIDIndex:              idx,
				MIB:                   "HOST-RESOURCES-MIB",
				PollerType:            "snmp",
				Unit:                  "bytes",
				SampleIntervalSeconds: 60,
				Labels: map[string]string{
					"target_id":   string(req.TargetID),
					"module":      snmpCollectorModuleStorage,
					"description": name,
				},
				Options: map[string]string{
					"alloc_units": strconv.FormatUint(allocUnits, 10),
				},
				Enabled: true,
			}
			if multiplier != 1 {
				recipe.Multiplier = multiplier
				recipe.HasMultiplier = true
			}
			recipes = append(recipes, recipe)
		}
	}
	return DiscoveryResult{Recipes: recipes}, nil
}
