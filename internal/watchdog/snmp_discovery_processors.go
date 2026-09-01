package watchdog

import (
	"context"
)

const (
	snmpCollectorModuleProcessors = "processors"

	MetricSNMPProcessorUsage = "watchdog_snmp_processor_usage_percent"
)

type SNMPProcessorsDiscoveryModule struct{}

func (SNMPProcessorsDiscoveryModule) Name() string { return snmpCollectorModuleProcessors }

func (m SNMPProcessorsDiscoveryModule) Discover(ctx context.Context, req SNMPCollectorDiscoveryContext) (SNMPCollectorDiscoveryResult, error) {
	if req.Query == nil {
		return SNMPCollectorDiscoveryResult{}, errSNMPCollectorQueryRequired
	}
	// The matched OS's os_discovery definition contributes first (it knows
	// skip rules and description templates); then every generic candidate
	// source the device answers is collected too — capability is probed, not
	// assumed from vendor or model.
	recipes := discoverDefinitionProcessors(ctx, req)
	recipes = append(recipes, probeCPUCandidates(ctx, req, claimedOIDSet(recipes))...)
	return SNMPCollectorDiscoveryResult{Recipes: recipes}, nil
}

// deviceCPUPercentRecipe duplicates a processor-load OID under the
// device-level metric name that the CPU/Memory overview panel queries.
func deviceCPUPercentRecipe(req SNMPCollectorDiscoveryContext, entityID ID, idx, oid, mib, descr string) SNMPCollectionRecipe {
	recipe := processorRecipe(req, entityID, idx, oid, mib, descr)
	recipe.ID = collectorStableID("snmp-recipe", string(req.TenantID), string(req.Device.ID), snmpCollectorModuleProcessors, string(SNMPCollectorEntityProcessor), string(entityID), MetricSNMPDeviceCPUPercent, idx, "")
	recipe.MetricName = MetricSNMPDeviceCPUPercent
	return recipe
}

func processorRecipe(req SNMPCollectorDiscoveryContext, entityID ID, idx, oid, mib, descr string) SNMPCollectionRecipe {
	return SNMPCollectionRecipe{
		ID:                    collectorStableID("snmp-recipe", string(req.TenantID), string(req.Device.ID), snmpCollectorModuleProcessors, string(SNMPCollectorEntityProcessor), string(entityID), MetricSNMPProcessorUsage, idx, ""),
		TenantID:              req.TenantID,
		DeviceID:              req.Device.ID,
		EntityType:            SNMPCollectorEntityProcessor,
		EntityID:              entityID,
		ModuleName:            snmpCollectorModuleProcessors,
		MetricName:            MetricSNMPProcessorUsage,
		ValueType:             SNMPCollectorValueGauge,
		OID:                   snmpMIBDisplayOID(oid),
		NumericOID:            oid,
		OIDIndex:              idx,
		MIB:                   mib,
		PollerType:            "snmp",
		Unit:                  "percent",
		SampleIntervalSeconds: 60,
		Labels: map[string]string{
			"target_id":   string(req.TargetID),
			"module":      snmpCollectorModuleProcessors,
			"description": descr,
		},
		Enabled: true,
	}
}
