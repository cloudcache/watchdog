package watchdog

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type SNMPCollectorDiscoveryImportResult struct {
	Device           NetworkDevice
	Ports            int
	Sensors          int
	PhysicalEntities int
	BGPSessions      int
	VLANs            int
	LAGs             int
	Recipes          int
	Events           int
	DeviceModules    int
}

func ImportSNMPCollectorDiscoveryResult(ctx context.Context, network NetworkRepository, collector SNMPCollectorRepository, tenantID ID, device NetworkDevice, result SNMPCollectorDiscoveryResult) (SNMPCollectorDiscoveryImportResult, error) {
	if network == nil {
		return SNMPCollectorDiscoveryImportResult{}, errors.New("network repository is required")
	}
	if collector == nil {
		return SNMPCollectorDiscoveryImportResult{}, errors.New("snmp collector repository is required")
	}
	if tenantID == "" {
		return SNMPCollectorDiscoveryImportResult{}, errors.New("tenant id is required")
	}
	if device.ID == "" {
		return SNMPCollectorDiscoveryImportResult{}, errors.New("network device id is required")
	}

	device = mergeSNMPCollectorDeviceUpdates(tenantID, device, result.DeviceUpdates)
	savedDevice, err := network.UpsertDevice(ctx, device)
	if err != nil {
		return SNMPCollectorDiscoveryImportResult{}, err
	}
	device = savedDevice

	if len(result.Ports) > 0 {
		ports := normalizeSNMPCollectorPorts(tenantID, device.ID, result.Ports)
		if previous, perr := network.ListPorts(ctx, tenantID, device.ID); perr == nil {
			result.Events = append(result.Events, portStatusChangeEvents(tenantID, device.ID, previous, ports)...)
		}
		if err := network.UpsertPorts(ctx, ports); err != nil {
			return SNMPCollectorDiscoveryImportResult{}, err
		}
		result.Ports = ports
	}

	sensors := prepareDeviceSensors(tenantID, device.ID, result.Sensors)
	if len(sensors) > 0 {
		if err := network.UpsertDeviceSensors(ctx, sensors); err != nil {
			return SNMPCollectorDiscoveryImportResult{}, err
		}
		result.Sensors = sensors
	}

	if len(result.PhysicalEntities) > 0 {
		if err := network.UpsertDevicePhysicalEntities(ctx, tenantID, device.ID, result.PhysicalEntities); err != nil {
			return SNMPCollectorDiscoveryImportResult{}, err
		}
	}
	if len(result.BGPSessions) > 0 {
		sessions := normalizeSNMPCollectorBGPSessions(tenantID, device.ID, result.BGPSessions)
		if err := network.UpsertBGPSessions(ctx, sessions); err != nil {
			return SNMPCollectorDiscoveryImportResult{}, err
		}
		result.BGPSessions = sessions
	}
	if len(result.VLANs) > 0 {
		if err := network.UpsertDeviceVLANs(ctx, tenantID, device.ID, result.VLANs); err != nil {
			return SNMPCollectorDiscoveryImportResult{}, err
		}
	}
	if len(result.LAGs) > 0 {
		if err := network.UpsertDeviceLAGGroups(ctx, tenantID, device.ID, result.LAGs); err != nil {
			return SNMPCollectorDiscoveryImportResult{}, err
		}
	}

	recipes := normalizeSNMPCollectorRecipes(tenantID, device.ID, result.Recipes)
	if len(recipes) > 0 {
		if err := collector.UpsertSNMPCollectionRecipes(ctx, recipes); err != nil {
			return SNMPCollectorDiscoveryImportResult{}, err
		}
		// Recipes for entities that vanished from this discovery would keep
		// the device permanently "due" and fail every poll — prune them.
		keep := make([]ID, 0, len(recipes))
		for _, recipe := range recipes {
			keep = append(keep, recipe.ID)
		}
		if _, err := collector.PruneSNMPCollectionRecipes(ctx, tenantID, device.ID, keep); err != nil {
			return SNMPCollectorDiscoveryImportResult{}, err
		}
	}

	moduleNames := snmpCollectorModuleNames(recipes)
	for _, moduleName := range moduleNames {
		if _, err := collector.UpsertSNMPDeviceModule(ctx, SNMPCollectorDeviceModule{
			ID:               collectorStableID("snmp-device-module", string(tenantID), string(device.ID), moduleName),
			TenantID:         tenantID,
			DeviceID:         device.ID,
			ModuleName:       moduleName,
			DiscoveryEnabled: true,
			PollingEnabled:   true,
			DiscoveryStatus:  SNMPCollectorModuleOK,
			PollingStatus:    SNMPCollectorModuleOK,
			LastDiscoveredAt: time.Now().UTC(),
		}); err != nil {
			return SNMPCollectorDiscoveryImportResult{}, err
		}
	}

	events := normalizeSNMPCollectorEvents(tenantID, device.ID, result.Events)
	for _, event := range events {
		if err := collector.CreateSNMPEvent(ctx, event); err != nil {
			return SNMPCollectorDiscoveryImportResult{}, err
		}
	}

	return SNMPCollectorDiscoveryImportResult{
		Device:           device,
		Ports:            len(result.Ports),
		Sensors:          len(result.Sensors),
		PhysicalEntities: len(result.PhysicalEntities),
		BGPSessions:      len(result.BGPSessions),
		VLANs:            len(result.VLANs),
		LAGs:             len(result.LAGs),
		Recipes:          len(recipes),
		Events:           len(events),
		DeviceModules:    len(moduleNames),
	}, nil
}

func mergeSNMPCollectorDeviceUpdates(tenantID ID, device NetworkDevice, updates NetworkDevice) NetworkDevice {
	updates.ID = firstID(updates.ID, device.ID)
	updates.TenantID = tenantID
	updates.TargetID = firstID(updates.TargetID, device.TargetID)
	updates.SNMPProfileID = firstID(updates.SNMPProfileID, device.SNMPProfileID)
	updates.SNMPPort = firstNonZeroUint16(updates.SNMPPort, device.SNMPPort)
	updates.SNMPSecurity = mergeStringMap(device.SNMPSecurity, updates.SNMPSecurity)
	return mergeDiscoveredDevice(device, updates)
}

// portStatusChangeEvents turns oper-status transitions observed between two
// discovery runs into events, so the device Events tab reflects link changes
// even without an SNMP trap receiver deployed.
func portStatusChangeEvents(tenantID, deviceID ID, previous, current []NetworkPort) []SNMPEvent {
	previousStatus := make(map[uint64]NetworkPort, len(previous))
	for _, port := range previous {
		previousStatus[port.IfIndex] = port
	}
	var events []SNMPEvent
	now := time.Now().UTC()
	for _, port := range current {
		before, seen := previousStatus[port.IfIndex]
		if !seen || before.OperStatus == "" || port.OperStatus == "" || before.OperStatus == port.OperStatus {
			continue
		}
		severity := "info"
		eventType := "interface_up"
		if port.OperStatus != "up" && port.OperStatus != "1" {
			severity = "warning"
			eventType = "interface_down"
		}
		events = append(events, SNMPEvent{
			TenantID:   tenantID,
			DeviceID:   deviceID,
			EntityType: SNMPCollectorEntityPort,
			EntityID:   port.ID,
			Source:     "discovery",
			Severity:   severity,
			EventType:  eventType,
			Message:    fmt.Sprintf("Interface %s changed %s -> %s", firstNonEmptySNMPString(port.IfName, port.IfDescr), before.OperStatus, port.OperStatus),
			OccurredAt: now,
		})
	}
	return events
}

func normalizeSNMPCollectorPorts(tenantID, deviceID ID, ports []NetworkPort) []NetworkPort {
	normalized := make([]NetworkPort, 0, len(ports))
	for _, port := range ports {
		port.TenantID = tenantID
		port.DeviceID = deviceID
		if port.ID == "" {
			port.ID = collectorStableID("port", string(tenantID), string(deviceID), fmt.Sprint(port.IfIndex))
		}
		if port.Metadata == nil {
			port.Metadata = map[string]string{}
		}
		normalized = append(normalized, port)
	}
	return normalized
}

func normalizeSNMPCollectorBGPSessions(tenantID, deviceID ID, sessions []BGPSession) []BGPSession {
	normalized := make([]BGPSession, 0, len(sessions))
	for _, session := range sessions {
		session.TenantID = tenantID
		session.DeviceID = deviceID
		if session.ID == "" {
			session.ID = collectorStableID("bgp", string(tenantID), string(deviceID), session.PeerAddr, fmt.Sprint(session.PeerAS), session.AFI, session.SAFI)
		}
		if session.Metadata == nil {
			session.Metadata = map[string]string{}
		}
		normalized = append(normalized, session)
	}
	return normalized
}

func normalizeSNMPCollectorRecipes(tenantID, deviceID ID, recipes []SNMPCollectionRecipe) []SNMPCollectionRecipe {
	normalized := make([]SNMPCollectionRecipe, 0, len(recipes))
	for _, recipe := range recipes {
		recipe.TenantID = tenantID
		recipe.DeviceID = deviceID
		if recipe.ID == "" {
			recipe.ID = collectorStableID("snmp-recipe", string(tenantID), string(deviceID), recipe.ModuleName, string(recipe.EntityType), string(recipe.EntityID), recipe.MetricName, recipe.OIDIndex, recipe.ContextName)
		}
		if recipe.SampleIntervalSeconds == 0 {
			recipe.SampleIntervalSeconds = 60
		}
		if recipe.PollerType == "" {
			recipe.PollerType = "snmp"
		}
		if recipe.Labels == nil {
			recipe.Labels = map[string]string{}
		}
		if recipe.Options == nil {
			recipe.Options = map[string]string{}
		}
		recipe.Enabled = true
		normalized = append(normalized, recipe)
	}
	return normalized
}

func normalizeSNMPCollectorEvents(tenantID, deviceID ID, events []SNMPEvent) []SNMPEvent {
	normalized := make([]SNMPEvent, 0, len(events))
	for _, event := range events {
		event.TenantID = tenantID
		event.DeviceID = deviceID
		if event.ID == "" {
			event.ID = collectorStableID("snmp-event", string(tenantID), string(deviceID), event.Source, event.EventType, event.Message, event.OccurredAt.String())
		}
		if event.OccurredAt.IsZero() {
			event.OccurredAt = time.Now().UTC()
		}
		normalized = append(normalized, event)
	}
	return normalized
}

func snmpCollectorModuleNames(recipes []SNMPCollectionRecipe) []string {
	seen := map[string]struct{}{}
	names := make([]string, 0)
	for _, recipe := range recipes {
		if recipe.ModuleName == "" {
			continue
		}
		if _, ok := seen[recipe.ModuleName]; ok {
			continue
		}
		seen[recipe.ModuleName] = struct{}{}
		names = append(names, recipe.ModuleName)
	}
	return names
}

func firstNonZeroUint16(values ...uint16) uint16 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}
