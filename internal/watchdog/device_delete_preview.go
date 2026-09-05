package watchdog

import (
	"context"
)

// PLAT-04 device delete impact preview: mirrors the target preview for network
// devices, whose delete cascades across ports, sensors, BGP sessions, VLANs,
// LAG groups, physical entities, interface addresses, SNMP recipes/modules and
// discovery jobs, and detaches aggregate graphs that charted this device's
// ports.

type DeviceDeletePreview struct {
	Device  NetworkDevice        `json:"device"`
	Impacts []TargetDeleteImpact `json:"impacts"`
}

type DeviceDeletePreviewRepository interface {
	PreviewDeviceDelete(ctx context.Context, tenantID, deviceID ID) (DeviceDeletePreview, error)
}

func (s *MySQLStore) PreviewDeviceDelete(ctx context.Context, tenantID, deviceID ID) (DeviceDeletePreview, error) {
	device, err := s.GetDevice(ctx, tenantID, deviceID)
	if err != nil {
		return DeviceDeletePreview{}, err
	}
	preview := DeviceDeletePreview{Device: device}

	ports, err := collectImpactItems(ctx, s.db, `
		SELECT id, COALESCE(NULLIF(if_name, ''), if_descr) FROM network_ports
		WHERE tenant_id = ? AND device_id = ? ORDER BY if_index
	`, tenantID, deviceID)
	if err != nil {
		return DeviceDeletePreview{}, err
	}
	if len(ports) > 0 {
		preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
			ResourceType: "network_port", Behavior: "deleted", Count: len(ports), Items: capImpactItems(ports),
		})
		graphs, err := collectImpactItems(ctx, s.db, `
			SELECT DISTINCT g.id, g.name
			FROM aggregate_graph_ports gp
			INNER JOIN network_ports p ON p.id = gp.port_id
			INNER JOIN aggregate_graphs g ON g.id = gp.aggregate_graph_id
			WHERE p.tenant_id = ? AND p.device_id = ?
			ORDER BY g.name
		`, tenantID, deviceID)
		if err != nil {
			return DeviceDeletePreview{}, err
		}
		if len(graphs) > 0 {
			preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
				ResourceType: "aggregate_graph", Behavior: "detached", Count: len(graphs), Items: capImpactItems(graphs),
				Detail: "graphs keep aggregating but lose this device's member ports",
			})
		}
	}

	for _, counted := range []struct {
		resourceType string
		query        string
	}{
		{"network_device_sensor", `SELECT COUNT(*) FROM network_device_sensors WHERE tenant_id = ? AND device_id = ?`},
		{"bgp_session", `SELECT COUNT(*) FROM bgp_sessions WHERE tenant_id = ? AND device_id = ?`},
		{"device_vlan", `SELECT COUNT(*) FROM device_vlans WHERE tenant_id = ? AND device_id = ?`},
		{"device_lag_group", `SELECT COUNT(*) FROM device_lag_groups WHERE tenant_id = ? AND device_id = ?`},
		{"device_physical_entity", `SELECT COUNT(*) FROM device_physical_entities WHERE tenant_id = ? AND device_id = ?`},
		{"network_interface_address", `SELECT COUNT(*) FROM network_interface_addresses WHERE tenant_id = ? AND device_id = ?`},
		{"snmp_collection_recipe", `SELECT COUNT(*) FROM snmp_collection_recipes WHERE tenant_id = ? AND device_id = ?`},
	} {
		var count int
		if err := s.db.QueryRowContext(ctx, counted.query, tenantID, deviceID).Scan(&count); err != nil {
			return DeviceDeletePreview{}, err
		}
		if count > 0 {
			preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
				ResourceType: counted.resourceType, Behavior: "deleted", Count: count,
			})
		}
	}

	preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
		ResourceType: "metric_series", Behavior: "deleted",
		Detail: `VictoriaMetrics series matching {device_id="` + string(deviceID) + `"}`,
	})
	return preview, nil
}
