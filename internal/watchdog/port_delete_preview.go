package watchdog

import (
	"context"
)

// PLAT-04 port delete impact preview: a port's delete cascades its owned rows
// (interface addresses, transceiver, policy) and detaches the aggregate graphs
// and billing accounts that referenced it as a member, plus export tasks and
// device sensors whose port_id is set null.

type PortDeletePreview struct {
	Port    NetworkPort          `json:"port"`
	Impacts []TargetDeleteImpact `json:"impacts"`
}

type PortDeletePreviewRepository interface {
	PreviewPortDelete(ctx context.Context, tenantID, portID ID) (PortDeletePreview, error)
}

func (s *MySQLStore) PreviewPortDelete(ctx context.Context, tenantID, portID ID) (PortDeletePreview, error) {
	port, err := s.GetPort(ctx, tenantID, portID)
	if err != nil {
		return PortDeletePreview{}, err
	}
	preview := PortDeletePreview{Port: port}

	graphs, err := collectImpactItems(ctx, s.db, `
		SELECT g.id, g.name
		FROM aggregate_graph_ports gp
		INNER JOIN aggregate_graphs g ON g.id = gp.aggregate_graph_id
		WHERE gp.tenant_id = ? AND gp.port_id = ?
		ORDER BY g.name
	`, tenantID, portID)
	if err != nil {
		return PortDeletePreview{}, err
	}
	if len(graphs) > 0 {
		preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
			ResourceType: "aggregate_graph", Behavior: "detached", Count: len(graphs), Items: capImpactItems(graphs),
			Detail: "graphs keep aggregating but lose this port",
		})
	}

	for _, counted := range []struct {
		resourceType string
		behavior     string
		detail       string
		query        string
	}{
		{"network_interface_address", "deleted", "", `SELECT COUNT(*) FROM network_interface_addresses WHERE tenant_id = ? AND port_id = ?`},
		{"network_port_transceiver", "deleted", "", `SELECT COUNT(*) FROM network_port_transceivers WHERE tenant_id = ? AND port_id = ?`},
		{"port_policy", "deleted", "", `SELECT COUNT(*) FROM port_policies WHERE tenant_id = ? AND port_id = ?`},
		{"billing_account", "detached", "billing accounts keep their history but lose this port", `SELECT COUNT(*) FROM billing_account_ports WHERE tenant_id = ? AND port_id = ?`},
		{"export_task", "detached", "tasks survive with their port reference cleared", `SELECT COUNT(*) FROM export_tasks WHERE tenant_id = ? AND port_id = ?`},
	} {
		var count int
		if err := s.db.QueryRowContext(ctx, counted.query, tenantID, portID).Scan(&count); err != nil {
			return PortDeletePreview{}, err
		}
		if count > 0 {
			preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
				ResourceType: counted.resourceType, Behavior: counted.behavior, Count: count, Detail: counted.detail,
			})
		}
	}

	preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
		ResourceType: "metric_series", Behavior: "deleted",
		Detail: `VictoriaMetrics series matching {port_id="` + string(portID) + `"}`,
	})
	return preview, nil
}
