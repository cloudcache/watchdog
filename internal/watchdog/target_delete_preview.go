package watchdog

import (
	"context"
	"database/sql"
)

// PLAT-04 delete impact preview: before a target is deleted the API can list
// every dependent resource, split into rows that will be deleted with it and
// rows that survive but lose their reference. The preview is read-only.

const targetDeletePreviewItemCap = 20

type TargetDeleteImpactItem struct {
	ID   ID     `json:"id"`
	Name string `json:"name"`
}

type TargetDeleteImpact struct {
	ResourceType string                   `json:"resource_type"`
	Behavior     string                   `json:"behavior"` // "deleted" or "detached"
	Count        int                      `json:"count"`
	Detail       string                   `json:"detail,omitempty"`
	Items        []TargetDeleteImpactItem `json:"items,omitempty"`
}

type TargetDeletePreview struct {
	Target  Target               `json:"target"`
	Impacts []TargetDeleteImpact `json:"impacts"`
}

type TargetDeletePreviewRepository interface {
	PreviewTargetDelete(ctx context.Context, tenantID, targetID ID) (TargetDeletePreview, error)
}

func (s *MySQLStore) PreviewTargetDelete(ctx context.Context, tenantID, targetID ID) (TargetDeletePreview, error) {
	target, err := s.GetTarget(ctx, tenantID, targetID)
	if err != nil {
		return TargetDeletePreview{}, err
	}
	preview := TargetDeletePreview{Target: target}

	devices, err := collectImpactItems(ctx, s.db, `
		SELECT id, COALESCE(sys_name, '') FROM network_devices
		WHERE tenant_id = ? AND target_id = ? ORDER BY id
	`, tenantID, targetID)
	if err != nil {
		return TargetDeletePreview{}, err
	}
	if len(devices) > 0 {
		preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
			ResourceType: "network_device", Behavior: "deleted", Count: len(devices), Items: capImpactItems(devices),
		})
		var portCount int
		if err := s.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM network_ports p
			INNER JOIN network_devices d ON d.id = p.device_id
			WHERE d.tenant_id = ? AND d.target_id = ?
		`, tenantID, targetID).Scan(&portCount); err != nil {
			return TargetDeletePreview{}, err
		}
		if portCount > 0 {
			preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
				ResourceType: "network_port", Behavior: "deleted", Count: portCount,
			})
		}
		graphs, err := collectImpactItems(ctx, s.db, `
			SELECT DISTINCT g.id, g.name
			FROM aggregate_graph_ports gp
			INNER JOIN network_ports p ON p.id = gp.port_id
			INNER JOIN network_devices d ON d.id = p.device_id
			INNER JOIN aggregate_graphs g ON g.id = gp.aggregate_graph_id
			WHERE d.tenant_id = ? AND d.target_id = ?
			ORDER BY g.name
		`, tenantID, targetID)
		if err != nil {
			return TargetDeletePreview{}, err
		}
		if len(graphs) > 0 {
			preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
				ResourceType: "aggregate_graph", Behavior: "detached", Count: len(graphs), Items: capImpactItems(graphs),
				Detail: "graphs keep aggregating but lose this target's member ports",
			})
		}
	}

	agents, err := collectImpactItems(ctx, s.db, `
		SELECT id, agent_type FROM target_agents
		WHERE tenant_id = ? AND target_id = ? ORDER BY id
	`, tenantID, targetID)
	if err != nil {
		return TargetDeletePreview{}, err
	}
	if len(agents) > 0 {
		preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
			ResourceType: "agent", Behavior: "deleted", Count: len(agents), Items: capImpactItems(agents),
		})
		var projectionCount int
		if err := s.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM collector_agents c
			INNER JOIN target_agents a ON a.id = c.id AND a.tenant_id = c.tenant_id
			WHERE a.tenant_id = ? AND a.target_id = ? AND c.created_by = ?
		`, tenantID, targetID, collectorCompatibilityActor).Scan(&projectionCount); err != nil {
			return TargetDeletePreview{}, err
		}
		if projectionCount > 0 {
			preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
				ResourceType: "collector_projection", Behavior: "deleted", Count: projectionCount,
			})
		}
	}

	for _, counted := range []struct {
		resourceType string
		behavior     string
		detail       string
		query        string
	}{
		{"agent_run_history", "deleted", "", `SELECT COUNT(*) FROM agent_run_history WHERE tenant_id = ? AND target_id = ?`},
		{"metric_retention_policy", "deleted", "", `SELECT COUNT(*) FROM metric_retention_policies WHERE tenant_id = ? AND target_id = ?`},
		{"export_task", "detached", "tasks survive with their target reference cleared", `SELECT COUNT(*) FROM export_tasks WHERE tenant_id = ? AND target_id = ?`},
	} {
		var count int
		if err := s.db.QueryRowContext(ctx, counted.query, tenantID, targetID).Scan(&count); err != nil {
			return TargetDeletePreview{}, err
		}
		if count > 0 {
			preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
				ResourceType: counted.resourceType, Behavior: counted.behavior, Count: count, Detail: counted.detail,
			})
		}
	}

	preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
		ResourceType: "metric_series", Behavior: "deleted",
		Detail: `VictoriaMetrics series matching {target_id="` + string(targetID) + `"}`,
	})
	return preview, nil
}

func collectImpactItems(ctx context.Context, db *sql.DB, query string, args ...any) ([]TargetDeleteImpactItem, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []TargetDeleteImpactItem
	for rows.Next() {
		var item TargetDeleteImpactItem
		if err := rows.Scan(&item.ID, &item.Name); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func capImpactItems(items []TargetDeleteImpactItem) []TargetDeleteImpactItem {
	if len(items) > targetDeletePreviewItemCap {
		return items[:targetDeletePreviewItemCap]
	}
	return items
}
