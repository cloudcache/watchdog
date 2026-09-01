package watchdog

import (
	"context"
	"errors"
	"time"
)

func (s *MySQLStore) CreateAggregateGraph(ctx context.Context, graph AggregateGraph) (AggregateGraph, error) {
	graph = normalizeAggregateGraph(graph)
	if graph.ID == "" {
		return AggregateGraph{}, errors.New("aggregate graph id is required")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO aggregate_graphs (
			id, tenant_id, name, aggregation, value_mode, unit, description
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, graph.ID, graph.TenantID, graph.Name, graph.Aggregation, graph.ValueMode, graph.Unit, graph.Description)
	if err != nil {
		return AggregateGraph{}, err
	}
	return s.GetAggregateGraph(ctx, graph.TenantID, graph.ID)
}

func (s *MySQLStore) GetAggregateGraph(ctx context.Context, tenantID, graphID ID) (AggregateGraph, error) {
	row := s.db.QueryRowContext(ctx, aggregateGraphSelect()+`
		WHERE tenant_id = ? AND id = ?
	`, tenantID, graphID)
	return scanAggregateGraph(row)
}

func (s *MySQLStore) ListAggregateGraphs(ctx context.Context, tenantID ID) ([]AggregateGraph, error) {
	rows, err := s.db.QueryContext(ctx, aggregateGraphSelect()+`
		WHERE tenant_id = ?
		ORDER BY name
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var graphs []AggregateGraph
	for rows.Next() {
		graph, err := scanAggregateGraph(rows)
		if err != nil {
			return nil, err
		}
		graphs = append(graphs, graph)
	}
	return graphs, rows.Err()
}

func (s *MySQLStore) UpdateAggregateGraph(ctx context.Context, graph AggregateGraph) (AggregateGraph, error) {
	graph = normalizeAggregateGraph(graph)
	if graph.ID == "" {
		return AggregateGraph{}, errors.New("aggregate graph id is required")
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE aggregate_graphs
		SET name = ?, aggregation = ?, value_mode = ?, unit = ?, description = ?,
		    updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, graph.Name, graph.Aggregation, graph.ValueMode, graph.Unit, graph.Description, graph.TenantID, graph.ID)
	if err != nil {
		return AggregateGraph{}, err
	}
	return s.GetAggregateGraph(ctx, graph.TenantID, graph.ID)
}

func (s *MySQLStore) DeleteAggregateGraph(ctx context.Context, tenantID, graphID ID) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM aggregate_graphs
		WHERE tenant_id = ? AND id = ?
	`, tenantID, graphID)
	return err
}

func (s *MySQLStore) ListAggregateGraphItems(ctx context.Context, tenantID, graphID ID) ([]AggregateGraphItem, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, aggregate_graph_id, sequence, metric, direction, label, graph_type, total, created_at
		FROM aggregate_graph_items
		WHERE tenant_id = ? AND aggregate_graph_id = ?
		ORDER BY sequence
	`, tenantID, graphID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AggregateGraphItem
	for rows.Next() {
		var item AggregateGraphItem
		if err := rows.Scan(&item.ID, &item.TenantID, &item.GraphID, &item.Sequence, &item.Metric, &item.Direction, &item.Label, &item.GraphType, &item.Total, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *MySQLStore) ReplaceAggregateGraphItems(ctx context.Context, tenantID, graphID ID, items []AggregateGraphItem) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM aggregate_graph_items
		WHERE tenant_id = ? AND aggregate_graph_id = ?
	`, tenantID, graphID); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO aggregate_graph_items (
			id, tenant_id, aggregate_graph_id, sequence, metric, direction, label, graph_type, total
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for seq, item := range items {
		if item.ID == "" {
			return errors.New("aggregate graph item id is required")
		}
		if item.Metric == "" {
			return errors.New("aggregate graph item metric is required")
		}
		direction := item.Direction
		if direction == "" {
			direction = "other"
		}
		graphType := item.GraphType
		if graphType == "" {
			graphType = "line"
		}
		if _, err := stmt.ExecContext(ctx, item.ID, tenantID, graphID, seq, item.Metric, direction, item.Label, graphType, item.Total); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *MySQLStore) ListAggregateGraphPorts(ctx context.Context, tenantID, graphID ID) ([]AggregateGraphPort, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT aggregate_graph_id, tenant_id, port_id, created_at
		FROM aggregate_graph_ports
		WHERE tenant_id = ? AND aggregate_graph_id = ?
		ORDER BY port_id
	`, tenantID, graphID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ports []AggregateGraphPort
	for rows.Next() {
		var port AggregateGraphPort
		if err := rows.Scan(&port.AggregateGraphID, &port.TenantID, &port.PortID, &port.CreatedAt); err != nil {
			return nil, err
		}
		ports = append(ports, port)
	}
	return ports, rows.Err()
}

func (s *MySQLStore) ReplaceAggregateGraphPorts(ctx context.Context, tenantID, graphID ID, ports []AggregateGraphPort) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM aggregate_graph_ports
		WHERE tenant_id = ? AND aggregate_graph_id = ?
	`, tenantID, graphID); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO aggregate_graph_ports (
			aggregate_graph_id, tenant_id, port_id
		) VALUES (?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, port := range ports {
		if _, err := stmt.ExecContext(ctx, graphID, tenantID, port.PortID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *MySQLStore) AppendAggregateGraphData(ctx context.Context, point AggregateGraphDataPoint) error {
	if point.ID == "" {
		return errors.New("aggregate graph data id is required")
	}
	// ON DUPLICATE KEY UPDATE is a deliberate no-op: once a timestamp is stored
	// its value is frozen so the snapshot stays auditable.
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO aggregate_graph_data (id, tenant_id, aggregate_graph_id, item_id, timestamp, value)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE timestamp = timestamp
	`, point.ID, point.TenantID, point.GraphID, point.ItemID, point.Timestamp, point.Value)
	return err
}

func (s *MySQLStore) ListAggregateGraphData(ctx context.Context, tenantID, graphID ID, start, end time.Time) ([]AggregateGraphDataPoint, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, aggregate_graph_id, item_id, timestamp, value, created_at
		FROM aggregate_graph_data
		WHERE tenant_id = ? AND aggregate_graph_id = ? AND timestamp >= ? AND timestamp <= ?
		ORDER BY timestamp
	`, tenantID, graphID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var points []AggregateGraphDataPoint
	for rows.Next() {
		var point AggregateGraphDataPoint
		if err := rows.Scan(&point.ID, &point.TenantID, &point.GraphID, &point.ItemID, &point.Timestamp, &point.Value, &point.CreatedAt); err != nil {
			return nil, err
		}
		points = append(points, point)
	}
	return points, rows.Err()
}

func (s *MySQLStore) ListAllAggregateGraphs(ctx context.Context) ([]AggregateGraph, error) {
	rows, err := s.db.QueryContext(ctx, aggregateGraphSelect() + `
		ORDER BY tenant_id, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var graphs []AggregateGraph
	for rows.Next() {
		graph, err := scanAggregateGraph(rows)
		if err != nil {
			return nil, err
		}
		graphs = append(graphs, graph)
	}
	return graphs, rows.Err()
}

func normalizeAggregateGraph(graph AggregateGraph) AggregateGraph {
	if graph.Aggregation == "" {
		graph.Aggregation = AggregateSum
	}
	if graph.ValueMode == "" {
		graph.ValueMode = MetricValueCorrected
	}
	return graph
}

func aggregateGraphSelect() string {
	return `
		SELECT id, tenant_id, name, aggregation, value_mode,
		       COALESCE(unit, ''), COALESCE(description, ''), created_at, updated_at
		FROM aggregate_graphs
	`
}

func scanAggregateGraph(row rowScanner) (AggregateGraph, error) {
	var graph AggregateGraph
	err := row.Scan(
		&graph.ID,
		&graph.TenantID,
		&graph.Name,
		&graph.Aggregation,
		&graph.ValueMode,
		&graph.Unit,
		&graph.Description,
		&graph.CreatedAt,
		&graph.UpdatedAt,
	)
	return graph, err
}

var _ AggregateGraphRepository = (*MySQLStore)(nil)
