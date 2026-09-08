package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
)

const dashboardColumns = `
	id, tenant_id, COALESCE(owner_id, ''), name, COALESCE(description, ''),
	layout_json, version, created_at, updated_at`

func scanDashboard(row rowScanner) (Dashboard, error) {
	var d Dashboard
	var layout []byte
	if err := row.Scan(&d.ID, &d.TenantID, &d.OwnerID, &d.Name, &d.Description,
		&layout, &d.Version, &d.CreatedAt, &d.UpdatedAt); err != nil {
		return Dashboard{}, err
	}
	d.Layout = layout
	return d, nil
}

func (s *MySQLStore) ListDashboards(ctx context.Context, tenantID ID, filter DashboardListFilter) ([]Dashboard, int64, error) {
	filter, err := normalizeDashboardListFilter(filter)
	if err != nil {
		return nil, 0, err
	}
	where := ` FROM dashboards WHERE tenant_id = ?`
	args := []any{tenantID}
	if filter.OwnerID != "" {
		where += ` AND owner_id = ?`
		args = append(args, filter.OwnerID)
	}
	if filter.Search != "" {
		like := "%" + escapeSQLLike(filter.Search) + "%"
		where += ` AND (name LIKE ? OR COALESCE(description, '') LIKE ?)`
		args = append(args, like, like)
	}
	var total int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*)`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	sortColumns := map[string]string{
		"name": "name", "owner_id": "owner_id", "version": "version",
		"created_at": "created_at", "updated_at": "updated_at",
	}
	direction := "ASC"
	if filter.Desc {
		direction = "DESC"
	}
	queryArgs := append(append([]any(nil), args...), filter.Limit, filter.Offset)
	rows, err := s.db.QueryContext(ctx, `SELECT `+dashboardColumns+where+
		` ORDER BY `+sortColumns[filter.Sort]+` `+direction+`, id `+direction+` LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	dashboards := make([]Dashboard, 0)
	for rows.Next() {
		d, err := scanDashboard(rows)
		if err != nil {
			return nil, 0, err
		}
		dashboards = append(dashboards, d)
	}
	return dashboards, total, rows.Err()
}

func (s *MySQLStore) ListDashboardGraphOptions(ctx context.Context, tenantID ID, filter DashboardGraphOptionListFilter) ([]AggregateGraph, int64, error) {
	filter, err := normalizeDashboardGraphOptionListFilter(filter)
	if err != nil {
		return nil, 0, err
	}
	where := ` WHERE tenant_id = ?`
	args := []any{tenantID}
	if filter.Search != "" {
		like := "%" + escapeSQLLike(filter.Search) + "%"
		where += ` AND (name LIKE ? OR COALESCE(description, '') LIKE ?)`
		args = append(args, like, like)
	}
	var total int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM aggregate_graphs`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	queryArgs := append(append([]any(nil), args...), filter.Limit, filter.Offset)
	rows, err := s.db.QueryContext(ctx, aggregateGraphSelect()+where+` ORDER BY name, id LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	graphs := make([]AggregateGraph, 0, filter.Limit)
	for rows.Next() {
		graph, scanErr := scanAggregateGraph(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		graphs = append(graphs, graph)
	}
	return graphs, total, rows.Err()
}

func (s *MySQLStore) GetDashboard(ctx context.Context, tenantID, id ID) (Dashboard, error) {
	return scanDashboard(s.db.QueryRowContext(ctx, `
		SELECT `+dashboardColumns+`
		FROM dashboards
		WHERE tenant_id = ? AND id = ?
	`, tenantID, id))
}

func (s *MySQLStore) CreateDashboard(ctx context.Context, dashboard Dashboard) (Dashboard, error) {
	dashboard, err := normalizeDashboard(dashboard)
	if err != nil {
		return Dashboard{}, err
	}
	if dashboard.ID == "" {
		id, err := newManagementID()
		if err != nil {
			return Dashboard{}, err
		}
		dashboard.ID = id
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO dashboards (id, tenant_id, owner_id, name, description, layout_json, version)
		VALUES (?, ?, NULLIF(?, ''), ?, ?, ?, ?)
	`, dashboard.ID, dashboard.TenantID, dashboard.OwnerID, dashboard.Name,
		dashboard.Description, string(dashboard.Layout), dashboardDefaultVersion); err != nil {
		return Dashboard{}, normalizeDashboardWriteError(err)
	}
	return s.GetDashboard(ctx, dashboard.TenantID, dashboard.ID)
}

// UpdateDashboard replaces the mutable fields and bumps the version. It returns
// sql.ErrNoRows when the dashboard does not exist in the tenant, so the API can
// answer 404 rather than silently succeeding.

func (s *MySQLStore) UpdateDashboard(ctx context.Context, dashboard Dashboard, expectedVersion uint32) (Dashboard, error) {
	dashboard, err := normalizeDashboard(dashboard)
	if err != nil {
		return Dashboard{}, err
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE dashboards
		SET name = ?, description = ?, layout_json = ?, version = version + 1,
		    updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND version = ?
	`, dashboard.Name, dashboard.Description, string(dashboard.Layout), dashboard.TenantID, dashboard.ID, expectedVersion)
	if err != nil {
		return Dashboard{}, normalizeDashboardWriteError(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return Dashboard{}, err
	}
	if affected == 0 {
		if _, getErr := s.GetDashboard(ctx, dashboard.TenantID, dashboard.ID); errors.Is(getErr, sql.ErrNoRows) {
			return Dashboard{}, sql.ErrNoRows
		} else if getErr != nil {
			return Dashboard{}, getErr
		}
		return Dashboard{}, ErrDashboardVersionConflict
	}
	return s.GetDashboard(ctx, dashboard.TenantID, dashboard.ID)
}

func (s *MySQLStore) DeleteDashboard(ctx context.Context, tenantID, id ID, expectedVersion uint32) error {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM dashboards WHERE tenant_id = ? AND id = ? AND version = ?
	`, tenantID, id, expectedVersion)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		if _, getErr := s.GetDashboard(ctx, tenantID, id); errors.Is(getErr, sql.ErrNoRows) {
			return sql.ErrNoRows
		} else if getErr != nil {
			return getErr
		}
		return ErrDashboardVersionConflict
	}
	return nil
}

func (s *MySQLStore) ResolveDashboardGraphReferences(ctx context.Context, tenantID ID, graphIDs []ID) ([]DashboardGraphReference, error) {
	if len(graphIDs) == 0 {
		return []DashboardGraphReference{}, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(graphIDs)), ",")
	args := make([]any, 0, len(graphIDs)+1)
	args = append(args, tenantID)
	for _, id := range graphIDs {
		args = append(args, id)
	}
	graphs := make(map[ID]AggregateGraph, len(graphIDs))
	rows, err := s.db.QueryContext(ctx, aggregateGraphSelect()+`
		WHERE tenant_id = ? AND id IN (`+placeholders+`)`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		graph, scanErr := scanAggregateGraph(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		graphs[graph.ID] = graph
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	series := make(map[ID][]AggregateGraphItem, len(graphs))
	rows, err = s.db.QueryContext(ctx, `
		SELECT id, tenant_id, aggregate_graph_id, sequence, metric, direction,
		       label, graph_type, total, created_at
		FROM aggregate_graph_items
		WHERE tenant_id = ? AND aggregate_graph_id IN (`+placeholders+`)
		ORDER BY aggregate_graph_id, sequence`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item AggregateGraphItem
		if err := rows.Scan(&item.ID, &item.TenantID, &item.GraphID, &item.Sequence,
			&item.Metric, &item.Direction, &item.Label, &item.GraphType, &item.Total, &item.CreatedAt); err != nil {
			return nil, err
		}
		series[item.GraphID] = append(series[item.GraphID], item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	resolved := make([]DashboardGraphReference, 0, len(graphIDs))
	for _, id := range graphIDs {
		ref := DashboardGraphReference{GraphID: id, Series: []AggregateGraphItem{}}
		if graph, ok := graphs[id]; ok {
			graphCopy := graph
			ref.Exists = true
			ref.Graph = &graphCopy
			ref.Series = series[id]
			if ref.Series == nil {
				ref.Series = []AggregateGraphItem{}
			}
		}
		resolved = append(resolved, ref)
	}
	return resolved, nil
}

func normalizeDashboardWriteError(err error) error {
	var mysqlErr *mysqldriver.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return ErrDashboardNameConflict
	}
	return err
}
