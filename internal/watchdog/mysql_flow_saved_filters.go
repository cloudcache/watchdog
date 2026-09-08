package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

const flowSavedFilterColumns = `
	f.id, f.tenant_id, COALESCE(f.owner_user_id, ''), COALESCE(u.name, ''),
	f.name, f.description, f.share_scope, f.filter_schema_version, f.filter_json,
	f.row_version, f.created_at, f.updated_at`

func scanFlowSavedFilter(row rowScanner) (FlowSavedFilter, error) {
	var item FlowSavedFilter
	var filterJSON []byte
	if err := row.Scan(
		&item.ID, &item.TenantID, &item.OwnerUserID, &item.OwnerName,
		&item.Name, &item.Description, &item.ShareScope, &item.FilterSchemaVersion, &filterJSON,
		&item.RowVersion, &item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return FlowSavedFilter{}, err
	}
	if item.FilterSchemaVersion != flowSavedFilterSchemaVersion {
		return FlowSavedFilter{}, errors.New("unsupported saved Flow filter schema version")
	}
	if err := json.Unmarshal(filterJSON, &item.Filter); err != nil {
		return FlowSavedFilter{}, err
	}
	canonical, err := flowquery.CanonicalFilter(item.Filter)
	if err != nil {
		return FlowSavedFilter{}, err
	}
	if !reflect.DeepEqual(canonical, item.Filter) {
		return FlowSavedFilter{}, errors.New("saved Flow filter contains a non-canonical AST")
	}
	return item, nil
}

func (s *MySQLStore) ListFlowSavedFilters(ctx context.Context, tenantID ID, query FlowSavedFilterListQuery) ([]FlowSavedFilter, int64, error) {
	query, err := normalizeFlowSavedFilterListQuery(query)
	if err != nil {
		return nil, 0, err
	}
	where, args := flowSavedFilterWhere(tenantID, query)
	var total int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_saved_filters f`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	sortColumns := map[string]string{
		"name": "f.name", "share_scope": "f.share_scope", "owner_user_id": "f.owner_user_id",
		"created_at": "f.created_at", "updated_at": "f.updated_at",
	}
	direction := "ASC"
	if query.Descending {
		direction = "DESC"
	}
	queryArgs := append(append([]any(nil), args...), query.Limit, query.Offset)
	rows, err := s.db.QueryContext(ctx, `SELECT `+flowSavedFilterColumns+`
		FROM flow_saved_filters f LEFT JOIN users u ON u.id = f.owner_user_id`+where+`
		ORDER BY `+sortColumns[query.SortBy]+` `+direction+`, f.id `+direction+`
		LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]FlowSavedFilter, 0, query.Limit)
	for rows.Next() {
		item, err := scanFlowSavedFilter(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func flowSavedFilterWhere(tenantID ID, query FlowSavedFilterListQuery) (string, []any) {
	where := ` WHERE f.tenant_id = ? AND f.deleted_at IS NULL
		AND (f.share_scope = 'tenant' OR f.owner_user_id = ?)`
	args := []any{tenantID, query.ViewerUserID}
	if query.Search != "" {
		like := "%" + escapeSQLLike(query.Search) + "%"
		where += ` AND (f.name LIKE ? ESCAPE '\\' OR f.description LIKE ? ESCAPE '\\')`
		args = append(args, like, like)
	}
	if query.ShareScope != "" {
		where += ` AND f.share_scope = ?`
		args = append(args, query.ShareScope)
	}
	if query.OwnerUserID != "" {
		where += ` AND f.owner_user_id = ?`
		args = append(args, query.OwnerUserID)
	}
	return where, args
}

func (s *MySQLStore) ListFlowSavedFilterOwners(ctx context.Context, tenantID, viewerUserID ID, search string, limit int) ([]FlowSavedFilterOwnerFacet, error) {
	search = strings.TrimSpace(search)
	if tenantID == "" || viewerUserID == "" || len(search) > 255 || limit < 1 || limit > 200 {
		return nil, fmtFlowSavedFilterInvalid("invalid owner facet query")
	}
	where := ` WHERE f.tenant_id = ? AND f.deleted_at IS NULL
		AND (f.share_scope = 'tenant' OR f.owner_user_id = ?)`
	args := []any{tenantID, viewerUserID}
	if search != "" {
		like := "%" + escapeSQLLike(search) + "%"
		where += ` AND (u.name LIKE ? ESCAPE '\\' OR u.email LIKE ? ESCAPE '\\')`
		args = append(args, like, like)
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `SELECT COALESCE(f.owner_user_id, ''), COALESCE(u.name, ''), COUNT(*)
		FROM flow_saved_filters f LEFT JOIN users u ON u.id = f.owner_user_id`+where+`
		GROUP BY f.owner_user_id, u.name
		ORDER BY COALESCE(u.name, ''), COALESCE(f.owner_user_id, '')
		LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]FlowSavedFilterOwnerFacet, 0)
	for rows.Next() {
		var item FlowSavedFilterOwnerFacet
		if err := rows.Scan(&item.OwnerUserID, &item.OwnerName, &item.Count); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *MySQLStore) GetFlowSavedFilter(ctx context.Context, tenantID, viewerUserID, filterID ID) (FlowSavedFilter, error) {
	return scanFlowSavedFilter(s.db.QueryRowContext(ctx, `SELECT `+flowSavedFilterColumns+`
		FROM flow_saved_filters f LEFT JOIN users u ON u.id = f.owner_user_id
		WHERE f.tenant_id = ? AND f.id = ? AND f.deleted_at IS NULL
		  AND (f.share_scope = 'tenant' OR f.owner_user_id = ?)`, tenantID, filterID, viewerUserID))
}

func (s *MySQLStore) CreateFlowSavedFilter(ctx context.Context, item FlowSavedFilter) (FlowSavedFilter, error) {
	item, err := normalizeFlowSavedFilter(item)
	if err != nil {
		return FlowSavedFilter{}, err
	}
	if item.TenantID == "" || item.OwnerUserID == "" {
		return FlowSavedFilter{}, fmtFlowSavedFilterInvalid("tenant and owner are required")
	}
	if item.ID == "" {
		item.ID, err = newManagementID()
		if err != nil {
			return FlowSavedFilter{}, err
		}
	}
	filterJSON, err := json.Marshal(item.Filter)
	if err != nil {
		return FlowSavedFilter{}, err
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO flow_saved_filters (
		id, tenant_id, owner_user_id, name, description, share_scope, filter_schema_version, filter_json
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		item.ID, item.TenantID, item.OwnerUserID, item.Name, item.Description, item.ShareScope,
		item.FilterSchemaVersion, filterJSON,
	); err != nil {
		return FlowSavedFilter{}, err
	}
	return s.GetFlowSavedFilter(ctx, item.TenantID, item.OwnerUserID, item.ID)
}

func (s *MySQLStore) UpdateFlowSavedFilter(ctx context.Context, item FlowSavedFilter, expectedVersion uint64) (FlowSavedFilter, error) {
	item, err := normalizeFlowSavedFilter(item)
	if err != nil {
		return FlowSavedFilter{}, err
	}
	if item.TenantID == "" || item.ID == "" {
		return FlowSavedFilter{}, fmtFlowSavedFilterInvalid("tenant and filter ID are required")
	}
	if expectedVersion == 0 {
		return FlowSavedFilter{}, fmtFlowSavedFilterInvalid("expected row version is required")
	}
	filterJSON, err := json.Marshal(item.Filter)
	if err != nil {
		return FlowSavedFilter{}, err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE flow_saved_filters
		SET name = ?, description = ?, share_scope = ?, filter_schema_version = ?, filter_json = ?,
		    row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND row_version = ? AND deleted_at IS NULL`,
		item.Name, item.Description, item.ShareScope, item.FilterSchemaVersion, filterJSON,
		item.TenantID, item.ID, expectedVersion,
	)
	if err != nil {
		return FlowSavedFilter{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return FlowSavedFilter{}, err
	}
	if affected == 0 {
		return FlowSavedFilter{}, s.flowSavedFilterWriteMiss(ctx, item.TenantID, item.ID)
	}
	return s.GetFlowSavedFilter(ctx, item.TenantID, item.OwnerUserID, item.ID)
}

func (s *MySQLStore) DeleteFlowSavedFilter(ctx context.Context, tenantID, filterID ID, expectedVersion uint64) error {
	if expectedVersion == 0 {
		return fmtFlowSavedFilterInvalid("expected row version is required")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE flow_saved_filters
		SET deleted_at = CURRENT_TIMESTAMP(3), row_version = row_version + 1,
		    updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND row_version = ? AND deleted_at IS NULL`,
		tenantID, filterID, expectedVersion,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return s.flowSavedFilterWriteMiss(ctx, tenantID, filterID)
	}
	return nil
}

func (s *MySQLStore) flowSavedFilterWriteMiss(ctx context.Context, tenantID, filterID ID) error {
	var rowVersion uint64
	err := s.db.QueryRowContext(ctx, `SELECT row_version FROM flow_saved_filters
		WHERE tenant_id = ? AND id = ? AND deleted_at IS NULL`, tenantID, filterID).Scan(&rowVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return sql.ErrNoRows
	}
	if err != nil {
		return err
	}
	return ErrFlowSavedFilterVersionConflict
}

func fmtFlowSavedFilterInvalid(message string) error {
	return fmt.Errorf("%w: %s", ErrFlowSavedFilterInvalid, message)
}
