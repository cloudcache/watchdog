package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
)

var ErrTargetHostExists = errors.New("target host already exists")

func (s *MySQLStore) ListTargets(ctx context.Context, tenantID ID) ([]Target, error) {
	rows, err := s.db.QueryContext(ctx, targetSelect()+`
		WHERE tenant_id = ?
		ORDER BY name
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var targets []Target
	for rows.Next() {
		target, err := scanTarget(rows)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

func (s *MySQLStore) ListTargetsPage(ctx context.Context, tenantID ID, all bool, allowedIDs []ID, filter TargetPageFilter) ([]Target, string, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	// A non-admin with no granted targets sees nothing; skip the query.
	if !all && len(allowedIDs) == 0 {
		return nil, "", nil
	}
	query := targetSelect() + ` WHERE tenant_id = ?`
	args := []any{tenantID}
	if filter.ExcludeKind != "" {
		query += ` AND kind != ?`
		args = append(args, filter.ExcludeKind)
	}
	if !all {
		query += ` AND id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(allowedIDs)), ",") + `)`
		for _, id := range allowedIDs {
			args = append(args, id)
		}
	}
	if filter.Cursor != "" {
		name, id, err := decodeStringCursor(filter.Cursor)
		if err != nil {
			return nil, "", err
		}
		query += ` AND (name > ? OR (name = ? AND id > ?))`
		args = append(args, name, name, id)
	}
	query += ` ORDER BY name, id LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var targets []Target
	for rows.Next() {
		target, err := scanTarget(rows)
		if err != nil {
			return nil, "", err
		}
		targets = append(targets, target)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	nextCursor := ""
	if len(targets) > limit {
		targets = targets[:limit]
		last := targets[limit-1]
		nextCursor = encodeStringCursor(last.Name, last.ID)
	}
	return targets, nextCursor, nil
}

var targetTableSortColumns = map[string]string{
	"":           "name",
	"name":       "name",
	"kind":       "kind",
	"host":       "host",
	"status":     "status",
	"updated_at": "updated_at",
}

func (s *MySQLStore) ListTargetsTablePage(ctx context.Context, tenantID ID, all bool, allowedIDs []ID, filter TargetTableQuery) ([]Target, int, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if !all && len(allowedIDs) == 0 {
		return nil, 0, nil
	}
	where, args := targetTableWhere(tenantID, all, allowedIDs, filter)
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM targets`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	sortColumn := targetTableSortColumns[filter.Sort]
	if sortColumn == "" {
		sortColumn = "name"
	}
	direction := "ASC"
	if filter.Desc {
		direction = "DESC"
	}
	query := targetSelect() + where + ` ORDER BY ` + sortColumn + ` ` + direction + `, id ` + direction + ` LIMIT ? OFFSET ?`
	pageArgs := append(append([]any{}, args...), limit, max(0, filter.Offset))
	rows, err := s.db.QueryContext(ctx, query, pageArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	targets := make([]Target, 0, min(limit, total))
	for rows.Next() {
		target, err := scanTarget(rows)
		if err != nil {
			return nil, 0, err
		}
		targets = append(targets, target)
	}
	return targets, total, rows.Err()
}

func targetTableWhere(tenantID ID, all bool, allowedIDs []ID, filter TargetTableQuery) (string, []any) {
	where := ` WHERE tenant_id = ?`
	args := []any{tenantID}
	if !all {
		where += ` AND id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(allowedIDs)), ",") + `)`
		for _, id := range allowedIDs {
			args = append(args, id)
		}
	}
	if filter.ExcludeKind != "" {
		where += ` AND kind <> ?`
		args = append(args, filter.ExcludeKind)
	}
	if filter.Kind != "" {
		where += ` AND kind = ?`
		args = append(args, filter.Kind)
	}
	if filter.Status != "" {
		where += ` AND status = ?`
		args = append(args, filter.Status)
	}
	if search := strings.TrimSpace(filter.Search); search != "" {
		like := "%" + escapeSQLLike(search) + "%"
		where += ` AND (name LIKE ? OR host LIKE ? OR kind LIKE ? OR status LIKE ?)`
		args = append(args, like, like, like, like)
	}
	return where, args
}

// GetTargetsByIDs batch-loads targets by id for a page of device summaries,
// avoiding a per-row lookup. Missing ids are simply absent from the map.
func (s *MySQLStore) GetTargetsByIDs(ctx context.Context, tenantID ID, ids []ID) (map[ID]Target, error) {
	out := make(map[ID]Target, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	query := targetSelect() + ` WHERE tenant_id = ? AND id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + `)`
	args := []any{tenantID}
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		target, err := scanTarget(rows)
		if err != nil {
			return nil, err
		}
		out[target.ID] = target
	}
	return out, rows.Err()
}

func (s *MySQLStore) GetTarget(ctx context.Context, tenantID, targetID ID) (Target, error) {
	row := s.db.QueryRowContext(ctx, targetSelect()+`
		WHERE tenant_id = ? AND id = ?
	`, tenantID, targetID)
	return scanTarget(row)
}

func (s *MySQLStore) CreateTarget(ctx context.Context, target Target) (Target, error) {
	if target.ID == "" {
		return Target{}, errors.New("target id is required")
	}
	labelsJSON, err := encodeStringMapJSON(target.Labels)
	if err != nil {
		return Target{}, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO targets (
			id, tenant_id, name, kind, host, mgmt_ip, status, labels_json
		) VALUES (?, ?, ?, ?, ?, INET6_ATON(?), ?, ?)
	`, target.ID, target.TenantID, target.Name, target.Kind, target.Host, target.Host, defaultString(target.Status, "pending"), labelsJSON)
	if err != nil {
		return Target{}, normalizeTargetWriteError(err)
	}
	return s.GetTarget(ctx, target.TenantID, target.ID)
}

func (s *MySQLStore) CreateNetworkTarget(ctx context.Context, target Target, device NetworkDevice) (Target, NetworkDevice, error) {
	if target.ID == "" || device.ID == "" {
		return Target{}, NetworkDevice{}, errors.New("target and device ids are required")
	}
	labelsJSON, err := encodeStringMapJSON(target.Labels)
	if err != nil {
		return Target{}, NetworkDevice{}, err
	}
	securityJSON, err := encodeStringMapJSON(device.SNMPSecurity)
	if err != nil {
		return Target{}, NetworkDevice{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Target{}, NetworkDevice{}, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO targets (
			id, tenant_id, name, kind, host, mgmt_ip, status, labels_json
		) VALUES (?, ?, ?, ?, ?, INET6_ATON(?), ?, ?)
	`, target.ID, target.TenantID, target.Name, target.Kind, target.Host, target.Host, defaultString(target.Status, "pending"), labelsJSON); err != nil {
		return Target{}, NetworkDevice{}, normalizeTargetWriteError(err)
	}
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO network_devices (
			id, tenant_id, target_id, vendor, model, platform, os_name, os_version,
			sys_object_id, sys_name, sys_descr, sys_location, uptime_seconds,
			snmp_profile_id, snmp_port, snmp_security_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?)
	`, device.ID, device.TenantID, device.TargetID, device.Vendor, device.Model,
		device.Platform, device.OSName, device.OSVersion, device.SysObjectID,
		device.SysName, device.SysDescr, device.SysLocation, uint64(device.Uptime.Seconds()),
		device.SNMPProfileID, normalizeSNMPPort(device.SNMPPort), securityJSON); err != nil {
		return Target{}, NetworkDevice{}, err
	}
	if err = tx.Commit(); err != nil {
		return Target{}, NetworkDevice{}, err
	}
	createdTarget, err := s.GetTarget(ctx, target.TenantID, target.ID)
	if err != nil {
		return Target{}, NetworkDevice{}, err
	}
	createdDevice, err := s.GetDevice(ctx, device.TenantID, device.ID)
	if err != nil {
		return Target{}, NetworkDevice{}, err
	}
	return createdTarget, createdDevice, nil
}

func (s *MySQLStore) UpdateTarget(ctx context.Context, target Target) (Target, error) {
	labelsJSON, err := encodeStringMapJSON(target.Labels)
	if err != nil {
		return Target{}, err
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE targets
		SET name = ?, kind = ?, host = ?, mgmt_ip = INET6_ATON(?), status = ?, labels_json = ?, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, target.Name, target.Kind, target.Host, target.Host, defaultString(target.Status, "pending"), labelsJSON, target.TenantID, target.ID)
	if err != nil {
		return Target{}, normalizeTargetWriteError(err)
	}
	return s.GetTarget(ctx, target.TenantID, target.ID)
}

func (s *MySQLStore) DeleteTarget(ctx context.Context, tenantID, targetID ID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// The targets cascade removes target_agents rows, but the legacy collector
	// projection has no FK back to targets — delete projection-owned collector
	// rows (and their bindings via cascade) in the same transaction so they
	// cannot outlive their agent. Registry-owned rows are never touched.
	if _, err := tx.ExecContext(ctx, `
		DELETE c FROM collector_agents c
		INNER JOIN target_agents a ON a.id = c.id AND a.tenant_id = c.tenant_id
		WHERE a.tenant_id = ? AND a.target_id = ? AND c.created_by = ?
	`, tenantID, targetID, collectorCompatibilityActor); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM targets
		WHERE tenant_id = ? AND id = ?
	`, tenantID, targetID); err != nil {
		return err
	}
	return tx.Commit()
}

func targetSelect() string {
	return `
		SELECT id, tenant_id, name, kind, host, status, labels_json, created_at, updated_at
		FROM targets
	`
}

func scanTarget(row rowScanner) (Target, error) {
	var target Target
	var labelsJSON []byte
	if err := row.Scan(&target.ID, &target.TenantID, &target.Name, &target.Kind, &target.Host, &target.Status, &labelsJSON, &target.CreatedAt, &target.UpdatedAt); err != nil {
		return target, err
	}
	labels, err := decodeStringMapJSON(labelsJSON)
	if err != nil {
		return target, err
	}
	target.Labels = labels
	return target, nil
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func normalizeTargetWriteError(err error) error {
	var mysqlErr *mysqldriver.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return ErrTargetHostExists
	}
	return err
}

func encodeLabelsForTest(labels map[string]string) ([]byte, error) {
	return json.Marshal(labels)
}

var _ TargetRepository = (*MySQLStore)(nil)
var _ NetworkTargetProvisioner = (*MySQLStore)(nil)
