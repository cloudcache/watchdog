package watchdog

import (
	"context"
	"encoding/json"
	"errors"
)

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
		INSERT INTO monitor_targets (
			id, tenant_id, name, target_type, host, status, labels_json
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, target.ID, target.TenantID, target.Name, target.Type, target.Host, defaultString(target.Status, "pending"), labelsJSON)
	if err != nil {
		return Target{}, err
	}
	return s.GetTarget(ctx, target.TenantID, target.ID)
}

func (s *MySQLStore) UpdateTarget(ctx context.Context, target Target) (Target, error) {
	labelsJSON, err := encodeStringMapJSON(target.Labels)
	if err != nil {
		return Target{}, err
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE monitor_targets
		SET name = ?, target_type = ?, host = ?, status = ?, labels_json = ?, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, target.Name, target.Type, target.Host, defaultString(target.Status, "pending"), labelsJSON, target.TenantID, target.ID)
	if err != nil {
		return Target{}, err
	}
	return s.GetTarget(ctx, target.TenantID, target.ID)
}

func (s *MySQLStore) DeleteTarget(ctx context.Context, tenantID, targetID ID) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM monitor_targets
		WHERE tenant_id = ? AND id = ?
	`, tenantID, targetID)
	return err
}

func targetSelect() string {
	return `
		SELECT id, tenant_id, name, target_type, host, status, labels_json, created_at, updated_at
		FROM monitor_targets
	`
}

func scanTarget(row rowScanner) (Target, error) {
	var target Target
	var labelsJSON []byte
	if err := row.Scan(&target.ID, &target.TenantID, &target.Name, &target.Type, &target.Host, &target.Status, &labelsJSON, &target.CreatedAt, &target.UpdatedAt); err != nil {
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

func encodeLabelsForTest(labels map[string]string) ([]byte, error) {
	return json.Marshal(labels)
}

var _ TargetRepository = (*MySQLStore)(nil)
