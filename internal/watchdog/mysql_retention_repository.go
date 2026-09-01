package watchdog

import (
	"context"
	"database/sql"
)

func (s *MySQLStore) ListRetentionPolicies(ctx context.Context, tenantID ID) ([]MetricRetentionPolicy, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, COALESCE(target_id, ''), high_precision_days,
		       manual_cleanup_enabled, COALESCE(notes, ''), created_at, updated_at
		FROM metric_retention_policies
		WHERE tenant_id = ?
		ORDER BY target_id IS NOT NULL, target_id, id
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var policies []MetricRetentionPolicy
	for rows.Next() {
		policy, err := scanRetentionPolicy(rows)
		if err != nil {
			return nil, err
		}
		policies = append(policies, policy)
	}
	return policies, rows.Err()
}

func (s *MySQLStore) UpsertRetentionPolicy(ctx context.Context, policy MetricRetentionPolicy) (MetricRetentionPolicy, error) {
	if policy.ID == "" {
		scope := "tenant"
		if policy.TargetID != "" {
			scope = string(policy.TargetID)
		}
		policy.ID = stableID("retention", string(policy.TenantID), scope)
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO metric_retention_policies (
			id, tenant_id, target_id, high_precision_days, manual_cleanup_enabled, notes
		) VALUES (?, ?, NULLIF(?, ''), ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			high_precision_days = VALUES(high_precision_days),
			manual_cleanup_enabled = VALUES(manual_cleanup_enabled),
			notes = VALUES(notes),
			updated_at = CURRENT_TIMESTAMP(3)
	`, policy.ID, policy.TenantID, policy.TargetID, policy.HighPrecisionDays, policy.ManualCleanupEnabled, policy.Notes)
	if err != nil {
		return MetricRetentionPolicy{}, err
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, COALESCE(target_id, ''), high_precision_days,
		       manual_cleanup_enabled, COALESCE(notes, ''), created_at, updated_at
		FROM metric_retention_policies
		WHERE tenant_id = ? AND id = ?
	`, policy.TenantID, policy.ID)
	return scanRetentionPolicy(row)
}

func (s *MySQLStore) DeleteRetentionPolicy(ctx context.Context, tenantID, policyID ID) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM metric_retention_policies
		WHERE tenant_id = ? AND id = ?
	`, tenantID, policyID)
	return err
}

func scanRetentionPolicy(row interface {
	Scan(dest ...any) error
}) (MetricRetentionPolicy, error) {
	var policy MetricRetentionPolicy
	var days uint32
	if err := row.Scan(
		&policy.ID,
		&policy.TenantID,
		&policy.TargetID,
		&days,
		&policy.ManualCleanupEnabled,
		&policy.Notes,
		&policy.CreatedAt,
		&policy.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return MetricRetentionPolicy{}, err
		}
		return MetricRetentionPolicy{}, err
	}
	policy.HighPrecisionDays = days
	return policy, nil
}
