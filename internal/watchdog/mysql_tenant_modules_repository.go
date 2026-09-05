package watchdog

import "context"

type TenantModuleRepository interface {
	ListTenantModuleStates(ctx context.Context, tenantID ID) (map[string]bool, error)
	SetTenantModuleEnabled(ctx context.Context, tenantID ID, moduleKey string, enabled bool, actorID ID) error
}

func (s *MySQLStore) ListTenantModuleStates(ctx context.Context, tenantID ID) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT module_key, enabled FROM tenant_modules WHERE tenant_id = ?
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	states := map[string]bool{}
	for rows.Next() {
		var key string
		var enabled bool
		if err := rows.Scan(&key, &enabled); err != nil {
			return nil, err
		}
		states[key] = enabled
	}
	return states, rows.Err()
}

func (s *MySQLStore) SetTenantModuleEnabled(ctx context.Context, tenantID ID, moduleKey string, enabled bool, actorID ID) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tenant_modules (tenant_id, module_key, enabled, updated_by)
		VALUES (?, ?, ?, NULLIF(?, ''))
		ON DUPLICATE KEY UPDATE enabled = VALUES(enabled), updated_by = VALUES(updated_by)
	`, tenantID, moduleKey, enabled, actorID)
	return err
}
