package watchdog

import "context"

// GetTenantUser is retained for the legacy export authorization snapshot until
// exports move to the single-domain RBAC repository. It reads authorization
// metadata only; authentication credentials are owned by the KISS server.
func (s *MySQLStore) GetTenantUser(ctx context.Context, tenantID, userID ID) (User, error) {
	return scanExportAuthorizationUser(s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, email, name, status, created_at, updated_at
		FROM users
		WHERE tenant_id = ? AND id = ?
	`, tenantID, userID))
}

func scanExportAuthorizationUser(row rowScanner) (User, error) {
	var user User
	err := row.Scan(&user.ID, &user.TenantID, &user.Email, &user.Name, &user.Status,
		&user.CreatedAt, &user.UpdatedAt)
	return user, err
}
