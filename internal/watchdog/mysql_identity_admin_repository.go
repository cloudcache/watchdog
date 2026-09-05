package watchdog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// newIdentityID returns a random 26-character identifier that fits the
// CHAR(26) primary keys used across the management schema.
func newIdentityID() (ID, error) {
	buf := make([]byte, 13)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return ID(hex.EncodeToString(buf)), nil
}

func (s *MySQLStore) ListTenantUsers(ctx context.Context, tenantID ID) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, email, name, status,
		       COALESCE(auth_provider, ''), COALESCE(external_subject_id, ''),
		       created_at, updated_at
		FROM users
		WHERE tenant_id = ?
		ORDER BY email
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		user, err := scanTenantUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

func (s *MySQLStore) GetTenantUser(ctx context.Context, tenantID, userID ID) (User, error) {
	return scanTenantUser(s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, email, name, status,
		       COALESCE(auth_provider, ''), COALESCE(external_subject_id, ''),
		       created_at, updated_at
		FROM users
		WHERE tenant_id = ? AND id = ?
	`, tenantID, userID))
}

func (s *MySQLStore) CreateUser(ctx context.Context, user User) (User, error) {
	if user.ID == "" {
		id, err := newIdentityID()
		if err != nil {
			return User{}, err
		}
		user.ID = id
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id, password_hash)
		VALUES (?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), NULL)
	`, user.ID, user.TenantID, user.Email, user.Name, user.Status, strings.ToLower(user.AuthProvider), user.ExternalSubjectID)
	if err != nil {
		return User{}, err
	}
	return s.GetTenantUser(ctx, user.TenantID, user.ID)
}

func (s *MySQLStore) UpdateUser(ctx context.Context, user User) (User, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE users
		SET email = ?, name = ?, status = ?,
		    auth_provider = NULLIF(?, ''), external_subject_id = NULLIF(?, '')
		WHERE tenant_id = ? AND id = ?
	`, user.Email, user.Name, user.Status, strings.ToLower(user.AuthProvider), user.ExternalSubjectID, user.TenantID, user.ID)
	if err != nil {
		return User{}, err
	}
	if count, err := result.RowsAffected(); err != nil {
		return User{}, err
	} else if count == 0 {
		if _, getErr := s.GetTenantUser(ctx, user.TenantID, user.ID); getErr != nil {
			return User{}, sql.ErrNoRows
		}
	}
	return s.GetTenantUser(ctx, user.TenantID, user.ID)
}

func (s *MySQLStore) DisableUser(ctx context.Context, tenantID, userID ID) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE users SET status = 'disabled'
		WHERE tenant_id = ? AND id = ?
	`, tenantID, userID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		if _, getErr := s.GetTenantUser(ctx, tenantID, userID); getErr != nil {
			return sql.ErrNoRows
		}
	}
	return nil
}

func (s *MySQLStore) ListRoles(ctx context.Context, tenantID ID) ([]Role, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, name, scope, created_at, updated_at
		FROM roles
		WHERE tenant_id = ?
		ORDER BY name
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var roles []Role
	for rows.Next() {
		var role Role
		if err := rows.Scan(&role.ID, &role.TenantID, &role.Name, &role.Scope, &role.CreatedAt, &role.UpdatedAt); err != nil {
			return nil, err
		}
		roles = append(roles, role)
	}
	return roles, rows.Err()
}

func (s *MySQLStore) CreateRole(ctx context.Context, role Role) (Role, error) {
	if role.ID == "" {
		id, err := newIdentityID()
		if err != nil {
			return Role{}, err
		}
		role.ID = id
	}
	if role.Scope == "" {
		role.Scope = "tenant"
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO roles (id, tenant_id, name, scope)
		VALUES (?, ?, ?, ?)
	`, role.ID, role.TenantID, role.Name, role.Scope); err != nil {
		return Role{}, err
	}
	return s.getRole(ctx, role.TenantID, role.ID)
}

func (s *MySQLStore) UpdateRole(ctx context.Context, role Role) (Role, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE roles SET name = ?, scope = ?
		WHERE tenant_id = ? AND id = ?
	`, role.Name, role.Scope, role.TenantID, role.ID)
	if err != nil {
		return Role{}, err
	}
	if count, err := result.RowsAffected(); err != nil {
		return Role{}, err
	} else if count == 0 {
		if _, getErr := s.getRole(ctx, role.TenantID, role.ID); getErr != nil {
			return Role{}, sql.ErrNoRows
		}
	}
	return s.getRole(ctx, role.TenantID, role.ID)
}

// DeleteRole removes the role, its memberships (via FK cascade) and any
// permission grants that referenced it, so a deleted role cannot keep
// authorizing anyone through a dangling subject row.
func (s *MySQLStore) DeleteRole(ctx context.Context, tenantID, roleID ID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `DELETE FROM roles WHERE tenant_id = ? AND id = ?`, tenantID, roleID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return sql.ErrNoRows
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM permissions
		WHERE tenant_id = ? AND subject_type = 'role' AND subject_id = ?
	`, tenantID, roleID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *MySQLStore) ReplaceUserRoles(ctx context.Context, tenantID, userID ID, roleIDs []ID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE tenant_id = ? AND id = ?`, tenantID, userID).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return sql.ErrNoRows
	}
	for _, roleID := range roleIDs {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM roles WHERE tenant_id = ? AND id = ?`, tenantID, roleID).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("role %s does not belong to this tenant: %w", roleID, errRoleNotInTenant)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE ur FROM user_roles ur
		INNER JOIN roles r ON r.id = ur.role_id
		WHERE ur.user_id = ? AND r.tenant_id = ?
	`, userID, tenantID); err != nil {
		return err
	}
	for _, roleID := range roleIDs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO user_roles (user_id, role_id) VALUES (?, ?)`, userID, roleID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

var errRoleNotInTenant = errors.New("role not in tenant")

func (s *MySQLStore) GetTenantRole(ctx context.Context, tenantID, roleID ID) (Role, error) {
	return s.getRole(ctx, tenantID, roleID)
}

func (s *MySQLStore) getRole(ctx context.Context, tenantID, roleID ID) (Role, error) {
	var role Role
	err := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, name, scope, created_at, updated_at
		FROM roles
		WHERE tenant_id = ? AND id = ?
	`, tenantID, roleID).Scan(&role.ID, &role.TenantID, &role.Name, &role.Scope, &role.CreatedAt, &role.UpdatedAt)
	return role, err
}

func scanTenantUser(row rowScanner) (User, error) {
	var user User
	err := row.Scan(&user.ID, &user.TenantID, &user.Email, &user.Name, &user.Status,
		&user.AuthProvider, &user.ExternalSubjectID, &user.CreatedAt, &user.UpdatedAt)
	return user, err
}
