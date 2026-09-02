package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLStore struct {
	db *sql.DB
}

func OpenMySQLStore(ctx context.Context, cfg MySQLConfig) (*MySQLStore, error) {
	if cfg.DSN == "" {
		return nil, errors.New("mysql dsn is required")
	}
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, err
	}
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns >= 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &MySQLStore{db: db}, nil
}

func NewMySQLStore(db *sql.DB) *MySQLStore {
	return &MySQLStore{db: db}
}

func (s *MySQLStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *MySQLStore) GetTenant(ctx context.Context, tenantID ID) (Tenant, error) {
	var tenant Tenant
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, status, created_at, updated_at
		FROM tenants
		WHERE id = ?
	`, tenantID).Scan(&tenant.ID, &tenant.Name, &tenant.Status, &tenant.CreatedAt, &tenant.UpdatedAt)
	return tenant, err
}

func (s *MySQLStore) ListTenantsForUser(ctx context.Context, userID ID) ([]Tenant, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT t.id, t.name, t.status, t.created_at, t.updated_at
		FROM tenants t
		INNER JOIN users u ON u.tenant_id = t.id
		WHERE u.id = ?
		ORDER BY t.name
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tenants []Tenant
	for rows.Next() {
		var tenant Tenant
		if err := rows.Scan(&tenant.ID, &tenant.Name, &tenant.Status, &tenant.CreatedAt, &tenant.UpdatedAt); err != nil {
			return nil, err
		}
		tenants = append(tenants, tenant)
	}
	return tenants, rows.Err()
}

func (s *MySQLStore) GetUser(ctx context.Context, userID ID) (User, error) {
	var user User
	err := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, email, name, status, created_at, updated_at
		FROM users
		WHERE id = ?
	`, userID).Scan(&user.ID, &user.TenantID, &user.Email, &user.Name, &user.Status, &user.CreatedAt, &user.UpdatedAt)
	return user, err
}

func (s *MySQLStore) ListUserRoleIDs(ctx context.Context, tenantID, userID ID) ([]ID, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ur.role_id
		FROM user_roles ur
		INNER JOIN roles r ON r.id = ur.role_id
		WHERE ur.user_id = ? AND r.tenant_id = ?
		ORDER BY ur.role_id
	`, userID, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var roleIDs []ID
	for rows.Next() {
		var roleID ID
		if err := rows.Scan(&roleID); err != nil {
			return nil, err
		}
		roleIDs = append(roleIDs, roleID)
	}
	return roleIDs, rows.Err()
}

func (s *MySQLStore) ListPermissionsForUser(ctx context.Context, tenantID, userID ID) ([]Permission, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT p.id, p.tenant_id, p.subject_type, p.subject_id, p.resource_type, p.resource_id, p.actions_json
		FROM permissions p
		WHERE p.tenant_id = ?
		  AND (
		    (p.subject_type = 'user' AND p.subject_id = ?)
		    OR
		    (p.subject_type = 'role' AND p.subject_id IN (
		      SELECT ur.role_id
		      FROM user_roles ur
		      INNER JOIN roles r ON r.id = ur.role_id
		      WHERE ur.user_id = ? AND r.tenant_id = ?
		    ))
		  )
		ORDER BY p.resource_type, p.resource_id, p.subject_type, p.subject_id
	`, tenantID, userID, userID, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var permissions []Permission
	for rows.Next() {
		var permission Permission
		var actionsJSON []byte
		if err := rows.Scan(
			&permission.ID,
			&permission.TenantID,
			&permission.SubjectType,
			&permission.SubjectID,
			&permission.ResourceType,
			&permission.ResourceID,
			&actionsJSON,
		); err != nil {
			return nil, err
		}
		actions, err := decodeActionsJSON(actionsJSON)
		if err != nil {
			return nil, err
		}
		permission.Actions = actions
		permissions = append(permissions, permission)
	}
	return permissions, rows.Err()
}

func (s *MySQLStore) ListPermissions(ctx context.Context, tenantID ID) ([]Permission, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, subject_type, subject_id, resource_type, resource_id, actions_json
		FROM permissions
		WHERE tenant_id = ?
		ORDER BY resource_type, resource_id, subject_type, subject_id
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var permissions []Permission
	for rows.Next() {
		var permission Permission
		var actionsJSON []byte
		if err := rows.Scan(
			&permission.ID,
			&permission.TenantID,
			&permission.SubjectType,
			&permission.SubjectID,
			&permission.ResourceType,
			&permission.ResourceID,
			&actionsJSON,
		); err != nil {
			return nil, err
		}
		actions, err := decodeActionsJSON(actionsJSON)
		if err != nil {
			return nil, err
		}
		permission.Actions = actions
		permissions = append(permissions, permission)
	}
	return permissions, rows.Err()
}

func (s *MySQLStore) ReplacePermission(ctx context.Context, grant Permission) error {
	if grant.ID == "" {
		return errors.New("permission id is required")
	}
	actionsJSON, err := encodeActionsJSON(grant.Actions)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO permissions (
			id, tenant_id, subject_type, subject_id, resource_type, resource_id, actions_json
		) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			actions_json = VALUES(actions_json),
			updated_at = CURRENT_TIMESTAMP(3)
	`, grant.ID, grant.TenantID, grant.SubjectType, grant.SubjectID, grant.ResourceType, grant.ResourceID, actionsJSON)
	return err
}

func (s *MySQLStore) DeletePermission(ctx context.Context, tenantID ID, subjectType SubjectType, subjectID ID, resourceType ResourceType, resourceID ID) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM permissions
		WHERE tenant_id = ? AND subject_type = ? AND subject_id = ? AND resource_type = ? AND resource_id = ?
	`, tenantID, subjectType, subjectID, resourceType, resourceID)
	return err
}

func (s *MySQLStore) CreateAuditLog(ctx context.Context, log AuditLog) error {
	if log.ID == "" {
		return errors.New("audit log id is required")
	}
	detailJSON, err := json.Marshal(log.Detail)
	if err != nil {
		return fmt.Errorf("marshal audit detail: %w", err)
	}
	createdAt := log.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO audit_logs (
			id, tenant_id, actor_id, action, resource_type, resource_id, detail_json, created_at
		) VALUES (?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), ?, ?)
	`, log.ID, log.TenantID, log.ActorID, log.Action, log.ResourceType, log.ResourceID, detailJSON, createdAt)
	return err
}

func encodeActionsJSON(actions []Action) ([]byte, error) {
	if len(actions) == 0 {
		return []byte("[]"), nil
	}
	values := make([]string, 0, len(actions))
	for _, action := range actions {
		values = append(values, string(action))
	}
	return json.Marshal(values)
}

func decodeActionsJSON(data []byte) ([]Action, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var values []string
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	actions := make([]Action, 0, len(values))
	for _, value := range values {
		actions = append(actions, Action(value))
	}
	return actions, nil
}

var _ TenantRepository = (*MySQLStore)(nil)
var _ IdentityRepository = (*MySQLStore)(nil)
var _ PermissionRepository = (*MySQLStore)(nil)
var _ AuditRepository = (*MySQLStore)(nil)
