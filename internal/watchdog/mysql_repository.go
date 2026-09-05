package watchdog

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

func (s *MySQLStore) ListIdentityProjections(ctx context.Context, provider, externalSubject string) ([]IdentityProjection, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	externalSubject = strings.TrimSpace(externalSubject)
	if provider == "" || externalSubject == "" {
		return nil, errors.New("auth provider and external subject are required")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			u.id, u.tenant_id, u.email, u.name, u.status, u.created_at, u.updated_at,
			t.id, t.name, t.status, t.created_at, t.updated_at
		FROM users u
		INNER JOIN tenants t ON t.id = u.tenant_id
		WHERE u.auth_provider = ? AND u.external_subject_id = ?
		ORDER BY t.name, t.id
	`, provider, externalSubject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	projections := make([]IdentityProjection, 0)
	for rows.Next() {
		var projection IdentityProjection
		if err := rows.Scan(
			&projection.User.ID,
			&projection.User.TenantID,
			&projection.User.Email,
			&projection.User.Name,
			&projection.User.Status,
			&projection.User.CreatedAt,
			&projection.User.UpdatedAt,
			&projection.Tenant.ID,
			&projection.Tenant.Name,
			&projection.Tenant.Status,
			&projection.Tenant.CreatedAt,
			&projection.Tenant.UpdatedAt,
		); err != nil {
			return nil, err
		}
		projections = append(projections, projection)
	}
	return projections, rows.Err()
}

func (s *MySQLStore) LinkExternalIdentity(ctx context.Context, tenantID, userID ID, provider, externalSubject string) error {
	provider = strings.ToLower(strings.TrimSpace(provider))
	externalSubject = strings.TrimSpace(externalSubject)
	if tenantID == "" || userID == "" || provider == "" || externalSubject == "" {
		return errors.New("tenant id, user id, auth provider, and external subject are required")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE users
		SET auth_provider = ?, external_subject_id = ?
		WHERE tenant_id = ? AND id = ? AND status = 'active'
	`, provider, externalSubject, tenantID, userID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *MySQLStore) IsUserTenantAdmin(ctx context.Context, tenantID, userID ID) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM user_roles ur
		INNER JOIN roles r ON r.id = ur.role_id
		WHERE ur.user_id = ? AND r.tenant_id = ? AND LOWER(r.name) = 'admin'
	`, userID, tenantID).Scan(&count)
	return count > 0, err
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
		id, err := newIdentityID()
		if err != nil {
			return err
		}
		log.ID = id
	}
	// The actor FK targets users(id); system actors and identities that are
	// not user projections would otherwise make the whole audit write vanish.
	// Keep the record and preserve the raw actor in the detail instead.
	if log.ActorID != "" {
		var exists int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE id = ?`, log.ActorID).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			if log.Detail == nil {
				log.Detail = map[string]any{}
			}
			log.Detail["actor"] = string(log.ActorID)
			log.ActorID = ""
		}
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

// AuditLogFilter narrows and pages ListAuditLogs. Cursor is the opaque value
// returned by the previous page.
type AuditLogFilter struct {
	ResourceType ResourceType
	ResourceID   ID
	ActorID      ID
	Action       string // prefix match
	Limit        int    // default 50, max 200
	Cursor       string
}

func (s *MySQLStore) ListAuditLogs(ctx context.Context, tenantID ID, filter AuditLogFilter) ([]AuditLog, string, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	query := `
		SELECT id, tenant_id, COALESCE(actor_id, ''), action, resource_type,
			COALESCE(resource_id, ''), detail_json, created_at
		FROM audit_logs
		WHERE tenant_id = ?`
	args := []any{tenantID}
	if filter.ResourceType != "" {
		query += ` AND resource_type = ?`
		args = append(args, filter.ResourceType)
	}
	if filter.ResourceID != "" {
		query += ` AND resource_id = ?`
		args = append(args, filter.ResourceID)
	}
	if filter.ActorID != "" {
		query += ` AND actor_id = ?`
		args = append(args, filter.ActorID)
	}
	if filter.Action != "" {
		query += ` AND action LIKE ?`
		args = append(args, escapeSQLLike(filter.Action)+"%")
	}
	if filter.Cursor != "" {
		cursorTime, cursorID, err := decodeAuditCursor(filter.Cursor)
		if err != nil {
			return nil, "", err
		}
		query += ` AND (created_at < ? OR (created_at = ? AND id < ?))`
		args = append(args, cursorTime, cursorTime, cursorID)
	}
	query += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var logs []AuditLog
	for rows.Next() {
		var log AuditLog
		var detailJSON []byte
		if err := rows.Scan(&log.ID, &log.TenantID, &log.ActorID, &log.Action,
			&log.ResourceType, &log.ResourceID, &detailJSON, &log.CreatedAt); err != nil {
			return nil, "", err
		}
		if len(detailJSON) > 0 {
			_ = json.Unmarshal(detailJSON, &log.Detail)
		}
		logs = append(logs, log)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	nextCursor := ""
	if len(logs) > limit {
		logs = logs[:limit]
		last := logs[len(logs)-1]
		nextCursor = encodeAuditCursor(last.CreatedAt, last.ID)
	}
	return logs, nextCursor, nil
}

func encodeAuditCursor(createdAt time.Time, id ID) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(createdAt.UTC().Format(time.RFC3339Nano) + "|" + string(id)))
}

func decodeAuditCursor(cursor string) (time.Time, ID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", errors.New("audit cursor is invalid")
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", errors.New("audit cursor is invalid")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", errors.New("audit cursor is invalid")
	}
	return createdAt, ID(parts[1]), nil
}

// encodeStringCursor / decodeStringCursor page a list ordered by a (string,
// id) composite key (e.g. name, id). The id is written first because ids are
// "|"-free CHAR(26), so the sort value — which may itself contain "|" — is the
// remainder after the first separator.
func encodeStringCursor(sortValue string, id ID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(string(id) + "|" + sortValue))
}

func decodeStringCursor(cursor string) (string, ID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", errors.New("cursor is invalid")
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return "", "", errors.New("cursor is invalid")
	}
	return parts[1], ID(parts[0]), nil
}

func escapeSQLLike(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "%", `\%`)
	return strings.ReplaceAll(value, "_", `\_`)
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
