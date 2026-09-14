package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// deliverResetToken logs the reset token for the self-host operator to relay.
// SMTP delivery is a later addition; an admin can also reset the password directly.
func (s *Server) deliverResetToken(userID, token string) {
	log.Printf("watchdog-server: password reset requested for user %s; reset token (valid %s): %s", userID, resetTTL, token)
}

// -------------------- Users --------------------

func (s *Server) listUsers(c *gin.Context) {
	rows, err := s.db.QueryContext(c.Request.Context(), `
		SELECT u.id, u.username, u.email, u.display_name, u.status,
		       COALESCE(GROUP_CONCAT(r.name ORDER BY r.name SEPARATOR ','), '')
		FROM users u
		LEFT JOIN user_roles ur ON ur.user_id = u.id
		LEFT JOIN roles r ON r.id = ur.role_id
		GROUP BY u.id, u.username, u.email, u.display_name, u.status
		ORDER BY u.username`)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var id, username, email, display, status, roles string
		if err := rows.Scan(&id, &username, &email, &display, &status, &roles); err != nil {
			fail(c, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		items = append(items, gin.H{"id": id, "username": username, "email": email,
			"display_name": display, "status": status, "roles": splitCSV(roles)})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) createUser(c *gin.Context) {
	var req struct {
		Username    string   `json:"username"`
		Email       string   `json:"email"`
		DisplayName string   `json:"display_name"`
		Password    string   `json:"password"`
		Roles       []string `json:"roles"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	req.Username = strings.TrimSpace(req.Username)
	req.Email = strings.TrimSpace(req.Email)
	req.DisplayName = strings.TrimSpace(req.DisplayName)
	if req.Username == "" || len(req.Username) > 190 || len(req.Email) > 190 || len(req.DisplayName) > 190 || !validPassword(req.Password) {
		fail(c, http.StatusBadRequest, "invalid_request", "username and password (8 to 72 bytes) required")
		return
	}
	actorPrincipal := currentPrincipal(c)
	if len(req.Roles) > 0 && !actorPrincipal.can("user.manage") {
		fail(c, http.StatusForbidden, "forbidden", "user.manage is required to assign roles")
		return
	}
	if containsRoleName(req.Roles, roleAdministrator) && !actorPrincipal.IsAdmin {
		fail(c, http.StatusForbidden, "forbidden", "only an administrator can assign the administrator role")
		return
	}
	ctx := c.Request.Context()
	hash, err := hashPassword(req.Password)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	id := newID()
	actor := currentPrincipal(c).UserID
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO users (id, username, email, display_name, password_hash, status, created_by)
		VALUES (?, ?, ?, ?, ?, 'active', ?)`,
		id, req.Username, req.Email, req.DisplayName, hash, actor); err != nil {
		fail(c, http.StatusConflict, "conflict", "username or email already exists")
		return
	}
	if err := replaceUserRolesTx(ctx, tx, id, req.Roles); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := tx.Commit(); err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.audit(ctx, actor, "user.create", "user", id)
	c.JSON(http.StatusCreated, gin.H{"id": id})
}

func (s *Server) getUser(c *gin.Context) {
	id := c.Param("id")
	var username, email, display, status string
	err := s.db.QueryRowContext(c.Request.Context(),
		`SELECT username, email, display_name, status FROM users WHERE id = ?`, id).
		Scan(&username, &email, &display, &status)
	if err == sql.ErrNoRows {
		fail(c, http.StatusNotFound, "not_found", "user not found")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": id, "username": username, "email": email,
		"display_name": display, "status": status, "roles": s.userRoleNames(c.Request.Context(), id)})
}

func (s *Server) updateUser(c *gin.Context) {
	id := c.Param("id")
	var req struct {
		DisplayName *string   `json:"display_name"`
		Email       *string   `json:"email"`
		Status      *string   `json:"status"`
		Roles       *[]string `json:"roles"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	ctx := c.Request.Context()
	actorPrincipal := currentPrincipal(c)
	actor := actorPrincipal.UserID
	if req.DisplayName != nil {
		trimmed := strings.TrimSpace(*req.DisplayName)
		req.DisplayName = &trimmed
	}
	if req.Email != nil {
		trimmed := strings.TrimSpace(*req.Email)
		req.Email = &trimmed
	}
	if (req.DisplayName != nil && len(*req.DisplayName) > 190) || (req.Email != nil && len(*req.Email) > 190) {
		fail(c, http.StatusBadRequest, "invalid_request", "email or display_name is too long")
		return
	}
	if req.Roles != nil && !actorPrincipal.can("user.manage") {
		fail(c, http.StatusForbidden, "forbidden", "user.manage is required to assign roles")
		return
	}
	if req.Roles != nil && containsRoleName(*req.Roles, roleAdministrator) && !actorPrincipal.IsAdmin {
		fail(c, http.StatusForbidden, "forbidden", "only an administrator can assign the administrator role")
		return
	}
	if req.Status != nil && id == actor && *req.Status == "disabled" {
		fail(c, http.StatusBadRequest, "invalid_request", "cannot disable your own account")
		return
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	defer tx.Rollback()
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM users WHERE id=?)`, id).Scan(&exists); err != nil {
		writeSQLError(c, err)
		return
	}
	if !exists {
		fail(c, http.StatusNotFound, "not_found", "user not found")
		return
	}
	if req.DisplayName != nil {
		if _, err := tx.ExecContext(ctx, `UPDATE users SET display_name = ?, updated_by = ?, row_version=row_version+1 WHERE id = ?`, *req.DisplayName, actor, id); err != nil {
			writeSQLError(c, err)
			return
		}
	}
	if req.Email != nil {
		if _, err := tx.ExecContext(ctx, `UPDATE users SET email = ?, updated_by = ?, row_version=row_version+1 WHERE id = ?`, *req.Email, actor, id); err != nil {
			writeSQLError(c, err)
			return
		}
	}
	if req.Status != nil {
		if *req.Status != "active" && *req.Status != "disabled" {
			fail(c, http.StatusBadRequest, "invalid_request", "status must be active or disabled")
			return
		}
		if _, err := tx.ExecContext(ctx, `UPDATE users SET status = ?, updated_by = ?, row_version=row_version+1 WHERE id = ?`, *req.Status, actor, id); err != nil {
			writeSQLError(c, err)
			return
		}
	}
	if req.Roles != nil {
		if err := replaceUserRolesTx(ctx, tx, id, *req.Roles); err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
	}
	if err := requireActiveAdministratorTx(ctx, tx); err != nil {
		fail(c, http.StatusConflict, "last_administrator", err.Error())
		return
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	if req.Status != nil && *req.Status == "disabled" {
		_ = s.revokeUserSessions(ctx, id) // disable is immediate
	}
	s.audit(ctx, actor, "user.update", "user", id)
	s.getUser(c)
}

func (s *Server) deleteUser(c *gin.Context) {
	id := c.Param("id")
	actor := currentPrincipal(c)
	if id == actor.UserID {
		fail(c, http.StatusBadRequest, "invalid_request", "cannot delete your own account")
		return
	}
	ctx := c.Request.Context()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `DELETE FROM users WHERE id = ?`, id)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if err := requireActiveAdministratorTx(ctx, tx); err != nil {
		fail(c, http.StatusConflict, "last_administrator", err.Error())
		return
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		fail(c, http.StatusNotFound, "not_found", "user not found")
		return
	}
	s.audit(c.Request.Context(), actor.UserID, "user.delete", "user", id)
	c.Status(http.StatusNoContent)
}

// POST /users/:id/password — administrator sets a user's password
func (s *Server) adminResetPassword(c *gin.Context) {
	id := c.Param("id")
	var req struct {
		NewPassword string `json:"new_password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || !validPassword(req.NewPassword) {
		fail(c, http.StatusBadRequest, "invalid_request", "new_password must be 8 to 72 bytes")
		return
	}
	if err := s.setUserPassword(c.Request.Context(), id, req.NewPassword); err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	_ = s.revokeUserSessions(c.Request.Context(), id)
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "user.password_reset", "user", id)
	c.Status(http.StatusNoContent)
}

func replaceUserRolesTx(ctx context.Context, tx *sql.Tx, userID string, roles []string) error {
	normalized, err := normalizeGrantValues("roles", roles, 100, 64)
	if err != nil {
		return err
	}
	roleIDs := make([]string, 0, len(normalized))
	for _, name := range normalized {
		var roleID string
		if err := tx.QueryRowContext(ctx, `SELECT id FROM roles WHERE name = ?`, name).Scan(&roleID); err == sql.ErrNoRows {
			return fmt.Errorf("unknown role: %s", name)
		} else if err != nil {
			return err
		}
		roleIDs = append(roleIDs, roleID)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM user_roles WHERE user_id = ?`, userID); err != nil {
		return err
	}
	for _, roleID := range roleIDs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO user_roles (user_id, role_id) VALUES (?, ?)`, userID, roleID); err != nil {
			return err
		}
	}
	return nil
}

func requireActiveAdministratorTx(ctx context.Context, tx *sql.Tx) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(DISTINCT u.id) FROM users u
		JOIN user_roles ur ON ur.user_id=u.id JOIN roles r ON r.id=ur.role_id
		WHERE u.status='active' AND r.name=?`, roleAdministrator).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return errors.New("at least one active administrator is required")
	}
	return nil
}

func containsRoleName(values []string, target string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

// -------------------- Roles --------------------

func (s *Server) listRoles(c *gin.Context) {
	rows, err := s.db.QueryContext(c.Request.Context(), `
		SELECT r.id, r.name, r.title, r.protected, COUNT(rp.permission_id)
		FROM roles r LEFT JOIN role_permissions rp ON rp.role_id = r.id
		GROUP BY r.id, r.name, r.title, r.protected ORDER BY r.name`)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var id, name, title string
		var protected bool
		var permCount int
		if err := rows.Scan(&id, &name, &title, &protected, &permCount); err != nil {
			fail(c, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		items = append(items, gin.H{"id": id, "name": name, "title": title, "protected": protected, "permission_count": permCount})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) getRole(c *gin.Context) {
	id := c.Param("id")
	var name, title string
	var protected bool
	err := s.db.QueryRowContext(c.Request.Context(), `SELECT name, title, protected FROM roles WHERE id = ?`, id).Scan(&name, &title, &protected)
	if err == sql.ErrNoRows {
		fail(c, http.StatusNotFound, "not_found", "role not found")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": id, "name": name, "title": title, "protected": protected, "permissions": s.rolePermissions(c.Request.Context(), id)})
}

func (s *Server) createRole(c *gin.Context) {
	var req struct {
		Name        string   `json:"name"`
		Title       string   `json:"title"`
		Permissions []string `json:"permissions"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	req.Title = strings.TrimSpace(req.Title)
	if req.Name == "" || len(req.Name) > 64 || len(req.Title) > 190 {
		fail(c, http.StatusBadRequest, "invalid_request", "name required")
		return
	}
	ctx := c.Request.Context()
	id := newID()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO roles (id, name, title, protected) VALUES (?, ?, ?, 0)`, id, req.Name, req.Title); err != nil {
		fail(c, http.StatusConflict, "conflict", "role name already exists")
		return
	}
	if err := replaceRolePermissionsTx(ctx, tx, id, req.Permissions); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(ctx, currentPrincipal(c).UserID, "role.create", "role", id)
	c.JSON(http.StatusCreated, gin.H{"id": id})
}

func (s *Server) updateRole(c *gin.Context) {
	id := c.Param("id")
	var req struct {
		Title       *string   `json:"title"`
		Permissions *[]string `json:"permissions"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	if req.Title != nil {
		trimmed := strings.TrimSpace(*req.Title)
		if len(trimmed) > 190 {
			fail(c, http.StatusBadRequest, "invalid_request", "title is too long")
			return
		}
		req.Title = &trimmed
	}
	ctx := c.Request.Context()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	var protected bool
	if err := tx.QueryRowContext(ctx, `SELECT protected FROM roles WHERE id = ? FOR UPDATE`, id).Scan(&protected); err != nil {
		fail(c, http.StatusNotFound, "not_found", "role not found")
		return
	}
	if protected && (req.Title != nil || req.Permissions != nil) {
		fail(c, http.StatusForbidden, "protected", "cannot change a built-in role")
		return
	}
	if req.Title != nil {
		if _, err := tx.ExecContext(ctx, `UPDATE roles SET title = ?, row_version=row_version+1 WHERE id = ?`, *req.Title, id); err != nil {
			writeSQLError(c, err)
			return
		}
	}
	if req.Permissions != nil {
		if err := replaceRolePermissionsTx(ctx, tx, id, *req.Permissions); err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(ctx, currentPrincipal(c).UserID, "role.update", "role", id)
	s.getRole(c)
}

func (s *Server) deleteRole(c *gin.Context) {
	id := c.Param("id")
	var protected bool
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT protected FROM roles WHERE id = ?`, id).Scan(&protected); err != nil {
		fail(c, http.StatusNotFound, "not_found", "role not found")
		return
	}
	if protected {
		fail(c, http.StatusForbidden, "protected", "cannot delete a built-in role")
		return
	}
	if _, err := s.db.ExecContext(c.Request.Context(), `DELETE FROM roles WHERE id = ?`, id); err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "role.delete", "role", id)
	c.Status(http.StatusNoContent)
}

func replaceRolePermissionsTx(ctx context.Context, tx *sql.Tx, roleID string, abilities []string) error {
	normalized, err := normalizeGrantValues("permissions", abilities, len(abilityCatalog), 64)
	if err != nil {
		return err
	}
	permissionIDs := make([]string, 0, len(normalized))
	for _, ability := range normalized {
		var permissionID string
		if err := tx.QueryRowContext(ctx, `SELECT id FROM permissions WHERE ability = ?`, ability).Scan(&permissionID); err == sql.ErrNoRows {
			return fmt.Errorf("unknown ability: %s", ability)
		} else if err != nil {
			return err
		}
		permissionIDs = append(permissionIDs, permissionID)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM role_permissions WHERE role_id = ?`, roleID); err != nil {
		return err
	}
	for _, permissionID := range permissionIDs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO role_permissions (role_id, permission_id) VALUES (?, ?)`, roleID, permissionID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) rolePermissions(ctx context.Context, roleID string) []string {
	rows, err := s.db.QueryContext(ctx,
		`SELECT p.ability FROM role_permissions rp JOIN permissions p ON p.id = rp.permission_id WHERE rp.role_id = ? ORDER BY p.ability`, roleID)
	if err != nil {
		return []string{}
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var a string
		if rows.Scan(&a) == nil {
			out = append(out, a)
		}
	}
	return out
}

// GET /permissions — the fixed ability catalogue
func (s *Server) listPermissions(c *gin.Context) {
	items := make([]gin.H, 0, len(abilityCatalog))
	for _, a := range abilityCatalog {
		items = append(items, gin.H{"ability": a, "subject": subjectOf(a)})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) audit(ctx context.Context, actor, action, resource, resourceID string) {
	_, _ = s.db.ExecContext(ctx,
		`INSERT INTO audit_logs (id, actor_id, action, resource, resource_id) VALUES (?, ?, ?, ?, ?)`,
		newID(), actor, action, resource, resourceID)
}

func splitCSV(s string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, ",")
}
