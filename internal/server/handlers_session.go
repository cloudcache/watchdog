package server

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/gin-gonic/gin"
)

// fail writes the platform error envelope {error:{code, message, retryable}}.
func fail(c *gin.Context, status int, code, msg string) {
	failDetails(c, status, code, msg, nil)
}

// failDetails writes the platform error envelope with an optional structured details
// map: {error:{code, message, retryable, details?}}. retryable follows the platform
// rule — a transient condition (429 or any 5xx) may be retried verbatim, while
// validation, permission and conflict failures need a changed request first.
func failDetails(c *gin.Context, status int, code, msg string, details map[string]any) {
	body := gin.H{
		"code":      code,
		"message":   msg,
		"retryable": status == http.StatusTooManyRequests || status >= 500,
	}
	if len(details) > 0 {
		body["details"] = details
	}
	c.AbortWithStatusJSON(status, gin.H{"error": body})
}

// POST /session/login
func (s *Server) login(c *gin.Context) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Username == "" {
		fail(c, http.StatusBadRequest, "invalid_request", "username and password required")
		return
	}
	var id, hash, status string
	err := s.db.QueryRowContext(c.Request.Context(),
		`SELECT id, COALESCE(password_hash, ''), status FROM users WHERE username = ?`, req.Username).
		Scan(&id, &hash, &status)
	if err != nil || hash == "" || status != "active" || !checkPassword(hash, req.Password) {
		fail(c, http.StatusUnauthorized, "invalid_credentials", "invalid username or password")
		return
	}
	if err := s.startSession(c, id); err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.writeCurrentUser(c, id)
}

// POST /session/logout
func (s *Server) logout(c *gin.Context) {
	s.revokeSessionCookie(c)
	c.Status(http.StatusNoContent)
}

// GET /session/current  (and GET /me)
func (s *Server) current(c *gin.Context) {
	p := currentPrincipal(c)
	if p == nil {
		fail(c, http.StatusUnauthorized, "unauthorized", "authentication required")
		return
	}
	s.writeCurrentUser(c, p.UserID)
}

// PATCH /me — self-service profile (display name, email only)
func (s *Server) updateProfile(c *gin.Context) {
	p := currentPrincipal(c)
	var req struct {
		DisplayName *string `json:"display_name"`
		Email       *string `json:"email"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	if req.DisplayName != nil {
		_, _ = s.db.ExecContext(c.Request.Context(), `UPDATE users SET display_name = ?, updated_by = ? WHERE id = ?`, *req.DisplayName, p.UserID, p.UserID)
	}
	if req.Email != nil {
		if _, err := s.db.ExecContext(c.Request.Context(), `UPDATE users SET email = ?, updated_by = ? WHERE id = ?`, *req.Email, p.UserID, p.UserID); err != nil {
			fail(c, http.StatusConflict, "conflict", "email already in use")
			return
		}
	}
	s.writeCurrentUser(c, p.UserID)
}

// POST /me/password — change own password (requires current password)
func (s *Server) changeOwnPassword(c *gin.Context) {
	p := currentPrincipal(c)
	var req struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || !validPassword(req.NewPassword) {
		fail(c, http.StatusBadRequest, "invalid_request", "new_password must be 8 to 72 bytes")
		return
	}
	var hash string
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT COALESCE(password_hash,'') FROM users WHERE id = ?`, p.UserID).Scan(&hash); err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if hash == "" || !checkPassword(hash, req.CurrentPassword) {
		fail(c, http.StatusForbidden, "invalid_credentials", "current password is incorrect")
		return
	}
	if err := s.setUserPassword(c.Request.Context(), p.UserID, req.NewPassword); err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	_ = s.revokeUserSessions(c.Request.Context(), p.UserID)
	s.revokeSessionCookie(c)
	c.Status(http.StatusNoContent)
}

// POST /session/forgot — self-service reset request (no user enumeration)
func (s *Server) forgotPassword(c *gin.Context) {
	var req struct {
		Identifier string `json:"identifier"` // username or email
	}
	_ = c.ShouldBindJSON(&req)
	generic := gin.H{"status": "if the account exists, a reset was initiated"}
	if req.Identifier == "" {
		c.JSON(http.StatusOK, generic)
		return
	}
	var userID string
	err := s.db.QueryRowContext(c.Request.Context(),
		`SELECT id FROM users WHERE (username = ? OR email = ?) AND status = 'active'`,
		req.Identifier, req.Identifier).Scan(&userID)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusOK, generic)
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	token, err := s.createResetToken(c.Request.Context(), userID)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	// Delivery channel (email/SMTP) is a later addition; until then the token is
	// logged for the self-host operator to relay, or an admin can reset directly.
	s.deliverResetToken(userID, token)
	c.JSON(http.StatusOK, generic)
}

// POST /session/reset — complete a reset with a token
func (s *Server) resetPassword(c *gin.Context) {
	var req struct {
		Token       string `json:"token"`
		NewPassword string `json:"new_password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Token == "" || !validPassword(req.NewPassword) {
		fail(c, http.StatusBadRequest, "invalid_request", "token and new_password (8 to 72 bytes) required")
		return
	}
	userID, err := s.consumeResetToken(c.Request.Context(), req.Token)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_token", "reset token is invalid or expired")
		return
	}
	if err := s.setUserPassword(c.Request.Context(), userID, req.NewPassword); err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	_ = s.revokeUserSessions(c.Request.Context(), userID)
	c.JSON(http.StatusOK, gin.H{"status": "password reset"})
}

func (s *Server) setUserPassword(ctx context.Context, userID, password string) error {
	hash, err := hashPassword(password)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE users SET password_hash = ?, row_version = row_version + 1 WHERE id = ?`, hash, userID)
	return err
}

func (s *Server) writeCurrentUser(c *gin.Context, userID string) {
	ctx := c.Request.Context()
	var u struct{ ID, Username, Email, DisplayName, Status string }
	if err := s.db.QueryRowContext(ctx,
		`SELECT id, username, email, display_name, status FROM users WHERE id = ?`, userID).
		Scan(&u.ID, &u.Username, &u.Email, &u.DisplayName, &u.Status); err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	roles := s.userRoleNames(ctx, userID)
	abilities, isAdmin, _ := loadPrincipalAbilities(ctx, s.db, userID)
	c.JSON(http.StatusOK, gin.H{
		"id": u.ID, "username": u.Username, "email": u.Email,
		"display_name": u.DisplayName, "status": u.Status,
		"is_admin": isAdmin, "roles": roles, "abilities": keysOf(abilities),
	})
}

func (s *Server) userRoleNames(ctx context.Context, userID string) []string {
	rows, err := s.db.QueryContext(ctx,
		`SELECT r.name FROM user_roles ur JOIN roles r ON r.id = ur.role_id WHERE ur.user_id = ? ORDER BY r.name`, userID)
	if err != nil {
		return []string{}
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var n string
		if rows.Scan(&n) == nil {
			out = append(out, n)
		}
	}
	return out
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
