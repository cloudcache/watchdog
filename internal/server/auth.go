package server

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

const (
	sessionCookie = "wd_session"
	csrfCookie    = "wd_csrf"
	sessionTTL    = 30 * 24 * time.Hour
	resetTTL      = time.Hour
)

var errNoSession = errors.New("no session")

func hashPassword(pw string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	return string(b), err
}

func checkPassword(hash, pw string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(pw)) == nil
}

// authenticate validates the session cookie and builds the principal.
func (s *Server) authenticate(c *gin.Context) (*principal, error) {
	token, err := c.Cookie(sessionCookie)
	if err != nil || token == "" {
		return nil, errNoSession
	}
	ctx := c.Request.Context()
	hash := sha256hex(token)
	var userID, username, status string
	err = s.db.QueryRowContext(ctx, `
		SELECT u.id, u.username, u.status
		FROM sessions s JOIN users u ON u.id = s.user_id
		WHERE s.token_sha256 = ? AND s.revoked_at IS NULL AND s.expires_at > NOW(3)`,
		hash).Scan(&userID, &username, &status)
	if err != nil {
		return nil, err
	}
	if status != "active" {
		return nil, errors.New("user disabled")
	}
	abilities, isAdmin, err := loadPrincipalAbilities(ctx, s.db, userID)
	if err != nil {
		return nil, err
	}
	_, _ = s.db.ExecContext(ctx, `UPDATE sessions SET last_active_at = NOW(3) WHERE token_sha256 = ?`, hash)
	return &principal{UserID: userID, Username: username, IsAdmin: isAdmin, Abilities: abilities}, nil
}

// startSession creates a session row and sets the session + CSRF cookies.
func (s *Server) startSession(c *gin.Context, userID string) error {
	token := randomToken()
	csrf := randomToken()
	id := newID()
	digest := clientDigest(c)
	if _, err := s.db.ExecContext(c.Request.Context(), `
		INSERT INTO sessions (id, user_id, token_sha256, client_digest, expires_at)
		VALUES (?, ?, ?, ?, DATE_ADD(NOW(3), INTERVAL ? SECOND))`,
		id, userID, sha256hex(token), digest, int(sessionTTL.Seconds())); err != nil {
		return err
	}
	s.setAuthCookies(c, token, csrf)
	return nil
}

func (s *Server) setAuthCookies(c *gin.Context, token, csrf string) {
	secure := c.Request.TLS != nil
	maxAge := int(sessionTTL.Seconds())
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(sessionCookie, token, maxAge, "/", "", secure, true) // HttpOnly
	c.SetCookie(csrfCookie, csrf, maxAge, "/", "", secure, false)    // readable by JS (double-submit)
}

func (s *Server) clearAuthCookies(c *gin.Context) {
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(sessionCookie, "", -1, "/", "", c.Request.TLS != nil, true)
	c.SetCookie(csrfCookie, "", -1, "/", "", c.Request.TLS != nil, false)
}

func (s *Server) revokeSessionCookie(c *gin.Context) {
	if token, err := c.Cookie(sessionCookie); err == nil && token != "" {
		_, _ = s.db.ExecContext(c.Request.Context(),
			`UPDATE sessions SET revoked_at = NOW(3) WHERE token_sha256 = ? AND revoked_at IS NULL`, sha256hex(token))
	}
	s.clearAuthCookies(c)
}

func clientDigest(c *gin.Context) string {
	d := c.ClientIP() + " " + c.Request.UserAgent()
	if len(d) > 250 {
		d = d[:250]
	}
	return d
}

// EnsureFirstAdmin creates the single bootstrap administrator on an empty install.
func (s *Server) EnsureFirstAdmin(ctx context.Context) error {
	var bootstrapped bool
	err := s.db.QueryRowContext(ctx, `SELECT admin_bootstrapped FROM watchdog_installation WHERE id = 1`).Scan(&bootstrapped)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if bootstrapped {
		return nil
	}
	username := strings.TrimSpace(s.cfg.Admin.Username)
	if username == "" {
		username = "admin"
	}
	password := s.cfg.Admin.Password
	generated := false
	if password == "" {
		password = randomToken()[:16]
		generated = true
	}
	hash, err := hashPassword(password)
	if err != nil {
		return err
	}
	userID := newID()
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO users (id, username, email, display_name, password_hash, status)
		VALUES (?, ?, '', 'Administrator', ?, 'active')`, userID, username, hash); err != nil {
		return err
	}
	var roleID string
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM roles WHERE name = ?`, roleAdministrator).Scan(&roleID); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO user_roles (user_id, role_id) VALUES (?, ?)`, userID, roleID); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE watchdog_installation SET admin_bootstrapped = 1 WHERE id = 1`); err != nil {
		return err
	}
	if generated {
		log.Printf("watchdog-server: bootstrapped administrator %q with generated password: %s", username, password)
		log.Printf("watchdog-server: set WATCHDOG_ADMIN_PASSWORD to control it, and change it after first login")
	}
	return nil
}

func (s *Server) createResetToken(ctx context.Context, userID string) (string, error) {
	token := randomToken()
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO password_reset_tokens (id, user_id, token_sha256, expires_at)
		VALUES (?, ?, ?, DATE_ADD(NOW(3), INTERVAL ? SECOND))`,
		newID(), userID, sha256hex(token), int(resetTTL.Seconds())); err != nil {
		return "", err
	}
	return token, nil
}

func (s *Server) consumeResetToken(ctx context.Context, token string) (string, error) {
	var id, userID string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, user_id FROM password_reset_tokens
		WHERE token_sha256 = ? AND used_at IS NULL AND expires_at > NOW(3)`,
		sha256hex(token)).Scan(&id, &userID)
	if err != nil {
		return "", err
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE password_reset_tokens SET used_at = NOW(3) WHERE id = ?`, id); err != nil {
		return "", err
	}
	return userID, nil
}

// revokeUserSessions ends all sessions for a user (on disable or password change).
func (s *Server) revokeUserSessions(ctx context.Context, userID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE sessions SET revoked_at = NOW(3) WHERE user_id = ? AND revoked_at IS NULL`, userID)
	return err
}
