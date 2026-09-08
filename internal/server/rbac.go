package server

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/gin-gonic/gin"
)

// abilityCatalog is the fixed permission vocabulary (docs/watchdog-kiss-architecture.md §5.1).
// New abilities are added here, not generated at runtime.
var abilityCatalog = []string{
	"user.view", "user.create", "user.update", "user.delete", "user.manage",
	"role.view", "role.create", "role.update", "role.delete",
	"device.view", "device.viewAll", "device.create", "device.update", "device.delete", "device.discover",
	"port.view", "port.viewAll", "port.update",
	"bill.view", "bill.viewAll", "bill.create", "bill.update", "bill.delete",
	"bill.calculate", "bill.reconcile", "bill.approve", "bill.export",
	"flow.view.customer", "flow.view.supplier", "flow.view.raw",
	"flow.export.customer", "flow.export.supplier", "flow.export.raw", "flow.reclassify", "flow.probe",
	"flow.device.view", "flow.device.manage",
	"agent.view", "agent.manage", "address.view", "address.manage", "address.publish",
	"job.view", "job.manage", "audit.view",
}

const roleAdministrator = "administrator"

// defaultRole is a built-in role and the abilities it grants.
type defaultRole struct {
	name      string
	title     string
	protected bool
	abilities []string // "*" means the whole catalogue
}

var defaultRoles = []defaultRole{
	{roleAdministrator, "Administrator", true, []string{"*"}},
	{"operator", "Operator", false, []string{
		"device.view", "device.viewAll", "device.create", "device.update", "device.delete", "device.discover",
		"port.view", "port.viewAll", "port.update",
		"agent.view", "agent.manage", "job.view", "job.manage", "address.view", "audit.view",
		"flow.device.view", "flow.device.manage",
	}},
	{"analyst", "Analyst", false, []string{
		"device.view", "device.viewAll", "port.view", "port.viewAll",
		"flow.view.customer", "flow.view.supplier", "flow.view.raw",
		"flow.export.customer", "flow.export.supplier", "flow.export.raw",
		"flow.device.view",
		"bill.view", "audit.view",
	}},
	{"billing", "Billing", false, []string{
		"bill.view", "bill.viewAll", "bill.create", "bill.update", "bill.delete",
		"bill.calculate", "bill.reconcile", "bill.approve", "bill.export",
		"flow.view.customer", "flow.view.supplier", "flow.view.raw", "device.view", "port.view",
	}},
	{"viewer", "Viewer", false, []string{
		"device.view", "device.viewAll", "port.view", "flow.view.customer", "bill.view",
	}},
}

// principal is the authenticated caller attached to the request context.
type principal struct {
	UserID    string
	Username  string
	IsAdmin   bool
	Abilities map[string]bool
}

func (p *principal) can(ability string) bool {
	if p == nil {
		return false
	}
	return p.IsAdmin || p.Abilities[ability]
}

const principalKey = "wd_principal"

func currentPrincipal(c *gin.Context) *principal {
	if v, ok := c.Get(principalKey); ok {
		if p, ok := v.(*principal); ok {
			return p
		}
	}
	return nil
}

// EnsureRBACSeed makes the fixed ability catalogue, the built-in roles, and their
// role_permissions exist. Idempotent; safe to run on every startup.
func EnsureRBACSeed(ctx context.Context, db *sql.DB) error {
	abilityIDs := map[string]string{}
	for _, ability := range abilityCatalog {
		id := newID()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO permissions (id, ability, subject) VALUES (?, ?, ?)
			 ON DUPLICATE KEY UPDATE ability = ability`,
			id, ability, subjectOf(ability)); err != nil {
			return err
		}
		if err := db.QueryRowContext(ctx, `SELECT id FROM permissions WHERE ability = ?`, ability).Scan(&id); err != nil {
			return err
		}
		abilityIDs[ability] = id
	}
	for _, r := range defaultRoles {
		roleID := newID()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO roles (id, name, title, protected) VALUES (?, ?, ?, ?)
			 ON DUPLICATE KEY UPDATE title = VALUES(title), protected = VALUES(protected)`,
			roleID, r.name, r.title, r.protected); err != nil {
			return err
		}
		if err := db.QueryRowContext(ctx, `SELECT id FROM roles WHERE name = ?`, r.name).Scan(&roleID); err != nil {
			return err
		}
		grant := r.abilities
		if len(grant) == 1 && grant[0] == "*" {
			grant = abilityCatalog
		}
		for _, ability := range grant {
			permID, ok := abilityIDs[ability]
			if !ok {
				continue
			}
			if _, err := db.ExecContext(ctx,
				`INSERT INTO role_permissions (role_id, permission_id) VALUES (?, ?)
				 ON DUPLICATE KEY UPDATE role_id = role_id`, roleID, permID); err != nil {
				return err
			}
		}
	}
	return nil
}

func subjectOf(ability string) string {
	for i := 0; i < len(ability); i++ {
		if ability[i] == '.' {
			return ability[:i]
		}
	}
	return ability
}

// loadPrincipalAbilities loads a user's effective abilities and admin flag.
func loadPrincipalAbilities(ctx context.Context, db *sql.DB, userID string) (map[string]bool, bool, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT p.ability, r.name
		FROM user_roles ur
		JOIN roles r ON r.id = ur.role_id
		JOIN role_permissions rp ON rp.role_id = ur.role_id
		JOIN permissions p ON p.id = rp.permission_id
		WHERE ur.user_id = ?`, userID)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	abilities := map[string]bool{}
	isAdmin := false
	for rows.Next() {
		var ability, roleName string
		if err := rows.Scan(&ability, &roleName); err != nil {
			return nil, false, err
		}
		abilities[ability] = true
		if roleName == roleAdministrator {
			isAdmin = true
		}
	}
	return abilities, isAdmin, rows.Err()
}

// requireAuth validates the session cookie and attaches the principal.
func (s *Server) requireAuth(c *gin.Context) {
	p, err := s.authenticate(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": gin.H{"code": "unauthorized", "message": "authentication required"}})
		return
	}
	c.Set(principalKey, p)
	c.Next()
}

// requirePermission gates an action by ability (administrator always passes).
func (s *Server) requirePermission(ability string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if p := currentPrincipal(c); p != nil && p.can(ability) {
			c.Next()
			return
		}
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": gin.H{"code": "forbidden", "message": "missing ability: " + ability}})
	}
}

// requireCSRF enforces a double-submit CSRF token on state-changing methods.
func (s *Server) requireCSRF(c *gin.Context) {
	switch c.Request.Method {
	case http.MethodPost, http.MethodPatch, http.MethodPut, http.MethodDelete:
		cookie, _ := c.Cookie(csrfCookie)
		header := c.GetHeader("X-CSRF-Token")
		if cookie == "" || header == "" || cookie != header {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": gin.H{"code": "csrf", "message": "invalid CSRF token"}})
			return
		}
	}
	c.Next()
}
