package server

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
)

// registerAccessRoutes wires per-user resource-grant management (the resource half
// of the RBAC two-gate; docs/watchdog-kiss-architecture.md §5.1). Viewing a user's
// grants needs user.view; changing them needs user.manage. Flow access is derived
// from device/port grants + the flow.* abilities, so it has no separate ACL table.
func (s *Server) registerAccessRoutes(auth *gin.RouterGroup) {
	auth.GET("/users/:id/access", s.requirePermission("user.view"), s.getUserAccess)
	auth.PUT("/users/:id/access", s.requirePermission("user.manage"), s.replaceUserAccess)
}

// grant table/column pairs are code constants, never request input.
var accessGrantTables = []struct {
	field  string // JSON field
	table  string
	column string
}{
	{"device_ids", "user_device_permissions", "device_id"},
	{"device_group_ids", "user_device_group_permissions", "device_group_id"},
	{"port_ids", "user_port_permissions", "port_id"},
	{"billing_account_ids", "user_billing_permissions", "account_id"},
}

func (s *Server) getUserAccess(c *gin.Context) {
	id := c.Param("id")
	ctx := c.Request.Context()
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE id = ?`, id).Scan(&exists); err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if exists == 0 {
		fail(c, http.StatusNotFound, "not_found", "user not found")
		return
	}
	out := gin.H{"user_id": id}
	for _, g := range accessGrantTables {
		ids, err := s.grantIDs(ctx, g.table, g.column, id)
		if err != nil {
			fail(c, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		out[g.field] = ids
	}
	c.JSON(http.StatusOK, out)
}

func (s *Server) replaceUserAccess(c *gin.Context) {
	id := c.Param("id")
	var req struct {
		DeviceIDs         []string `json:"device_ids"`
		DeviceGroupIDs    []string `json:"device_group_ids"`
		PortIDs           []string `json:"port_ids"`
		BillingAccountIDs []string `json:"billing_account_ids"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	byField := map[string][]string{
		"device_ids":          req.DeviceIDs,
		"device_group_ids":    req.DeviceGroupIDs,
		"port_ids":            req.PortIDs,
		"billing_account_ids": req.BillingAccountIDs,
	}
	ctx := c.Request.Context()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	defer tx.Rollback()
	for _, g := range accessGrantTables {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+g.table+" WHERE user_id = ?", id); err != nil {
			fail(c, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		for _, rid := range byField[g.field] {
			if _, err := tx.ExecContext(ctx, "INSERT INTO "+g.table+" (user_id, "+g.column+") VALUES (?, ?)", id, rid); err != nil {
				// FK violation → the granted object id does not exist
				fail(c, http.StatusBadRequest, "invalid_request", "unknown "+g.column+": "+rid)
				return
			}
		}
	}
	if err := tx.Commit(); err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	s.audit(ctx, currentPrincipal(c).UserID, "user.access_update", "user", id)
	s.getUserAccess(c)
}

func (s *Server) grantIDs(ctx context.Context, table, column, userID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+column+" FROM "+table+" WHERE user_id = ? ORDER BY "+column, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
