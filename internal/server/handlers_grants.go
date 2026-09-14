package server

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/cloudcache/watchdog/internal/watchdog"
	"github.com/gin-gonic/gin"
)

// registerAccessRoutes wires per-user resource-grant management (the resource half
// of the RBAC two-gate; docs/watchdog-kiss-architecture.md §5.1). Viewing a user's
// grants needs user.view; changing them needs user.manage. Flow access is derived
// from device/port grants + the flow.* abilities, so it has no separate ACL table.
func (s *Server) registerAccessRoutes(auth *gin.RouterGroup) {
	auth.GET("/users/:id/access", s.requirePermission("user.view"), s.getUserAccess)
	auth.PUT("/users/:id/access", s.requirePermission("user.manage"), s.replaceUserAccess)
	auth.GET("/users/:id/access-options", s.requirePermission("user.manage"), s.listUserAccessOptions)
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
	{"aggregate_graph_ids", "user_aggregate_graph_permissions", "aggregate_graph_id"},
	{"metrics", "user_metric_permissions", "metric"},
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
		AggregateGraphIDs []string `json:"aggregate_graph_ids"`
		Metrics           []string `json:"metrics"`
	}
	if !decodeStrictBody(c, &req) {
		return
	}
	byField := map[string][]string{
		"device_ids":          req.DeviceIDs,
		"device_group_ids":    req.DeviceGroupIDs,
		"port_ids":            req.PortIDs,
		"billing_account_ids": req.BillingAccountIDs,
		"aggregate_graph_ids": req.AggregateGraphIDs,
		"metrics":             req.Metrics,
	}
	ctx := c.Request.Context()
	var exists bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE id=?)", id).Scan(&exists); err != nil {
		writeSQLError(c, err)
		return
	}
	if !exists {
		fail(c, http.StatusNotFound, "not_found", "user not found")
		return
	}
	for _, g := range accessGrantTables {
		maxLength := 26
		if g.field == "aggregate_graph_ids" {
			maxLength = 64
		} else if g.field == "metrics" {
			maxLength = 190
		}
		normalized, err := normalizeGrantValues(g.field, byField[g.field], 10000, maxLength)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		byField[g.field] = normalized
	}
	for _, metric := range byField["metrics"] {
		if !watchdog.IsKnownMetric(metric) {
			fail(c, http.StatusBadRequest, "invalid_request", "metrics contains an unknown metric")
			return
		}
	}
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
				writeSQLError(c, err)
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

func normalizeGrantValues(field string, values []string, maxCount, maxLength int) ([]string, error) {
	if len(values) > maxCount {
		return nil, fmt.Errorf("%s accepts at most %d values", field, maxCount)
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" || len(value) > maxLength {
			return nil, fmt.Errorf("%s contains an invalid value", field)
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out, nil
}

type accessOption struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Description string `json:"description"`
}

// listUserAccessOptions is an administrator-only, paged resource picker. It
// deliberately reads the management catalogue rather than a caller-scoped
// list: an administrator must be able to grant resources they do not consume.
func (s *Server) listUserAccessOptions(c *gin.Context) {
	ctx := c.Request.Context()
	var exists bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE id=?)", c.Param("id")).Scan(&exists); err != nil {
		writeSQLError(c, err)
		return
	}
	if !exists {
		fail(c, http.StatusNotFound, "not_found", "user not found")
		return
	}
	page, ok := parseInventoryPage(c, []string{"type"}, map[string]string{"label": "label"}, "label")
	if !ok {
		return
	}
	kind := strings.TrimSpace(c.Query("type"))
	q := strings.TrimSpace(c.Query("q"))
	if kind == "metric" {
		items := make([]accessOption, 0, len(watchdog.MetricCatalog))
		needle := strings.ToLower(q)
		for _, metric := range watchdog.MetricCatalog {
			haystack := strings.ToLower(metric.Name + " " + metric.Family + " " + metric.Description)
			if needle == "" || strings.Contains(haystack, needle) {
				items = append(items, accessOption{ID: metric.Name, Label: metric.Name, Description: metric.Family + " · " + metric.Description})
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Label < items[j].Label })
		total := len(items)
		if page.Offset >= total {
			items = []accessOption{}
		} else {
			end := page.Offset + page.Limit
			if end > total {
				end = total
			}
			items = items[page.Offset:end]
		}
		c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
		return
	}

	var selectSQL string
	switch kind {
	case "device":
		selectSQL = `SELECT d.id,COALESCE(NULLIF(d.display_name,''),NULLIF(d.sys_name,''),d.host) label,d.host description FROM devices d`
	case "device_group":
		selectSQL = `SELECT g.id,g.name label,g.kind description FROM device_groups g`
	case "port":
		selectSQL = `SELECT p.id,CONCAT(COALESCE(NULLIF(d.display_name,''),NULLIF(d.sys_name,''),d.host),' / ',COALESCE(NULLIF(p.if_name,''),p.if_descr)) label,COALESCE(NULLIF(p.if_alias,''),p.if_descr) description FROM ports p JOIN devices d ON d.id=p.device_id`
	case "billing_account":
		selectSQL = `SELECT a.id,a.name label,CONCAT(a.bill_type,IF(a.ref='', '',CONCAT(' · ',a.ref))) description FROM billing_accounts a`
	case "aggregate_graph":
		selectSQL = `SELECT g.id,g.name label,COALESCE(g.description,'') description FROM aggregate_graphs g`
	default:
		fail(c, http.StatusBadRequest, "invalid_filter", "type must be device, device_group, port, billing_account, aggregate_graph or metric")
		return
	}
	where, args := "", []any{}
	if q != "" {
		where = ` WHERE label LIKE ? ESCAPE '\\' OR description LIKE ? ESCAPE '\\'`
		like := "%" + escapeLike(q) + "%"
		args = append(args, like, like)
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM (`+selectSQL+`) access_options`+where, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	queryArgs := append(append([]any{}, args...), page.Limit, page.Offset)
	rows, err := s.db.QueryContext(ctx, `SELECT id,label,description FROM (`+selectSQL+`) access_options`+where+` ORDER BY label `+page.Order+`,id LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]accessOption, 0, page.Limit)
	for rows.Next() {
		var item accessOption
		if err := rows.Scan(&item.ID, &item.Label, &item.Description); err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
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
