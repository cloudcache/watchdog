// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowvpn"
	"github.com/gin-gonic/gin"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// VPN rules are editable management state (KISS-06 phase-1c, de-tenanted port of
// the hub flow_vpn_rules domain). A worker never consumes these rows directly;
// publication freezes canonical rules into an immutable rule set. The rule AST
// (match/effect/weight/priority) reuses the committed flowvpn scorer types.
const (
	vpnRuleKindPassive      = string(flowvpn.RuleKindPassive)
	vpnRuleKindIntelligence = string(flowvpn.RuleKindIntelligence)
	vpnRuleKindProbe        = string(flowvpn.RuleKindProbe)

	vpnRuleStatusDraft     = "draft"
	vpnRuleStatusActive    = "active"
	vpnRuleStatusSuspended = "suspended"
	vpnRuleStatusRetired   = "retired"
)

var errVPNRuleInvalid = errors.New("VPN rule is invalid")
var errVPNRuleNameConflict = errors.New("VPN rule name already exists")

type vpnRule struct {
	ID                string                 `json:"id"`
	Name              string                 `json:"name"`
	Kind              string                 `json:"kind"`
	RuleSchemaVersion uint16                 `json:"rule_schema_version"`
	Match             flowvpn.Match          `json:"match"`
	Effect            flowvpn.RuleEffect     `json:"effect"`
	Weight            uint16                 `json:"weight"`
	Priority          uint16                 `json:"priority"`
	FamilyHint        flowvpn.ProtocolFamily `json:"family_hint,omitempty"`
	Status            string                 `json:"status"`
	RowVersion        uint64                 `json:"row_version"`
	CreatedBy         string                 `json:"created_by"`
	UpdatedBy         string                 `json:"updated_by"`
	CreatedAt         time.Time              `json:"created_at"`
	UpdatedAt         time.Time              `json:"updated_at"`
}

type vpnRuleInput struct {
	Name       string                 `json:"name"`
	Kind       string                 `json:"kind"`
	Match      flowvpn.Match          `json:"match"`
	Effect     flowvpn.RuleEffect     `json:"effect"`
	Weight     uint16                 `json:"weight"`
	Priority   uint16                 `json:"priority"`
	FamilyHint flowvpn.ProtocolFamily `json:"family_hint,omitempty"`
	Status     string                 `json:"status"`
}

// normalizeVPNRule validates identity/name/kind/status and canonicalizes the rule
// AST through the flowvpn scorer, which enforces the effect/weight invariants.
func normalizeVPNRule(item vpnRule) (vpnRule, error) {
	item.Name = strings.TrimSpace(item.Name)
	item.Kind = strings.ToLower(strings.TrimSpace(item.Kind))
	item.Status = strings.ToLower(strings.TrimSpace(item.Status))
	if item.ID == "" || item.CreatedBy == "" || item.UpdatedBy == "" {
		return vpnRule{}, fmt.Errorf("%w: identity and actors are required", errVPNRuleInvalid)
	}
	if item.Name == "" || len(item.Name) > 190 {
		return vpnRule{}, fmt.Errorf("%w: name must contain 1..190 characters", errVPNRuleInvalid)
	}
	if item.Kind == "" {
		item.Kind = vpnRuleKindPassive
	}
	if !validVPNRuleKind(item.Kind) {
		return vpnRule{}, fmt.Errorf("%w: unsupported kind", errVPNRuleInvalid)
	}
	if item.Status == "" {
		item.Status = vpnRuleStatusDraft
	}
	if !validVPNRuleStatus(item.Status) {
		return vpnRule{}, fmt.Errorf("%w: unsupported status", errVPNRuleInvalid)
	}
	canonical, err := flowvpn.NormalizeRule(flowvpn.Rule{
		ID: item.ID, Effect: item.Effect, Weight: item.Weight, Priority: item.Priority, Match: item.Match, FamilyHint: item.FamilyHint,
	})
	if err != nil {
		return vpnRule{}, fmt.Errorf("%w: %v", errVPNRuleInvalid, err)
	}
	item.RuleSchemaVersion = uint16(flowvpn.RuleSchemaV1)
	item.Match = canonical.Match
	return item, nil
}

func validVPNRuleKind(value string) bool {
	return value == vpnRuleKindPassive || value == vpnRuleKindIntelligence || value == vpnRuleKindProbe
}

func validVPNRuleStatus(value string) bool {
	return value == vpnRuleStatusDraft || value == vpnRuleStatusActive || value == vpnRuleStatusSuspended || value == vpnRuleStatusRetired
}

const vpnRuleSelect = `SELECT id, name, kind, rule_schema_version, match_json, effect, weight, priority,
	family_hint, status, row_version, created_by, updated_by, created_at, updated_at FROM flow_vpn_rules`

func scanVPNRule(row rowScanner) (vpnRule, error) {
	var item vpnRule
	var matchJSON []byte
	if err := row.Scan(&item.ID, &item.Name, &item.Kind, &item.RuleSchemaVersion, &matchJSON,
		&item.Effect, &item.Weight, &item.Priority, &item.FamilyHint, &item.Status, &item.RowVersion,
		&item.CreatedBy, &item.UpdatedBy, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return vpnRule{}, err
	}
	if item.RuleSchemaVersion != uint16(flowvpn.RuleSchemaV1) {
		return vpnRule{}, errVPNRuleInvalid
	}
	if err := json.Unmarshal(matchJSON, &item.Match); err != nil {
		return vpnRule{}, err
	}
	return normalizeVPNRule(item)
}

func (s *Server) readVPNRule(ctx context.Context, id string) (vpnRule, error) {
	return scanVPNRule(s.db.QueryRowContext(ctx, vpnRuleSelect+" WHERE id=? AND deleted_at IS NULL", id))
}

func (s *Server) registerFlowVPNRuleRoutes(auth *gin.RouterGroup) {
	view := s.requirePermission("flow.vpn.view")
	manage := s.requirePermission("flow.vpn.manage")
	rules := auth.Group("/flow/vpn/rules")
	rules.GET("", view, s.listVPNRules)
	rules.POST("", manage, s.createVPNRule)
	rules.GET("/:rule_id", view, s.getVPNRule)
	rules.PATCH("/:rule_id", manage, s.updateVPNRule)
	rules.DELETE("/:rule_id", manage, s.deleteVPNRule)
}

func (s *Server) listVPNRules(c *gin.Context) {
	page, ok := parseInventoryPage(c, []string{"kind", "effect", "status"}, map[string]string{
		"name": "name", "kind": "kind", "effect": "effect", "weight": "weight",
		"priority": "priority", "status": "status", "created_at": "created_at", "updated_at": "updated_at",
	}, "updated_at")
	if !ok {
		return
	}
	where := []string{"deleted_at IS NULL"}
	args := []any{}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, "(name LIKE ? OR id LIKE ?)")
		args = append(args, like, like)
	}
	for _, filter := range []struct{ param, column string }{
		{"kind", "kind"}, {"effect", "effect"}, {"status", "status"},
	} {
		if value := strings.ToLower(strings.TrimSpace(c.Query(filter.param))); value != "" {
			where = append(where, filter.column+"=?")
			args = append(args, value)
		}
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM flow_vpn_rules"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	query := vpnRuleSelect + clause + fmt.Sprintf(" ORDER BY %s %s, id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order)
	queryArgs := append(append([]any{}, args...), page.Limit, page.Offset)
	rows, err := s.db.QueryContext(c.Request.Context(), query, queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]vpnRule, 0, page.Limit)
	for rows.Next() {
		item, err := scanVPNRule(rows)
		if err != nil {
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

func (s *Server) getVPNRule(c *gin.Context) {
	item, err := s.readVPNRule(c.Request.Context(), c.Param("rule_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(item.RowVersion))
	c.JSON(http.StatusOK, item)
}

func (s *Server) createVPNRule(c *gin.Context) {
	var input vpnRuleInput
	if !addressDecodeStrict(c, &input, 256<<10) {
		return
	}
	p := currentPrincipal(c)
	item, err := normalizeVPNRule(vpnRuleFromInput(newID(), p.UserID, input))
	if err != nil {
		writeVPNRuleError(c, err)
		return
	}
	matchJSON, err := json.Marshal(item.Match)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", "encode match")
		return
	}
	if _, err := s.db.ExecContext(c.Request.Context(), `INSERT INTO flow_vpn_rules
		(id, name, kind, rule_schema_version, match_json, effect, weight, priority, family_hint, status, created_by, updated_by)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		item.ID, item.Name, item.Kind, item.RuleSchemaVersion, matchJSON, string(item.Effect),
		item.Weight, item.Priority, string(item.FamilyHint), item.Status, item.CreatedBy, item.UpdatedBy,
	); err != nil {
		writeVPNRuleError(c, mapVPNRuleWriteError(err))
		return
	}
	created, err := s.readVPNRule(c.Request.Context(), item.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), p.UserID, "flow.vpn_rule.create", "flow_vpn_rule", item.ID)
	c.Header("ETag", etag(created.RowVersion))
	c.Header("Location", "/api/v1/flow/vpn/rules/"+item.ID)
	c.JSON(http.StatusCreated, created)
}

func (s *Server) updateVPNRule(c *gin.Context) {
	ruleID := c.Param("rule_id")
	existing, err := s.readVPNRule(c.Request.Context(), ruleID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	} else if supplied && expected != existing.RowVersion {
		writeSQLError(c, errVersionConflict)
		return
	}
	var input vpnRuleInput
	if !addressDecodeStrict(c, &input, 256<<10) {
		return
	}
	p := currentPrincipal(c)
	item := vpnRuleFromInput(ruleID, p.UserID, input)
	item.CreatedBy = existing.CreatedBy
	item, err = normalizeVPNRule(item)
	if err != nil {
		writeVPNRuleError(c, err)
		return
	}
	matchJSON, err := json.Marshal(item.Match)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", "encode match")
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), `UPDATE flow_vpn_rules
		SET name=?, kind=?, rule_schema_version=?, match_json=?, effect=?, weight=?, priority=?,
		    family_hint=?, status=?, updated_by=?, row_version=row_version+1, updated_at=CURRENT_TIMESTAMP(3)
		WHERE id=? AND row_version=? AND deleted_at IS NULL`,
		item.Name, item.Kind, item.RuleSchemaVersion, matchJSON, string(item.Effect), item.Weight,
		item.Priority, string(item.FamilyHint), item.Status, p.UserID, ruleID, existing.RowVersion,
	)
	if err != nil {
		writeVPNRuleError(c, mapVPNRuleWriteError(err))
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	updated, err := s.readVPNRule(c.Request.Context(), ruleID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), p.UserID, "flow.vpn_rule.update", "flow_vpn_rule", ruleID)
	c.Header("ETag", etag(updated.RowVersion))
	c.JSON(http.StatusOK, updated)
}

func (s *Server) deleteVPNRule(c *gin.Context) {
	ruleID := c.Param("rule_id")
	existing, err := s.readVPNRule(c.Request.Context(), ruleID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	} else if supplied && expected != existing.RowVersion {
		writeSQLError(c, errVersionConflict)
		return
	}
	p := currentPrincipal(c)
	result, err := s.db.ExecContext(c.Request.Context(), `UPDATE flow_vpn_rules
		SET status='retired', deleted_at=CURRENT_TIMESTAMP(3), updated_by=?,
		    row_version=row_version+1, updated_at=CURRENT_TIMESTAMP(3)
		WHERE id=? AND row_version=? AND deleted_at IS NULL`, p.UserID, ruleID, existing.RowVersion)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	s.audit(c.Request.Context(), p.UserID, "flow.vpn_rule.delete", "flow_vpn_rule", ruleID)
	c.Status(http.StatusNoContent)
}

func vpnRuleFromInput(id, actorID string, input vpnRuleInput) vpnRule {
	return vpnRule{
		ID: id, Name: input.Name, Kind: input.Kind, Match: input.Match, Effect: input.Effect,
		Weight: input.Weight, Priority: input.Priority, FamilyHint: input.FamilyHint, Status: input.Status,
		CreatedBy: actorID, UpdatedBy: actorID,
	}
}

func mapVPNRuleWriteError(err error) error {
	var mysqlError *mysqldriver.MySQLError
	if errors.As(err, &mysqlError) && mysqlError.Number == 1062 {
		return errVPNRuleNameConflict
	}
	return err
}

func writeVPNRuleError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errVPNRuleNameConflict):
		fail(c, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, errVPNRuleInvalid):
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
	default:
		writeSQLError(c, err)
	}
}
