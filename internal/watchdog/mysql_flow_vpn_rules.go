package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/cloudcache/watchdog/internal/flowvpn"
	mysqldriver "github.com/go-sql-driver/mysql"
)

var vpnRuleSortColumns = map[string]string{
	"name": "name", "kind": "kind", "effect": "effect", "weight": "weight",
	"priority": "priority", "status": "status", "created_at": "created_at", "updated_at": "updated_at",
}

const vpnRuleSelect = `SELECT id, tenant_id, name, kind, rule_schema_version, match_json,
	effect, weight, priority, status, row_version, created_by, updated_by, created_at, updated_at
	FROM flow_vpn_rules`

func (s *MySQLStore) ListVPNRules(ctx context.Context, tenantID ID, filter VPNRuleListFilter) ([]VPNRule, int64, error) {
	filter, err := normalizeVPNRuleListFilter(filter)
	if err != nil || tenantID == "" {
		return nil, 0, ErrVPNRuleInvalid
	}
	where, args := vpnRuleWhere(tenantID, filter)
	var total int64
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM flow_vpn_rules WHERE "+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	direction := "ASC"
	if filter.Descending {
		direction = "DESC"
	}
	args = append(args, filter.Limit, filter.Offset)
	rows, err := s.db.QueryContext(ctx, vpnRuleSelect+" WHERE "+where+" ORDER BY "+vpnRuleSortColumns[filter.SortBy]+" "+direction+", id "+direction+" LIMIT ? OFFSET ?", args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]VPNRule, 0, filter.Limit)
	for rows.Next() {
		item, err := scanVPNRule(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *MySQLStore) GetVPNRule(ctx context.Context, tenantID, ruleID ID) (VPNRule, error) {
	return scanVPNRule(s.db.QueryRowContext(ctx, vpnRuleSelect+" WHERE tenant_id = ? AND id = ? AND deleted_at IS NULL", tenantID, ruleID))
}

func (s *MySQLStore) CreateVPNRule(ctx context.Context, item VPNRule) (VPNRule, error) {
	item, err := normalizeVPNRule(item)
	if err != nil {
		return VPNRule{}, err
	}
	matchJSON, err := json.Marshal(item.Match)
	if err != nil {
		return VPNRule{}, err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO flow_vpn_rules (
		id, tenant_id, name, kind, rule_schema_version, match_json, effect, weight, priority,
		behavior_json, intelligence_json, probe_policy_json, status, created_by, updated_by
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, JSON_OBJECT(), JSON_OBJECT(), JSON_OBJECT(), ?, ?, ?)`,
		item.ID, item.TenantID, item.Name, item.Kind, item.RuleSchemaVersion, matchJSON,
		item.Effect, item.Weight, item.Priority, item.Status, item.CreatedBy, item.UpdatedBy)
	if err != nil {
		return VPNRule{}, mapVPNRuleWriteError(err)
	}
	return s.GetVPNRule(ctx, item.TenantID, item.ID)
}

func (s *MySQLStore) UpdateVPNRule(ctx context.Context, item VPNRule, expectedVersion uint64) (VPNRule, error) {
	item, err := normalizeVPNRule(item)
	if err != nil || expectedVersion == 0 {
		return VPNRule{}, ErrVPNRuleInvalid
	}
	matchJSON, err := json.Marshal(item.Match)
	if err != nil {
		return VPNRule{}, err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE flow_vpn_rules
		SET name = ?, kind = ?, rule_schema_version = ?, match_json = ?, effect = ?, weight = ?,
		    priority = ?, status = ?, updated_by = ?, row_version = row_version + 1,
		    updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND row_version = ? AND deleted_at IS NULL`,
		item.Name, item.Kind, item.RuleSchemaVersion, matchJSON, item.Effect, item.Weight,
		item.Priority, item.Status, item.UpdatedBy, item.TenantID, item.ID, expectedVersion)
	if err != nil {
		return VPNRule{}, mapVPNRuleWriteError(err)
	}
	if err := requireVPNRuleWrite(result, s, ctx, item.TenantID, item.ID); err != nil {
		return VPNRule{}, err
	}
	return s.GetVPNRule(ctx, item.TenantID, item.ID)
}

func (s *MySQLStore) DeleteVPNRule(ctx context.Context, tenantID, ruleID, actorID ID, expectedVersion uint64) error {
	if tenantID == "" || ruleID == "" || actorID == "" || expectedVersion == 0 {
		return ErrVPNRuleInvalid
	}
	result, err := s.db.ExecContext(ctx, `UPDATE flow_vpn_rules
		SET status = 'deleted', deleted_at = CURRENT_TIMESTAMP(3), updated_by = ?,
		    row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND row_version = ? AND deleted_at IS NULL`, actorID, tenantID, ruleID, expectedVersion)
	if err != nil {
		return err
	}
	return requireVPNRuleWrite(result, s, ctx, tenantID, ruleID)
}

func vpnRuleWhere(tenantID ID, filter VPNRuleListFilter) (string, []any) {
	clauses := []string{"tenant_id = ?", "deleted_at IS NULL"}
	args := []any{tenantID}
	if filter.Search != "" {
		clauses = append(clauses, "(LOCATE(LOWER(?), LOWER(name)) > 0 OR LOCATE(LOWER(?), LOWER(id)) > 0)")
		args = append(args, filter.Search, filter.Search)
	}
	for _, entry := range []struct{ column, value string }{
		{"kind", filter.Kind}, {"effect", filter.Effect}, {"status", filter.Status},
	} {
		if entry.value != "" {
			clauses = append(clauses, entry.column+" = ?")
			args = append(args, entry.value)
		}
	}
	return strings.Join(clauses, " AND "), args
}

type vpnRuleScanner interface{ Scan(...any) error }

func scanVPNRule(scanner vpnRuleScanner) (VPNRule, error) {
	var item VPNRule
	var matchJSON []byte
	if err := scanner.Scan(&item.ID, &item.TenantID, &item.Name, &item.Kind, &item.RuleSchemaVersion,
		&matchJSON, &item.Effect, &item.Weight, &item.Priority, &item.Status, &item.RowVersion,
		&item.CreatedBy, &item.UpdatedBy, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return VPNRule{}, err
	}
	if item.RuleSchemaVersion != uint16(flowvpn.RuleSchemaV1) {
		return VPNRule{}, ErrVPNRuleInvalid
	}
	if err := json.Unmarshal(matchJSON, &item.Match); err != nil {
		return VPNRule{}, err
	}
	canonical, err := normalizeVPNRule(item)
	if err != nil {
		return VPNRule{}, err
	}
	return canonical, nil
}

func requireVPNRuleWrite(result sql.Result, store *MySQLStore, ctx context.Context, tenantID, ruleID ID) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 1 {
		return nil
	}
	var version uint64
	if err := store.db.QueryRowContext(ctx, "SELECT row_version FROM flow_vpn_rules WHERE tenant_id = ? AND id = ? AND deleted_at IS NULL", tenantID, ruleID).Scan(&version); errors.Is(err, sql.ErrNoRows) {
		return sql.ErrNoRows
	} else if err != nil {
		return err
	}
	return ErrVPNRuleVersionConflict
}

func mapVPNRuleWriteError(err error) error {
	var mysqlError *mysqldriver.MySQLError
	if errors.As(err, &mysqlError) && mysqlError.Number == 1062 {
		return ErrVPNRuleNameConflict
	}
	return err
}
