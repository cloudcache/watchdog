// SPDX-License-Identifier: AGPL-3.0-only

package billing

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const accountColumns = `id,COALESCE(party_id,''),name,status,bill_type,algorithm,billing_day,timezone,direction,default_layer,
	cdr_bps,quota_bytes,reconcile_abs,reconcile_percent,ref,notes,row_version,
	COALESCE(created_by,''),COALESCE(updated_by,''),created_at,updated_at`

func scanAccount(scanner interface{ Scan(...any) error }) (Account, error) {
	var item Account
	var cdr, quota sql.NullString
	err := scanner.Scan(&item.ID, &item.PartyID, &item.Name, &item.Status, &item.BillType, &item.Algorithm, &item.BillingDay,
		&item.Timezone, &item.Direction, &item.DefaultLayer, &cdr, &quota, &item.ReconcileAbs, &item.ReconcilePercent,
		&item.Ref, &item.Notes, &item.RowVersion, &item.CreatedBy, &item.UpdatedBy, &item.CreatedAt, &item.UpdatedAt)
	if err != nil {
		return Account{}, normalizeSQLError(err)
	}
	if cdr.Valid {
		if value, parseErr := strconv.ParseUint(cdr.String, 10, 64); parseErr == nil {
			item.CDRBPS = &value
		} else {
			return Account{}, parseErr
		}
	}
	if quota.Valid {
		if value, parseErr := strconv.ParseUint(quota.String, 10, 64); parseErr == nil {
			item.QuotaBytes = &value
		} else {
			return Account{}, parseErr
		}
	}
	return item, nil
}

func accountArgs(item Account, actor string) []any {
	return []any{nullString(item.PartyID), item.Name, item.Status, item.BillType, item.Algorithm, item.BillingDay, item.Timezone,
		item.Direction, item.DefaultLayer, item.CDRBPS, item.QuotaBytes, item.ReconcileAbs, item.ReconcilePercent,
		item.Ref, item.Notes, nullString(actor)}
}

func normalizeAccount(item Account) Account {
	item.PartyID, item.Name, item.Timezone = strings.TrimSpace(item.PartyID), strings.TrimSpace(item.Name), strings.TrimSpace(item.Timezone)
	item.Ref, item.Notes = strings.TrimSpace(item.Ref), strings.TrimSpace(item.Notes)
	if item.Status == "" {
		item.Status = "active"
	}
	if item.BillType == "" {
		item.BillType = "cdr"
	}
	if item.Algorithm == "" {
		item.Algorithm = Algorithm95th
	}
	if item.BillingDay == 0 {
		item.BillingDay = 1
	}
	if item.Timezone == "" {
		item.Timezone = "UTC"
	}
	if item.Direction == "" {
		item.Direction = DirectionAgg
	}
	if item.DefaultLayer == "" {
		item.DefaultLayer = LayerCustomer
	}
	if item.BillType == "cdr" {
		item.QuotaBytes = nil
	} else if item.BillType == "quota" {
		item.CDRBPS = nil
	}
	return item
}

func (s *Store) GetAccount(ctx context.Context, id string) (Account, error) {
	return scanAccount(s.db.QueryRowContext(ctx, `SELECT `+accountColumns+` FROM billing_accounts WHERE id=?`, id))
}

func (s *Store) ListAccounts(ctx context.Context, filter PageFilter, userID string, viewAll bool) ([]Account, int, error) {
	filter, order, err := normalizePage(filter, map[string]string{"name": "a.name", "status": "a.status", "bill_type": "a.bill_type", "algorithm": "a.algorithm", "created_at": "a.created_at", "updated_at": "a.updated_at"}, "name")
	if err != nil {
		return nil, 0, err
	}
	where, args := []string{"1=1"}, []any{}
	if !viewAll {
		where, args = append(where, "EXISTS(SELECT 1 FROM user_billing_permissions ubp WHERE ubp.account_id=a.id AND ubp.user_id=?)"), append(args, userID)
	}
	if filter.Query != "" {
		where, args = append(where, "(a.name LIKE ? OR a.ref LIKE ? OR a.notes LIKE ?)"), append(args, "%"+filter.Query+"%", "%"+filter.Query+"%", "%"+filter.Query+"%")
	}
	if filter.Status != "" {
		where, args = append(where, "a.status=?"), append(args, filter.Status)
	}
	if filter.Type != "" {
		where, args = append(where, "a.bill_type=?"), append(args, filter.Type)
	}
	predicate := strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM billing_accounts a WHERE `+predicate, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+accountColumns+` FROM billing_accounts a WHERE `+predicate+` ORDER BY `+order+`,a.id LIMIT ? OFFSET ?`, append(args, filter.Limit, filter.Offset)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]Account, 0)
	for rows.Next() {
		item, scanErr := scanAccount(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *Store) CreateAccount(ctx context.Context, item Account, actor string) (Account, error) {
	item, item.ID = normalizeAccount(item), NewID()
	if err := ValidateAccount(item); err != nil {
		return Account{}, err
	}
	args := append([]any{item.ID}, accountArgs(item, actor)...)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Account{}, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO billing_accounts
		(id,party_id,name,status,bill_type,algorithm,billing_day,timezone,direction,default_layer,cdr_bps,quota_bytes,reconcile_abs,reconcile_percent,ref,notes,created_by,updated_by)
		VALUES (?,NULLIF(?,''),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, append(args, nullString(actor))...)
	if err != nil {
		return Account{}, fmt.Errorf("create billing account: %w", err)
	}
	if err := auditTx(ctx, tx, actor, "billing.account.create", "billing_account", item.ID, fmt.Sprintf(`{"bill_type":%q,"algorithm":%q}`, item.BillType, item.Algorithm)); err != nil {
		return Account{}, err
	}
	if err := tx.Commit(); err != nil {
		return Account{}, err
	}
	return s.GetAccount(ctx, item.ID)
}

func (s *Store) UpdateAccount(ctx context.Context, item Account, expected uint64, actor string) (Account, error) {
	item = normalizeAccount(item)
	if item.ID == "" || expected == 0 {
		return Account{}, errors.New("billing account id and row version are required")
	}
	if err := ValidateAccount(item); err != nil {
		return Account{}, err
	}
	args := append(accountArgs(item, actor), item.ID, expected)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Account{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE billing_accounts SET party_id=?,name=?,status=?,bill_type=?,algorithm=?,billing_day=?,timezone=?,direction=?,default_layer=?,
		cdr_bps=?,quota_bytes=?,reconcile_abs=?,reconcile_percent=?,ref=?,notes=?,updated_by=?,row_version=row_version+1 WHERE id=? AND row_version=?`, args...)
	if err != nil {
		return Account{}, fmt.Errorf("update billing account: %w", err)
	}
	if err := changed(result); err != nil {
		return Account{}, err
	}
	if err := auditTx(ctx, tx, actor, "billing.account.update", "billing_account", item.ID, fmt.Sprintf(`{"row_version":%d}`, expected+1)); err != nil {
		return Account{}, err
	}
	if err := tx.Commit(); err != nil {
		return Account{}, err
	}
	return s.GetAccount(ctx, item.ID)
}

func (s *Store) DeleteAccount(ctx context.Context, id string, expected uint64, actor string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current uint64
	if err := tx.QueryRowContext(ctx, `SELECT row_version FROM billing_accounts WHERE id=? FOR UPDATE`, id).Scan(&current); err != nil {
		return normalizeSQLError(err)
	}
	if current != expected {
		return ErrConflict
	}
	var hasPeriods bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM billing_periods WHERE account_id=?)`, id).Scan(&hasPeriods); err != nil {
		return err
	}
	if hasPeriods {
		return ErrInUse
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM billing_accounts WHERE id=? AND row_version=?`, id, expected)
	if err != nil {
		return err
	}
	if err := changed(result); err != nil {
		return err
	}
	if err := auditTx(ctx, tx, actor, "billing.account.delete", "billing_account", id, fmt.Sprintf(`{"row_version":%d}`, expected)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ListAccountPorts(ctx context.Context, accountID string) ([]AccountPort, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT bap.account_id,bap.port_id,p.device_id,p.if_index,p.if_name,bap.direction,
		COALESCE(bap.created_by,''),bap.created_at FROM billing_account_ports bap JOIN ports p ON p.id=bap.port_id
		WHERE bap.account_id=? ORDER BY p.device_id,p.if_index,p.id`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]AccountPort, 0)
	for rows.Next() {
		var item AccountPort
		if err := rows.Scan(&item.AccountID, &item.PortID, &item.DeviceID, &item.IfIndex, &item.IfName, &item.Direction, &item.CreatedBy, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ListAccountPortsPage(ctx context.Context, accountID string, filter PageFilter) ([]AccountPort, int, error) {
	filter, order, err := normalizePage(filter, map[string]string{
		"device_id": "p.device_id", "if_index": "p.if_index", "if_name": "p.if_name", "direction": "bap.direction", "created_at": "bap.created_at",
	}, "device_id")
	if err != nil {
		return nil, 0, err
	}
	where, args := []string{"bap.account_id=?"}, []any{accountID}
	if filter.Query != "" {
		where = append(where, "(p.id LIKE ? OR p.device_id LIKE ? OR p.if_name LIKE ?)")
		like := "%" + filter.Query + "%"
		args = append(args, like, like, like)
	}
	if filter.Type != "" {
		where, args = append(where, "bap.direction=?"), append(args, filter.Type)
	}
	predicate := strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM billing_account_ports bap JOIN ports p ON p.id=bap.port_id WHERE `+predicate, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT bap.account_id,bap.port_id,p.device_id,p.if_index,p.if_name,bap.direction,
		COALESCE(bap.created_by,''),bap.created_at FROM billing_account_ports bap JOIN ports p ON p.id=bap.port_id
		WHERE `+predicate+` ORDER BY `+order+`,p.id LIMIT ? OFFSET ?`, append(args, filter.Limit, filter.Offset)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]AccountPort, 0)
	for rows.Next() {
		var item AccountPort
		if err := rows.Scan(&item.AccountID, &item.PortID, &item.DeviceID, &item.IfIndex, &item.IfName, &item.Direction, &item.CreatedBy, &item.CreatedAt); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *Store) ReplaceAccountPorts(ctx context.Context, accountID string, ports []AccountPort, expected uint64, actor string) error {
	if accountID == "" || expected == 0 || len(ports) > 1000 {
		return errors.New("billing account and at most 1000 ports are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current uint64
	var defaultDirection Direction
	if err := tx.QueryRowContext(ctx, `SELECT row_version,direction FROM billing_accounts WHERE id=? FOR UPDATE`, accountID).Scan(&current, &defaultDirection); err != nil {
		return normalizeSQLError(err)
	}
	if current != expected {
		return ErrConflict
	}
	seen := make(map[string]bool, len(ports))
	for i := range ports {
		ports[i].PortID = strings.TrimSpace(ports[i].PortID)
		if ports[i].Direction == "" {
			ports[i].Direction = defaultDirection
		}
		if ports[i].PortID == "" || !ValidDirection(ports[i].Direction) || seen[ports[i].PortID] {
			return errors.New("billing port scope is invalid or duplicated")
		}
		seen[ports[i].PortID] = true
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM billing_account_ports WHERE account_id=?`, accountID); err != nil {
		return err
	}
	for _, port := range ports {
		if _, err := tx.ExecContext(ctx, `INSERT INTO billing_account_ports (account_id,port_id,direction,created_by)
			VALUES (?,?,?,NULLIF(?,''))`, accountID, port.PortID, port.Direction, actor); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE billing_accounts SET updated_by=NULLIF(?,''),row_version=row_version+1 WHERE id=? AND row_version=?`, actor, accountID, expected)
	if err != nil {
		return err
	}
	if err := changed(result); err != nil {
		return err
	}
	if err := auditTx(ctx, tx, actor, "billing.account.ports.replace", "billing_account", accountID, fmt.Sprintf(`{"port_count":%d}`, len(ports))); err != nil {
		return err
	}
	return tx.Commit()
}

func nullString(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}
