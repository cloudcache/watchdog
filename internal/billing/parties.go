// SPDX-License-Identifier: AGPL-3.0-only

package billing

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const partyColumns = `id,kind,status,name,ref,notes,row_version,
	COALESCE(created_by,''),COALESCE(updated_by,''),created_at,updated_at`

func validateParty(item Party) error {
	if strings.TrimSpace(item.Name) == "" {
		return errors.New("party name is required")
	}
	if item.Kind != "customer" && item.Kind != "supplier" {
		return errors.New("party kind must be customer or supplier")
	}
	if item.Status != "active" && item.Status != "inactive" {
		return errors.New("party status must be active or inactive")
	}
	return nil
}

func scanParty(scanner interface{ Scan(...any) error }) (Party, error) {
	var item Party
	err := scanner.Scan(&item.ID, &item.Kind, &item.Status, &item.Name, &item.Ref, &item.Notes, &item.RowVersion,
		&item.CreatedBy, &item.UpdatedBy, &item.CreatedAt, &item.UpdatedAt)
	return item, normalizeSQLError(err)
}

func (s *Store) GetParty(ctx context.Context, id string) (Party, error) {
	return scanParty(s.db.QueryRowContext(ctx, `SELECT `+partyColumns+` FROM parties WHERE id=?`, id))
}

func (s *Store) ListParties(ctx context.Context, filter PageFilter) ([]Party, int, error) {
	filter, order, err := normalizePage(filter, map[string]string{"name": "name", "kind": "kind", "status": "status", "created_at": "created_at", "updated_at": "updated_at"}, "name", s.limits.MaxPageSize)
	if err != nil {
		return nil, 0, err
	}
	where, args := []string{"1=1"}, []any{}
	if filter.Query != "" {
		where, args = append(where, "(name LIKE ? OR ref LIKE ? OR notes LIKE ?)"), append(args, "%"+filter.Query+"%", "%"+filter.Query+"%", "%"+filter.Query+"%")
	}
	if filter.Kind != "" {
		where, args = append(where, "kind=?"), append(args, filter.Kind)
	}
	if filter.Status != "" {
		where, args = append(where, "status=?"), append(args, filter.Status)
	}
	predicate := strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM parties WHERE `+predicate, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+partyColumns+` FROM parties WHERE `+predicate+` ORDER BY `+order+`,id LIMIT ? OFFSET ?`, append(args, filter.Limit, filter.Offset)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]Party, 0)
	for rows.Next() {
		item, scanErr := scanParty(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *Store) CreateParty(ctx context.Context, item Party, actor string) (Party, error) {
	item.ID, item.Name, item.Ref, item.Notes = NewID(), strings.TrimSpace(item.Name), strings.TrimSpace(item.Ref), strings.TrimSpace(item.Notes)
	if item.Status == "" {
		item.Status = "active"
	}
	if err := validateParty(item); err != nil {
		return Party{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Party{}, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO parties (id,kind,status,name,ref,notes,created_by,updated_by)
		VALUES (?,?,?,?,?,?,NULLIF(?,''),NULLIF(?,''))`, item.ID, item.Kind, item.Status, item.Name, item.Ref, item.Notes, actor, actor)
	if err != nil {
		return Party{}, fmt.Errorf("create party: %w", err)
	}
	if err := auditTx(ctx, tx, actor, "billing.party.create", "party", item.ID, fmt.Sprintf(`{"kind":%q}`, item.Kind)); err != nil {
		return Party{}, err
	}
	if err := tx.Commit(); err != nil {
		return Party{}, err
	}
	return s.GetParty(ctx, item.ID)
}

func (s *Store) UpdateParty(ctx context.Context, item Party, expected uint64, actor string) (Party, error) {
	item.Name, item.Ref, item.Notes = strings.TrimSpace(item.Name), strings.TrimSpace(item.Ref), strings.TrimSpace(item.Notes)
	if item.ID == "" || expected == 0 {
		return Party{}, errors.New("party id and row version are required")
	}
	if err := validateParty(item); err != nil {
		return Party{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Party{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE parties SET kind=?,status=?,name=?,ref=?,notes=?,updated_by=NULLIF(?,''),row_version=row_version+1
		WHERE id=? AND row_version=?`, item.Kind, item.Status, item.Name, item.Ref, item.Notes, actor, item.ID, expected)
	if err != nil {
		return Party{}, fmt.Errorf("update party: %w", err)
	}
	if err := changed(result); err != nil {
		return Party{}, err
	}
	if err := auditTx(ctx, tx, actor, "billing.party.update", "party", item.ID, fmt.Sprintf(`{"row_version":%d}`, expected+1)); err != nil {
		return Party{}, err
	}
	if err := tx.Commit(); err != nil {
		return Party{}, err
	}
	return s.GetParty(ctx, item.ID)
}

func (s *Store) DeleteParty(ctx context.Context, id string, expected uint64, actor string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current uint64
	if err := tx.QueryRowContext(ctx, `SELECT row_version FROM parties WHERE id=? FOR UPDATE`, id).Scan(&current); err != nil {
		return normalizeSQLError(err)
	}
	if current != expected {
		return ErrConflict
	}
	var inUse bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM billing_accounts WHERE party_id=?)`, id).Scan(&inUse); err != nil {
		return err
	}
	if inUse {
		return ErrInUse
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM parties WHERE id=? AND row_version=?`, id, expected)
	if err != nil {
		return err
	}
	if err := changed(result); err != nil {
		return err
	}
	if err := auditTx(ctx, tx, actor, "billing.party.delete", "party", id, fmt.Sprintf(`{"row_version":%d}`, expected)); err != nil {
		return err
	}
	return tx.Commit()
}
