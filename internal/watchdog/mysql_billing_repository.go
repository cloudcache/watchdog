package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func (s *MySQLStore) CreateBillingAccount(ctx context.Context, account BillingAccount) (BillingAccount, error) {
	account = normalizeBillingAccount(account)
	if account.ID == "" {
		return BillingAccount{}, errors.New("billing account id is required")
	}
	quota := sql.NullInt64{}
	if account.QuotaBytes > 0 {
		quota.Valid = true
		quota.Int64 = int64(account.QuotaBytes)
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO billing_accounts (
			id, tenant_id, name, status, billing_day, aggregation, value_mode, quota_bytes, notes
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, account.ID, account.TenantID, account.Name, account.Status, account.BillingDay, account.Aggregation, account.ValueMode, quota, account.Notes)
	if err != nil {
		return BillingAccount{}, err
	}
	return s.GetBillingAccount(ctx, account.TenantID, account.ID)
}

func (s *MySQLStore) GetBillingAccount(ctx context.Context, tenantID, accountID ID) (BillingAccount, error) {
	row := s.db.QueryRowContext(ctx, billingAccountSelect()+`
		WHERE tenant_id = ? AND id = ?
	`, tenantID, accountID)
	return scanBillingAccount(row)
}

func (s *MySQLStore) ListBillingAccounts(ctx context.Context, tenantID ID) ([]BillingAccount, error) {
	rows, err := s.db.QueryContext(ctx, billingAccountSelect()+`
		WHERE tenant_id = ?
		ORDER BY name
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var accounts []BillingAccount
	for rows.Next() {
		account, err := scanBillingAccount(rows)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}
	return accounts, rows.Err()
}

func (s *MySQLStore) UpdateBillingAccount(ctx context.Context, account BillingAccount) (BillingAccount, error) {
	account = normalizeBillingAccount(account)
	if account.ID == "" {
		return BillingAccount{}, errors.New("billing account id is required")
	}
	quota := sql.NullInt64{}
	if account.QuotaBytes > 0 {
		quota.Valid = true
		quota.Int64 = int64(account.QuotaBytes)
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE billing_accounts
		SET name = ?, status = ?, billing_day = ?, aggregation = ?, value_mode = ?, quota_bytes = ?, notes = ?,
		    updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, account.Name, account.Status, account.BillingDay, account.Aggregation, account.ValueMode, quota, account.Notes, account.TenantID, account.ID)
	if err != nil {
		return BillingAccount{}, err
	}
	return s.GetBillingAccount(ctx, account.TenantID, account.ID)
}

func (s *MySQLStore) ListBillingAccountPorts(ctx context.Context, tenantID, accountID ID) ([]BillingAccountPort, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT billing_account_id, tenant_id, port_id, direction, created_at
		FROM billing_account_ports
		WHERE tenant_id = ? AND billing_account_id = ?
		ORDER BY port_id
	`, tenantID, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ports []BillingAccountPort
	for rows.Next() {
		var port BillingAccountPort
		if err := rows.Scan(&port.BillingAccountID, &port.TenantID, &port.PortID, &port.Direction, &port.CreatedAt); err != nil {
			return nil, err
		}
		ports = append(ports, port)
	}
	return ports, rows.Err()
}

func (s *MySQLStore) ReplaceBillingAccountPorts(ctx context.Context, tenantID, accountID ID, ports []BillingAccountPort) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM billing_account_ports
		WHERE tenant_id = ? AND billing_account_id = ?
	`, tenantID, accountID); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO billing_account_ports (
			billing_account_id, tenant_id, port_id, direction
		) VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, port := range ports {
		direction := port.Direction
		if direction == "" {
			direction = BillingDirectionMax
		}
		if _, err := stmt.ExecContext(ctx, accountID, tenantID, port.PortID, direction); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *MySQLStore) CreateBillingPeriod(ctx context.Context, period BillingPeriod) (BillingPeriod, error) {
	period = normalizeBillingPeriod(period)
	if period.ID == "" {
		return BillingPeriod{}, errors.New("billing period id is required")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO billing_periods (
			id, tenant_id, billing_account_id, range_start, range_end, status
		) VALUES (?, ?, ?, ?, ?, ?)
	`, period.ID, period.TenantID, period.BillingAccountID, period.RangeStart, period.RangeEnd, period.Status)
	if err != nil {
		return BillingPeriod{}, err
	}
	return s.getBillingPeriod(ctx, period.TenantID, period.ID)
}

func (s *MySQLStore) MarkBillingPeriodComputed(ctx context.Context, tenantID, periodID ID, computedValue float64, totalBytes uint64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE billing_periods
		SET status = ?, computed_value = ?, total_bytes = ?, computed_at = ?, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, BillingPeriodComputed, computedValue, totalBytes, time.Now().UTC(), tenantID, periodID)
	return err
}

func (s *MySQLStore) GetBillingPeriod(ctx context.Context, tenantID, periodID ID) (BillingPeriod, error) {
	return s.getBillingPeriod(ctx, tenantID, periodID)
}

func (s *MySQLStore) UpdateBillingPeriodStatus(ctx context.Context, tenantID, periodID ID, status BillingPeriodStatus) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE billing_periods
		SET status = ?, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, status, tenantID, periodID)
	return err
}

func (s *MySQLStore) getBillingPeriod(ctx context.Context, tenantID, periodID ID) (BillingPeriod, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, billing_account_id, range_start, range_end, status,
		       COALESCE(computed_value, 0), COALESCE(total_bytes, 0), computed_at,
		       created_at, updated_at
		FROM billing_periods
		WHERE tenant_id = ? AND id = ?
	`, tenantID, periodID)
	return scanBillingPeriod(row)
}

func normalizeBillingAccount(account BillingAccount) BillingAccount {
	if account.Status == "" {
		account.Status = BillingStatusActive
	}
	if account.BillingDay == 0 {
		account.BillingDay = 1
	}
	if account.Aggregation == "" {
		account.Aggregation = AggregationP95FiveMinute
	}
	if account.ValueMode == "" {
		account.ValueMode = ExportValueCorrected
	}
	return account
}

func normalizeBillingPeriod(period BillingPeriod) BillingPeriod {
	if period.Status == "" {
		period.Status = BillingPeriodOpen
	}
	return period
}

func billingAccountSelect() string {
	return `
		SELECT id, tenant_id, name, status, billing_day, aggregation, value_mode,
		       COALESCE(quota_bytes, 0), COALESCE(notes, ''), created_at, updated_at
		FROM billing_accounts
	`
}

func scanBillingAccount(row rowScanner) (BillingAccount, error) {
	var account BillingAccount
	err := row.Scan(&account.ID, &account.TenantID, &account.Name, &account.Status, &account.BillingDay, &account.Aggregation, &account.ValueMode, &account.QuotaBytes, &account.Notes, &account.CreatedAt, &account.UpdatedAt)
	return account, err
}

func scanBillingPeriod(row rowScanner) (BillingPeriod, error) {
	var period BillingPeriod
	var computedAt sql.NullTime
	if err := row.Scan(&period.ID, &period.TenantID, &period.BillingAccountID, &period.RangeStart, &period.RangeEnd, &period.Status, &period.ComputedValue, &period.TotalBytes, &computedAt, &period.CreatedAt, &period.UpdatedAt); err != nil {
		return period, err
	}
	if computedAt.Valid {
		period.ComputedAt = computedAt.Time
	}
	return period, nil
}

var _ BillingRepository = (*MySQLStore)(nil)
