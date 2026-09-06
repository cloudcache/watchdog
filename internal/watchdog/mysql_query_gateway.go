package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
)

const queryDatasetPolicyColumns = `
	tenant_id, dataset_key, enabled, allow_raw, allow_supplier, allow_customer,
	max_range_seconds, max_concurrent, max_result_rows, query_timeout_ms,
	row_version, COALESCE(updated_by, ''), created_at, updated_at`

func scanQueryDatasetPolicy(scanner interface{ Scan(...any) error }) (QueryDatasetPolicy, error) {
	var policy QueryDatasetPolicy
	err := scanner.Scan(
		&policy.TenantID, &policy.DatasetKey, &policy.Enabled, &policy.AllowRaw,
		&policy.AllowSupplier, &policy.AllowCustomer, &policy.MaxRangeSeconds,
		&policy.MaxConcurrent, &policy.MaxResultRows, &policy.QueryTimeoutMS,
		&policy.RowVersion, &policy.UpdatedBy, &policy.CreatedAt, &policy.UpdatedAt,
	)
	return policy, err
}

func (s *MySQLStore) GetQueryDatasetPolicy(ctx context.Context, tenantID ID, datasetKey string) (QueryDatasetPolicy, error) {
	return scanQueryDatasetPolicy(s.db.QueryRowContext(ctx, `
		SELECT `+queryDatasetPolicyColumns+`
		FROM query_dataset_policies
		WHERE tenant_id = ? AND dataset_key = ?
	`, tenantID, strings.TrimSpace(datasetKey)))
}

func (s *MySQLStore) ListQueryDatasetPolicies(ctx context.Context, tenantID ID) ([]QueryDatasetPolicy, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+queryDatasetPolicyColumns+`
		FROM query_dataset_policies
		WHERE tenant_id = ?
		ORDER BY dataset_key
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]QueryDatasetPolicy, 0)
	for rows.Next() {
		policy, err := scanQueryDatasetPolicy(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, policy)
	}
	return items, rows.Err()
}

func (s *MySQLStore) PutQueryDatasetPolicy(ctx context.Context, policy QueryDatasetPolicy, expectedVersion uint64) (QueryDatasetPolicy, error) {
	policy.DatasetKey = strings.TrimSpace(policy.DatasetKey)
	if err := validateQueryDatasetPolicy(policy); err != nil {
		return QueryDatasetPolicy{}, err
	}
	if expectedVersion == 0 {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO query_dataset_policies (
				tenant_id, dataset_key, enabled, allow_raw, allow_supplier, allow_customer,
				max_range_seconds, max_concurrent, max_result_rows, query_timeout_ms, updated_by
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''))
		`, policy.TenantID, policy.DatasetKey, policy.Enabled, policy.AllowRaw, policy.AllowSupplier,
			policy.AllowCustomer, policy.MaxRangeSeconds, policy.MaxConcurrent, policy.MaxResultRows,
			policy.QueryTimeoutMS, policy.UpdatedBy)
		if err != nil {
			var mysqlErr *mysqldriver.MySQLError
			if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
				return QueryDatasetPolicy{}, ErrQueryDatasetPolicyConflict
			}
			return QueryDatasetPolicy{}, err
		}
		return s.GetQueryDatasetPolicy(ctx, policy.TenantID, policy.DatasetKey)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE query_dataset_policies SET
			enabled = ?, allow_raw = ?, allow_supplier = ?, allow_customer = ?,
			max_range_seconds = ?, max_concurrent = ?, max_result_rows = ?,
			query_timeout_ms = ?, updated_by = NULLIF(?, ''), row_version = row_version + 1
		WHERE tenant_id = ? AND dataset_key = ? AND row_version = ?
	`, policy.Enabled, policy.AllowRaw, policy.AllowSupplier, policy.AllowCustomer,
		policy.MaxRangeSeconds, policy.MaxConcurrent, policy.MaxResultRows, policy.QueryTimeoutMS,
		policy.UpdatedBy, policy.TenantID, policy.DatasetKey, expectedVersion)
	if err != nil {
		return QueryDatasetPolicy{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return QueryDatasetPolicy{}, err
	}
	if affected != 1 {
		return QueryDatasetPolicy{}, ErrQueryDatasetPolicyConflict
	}
	return s.GetQueryDatasetPolicy(ctx, policy.TenantID, policy.DatasetKey)
}

func (s *MySQLStore) DeleteQueryDatasetPolicy(ctx context.Context, tenantID ID, datasetKey string, expectedVersion uint64) error {
	if expectedVersion == 0 {
		return ErrQueryDatasetPolicyConflict
	}
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM query_dataset_policies
		WHERE tenant_id = ? AND dataset_key = ? AND row_version = ?
	`, tenantID, strings.TrimSpace(datasetKey), expectedVersion)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 1 {
		return nil
	}
	if _, err := s.GetQueryDatasetPolicy(ctx, tenantID, datasetKey); errors.Is(err, sql.ErrNoRows) {
		return sql.ErrNoRows
	} else if err != nil {
		return err
	}
	return ErrQueryDatasetPolicyConflict
}
