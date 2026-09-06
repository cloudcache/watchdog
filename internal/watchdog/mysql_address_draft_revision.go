package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

const addressDraftRevisionColumns = `
	id, tenant_id, scope, base_digest, request_digest, COALESCE(result_digest, ''),
	operations_json, preview_json, operation_count, status, row_version,
	COALESCE(created_by, ''), COALESCE(applied_by, ''), created_at, expires_at, applied_at`

type addressPrefixRowsQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (s *MySQLStore) PrepareAddressPrefixRevision(ctx context.Context, tenantID, actorID ID, operations []AddressPrefixBatchOperation) (AddressDraftRevision, error) {
	if s == nil || s.db == nil || tenantID == "" || actorID == "" {
		return AddressDraftRevision{}, ErrAddressDraftRevisionInvalid
	}
	current, err := loadAddressPrefixesForRevision(ctx, s.db, tenantID, false)
	if err != nil {
		return AddressDraftRevision{}, err
	}
	prepared, err := prepareAddressPrefixRevision(tenantID, current, operations)
	if err != nil {
		return AddressDraftRevision{}, err
	}
	if err := validateAddressPrefixRevisionReferences(ctx, s.db, tenantID, prepared.Operations, false); err != nil {
		return AddressDraftRevision{}, err
	}
	operationsJSON, err := json.Marshal(prepared.Operations)
	if err != nil {
		return AddressDraftRevision{}, err
	}
	previewJSON, err := json.Marshal(prepared.Preview)
	if err != nil {
		return AddressDraftRevision{}, err
	}
	revisionID, err := newIdentityID()
	if err != nil {
		return AddressDraftRevision{}, err
	}
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO address_draft_revisions (
			id, tenant_id, scope, base_digest, request_digest, operations_json,
			preview_json, operation_count, status, created_by, expires_at
		) VALUES (?, ?, 'prefix', ?, ?, ?, ?, ?, 'prepared', ?, ?)
	`, revisionID, tenantID, prepared.BaseDigest, prepared.RequestDigest, operationsJSON,
		previewJSON, len(prepared.Operations), actorID, now.Add(24*time.Hour))
	if err != nil {
		var mysqlErr *mysqldriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			existing, getErr := s.getAddressDraftRevisionByRequestDigest(ctx, tenantID, prepared.RequestDigest)
			if getErr != nil {
				return AddressDraftRevision{}, getErr
			}
			if existing.Status == AddressDraftRevisionStatusPrepared && !existing.ExpiresAt.After(now) {
				if _, refreshErr := s.db.ExecContext(ctx, `UPDATE address_draft_revisions
					SET expires_at = ?, row_version = row_version + 1
					WHERE tenant_id = ? AND id = ? AND status = 'prepared' AND row_version = ?`,
					now.Add(24*time.Hour), tenantID, existing.ID, existing.RowVersion); refreshErr != nil {
					return AddressDraftRevision{}, refreshErr
				}
				return s.GetAddressDraftRevision(ctx, tenantID, existing.ID)
			}
			return existing, nil
		}
		return AddressDraftRevision{}, err
	}
	return s.GetAddressDraftRevision(ctx, tenantID, revisionID)
}

var addressDraftRevisionSortColumns = map[string]string{
	"":           "created_at",
	"status":     "status",
	"operations": "operation_count",
	"creates":    "CAST(JSON_UNQUOTE(JSON_EXTRACT(preview_json, '$.create_count')) AS UNSIGNED)",
	"deletes":    "CAST(JSON_UNQUOTE(JSON_EXTRACT(preview_json, '$.delete_count')) AS UNSIGNED)",
	"before":     "CAST(JSON_UNQUOTE(JSON_EXTRACT(preview_json, '$.before_prefix_count')) AS UNSIGNED)",
	"after":      "CAST(JSON_UNQUOTE(JSON_EXTRACT(preview_json, '$.after_prefix_count')) AS UNSIGNED)",
	"created":    "created_at",
	"expires":    "expires_at",
}

func (s *MySQLStore) ListAddressDraftRevisions(ctx context.Context, tenantID ID, filter AddressDraftRevisionListFilter) ([]AddressDraftRevision, string, int, error) {
	filter.Status = strings.ToLower(strings.TrimSpace(filter.Status))
	filter.Search = strings.TrimSpace(filter.Search)
	if filter.Status != "" && !validAddressDraftRevisionStatus(filter.Status) {
		return nil, "", 0, ErrAddressDraftRevisionInvalid
	}
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 500 {
		filter.Limit = 500
	}
	sortColumn, validSort := addressDraftRevisionSortColumns[filter.Sort]
	if len(filter.Search) > 255 || filter.Offset < 0 || !validSort {
		return nil, "", 0, ErrAddressDraftRevisionInvalid
	}
	where := ` WHERE tenant_id = ?`
	args := []any{tenantID}
	if filter.Status != "" {
		where += ` AND status = ?`
		args = append(args, filter.Status)
	}
	if filter.Search != "" {
		like := "%" + escapeSQLLike(filter.Search) + "%"
		where += ` AND (id LIKE ? OR base_digest LIKE ? OR request_digest LIKE ? OR COALESCE(result_digest, '') LIKE ? OR COALESCE(created_by, '') LIKE ?)`
		args = append(args, like, like, like, like, like)
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM address_draft_revisions`+where, args...).Scan(&total); err != nil {
		return nil, "", 0, err
	}
	query := `SELECT ` + addressDraftRevisionColumns + ` FROM address_draft_revisions` + where
	if filter.Cursor != "" {
		createdAt, id, err := decodeAuditCursor(filter.Cursor)
		if err != nil {
			return nil, "", 0, ErrAddressDraftRevisionInvalid
		}
		query += ` AND (created_at < ? OR (created_at = ? AND id < ?))`
		args = append(args, createdAt, createdAt, id)
	}
	if filter.TableMode {
		direction := "ASC"
		if filter.Desc {
			direction = "DESC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s, id %s LIMIT ? OFFSET ?", sortColumn, direction, direction)
		args = append(args, filter.Limit, filter.Offset)
	} else {
		query += ` ORDER BY created_at DESC, id DESC LIMIT ?`
		args = append(args, filter.Limit+1)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close()
	items := make([]AddressDraftRevision, 0, filter.Limit)
	for rows.Next() {
		item, scanErr := scanAddressDraftRevision(rows)
		if scanErr != nil {
			return nil, "", 0, scanErr
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, err
	}
	next := ""
	if !filter.TableMode && len(items) > filter.Limit {
		items = items[:filter.Limit]
		last := items[len(items)-1]
		next = encodeAuditCursor(last.CreatedAt, last.ID)
	}
	return items, next, total, nil
}

func (s *MySQLStore) GetAddressDraftRevision(ctx context.Context, tenantID, revisionID ID) (AddressDraftRevision, error) {
	return scanAddressDraftRevision(s.db.QueryRowContext(ctx, `SELECT `+addressDraftRevisionColumns+`
		FROM address_draft_revisions WHERE tenant_id = ? AND id = ?`, tenantID, revisionID))
}

func (s *MySQLStore) getAddressDraftRevisionByRequestDigest(ctx context.Context, tenantID ID, requestDigest string) (AddressDraftRevision, error) {
	return scanAddressDraftRevision(s.db.QueryRowContext(ctx, `SELECT `+addressDraftRevisionColumns+`
		FROM address_draft_revisions WHERE tenant_id = ? AND request_digest = ?`, tenantID, requestDigest))
}

func (s *MySQLStore) ApplyAddressDraftRevision(ctx context.Context, tenantID, actorID, revisionID ID, expectedVersion uint64) (AddressDraftRevision, error) {
	if s == nil || s.db == nil || tenantID == "" || actorID == "" || revisionID == "" || expectedVersion == 0 {
		return AddressDraftRevision{}, ErrAddressDraftRevisionInvalid
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return AddressDraftRevision{}, err
	}
	defer tx.Rollback()
	revision, err := scanAddressDraftRevision(tx.QueryRowContext(ctx, `SELECT `+addressDraftRevisionColumns+`
		FROM address_draft_revisions WHERE tenant_id = ? AND id = ? FOR UPDATE`, tenantID, revisionID))
	if err != nil {
		return AddressDraftRevision{}, err
	}
	if revision.RowVersion != expectedVersion {
		return AddressDraftRevision{}, ErrAddressDraftRevisionConflict
	}
	if revision.Status != AddressDraftRevisionStatusPrepared {
		return AddressDraftRevision{}, ErrAddressDraftRevisionNotPrepared
	}
	if !revision.ExpiresAt.After(time.Now().UTC()) {
		return AddressDraftRevision{}, ErrAddressDraftRevisionExpired
	}
	current, err := loadAddressPrefixesForRevision(ctx, tx, tenantID, true)
	if err != nil {
		return AddressDraftRevision{}, err
	}
	baseDigest, err := digestAddressPrefixes(current)
	if err != nil {
		return AddressDraftRevision{}, err
	}
	if baseDigest != revision.BaseDigest {
		if _, err := tx.ExecContext(ctx, `UPDATE address_draft_revisions
			SET status = 'superseded', row_version = row_version + 1
			WHERE tenant_id = ? AND id = ? AND status = 'prepared' AND row_version = ?`, tenantID, revisionID, expectedVersion); err != nil {
			return AddressDraftRevision{}, err
		}
		if err := tx.Commit(); err != nil {
			return AddressDraftRevision{}, err
		}
		return AddressDraftRevision{}, ErrAddressDraftRevisionChanged
	}
	operations, err := normalizePersistedAddressPrefixOperations(tenantID, revision.Operations)
	if err != nil {
		return AddressDraftRevision{}, err
	}
	requestOperations := append([]AddressPrefixBatchOperation(nil), operations...)
	for index := range requestOperations {
		if requestOperations[index].Action == "create" {
			requestOperations[index].PrefixID = ""
		}
	}
	requestDigest, err := digestAddressPrefixRevisionRequest(baseDigest, requestOperations)
	if err != nil || requestDigest != revision.RequestDigest {
		return AddressDraftRevision{}, ErrAddressDraftRevisionInvalid
	}
	result, preview, err := evaluateAddressPrefixBatch(current, operations)
	if err != nil {
		return AddressDraftRevision{}, err
	}
	resultDigest, err := digestAddressPrefixes(result)
	if err != nil || resultDigest != revision.Preview.ExpectedResultDigest {
		return AddressDraftRevision{}, ErrAddressDraftRevisionInvalid
	}
	if err := validateAddressPrefixRevisionReferences(ctx, tx, tenantID, operations, true); err != nil {
		return AddressDraftRevision{}, err
	}
	for _, operation := range operations {
		if operation.Action != "delete" {
			continue
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM address_prefixes
			WHERE tenant_id = ? AND id = ? AND row_version = ?`, tenantID, operation.PrefixID, operation.ExpectedVersion)
		if err != nil {
			return AddressDraftRevision{}, err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return AddressDraftRevision{}, ErrAddressDraftRevisionChanged
		}
	}
	for _, operation := range operations {
		if operation.Action != "create" {
			continue
		}
		if err := insertAddressPrefixBatchOperation(ctx, tx, tenantID, operation); err != nil {
			return AddressDraftRevision{}, err
		}
	}
	appliedAt := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE address_draft_revisions SET
		status = 'applied', result_digest = ?, applied_by = ?, applied_at = ?, row_version = row_version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'prepared' AND row_version = ?`,
		resultDigest, actorID, appliedAt, tenantID, revisionID, expectedVersion); err != nil {
		return AddressDraftRevision{}, err
	}
	if err := insertAddressPrefixRevisionAudit(ctx, tx, tenantID, actorID, revisionID, operations, preview, appliedAt); err != nil {
		return AddressDraftRevision{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDraftRevision{}, err
	}
	return s.GetAddressDraftRevision(ctx, tenantID, revisionID)
}

func loadAddressPrefixesForRevision(ctx context.Context, queryer addressPrefixRowsQueryer, tenantID ID, forUpdate bool) ([]AddressPrefix, error) {
	query := `SELECT ` + addressPrefixColumns + ` FROM address_prefixes WHERE tenant_id = ? ORDER BY cidr, id`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	rows, err := queryer.QueryContext(ctx, query, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]AddressPrefix, 0)
	for rows.Next() {
		item, err := scanAddressPrefix(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func normalizePersistedAddressPrefixOperations(tenantID ID, operations []AddressPrefixBatchOperation) ([]AddressPrefixBatchOperation, error) {
	if len(operations) == 0 || len(operations) > AddressDraftRevisionMaxOperations {
		return nil, ErrAddressDraftRevisionInvalid
	}
	result := make([]AddressPrefixBatchOperation, 0, len(operations))
	for _, operation := range operations {
		normalized, err := normalizeAddressPrefixBatchOperation(tenantID, operation, true)
		if err != nil {
			return nil, err
		}
		result = append(result, normalized)
	}
	sortAddressPrefixBatchOperations(result)
	return result, nil
}

func validateAddressPrefixRevisionReferences(ctx context.Context, queryer addressPrefixRowsQueryer, tenantID ID, operations []AddressPrefixBatchOperation, lock bool) error {
	geoIDs := make(map[ID]struct{})
	operatorIDs := make(map[ID]struct{})
	for _, operation := range operations {
		if operation.Action != "create" {
			continue
		}
		if operation.GeoLeafID != "" {
			geoIDs[operation.GeoLeafID] = struct{}{}
		}
		if operation.OperatorID != "" {
			operatorIDs[operation.OperatorID] = struct{}{}
		}
	}
	if err := validateEnabledAddressReferences(ctx, queryer, tenantID, "geo_dict", geoIDs, lock); err != nil {
		return err
	}
	return validateEnabledAddressReferences(ctx, queryer, tenantID, "isp_operators", operatorIDs, lock)
}

func validateEnabledAddressReferences(ctx context.Context, queryer addressPrefixRowsQueryer, tenantID ID, table string, ids map[ID]struct{}, lock bool) error {
	if len(ids) == 0 {
		return nil
	}
	if table != "geo_dict" && table != "isp_operators" {
		return ErrAddressDraftRevisionInvalid
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, len(ids)+1)
	args = append(args, tenantID)
	for id := range ids {
		args = append(args, id)
	}
	query := `SELECT id FROM ` + table + ` WHERE tenant_id = ? AND enabled = 1 AND id IN (` + placeholders + `)`
	if lock {
		query += ` FOR SHARE`
	}
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	found := 0
	for rows.Next() {
		var id ID
		if err := rows.Scan(&id); err != nil {
			return err
		}
		found++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if found != len(ids) {
		return fmt.Errorf("%w: referenced %s row is missing or disabled", ErrAddressDraftRevisionInvalid, table)
	}
	return nil
}

func insertAddressPrefixBatchOperation(ctx context.Context, tx *sql.Tx, tenantID ID, operation AddressPrefixBatchOperation) error {
	prefix := addressPrefixFromBatchOperation(operation)
	prefix.TenantID = tenantID
	prefix, start, end, err := normalizeAddressPrefix(prefix)
	if err != nil {
		return err
	}
	labelsJSON, err := json.Marshal(prefix.Labels)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO address_prefixes (
		id, tenant_id, cidr, family, prefix_length, ip_start, ip_end,
		labels, geo_leaf_id, operator_id, asn, source, row_version
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, 1)`,
		prefix.ID, tenantID, prefix.CIDR, prefix.Family, prefix.PrefixLength, start[:], end[:],
		labelsJSON, prefix.GeoLeafID, prefix.OperatorID, prefix.ASN, prefix.Source)
	return err
}

func insertAddressPrefixRevisionAudit(ctx context.Context, tx *sql.Tx, tenantID, actorID, revisionID ID, operations []AddressPrefixBatchOperation, preview AddressPrefixBatchPreview, createdAt time.Time) error {
	changes := make(map[string]AddressPrefixBatchChange, len(preview.Changes))
	for _, change := range preview.Changes {
		changes[change.Action+"\x00"+change.PrefixID] = change
	}
	for ordinal, operation := range operations {
		auditID, err := newIdentityID()
		if err != nil {
			return err
		}
		change := changes[operation.Action+"\x00"+operation.PrefixID]
		detail, err := json.Marshal(map[string]any{
			"revision_id": revisionID, "operation_ordinal": ordinal,
			"prefix_id": operation.PrefixID, "cidr": change.CIDR,
		})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO audit_logs (
			id, tenant_id, actor_id, action, resource_type, resource_id, detail_json, created_at
		) VALUES (?, ?, ?, ?, 'address_revision', ?, ?, ?)`,
			auditID, tenantID, actorID, "address_prefix."+operation.Action+"d", revisionID, detail, createdAt); err != nil {
			return err
		}
	}
	return nil
}

func scanAddressDraftRevision(row rowScanner) (AddressDraftRevision, error) {
	var item AddressDraftRevision
	var operationsJSON, previewJSON []byte
	var appliedAt sql.NullTime
	if err := row.Scan(&item.ID, &item.TenantID, &item.Scope, &item.BaseDigest, &item.RequestDigest,
		&item.ResultDigest, &operationsJSON, &previewJSON, &item.OperationCount, &item.Status,
		&item.RowVersion, &item.CreatedBy, &item.AppliedBy, &item.CreatedAt, &item.ExpiresAt, &appliedAt); err != nil {
		return AddressDraftRevision{}, err
	}
	if err := json.Unmarshal(operationsJSON, &item.Operations); err != nil {
		return AddressDraftRevision{}, err
	}
	if err := json.Unmarshal(previewJSON, &item.Preview); err != nil {
		return AddressDraftRevision{}, err
	}
	if appliedAt.Valid {
		item.AppliedAt = &appliedAt.Time
	}
	return item, nil
}

func validAddressDraftRevisionStatus(status string) bool {
	switch status {
	case AddressDraftRevisionStatusPrepared, AddressDraftRevisionStatusApplied,
		AddressDraftRevisionStatusSuperseded, AddressDraftRevisionStatusCancelled:
		return true
	default:
		return false
	}
}
