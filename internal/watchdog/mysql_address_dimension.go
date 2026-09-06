package watchdog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	mysqldriver "github.com/go-sql-driver/mysql"
)

type MySQLAddressDimensionPublisher struct {
	store   *MySQLStore
	objects DimensionObjectStore
}

func NewMySQLAddressDimensionPublisher(store *MySQLStore, objects DimensionObjectStore) (*MySQLAddressDimensionPublisher, error) {
	if store == nil || store.db == nil || objects == nil {
		return nil, errors.New("MySQL store and dimension object store are required")
	}
	return &MySQLAddressDimensionPublisher{store: store, objects: objects}, nil
}

func (p *MySQLAddressDimensionPublisher) PreviewAddressDimension(ctx context.Context, tenantID ID, effectiveFrom time.Time) (AddressDimensionPreview, error) {
	if p == nil || p.store == nil || tenantID == "" || !isUTCMinute(effectiveFrom) {
		return AddressDimensionPreview{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return AddressDimensionPreview{}, err
	}
	defer tx.Rollback()
	draft, digest, err := loadAddressDimensionDraft(ctx, tx, tenantID)
	if err != nil {
		return AddressDimensionPreview{}, err
	}
	data, compiled, _, err := encodeAddressDimensionBundle(draft, "preview-address", string(tenantID), 1, effectiveFrom)
	if err != nil {
		return AddressDimensionPreview{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionPreview{}, err
	}
	metadata := compiled.Metadata()
	return AddressDimensionPreview{
		DraftDigest: digest, BundleSchemaVersion: flowdimension.BundleSchemaVersion, EffectiveFrom: effectiveFrom.UTC(),
		PrefixCount: uint64(len(draft.Prefixes)), AddressSetCount: uint64(len(draft.AddressSets)),
		EnabledAddressSetCount: uint64(metadata.EnabledAddressSetCount), MaxAddressSetsPerRecord: uint32(metadata.MaxAddressSetsPerRecord),
		EstimatedBundleBytes: uint64(len(data)),
	}, nil
}

func (p *MySQLAddressDimensionPublisher) PublishAddressDimension(ctx context.Context, tenantID, actorID ID, request AddressDimensionPublishRequest) (AddressDimensionSnapshot, error) {
	if p == nil || p.store == nil || tenantID == "" || actorID == "" || !isUTCMinute(request.EffectiveFrom) || !validSHA256Digest(request.PreviewDigest) {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	defer tx.Rollback()
	var lockedTenant ID
	if err := tx.QueryRowContext(ctx, `SELECT id FROM tenants WHERE id = ? FOR UPDATE`, tenantID).Scan(&lockedTenant); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	var version uint64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(version), 0) + 1 FROM dimension_snapshots
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ?
	`, tenantID, AddressDimensionModuleKey, AddressDimensionKey).Scan(&version); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	draft, digest, err := loadAddressDimensionDraft(ctx, tx, tenantID)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if digest != request.PreviewDigest {
		return AddressDimensionSnapshot{}, ErrAddressDimensionDraftChanged
	}
	snapshotID, err := newIdentityID()
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	data, compiled, checksum, err := encodeAddressDimensionBundle(draft, string(snapshotID), string(tenantID), version, request.EffectiveFrom)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	object, err := p.objects.SaveDimensionObject(ctx, tenantID, snapshotID, data)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	retained := false
	defer func() {
		if !retained {
			_ = p.objects.RemoveDimensionObject(object.Ref)
		}
	}()
	if object.Checksum != checksum {
		return AddressDimensionSnapshot{}, errors.New("dimension object checksum changed while saving")
	}
	metadata := compiled.Metadata()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO dimension_snapshots (
			id, tenant_id, module_key, dimension_key, version, effective_from,
			object_ref, checksum, draft_digest, bundle_schema_version, entry_count,
			prefix_count, address_set_count, max_address_sets_per_record, status, created_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)
	`, snapshotID, tenantID, AddressDimensionModuleKey, AddressDimensionKey, version, request.EffectiveFrom.UTC(),
		object.Ref, checksum, digest, flowdimension.BundleSchemaVersion, len(draft.Prefixes)+len(draft.AddressSets),
		len(draft.Prefixes), len(draft.AddressSets), metadata.MaxAddressSetsPerRecord, actorID)
	if err != nil {
		var mysqlErr *mysqldriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return AddressDimensionSnapshot{}, ErrAddressDimensionConflict
		}
		return AddressDimensionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	retained = true
	return p.GetAddressDimensionSnapshot(ctx, tenantID, snapshotID)
}

func loadAddressDimensionDraft(ctx context.Context, tx *sql.Tx, tenantID ID) (AddressDimensionDraft, string, error) {
	prefixRows, err := tx.QueryContext(ctx, `SELECT `+addressPrefixColumns+` FROM address_prefixes WHERE tenant_id = ? ORDER BY cidr`, tenantID)
	if err != nil {
		return AddressDimensionDraft{}, "", err
	}
	var source AddressDimensionDraftSource
	for prefixRows.Next() {
		item, scanErr := scanAddressPrefix(prefixRows)
		if scanErr != nil {
			prefixRows.Close()
			return AddressDimensionDraft{}, "", scanErr
		}
		source.Prefixes = append(source.Prefixes, item)
	}
	if err := prefixRows.Err(); err != nil {
		prefixRows.Close()
		return AddressDimensionDraft{}, "", err
	}
	if err := prefixRows.Close(); err != nil {
		return AddressDimensionDraft{}, "", err
	}

	setRows, err := tx.QueryContext(ctx, `SELECT `+addressSetColumns+` FROM address_sets WHERE tenant_id = ? ORDER BY id`, tenantID)
	if err != nil {
		return AddressDimensionDraft{}, "", err
	}
	for setRows.Next() {
		item, scanErr := scanAddressSet(setRows)
		if scanErr != nil {
			setRows.Close()
			return AddressDimensionDraft{}, "", scanErr
		}
		source.Sets = append(source.Sets, item)
	}
	if err := setRows.Err(); err != nil {
		setRows.Close()
		return AddressDimensionDraft{}, "", err
	}
	if err := setRows.Close(); err != nil {
		return AddressDimensionDraft{}, "", err
	}

	geoRows, err := tx.QueryContext(ctx, `SELECT `+geoDictionaryColumns+` FROM geo_dict WHERE tenant_id = ? ORDER BY id`, tenantID)
	if err != nil {
		return AddressDimensionDraft{}, "", err
	}
	for geoRows.Next() {
		item, scanErr := scanGeoDictionaryNode(geoRows)
		if scanErr != nil {
			geoRows.Close()
			return AddressDimensionDraft{}, "", scanErr
		}
		source.Geography = append(source.Geography, item)
	}
	if err := geoRows.Err(); err != nil {
		geoRows.Close()
		return AddressDimensionDraft{}, "", err
	}
	if err := geoRows.Close(); err != nil {
		return AddressDimensionDraft{}, "", err
	}

	operatorRows, err := tx.QueryContext(ctx, `SELECT `+ispOperatorColumns+` FROM isp_operators WHERE tenant_id = ? ORDER BY id`, tenantID)
	if err != nil {
		return AddressDimensionDraft{}, "", err
	}
	for operatorRows.Next() {
		item, scanErr := scanISPOperator(operatorRows)
		if scanErr != nil {
			operatorRows.Close()
			return AddressDimensionDraft{}, "", scanErr
		}
		source.Operators = append(source.Operators, item)
	}
	if err := operatorRows.Err(); err != nil {
		operatorRows.Close()
		return AddressDimensionDraft{}, "", err
	}
	if err := operatorRows.Close(); err != nil {
		return AddressDimensionDraft{}, "", err
	}
	return CompileAddressDimensionDraft(source)
}

func (p *MySQLAddressDimensionPublisher) GetAddressDimensionSnapshot(ctx context.Context, tenantID, snapshotID ID) (AddressDimensionSnapshot, error) {
	return scanAddressDimensionSnapshot(p.store.db.QueryRowContext(ctx, `SELECT `+addressDimensionSnapshotColumns+`
		FROM dimension_snapshots WHERE tenant_id = ? AND id = ?`, tenantID, snapshotID))
}

func (p *MySQLAddressDimensionPublisher) ListAddressDimensionSnapshots(ctx context.Context, tenantID ID, filter AddressDimensionListFilter) ([]AddressDimensionSnapshot, string, error) {
	filter.Status = strings.TrimSpace(filter.Status)
	if filter.Status != "" && filter.Status != AddressDimensionStatusActive && filter.Status != AddressDimensionStatusRetired {
		return nil, "", ErrAddressDimensionInvalid
	}
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 500 {
		filter.Limit = 500
	}
	query := `SELECT ` + addressDimensionSnapshotColumns + ` FROM dimension_snapshots
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ?`
	args := []any{tenantID, AddressDimensionModuleKey, AddressDimensionKey}
	if filter.Status != "" {
		query += ` AND status = ?`
		args = append(args, filter.Status)
	}
	if filter.Cursor != "" {
		cursor, err := strconv.ParseUint(filter.Cursor, 10, 64)
		if err != nil || cursor == 0 {
			return nil, "", ErrAddressDimensionInvalid
		}
		query += ` AND version < ?`
		args = append(args, cursor)
	}
	query += ` ORDER BY version DESC LIMIT ?`
	args = append(args, filter.Limit+1)
	rows, err := p.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	items := make([]AddressDimensionSnapshot, 0, filter.Limit)
	for rows.Next() {
		item, scanErr := scanAddressDimensionSnapshot(rows)
		if scanErr != nil {
			return nil, "", scanErr
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(items) > filter.Limit {
		items = items[:filter.Limit]
		next = strconv.FormatUint(items[len(items)-1].Version, 10)
	}
	return items, next, nil
}

const addressDimensionSnapshotColumns = `
	id, tenant_id, module_key, dimension_key, version, effective_from, object_ref,
	checksum, draft_digest, bundle_schema_version, entry_count, prefix_count,
	address_set_count, max_address_sets_per_record, status, approval_state,
	COALESCE(decided_by, ''), decided_at, COALESCE(decision_reason, ''),
	COALESCE(signature_algorithm, ''), COALESCE(signing_key_id, ''), signature,
	signed_at, retention_until, object_deleted_at, row_version,
	COALESCE(created_by, ''), COALESCE(retired_by, ''), created_at, retired_at`

func scanAddressDimensionSnapshot(row rowScanner) (AddressDimensionSnapshot, error) {
	var item AddressDimensionSnapshot
	err := row.Scan(&item.ID, &item.TenantID, &item.ModuleKey, &item.DimensionKey, &item.Version,
		&item.EffectiveFrom, &item.ObjectRef, &item.Checksum, &item.DraftDigest, &item.BundleSchemaVersion,
		&item.EntryCount, &item.PrefixCount, &item.AddressSetCount, &item.MaxAddressSetsPerRecord,
		&item.Status, &item.ApprovalState, &item.DecidedBy, &item.DecidedAt, &item.DecisionReason,
		&item.SignatureAlgorithm, &item.SigningKeyID, &item.Signature, &item.SignedAt,
		&item.RetentionUntil, &item.ObjectDeletedAt, &item.RowVersion, &item.CreatedBy,
		&item.RetiredBy, &item.CreatedAt, &item.RetiredAt)
	return item, err
}

func isUTCMinute(value time.Time) bool {
	_, offset := value.Zone()
	return !value.IsZero() && offset == 0 && value.Second() == 0 && value.Nanosecond() == 0
}

func validSHA256Digest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && len(decoded) == sha256.Size
}

var _ AddressDimensionPublisher = (*MySQLAddressDimensionPublisher)(nil)
