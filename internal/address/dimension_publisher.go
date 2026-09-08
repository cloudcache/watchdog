package address

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

func (p *Publisher) PreviewAddressDimension(ctx context.Context, effectiveFrom time.Time) (AddressDimensionPreview, error) {
	if p == nil || p.store == nil || !isUTCMinute(effectiveFrom) {
		return AddressDimensionPreview{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return AddressDimensionPreview{}, err
	}
	defer tx.Rollback()
	draft, digest, err := loadAddressDimensionDraft(ctx, tx, false)
	if err != nil {
		return AddressDimensionPreview{}, err
	}
	data, compiled, _, err := encodeAddressDimensionBundle(draft, "preview-address", 1, effectiveFrom)
	if err != nil {
		return AddressDimensionPreview{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionPreview{}, err
	}
	metadata := compiled.Metadata()
	sourcePrefixCount, err := countAddressDimensionSourcePrefixes(draft.Sources)
	if err != nil {
		return AddressDimensionPreview{}, err
	}
	return AddressDimensionPreview{
		DraftDigest: digest, BundleSchemaVersion: flowdimension.BundleSchemaVersion, EffectiveFrom: effectiveFrom.UTC(),
		SourceManifestVersion: AddressDimensionSourceManifestV1,
		SourceManifest:        append([]AddressDimensionSource(nil), draft.Sources...), SourcePrefixCount: sourcePrefixCount,
		PrefixCount: uint64(len(draft.Prefixes)), AddressSetCount: uint64(len(draft.AddressSets)), OperatorCount: uint64(len(draft.Operators)),
		GeoNodeCount:           uint64(len(draft.GeoNodes)),
		EnabledAddressSetCount: uint64(metadata.EnabledAddressSetCount), MaxAddressSetsPerRecord: uint32(metadata.MaxAddressSetsPerRecord),
		DefinitionBytes: uint64(len(data)),
	}, nil
}

func loadAddressDimensionDraft(ctx context.Context, tx *sql.Tx, lockSources bool) (AddressDimensionDraft, string, error) {
	sources, err := loadAddressDimensionSources(ctx, tx, lockSources)
	if err != nil {
		return AddressDimensionDraft{}, "", err
	}
	source := AddressDimensionDraftSource{Sources: sources}
	prefixRows, err := tx.QueryContext(ctx, `SELECT `+addressPrefixColumns+` FROM address_prefixes ORDER BY cidr`)
	if err != nil {
		return AddressDimensionDraft{}, "", err
	}
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

	setRows, err := tx.QueryContext(ctx, `SELECT `+addressSetColumns+` FROM address_sets ORDER BY id`)
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

	geoRows, err := tx.QueryContext(ctx, `SELECT `+geoDictionaryColumns+` FROM geo_dict ORDER BY id`)
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

	operatorRows, err := tx.QueryContext(ctx, `SELECT `+ispOperatorColumns+` FROM isp_operators ORDER BY id`)
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

func loadAddressDimensionSources(ctx context.Context, tx *sql.Tx, lock bool) ([]AddressDimensionSource, error) {
	query := `
		SELECT slots.source_slot, slots.import_id, slots.row_version,
		       imports.checksum_sha256, imports.row_count_v4, imports.row_count_v6, imports.status
		FROM address_import_slots AS slots
		JOIN address_imports AS imports ON imports.id = slots.import_id
	`
	if lock {
		query += ` FOR SHARE`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sources []AddressDimensionSource
	for rows.Next() {
		var source AddressDimensionSource
		var status string
		if err := rows.Scan(&source.Slot, &source.ImportID, &source.SlotRowVersion, &source.ChecksumSHA256, &source.RowCountV4, &source.RowCountV6, &status); err != nil {
			return nil, err
		}
		if status != AddressImportStatusReady {
			return nil, fmt.Errorf("%w: active address import %s is not ready", ErrAddressDimensionInvalid, source.ImportID)
		}
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return canonicalAddressDimensionSources(sources)
}

func countAddressDimensionSourcePrefixes(sources []AddressDimensionSource) (uint64, error) {
	var count uint64
	for _, source := range sources {
		if ^uint64(0)-count < source.RowCountV4 {
			return 0, fmt.Errorf("%w: address import source prefix count overflow", ErrAddressDimensionInvalid)
		}
		count += source.RowCountV4
		if ^uint64(0)-count < source.RowCountV6 {
			return 0, fmt.Errorf("%w: address import source prefix count overflow", ErrAddressDimensionInvalid)
		}
		count += source.RowCountV6
	}
	return count, nil
}

var addressDimensionSnapshotSortColumns = map[string]string{
	"":               "version",
	"version":        "version",
	"status":         "status",
	"effective":      "effective_from",
	"prefixes":       "prefix_count",
	"sets":           "address_set_count",
	"max_membership": "max_address_sets_per_record",
	"schema":         "bundle_schema_version",
	"checksum":       "checksum",
	"created":        "created_at",
}

func (p *Publisher) ListAddressDimensionSnapshots(ctx context.Context, filter AddressDimensionListFilter) ([]AddressDimensionSnapshot, string, int, error) {
	filter.Status = strings.TrimSpace(filter.Status)
	filter.Search = strings.TrimSpace(filter.Search)
	if filter.Status != "" && filter.Status != AddressDimensionStatusActive && filter.Status != AddressDimensionStatusRetired {
		return nil, "", 0, ErrAddressDimensionInvalid
	}
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 500 {
		filter.Limit = 500
	}
	sortColumn, validSort := addressDimensionSnapshotSortColumns[filter.Sort]
	if len(filter.Search) > 255 || filter.Offset < 0 || !validSort {
		return nil, "", 0, ErrAddressDimensionInvalid
	}
	where := ` WHERE module_key = ? AND dimension_key = ?`
	args := []any{p.scope.ModuleKey, p.scope.DimensionKey}
	if filter.Status != "" {
		where += ` AND status = ?`
		args = append(args, filter.Status)
	}
	if filter.Search != "" {
		like := "%" + escapeSQLLike(filter.Search) + "%"
		where += ` AND (id LIKE ? OR CAST(version AS CHAR) LIKE ? OR checksum LIKE ? OR draft_digest LIKE ?)`
		args = append(args, like, like, like, like)
	}
	var total int
	if err := p.store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dimension_snapshots`+where, args...).Scan(&total); err != nil {
		return nil, "", 0, err
	}
	query := `SELECT ` + addressDimensionSnapshotColumns + ` FROM dimension_snapshots` + where
	if filter.Cursor != "" {
		cursor, err := strconv.ParseUint(filter.Cursor, 10, 64)
		if err != nil || cursor == 0 {
			return nil, "", 0, ErrAddressDimensionInvalid
		}
		query += ` AND version < ?`
		args = append(args, cursor)
	}
	if filter.TableMode {
		direction := "ASC"
		if filter.Desc {
			direction = "DESC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s, id %s LIMIT ? OFFSET ?", sortColumn, direction, direction)
		args = append(args, filter.Limit, filter.Offset)
	} else {
		query += ` ORDER BY version DESC LIMIT ?`
		args = append(args, filter.Limit+1)
	}
	rows, err := p.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close()
	items := make([]AddressDimensionSnapshot, 0, filter.Limit)
	for rows.Next() {
		item, scanErr := scanAddressDimensionSnapshot(rows)
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
		next = strconv.FormatUint(items[len(items)-1].Version, 10)
	}
	return items, next, total, nil
}

const addressDimensionSnapshotColumns = `
	id, module_key, dimension_key, version, effective_from, object_ref,
	object_format, object_format_version, builder_version, COALESCE(build_job_id, ''),
	checksum, draft_digest, source_manifest_version, source_manifest, source_prefix_count, bundle_schema_version, entry_count, prefix_count,
	address_set_count, max_address_sets_per_record, status, approval_state,
	COALESCE(decided_by, ''), decided_at, COALESCE(decision_reason, ''),
	retention_until, object_deleted_at, row_version,
	COALESCE(created_by, ''), COALESCE(retired_by, ''), created_at, retired_at`

func scanAddressDimensionSnapshot(row rowScanner) (AddressDimensionSnapshot, error) {
	var item AddressDimensionSnapshot
	var sourceManifest []byte
	err := row.Scan(&item.ID, &item.ModuleKey, &item.DimensionKey, &item.Version,
		&item.EffectiveFrom, &item.ObjectRef, &item.ObjectFormat, &item.ObjectFormatVersion, &item.BuilderVersion, &item.BuildJobID,
		&item.Checksum, &item.DraftDigest, &item.SourceManifestVersion, &sourceManifest, &item.SourcePrefixCount, &item.BundleSchemaVersion,
		&item.EntryCount, &item.PrefixCount, &item.AddressSetCount, &item.MaxAddressSetsPerRecord,
		&item.Status, &item.ApprovalState, &item.DecidedBy, &item.DecidedAt, &item.DecisionReason,
		&item.RetentionUntil, &item.ObjectDeletedAt, &item.RowVersion, &item.CreatedBy,
		&item.RetiredBy, &item.CreatedAt, &item.RetiredAt)
	if err != nil {
		return item, err
	}
	if err := json.Unmarshal(sourceManifest, &item.SourceManifest); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if item.SourceManifest == nil {
		item.SourceManifest = []AddressDimensionSource{}
	}
	return item, nil
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
