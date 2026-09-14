package address

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

const (
	maxAddressImportBatch         = 5_000
	maxAddressImportStatementRows = 1_000
)

type addressImportBatchRow struct {
	record AddressImportRecord
	family uint8
	bits   uint8
	start  [16]byte
	end    [16]byte
	labels []byte
}

func (s *Store) CreateAddressImport(ctx context.Context, item AddressImport) (AddressImport, error) {
	if err := validateAddressImport(item); err != nil {
		return AddressImport{}, err
	}
	if item.ID == "" {
		id, err := newManagementID()
		if err != nil {
			return AddressImport{}, err
		}
		item.ID = id
	}
	if item.Status == "" {
		item.Status = AddressImportStatusQuarantined
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO address_imports (
			id, source_slot, format, original_name, artifact_ref,
			checksum_sha256, size_bytes, status, created_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''))
	`, item.ID, item.SourceSlot, item.Format, item.OriginalName,
		item.ArtifactRef, strings.ToLower(item.ChecksumSHA256), item.SizeBytes, item.Status, item.CreatedBy)
	if err != nil {
		return AddressImport{}, err
	}
	return s.GetAddressImport(ctx, item.ID)
}

func validateAddressImport(item AddressImport) error {
	if strings.TrimSpace(item.OriginalName) == "" || strings.TrimSpace(item.ArtifactRef) == "" || item.SizeBytes == 0 {
		return fmt.Errorf("%w: artifact name/ref and non-zero size are required", ErrAddressImportInvalid)
	}
	if !validAddressImportSlot(item.SourceSlot) {
		return fmt.Errorf("%w: unsupported source slot %q", ErrAddressImportInvalid, item.SourceSlot)
	}
	if item.Format != AddressImportFormatMMDB && item.Format != AddressImportFormatIPDB {
		return fmt.Errorf("%w: unsupported format %q", ErrAddressImportInvalid, item.Format)
	}
	checksum, err := hex.DecodeString(item.ChecksumSHA256)
	if err != nil || len(checksum) != 32 {
		return fmt.Errorf("%w: checksum_sha256 must be 64 hexadecimal characters", ErrAddressImportInvalid)
	}
	if item.Status != "" && item.Status != AddressImportStatusQuarantined && item.Status != AddressImportStatusQueued {
		return fmt.Errorf("%w: a new import must be quarantined or queued", ErrAddressImportInvalid)
	}
	return nil
}

func validAddressImportSlot(value string) bool {
	return value == AddressImportSlotGeo || value == AddressImportSlotASN || value == AddressImportSlotCombined
}

const addressImportColumns = `
	id, source_slot, format, original_name, artifact_ref,
	checksum_sha256, size_bytes, COALESCE(database_type, ''), build_epoch,
	ip_version, COALESCE(language, ''), status, row_count_v4, row_count_v6,
	COALESCE(created_by, ''), COALESCE(error_code, ''), COALESCE(error_detail, ''),
	created_at, updated_at, ready_at, activated_at`

func scanAddressImport(row rowScanner) (AddressImport, error) {
	var item AddressImport
	var buildEpoch, ipVersion sql.NullInt64
	var readyAt, activatedAt sql.NullTime
	if err := row.Scan(
		&item.ID, &item.SourceSlot, &item.Format, &item.OriginalName, &item.ArtifactRef,
		&item.ChecksumSHA256, &item.SizeBytes, &item.DatabaseType, &buildEpoch,
		&ipVersion, &item.Language, &item.Status, &item.RowCountV4, &item.RowCountV6,
		&item.CreatedBy, &item.ErrorCode, &item.ErrorDetail,
		&item.CreatedAt, &item.UpdatedAt, &readyAt, &activatedAt,
	); err != nil {
		return AddressImport{}, err
	}
	if buildEpoch.Valid {
		value := buildEpoch.Int64
		item.BuildEpoch = &value
	}
	if ipVersion.Valid {
		value := uint8(ipVersion.Int64)
		item.IPVersion = &value
	}
	if readyAt.Valid {
		value := readyAt.Time
		item.ReadyAt = &value
	}
	if activatedAt.Valid {
		value := activatedAt.Time
		item.ActivatedAt = &value
	}
	return item, nil
}

func (s *Store) GetAddressImport(ctx context.Context, importID ID) (AddressImport, error) {
	return scanAddressImport(s.db.QueryRowContext(ctx, `
		SELECT `+addressImportColumns+` FROM address_imports WHERE id = ?
	`, importID))
}

var addressImportSortColumns = map[string]string{
	"":        "created_at",
	"name":    "original_name",
	"slot":    "source_slot",
	"format":  "format",
	"type":    "database_type",
	"status":  "status",
	"rows":    "(row_count_v4 + row_count_v6)",
	"size":    "size_bytes",
	"created": "created_at",
}

func (s *Store) ListAddressImports(ctx context.Context, filter AddressImportListFilter) ([]AddressImport, string, int, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	filter.Search = strings.TrimSpace(filter.Search)
	filter.SourceSlot = strings.TrimSpace(filter.SourceSlot)
	filter.Status = strings.TrimSpace(filter.Status)
	filter.Format = strings.TrimSpace(filter.Format)
	sortColumn, validSort := addressImportSortColumns[filter.Sort]
	if len(filter.Search) > 255 || filter.Offset < 0 || !validSort {
		return nil, "", 0, fmt.Errorf("%w: invalid address import filter", ErrAddressImportInvalid)
	}
	where := ` WHERE 1=1`
	args := []any{}
	if filter.SourceSlot != "" {
		if !validAddressImportSlot(filter.SourceSlot) {
			return nil, "", 0, fmt.Errorf("%w: unsupported source slot", ErrAddressImportInvalid)
		}
		where += ` AND source_slot = ?`
		args = append(args, filter.SourceSlot)
	}
	if filter.Status != "" {
		switch filter.Status {
		case AddressImportStatusQuarantined, AddressImportStatusQueued, AddressImportStatusImporting, AddressImportStatusReady, AddressImportStatusFailed, AddressImportStatusRetired:
		default:
			return nil, "", 0, fmt.Errorf("%w: unsupported import status", ErrAddressImportInvalid)
		}
		where += ` AND status = ?`
		args = append(args, filter.Status)
	}
	if filter.Format != "" {
		if filter.Format != AddressImportFormatMMDB && filter.Format != AddressImportFormatIPDB {
			return nil, "", 0, fmt.Errorf("%w: unsupported import format", ErrAddressImportInvalid)
		}
		where += ` AND format = ?`
		args = append(args, filter.Format)
	}
	if filter.Search != "" {
		like := "%" + escapeSQLLike(filter.Search) + "%"
		where += ` AND (original_name LIKE ? OR id LIKE ? OR COALESCE(database_type, '') LIKE ? OR COALESCE(error_code, '') LIKE ? OR COALESCE(error_detail, '') LIKE ?)`
		args = append(args, like, like, like, like, like)
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM address_imports`+where, args...).Scan(&total); err != nil {
		return nil, "", 0, err
	}
	query := `SELECT ` + addressImportColumns + ` FROM address_imports` + where
	if filter.Cursor != "" {
		cursorTime, cursorID, err := decodeAuditCursor(filter.Cursor)
		if err != nil {
			return nil, "", 0, err
		}
		query += ` AND (created_at < ? OR (created_at = ? AND id < ?))`
		args = append(args, cursorTime, cursorTime, cursorID)
	}
	if filter.TableMode {
		direction := "ASC"
		if filter.Desc {
			direction = "DESC"
		}
		query += fmt.Sprintf(" ORDER BY %s %s, id %s LIMIT ? OFFSET ?", sortColumn, direction, direction)
		args = append(args, limit, filter.Offset)
	} else {
		query += ` ORDER BY created_at DESC, id DESC LIMIT ?`
		args = append(args, limit+1)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close()
	items := make([]AddressImport, 0, limit)
	for rows.Next() {
		item, err := scanAddressImport(rows)
		if err != nil {
			return nil, "", 0, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, err
	}
	nextCursor := ""
	if !filter.TableMode && len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		nextCursor = encodeAuditCursor(last.CreatedAt, last.ID)
	}
	return items, nextCursor, total, nil
}

func (s *Store) BeginAddressImport(ctx context.Context, importID ID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status string
	if err := tx.QueryRowContext(ctx, `
		SELECT status FROM address_imports WHERE id = ? FOR UPDATE
	`, importID).Scan(&status); err != nil {
		return err
	}
	if status != AddressImportStatusQuarantined && status != AddressImportStatusQueued && status != AddressImportStatusImporting {
		return ErrAddressImportNotWritable
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE address_imports
		SET status = 'importing', error_code = NULL, error_detail = NULL
		WHERE id = ?
	`, importID); err != nil {
		return err
	}
	return tx.Commit()
}

func prepareAddressImportBatch(records []AddressImportRecord) ([]addressImportBatchRow, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if len(records) > maxAddressImportBatch {
		return nil, fmt.Errorf("%w: batch has %d records, limit is %d", ErrAddressImportInvalid, len(records), maxAddressImportBatch)
	}
	rows := make([]addressImportBatchRow, 0, len(records))
	for index, record := range records {
		prefix, err := addressRecordPrefix(record)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", index, err)
		}
		start, end := addressPrefixBounds(prefix)
		family := uint8(6)
		if prefix.Addr().Is4() {
			family = 4
		}
		labels, err := json.Marshal(map[string]string{"source": record.Source})
		if err != nil {
			return nil, fmt.Errorf("record %d labels: %w", index, err)
		}
		rows = append(rows, addressImportBatchRow{record: record, family: family, bits: uint8(prefix.Bits()), start: start, end: end, labels: labels})
	}
	return rows, nil
}

func (s *Store) InsertAddressImportBatch(ctx context.Context, importID ID, records []AddressImportRecord) error {
	batch, err := prepareAddressImportBatch(records)
	if err != nil || len(batch) == 0 {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status string
	if err := tx.QueryRowContext(ctx, `
		SELECT status FROM address_imports WHERE id = ? FOR UPDATE
	`, importID).Scan(&status); err != nil {
		return err
	}
	if status != AddressImportStatusImporting {
		return ErrAddressImportNotWritable
	}
	const statementPrefix = `
		INSERT INTO address_base_prefixes (
			import_id, family, prefix_length, cidr, ip_start, ip_end,
			continent_code, country_code, country_name, subdivision_code,
			subdivision_name, city_code, city_name, asn, operator_name,
			latitude, longitude, labels
		) VALUES `
	const rowPlaceholder = `(?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''),
			NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, 0),
			NULLIF(?, ''), ?, ?, ?)`
	const statementSuffix = `
		ON DUPLICATE KEY UPDATE
			continent_code = VALUES(continent_code), country_code = VALUES(country_code),
			country_name = VALUES(country_name), subdivision_code = VALUES(subdivision_code),
			subdivision_name = VALUES(subdivision_name), city_code = VALUES(city_code),
			city_name = VALUES(city_name), asn = VALUES(asn), operator_name = VALUES(operator_name),
			latitude = VALUES(latitude), longitude = VALUES(longitude), labels = VALUES(labels)
	`
	for start := 0; start < len(batch); start += maxAddressImportStatementRows {
		end := min(start+maxAddressImportStatementRows, len(batch))
		var query strings.Builder
		query.Grow(len(statementPrefix) + (end-start)*len(rowPlaceholder) + len(statementSuffix))
		query.WriteString(statementPrefix)
		args := make([]any, 0, (end-start)*18)
		for index, row := range batch[start:end] {
			if index != 0 {
				query.WriteByte(',')
			}
			query.WriteString(rowPlaceholder)
			record := row.record
			args = append(args,
				importID, row.family, row.bits, record.Prefix, row.start[:], row.end[:],
				record.ContinentCode, record.CountryCode, record.CountryName, record.SubdivisionCode,
				record.SubdivisionName, record.CityCode, record.CityName, record.ASN, record.Operator,
				record.Latitude, record.Longitude, row.labels,
			)
		}
		query.WriteString(statementSuffix)
		if _, err := tx.ExecContext(ctx, query.String(), args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) CompleteAddressImport(ctx context.Context, importID ID, metadata AddressImportMetadata, language string) (AddressImport, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressImport{}, err
	}
	defer tx.Rollback()
	var status string
	if err := tx.QueryRowContext(ctx, `
		SELECT status FROM address_imports WHERE id = ? FOR UPDATE
	`, importID).Scan(&status); err != nil {
		return AddressImport{}, err
	}
	if status == AddressImportStatusReady {
		if err := tx.Rollback(); err != nil {
			return AddressImport{}, err
		}
		return s.GetAddressImport(ctx, importID)
	}
	if status != AddressImportStatusImporting {
		return AddressImport{}, ErrAddressImportNotWritable
	}
	var countV4, countV6 uint64
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(CASE WHEN family = 4 THEN 1 END), COUNT(CASE WHEN family = 6 THEN 1 END)
		FROM address_base_prefixes WHERE import_id = ?
	`, importID).Scan(&countV4, &countV6); err != nil {
		return AddressImport{}, err
	}
	var buildEpoch any
	if !metadata.BuildTime.IsZero() {
		buildEpoch = metadata.BuildTime.Unix()
	}
	var ipVersion any
	if metadata.IPVersion != 0 {
		ipVersion = metadata.IPVersion
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE address_imports SET status = 'ready', database_type = NULLIF(?, ''),
			build_epoch = ?, ip_version = ?, language = NULLIF(?, ''),
			row_count_v4 = ?, row_count_v6 = ?, ready_at = CURRENT_TIMESTAMP(3),
			error_code = NULL, error_detail = NULL
		WHERE id = ? AND status = 'importing'
	`, metadata.DatabaseType, buildEpoch, ipVersion, strings.TrimSpace(language), countV4, countV6, importID); err != nil {
		return AddressImport{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressImport{}, err
	}
	return s.GetAddressImport(ctx, importID)
}

func (s *Store) FailAddressImport(ctx context.Context, importID ID, code, detail string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE address_imports SET status = 'failed', error_code = NULLIF(LEFT(?, 64), ''),
			error_detail = NULLIF(LEFT(?, 4096), '')
		WHERE id = ? AND status IN ('quarantined','queued','importing','failed')
	`, strings.TrimSpace(code), strings.TrimSpace(detail), importID)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count > 0 {
		return nil
	}
	var status string
	if err := s.db.QueryRowContext(ctx, `
		SELECT status FROM address_imports WHERE id = ?
	`, importID).Scan(&status); err != nil {
		return err
	}
	if status == AddressImportStatusFailed {
		return nil
	}
	return ErrAddressImportNotWritable
}

// DeleteAddressImport removes an import generation and its base prefixes (FK
// cascade) and returns the artifact ref so the caller can delete the stored source
// file. An import that still backs an active slot is refused (ErrAddressImportActive)
// — activate a different generation into that slot first.
func (s *Store) DeleteAddressImport(ctx context.Context, importID ID) (string, error) {
	if importID == "" {
		return "", fmt.Errorf("%w: import is required", ErrAddressImportInvalid)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var artifactRef string
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(artifact_ref, '') FROM address_imports WHERE id = ? FOR UPDATE
	`, importID).Scan(&artifactRef); err != nil {
		return "", err // sql.ErrNoRows -> not found
	}
	var slotCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM address_import_slots WHERE import_id = ?
	`, importID).Scan(&slotCount); err != nil {
		return "", err
	}
	if slotCount > 0 {
		return "", ErrAddressImportActive
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM address_imports WHERE id = ?`, importID); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return artifactRef, nil
}

func scanAddressImportSlot(row rowScanner) (AddressImportSlot, error) {
	var slot AddressImportSlot
	err := row.Scan(&slot.SourceSlot, &slot.ImportID, &slot.RowVersion, &slot.ActivatedBy, &slot.ActivatedAt)
	return slot, err
}

func (s *Store) GetAddressImportSlot(ctx context.Context, sourceSlot string) (AddressImportSlot, error) {
	return scanAddressImportSlot(s.db.QueryRowContext(ctx, `
		SELECT source_slot, import_id, row_version, COALESCE(activated_by, ''), activated_at
		FROM address_import_slots WHERE source_slot = ?
	`, sourceSlot))
}

func (s *Store) ActivateAddressImport(ctx context.Context, importID, actorID ID, expectedVersion uint64) (AddressImportSlot, error) {
	if importID == "" {
		return AddressImportSlot{}, fmt.Errorf("%w: import is required", ErrAddressImportInvalid)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressImportSlot{}, err
	}
	defer tx.Rollback()
	var sourceSlot, status string
	if err := tx.QueryRowContext(ctx, `
		SELECT source_slot, status FROM address_imports WHERE id = ? FOR UPDATE
	`, importID).Scan(&sourceSlot, &status); err != nil {
		return AddressImportSlot{}, err
	}
	if status != AddressImportStatusReady {
		return AddressImportSlot{}, ErrAddressImportNotWritable
	}
	var currentVersion uint64
	err = tx.QueryRowContext(ctx, `
		SELECT row_version FROM address_import_slots WHERE source_slot = ? FOR UPDATE
	`, sourceSlot).Scan(&currentVersion)
	switch {
	case errors.Is(err, sql.ErrNoRows) && expectedVersion != 0:
		return AddressImportSlot{}, ErrAddressImportVersionConflict
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `
			INSERT INTO address_import_slots (source_slot, import_id, row_version, activated_by)
			VALUES (?, ?, 1, NULLIF(?, ''))
		`, sourceSlot, importID, actorID)
	case err != nil:
		return AddressImportSlot{}, err
	case currentVersion != expectedVersion:
		return AddressImportSlot{}, ErrAddressImportVersionConflict
	default:
		_, err = tx.ExecContext(ctx, `
			UPDATE address_import_slots SET import_id = ?, row_version = row_version + 1,
				activated_by = NULLIF(?, ''), activated_at = CURRENT_TIMESTAMP(3)
			WHERE source_slot = ? AND row_version = ?
		`, importID, actorID, sourceSlot, expectedVersion)
	}
	if err != nil {
		return AddressImportSlot{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE address_imports SET activated_at = CURRENT_TIMESTAMP(3) WHERE id = ?
	`, importID); err != nil {
		return AddressImportSlot{}, err
	}
	slot, err := scanAddressImportSlot(tx.QueryRowContext(ctx, `
		SELECT source_slot, import_id, row_version, COALESCE(activated_by, ''), activated_at
		FROM address_import_slots WHERE source_slot = ?
	`, sourceSlot))
	if err != nil {
		return AddressImportSlot{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressImportSlot{}, err
	}
	return slot, nil
}

const addressBasePrefixColumns = `
	id, import_id, family, prefix_length, cidr,
	COALESCE(continent_code, ''), COALESCE(country_code, ''), COALESCE(country_name, ''),
	COALESCE(subdivision_code, ''), COALESCE(subdivision_name, ''),
	COALESCE(city_code, ''), COALESCE(city_name, ''), asn,
	COALESCE(operator_name, ''), latitude, longitude, labels, created_at`

func scanAddressBasePrefix(row rowScanner) (AddressBasePrefix, error) {
	var item AddressBasePrefix
	var asn sql.NullInt64
	var latitude, longitude sql.NullFloat64
	var labels []byte
	if err := row.Scan(
		&item.ID, &item.ImportID, &item.Family, &item.PrefixLength, &item.CIDR,
		&item.ContinentCode, &item.CountryCode, &item.CountryName,
		&item.SubdivisionCode, &item.SubdivisionName, &item.CityCode, &item.CityName, &asn,
		&item.OperatorName, &latitude, &longitude, &labels, &item.CreatedAt,
	); err != nil {
		return AddressBasePrefix{}, err
	}
	if asn.Valid {
		value := uint32(asn.Int64)
		item.ASN = &value
	}
	if latitude.Valid {
		value := latitude.Float64
		item.Latitude = &value
	}
	if longitude.Valid {
		value := longitude.Float64
		item.Longitude = &value
	}
	item.Labels = map[string]string{}
	if err := json.Unmarshal(labels, &item.Labels); err != nil {
		return AddressBasePrefix{}, fmt.Errorf("decode address base labels: %w", err)
	}
	return item, nil
}

var addressBasePrefixSortColumns = map[string]string{
	"":         "id",
	"family":   "family",
	"country":  "country_code",
	"region":   "subdivision_name",
	"asn":      "asn",
	"operator": "operator_name",
}

func (s *Store) ListAddressBasePrefixes(ctx context.Context, importID ID, filter AddressBasePrefixFilter) ([]AddressBasePrefix, string, int, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if filter.Family != 0 && filter.Family != 4 && filter.Family != 6 {
		return nil, "", 0, fmt.Errorf("%w: family must be 4 or 6", ErrAddressImportInvalid)
	}
	filter.Search = strings.TrimSpace(filter.Search)
	filter.CountryCode = strings.TrimSpace(filter.CountryCode)
	filter.Operator = strings.TrimSpace(filter.Operator)
	if len(filter.Search) > 255 || len(filter.CountryCode) > 2 || len(filter.Operator) > 255 || filter.Offset < 0 {
		return nil, "", 0, fmt.Errorf("%w: invalid address prefix filter", ErrAddressImportInvalid)
	}
	if filter.Sort != "cidr" {
		if _, ok := addressBasePrefixSortColumns[filter.Sort]; !ok {
			return nil, "", 0, fmt.Errorf("%w: invalid address prefix sort", ErrAddressImportInvalid)
		}
	}
	where := ` WHERE import_id = ?`
	args := []any{importID}
	if filter.Family != 0 {
		where += ` AND family = ?`
		args = append(args, filter.Family)
	}
	if filter.CountryCode != "" {
		where += ` AND country_code = ?`
		args = append(args, strings.ToUpper(strings.TrimSpace(filter.CountryCode)))
	}
	if filter.ASN != nil {
		where += ` AND asn = ?`
		args = append(args, *filter.ASN)
	}
	if filter.Operator != "" {
		where += ` AND operator_name = ?`
		args = append(args, strings.TrimSpace(filter.Operator))
	}
	if search := strings.TrimSpace(filter.Search); search != "" {
		like := "%" + escapeSQLLike(search) + "%"
		where += ` AND (cidr LIKE ? OR COALESCE(country_name, '') LIKE ? OR COALESCE(subdivision_name, '') LIKE ? OR COALESCE(city_name, '') LIKE ? OR COALESCE(operator_name, '') LIKE ? OR CAST(asn AS CHAR) LIKE ?)`
		args = append(args, like, like, like, like, like, like)
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM address_base_prefixes`+where, args...).Scan(&total); err != nil {
		return nil, "", 0, err
	}
	query := `SELECT ` + addressBasePrefixColumns + ` FROM address_base_prefixes` + where
	if filter.Cursor != "" {
		cursor, err := decodeAddressBaseCursor(filter.Cursor)
		if err != nil {
			return nil, "", 0, err
		}
		query += ` AND id > ?`
		args = append(args, cursor)
	}
	if filter.TableMode {
		direction := "ASC"
		if filter.Desc {
			direction = "DESC"
		}
		if filter.Sort == "cidr" {
			query += fmt.Sprintf(" ORDER BY family %s, ip_start %s, prefix_length %s, id %s LIMIT ? OFFSET ?", direction, direction, direction, direction)
		} else {
			query += fmt.Sprintf(" ORDER BY %s %s, id %s LIMIT ? OFFSET ?", addressBasePrefixSortColumns[filter.Sort], direction, direction)
		}
		args = append(args, limit, filter.Offset)
	} else {
		query += ` ORDER BY id LIMIT ?`
		args = append(args, limit+1)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close()
	items := make([]AddressBasePrefix, 0, limit)
	for rows.Next() {
		item, err := scanAddressBasePrefix(rows)
		if err != nil {
			return nil, "", 0, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, err
	}
	nextCursor := ""
	if !filter.TableMode && len(items) > limit {
		items = items[:limit]
		nextCursor = encodeAddressBaseCursor(items[len(items)-1].ID)
	}
	return items, nextCursor, total, nil
}

func (s *Store) LookupAddressBasePrefixes(ctx context.Context, importID ID, value string, limit int) ([]AddressBasePrefix, error) {
	address, err := netip.ParseAddr(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid lookup IP", ErrAddressImportInvalid)
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	family := uint8(6)
	if address.Is4() {
		family = 4
	}
	encoded := addressNumberBytes(addressNumberFromAddr(address))
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+addressBasePrefixColumns+` FROM address_base_prefixes
		WHERE import_id = ? AND family = ? AND ip_start <= ? AND ip_end >= ?
		ORDER BY prefix_length DESC, id LIMIT ?
	`, importID, family, encoded[:], encoded[:], limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]AddressBasePrefix, 0)
	for rows.Next() {
		item, err := scanAddressBasePrefix(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func encodeAddressBaseCursor(id uint64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatUint(id, 10)))
}

func decodeAddressBaseCursor(cursor string) (uint64, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid address prefix cursor", ErrAddressImportInvalid)
	}
	id, err := strconv.ParseUint(string(decoded), 10, 64)
	if err != nil || id == 0 {
		return 0, fmt.Errorf("%w: invalid address prefix cursor", ErrAddressImportInvalid)
	}
	return id, nil
}

var _ AddressImportRepository = (*Store)(nil)
