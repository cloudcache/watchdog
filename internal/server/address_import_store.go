package server

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strings"
)

const (
	maxAddressImportBatch         = 5_000
	maxAddressImportStatementRows = 1_000
)

// --- imports (generation records) ------------------------------------------

func validateAddressImport(item AddressImport) error {
	if strings.TrimSpace(item.OriginalName) == "" || strings.TrimSpace(item.ArtifactRef) == "" || item.SizeBytes == 0 {
		return fmt.Errorf("%w: artifact name/ref and non-zero size are required", errAddressImportInvalid)
	}
	if !validAddressImportSlot(item.SourceSlot) {
		return fmt.Errorf("%w: unsupported source slot %q", errAddressImportInvalid, item.SourceSlot)
	}
	if item.Format != AddressImportFormatMMDB && item.Format != AddressImportFormatIPDB {
		return fmt.Errorf("%w: unsupported format %q", errAddressImportInvalid, item.Format)
	}
	checksum, err := hex.DecodeString(item.ChecksumSHA256)
	if err != nil || len(checksum) != 32 {
		return fmt.Errorf("%w: checksum_sha256 must be 64 hexadecimal characters", errAddressImportInvalid)
	}
	return nil
}

func (s *Server) createAddressImport(ctx context.Context, item AddressImport) (AddressImport, error) {
	if err := validateAddressImport(item); err != nil {
		return AddressImport{}, err
	}
	if item.ID == "" {
		item.ID = newID()
	}
	if item.Status == "" {
		item.Status = AddressImportStatusQueued
	}
	var createdBy any
	if item.CreatedBy != "" {
		createdBy = item.CreatedBy
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO address_imports (
			id, source_slot, format, original_name, artifact_ref,
			checksum_sha256, size_bytes, status, created_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, item.ID, item.SourceSlot, item.Format, item.OriginalName,
		item.ArtifactRef, strings.ToLower(item.ChecksumSHA256), item.SizeBytes, item.Status, createdBy)
	if err != nil {
		return AddressImport{}, err
	}
	return s.getAddressImport(ctx, item.ID)
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

func (s *Server) getAddressImport(ctx context.Context, importID string) (AddressImport, error) {
	return scanAddressImport(s.db.QueryRowContext(ctx,
		`SELECT `+addressImportColumns+` FROM address_imports WHERE id = ?`, importID))
}

func (s *Server) listAddressImports(ctx context.Context, filter addressImportListFilter) ([]AddressImport, int, error) {
	where := ` WHERE 1=1`
	args := []any{}
	if slot := strings.TrimSpace(filter.SourceSlot); slot != "" {
		if !validAddressImportSlot(slot) {
			return nil, 0, fmt.Errorf("%w: unsupported source slot", errAddressImportInvalid)
		}
		where += ` AND source_slot = ?`
		args = append(args, slot)
	}
	if status := strings.TrimSpace(filter.Status); status != "" {
		where += ` AND status = ?`
		args = append(args, status)
	}
	if format := strings.TrimSpace(filter.Format); format != "" {
		where += ` AND format = ?`
		args = append(args, format)
	}
	if search := strings.TrimSpace(filter.Search); search != "" {
		like := "%" + escapeSQLLike(search) + "%"
		where += ` AND (original_name LIKE ? OR id LIKE ? OR COALESCE(database_type, '') LIKE ?)`
		args = append(args, like, like, like)
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM address_imports`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	query := `SELECT ` + addressImportColumns + ` FROM address_imports` + where + ` ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?`
	args = append(args, filter.Limit, filter.Offset)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]AddressImport, 0, filter.Limit)
	for rows.Next() {
		item, err := scanAddressImport(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

// beginAddressImport moves a writable generation into the importing state so a
// batch insert is accepted. Idempotent for an import already importing.
func (s *Server) beginAddressImport(ctx context.Context, importID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status string
	if err := tx.QueryRowContext(ctx,
		`SELECT status FROM address_imports WHERE id = ? FOR UPDATE`, importID).Scan(&status); err != nil {
		return err
	}
	if status != AddressImportStatusQuarantined && status != AddressImportStatusQueued && status != AddressImportStatusImporting {
		return errAddressImportNotWritable
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE address_imports SET status = 'importing', error_code = NULL, error_detail = NULL WHERE id = ?`,
		importID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Server) failAddressImport(ctx context.Context, importID, code, detail string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE address_imports SET status = 'failed', error_code = NULLIF(LEFT(?, 64), ''),
			error_detail = NULLIF(LEFT(?, 4096), '')
		WHERE id = ? AND status IN ('quarantined','queued','importing','failed')
	`, strings.TrimSpace(code), strings.TrimSpace(detail), importID)
	return err
}

func (s *Server) completeAddressImport(ctx context.Context, importID string, metadata AddressImportMetadata, language string) (AddressImport, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressImport{}, err
	}
	defer tx.Rollback()
	var status string
	if err := tx.QueryRowContext(ctx,
		`SELECT status FROM address_imports WHERE id = ? FOR UPDATE`, importID).Scan(&status); err != nil {
		return AddressImport{}, err
	}
	if status == AddressImportStatusReady {
		_ = tx.Rollback()
		return s.getAddressImport(ctx, importID)
	}
	if status != AddressImportStatusImporting {
		return AddressImport{}, errAddressImportNotWritable
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
	return s.getAddressImport(ctx, importID)
}

// --- base prefixes (batched insert + range lookup) -------------------------

type addressImportBatchRow struct {
	record AddressImportRecord
	family uint8
	bits   uint8
	start  [16]byte
	end    [16]byte
	labels []byte
}

func prepareAddressImportBatch(records []AddressImportRecord) ([]addressImportBatchRow, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if len(records) > maxAddressImportBatch {
		return nil, fmt.Errorf("%w: batch has %d records, limit is %d", errAddressImportInvalid, len(records), maxAddressImportBatch)
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

func addressPrefixBounds(prefix netip.Prefix) ([16]byte, [16]byte) {
	prefix = prefix.Masked()
	startNumber := addressNumberFromAddr(prefix.Addr())
	width := 128
	if prefix.Addr().Is4() {
		width = 32
	}
	endNumber := addressBlockEnd(startNumber, width-prefix.Bits())
	return addressNumberBytes(startNumber), addressNumberBytes(endNumber)
}

func addressNumberBytes(number addressNumber) [16]byte {
	address := addressFromNumber(number, 6)
	return address.As16()
}

// insertAddressImportBatch upserts a batch of decoded prefixes while the import
// is importing. The multi-row INSERT ... ON DUPLICATE KEY UPDATE (chunked to
// maxAddressImportStatementRows) is the throughput-critical path, kept verbatim
// from the SaaS store with tenant scoping removed.
func (s *Server) insertAddressImportBatch(ctx context.Context, importID string, records []AddressImportRecord) error {
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
	if err := tx.QueryRowContext(ctx,
		`SELECT status FROM address_imports WHERE id = ? FOR UPDATE`, importID).Scan(&status); err != nil {
		return err
	}
	if status != AddressImportStatusImporting {
		return errAddressImportNotWritable
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
	if len(labels) > 0 {
		if err := json.Unmarshal(labels, &item.Labels); err != nil {
			return AddressBasePrefix{}, fmt.Errorf("decode address base labels: %w", err)
		}
	}
	return item, nil
}

func (s *Server) listAddressBasePrefixes(ctx context.Context, importID string, filter addressBasePrefixFilter) ([]AddressBasePrefix, int, error) {
	if filter.Family != 0 && filter.Family != 4 && filter.Family != 6 {
		return nil, 0, fmt.Errorf("%w: family must be 4 or 6", errAddressImportInvalid)
	}
	where := ` WHERE import_id = ?`
	args := []any{importID}
	if filter.Family != 0 {
		where += ` AND family = ?`
		args = append(args, filter.Family)
	}
	if code := strings.TrimSpace(filter.CountryCode); code != "" {
		where += ` AND country_code = ?`
		args = append(args, strings.ToUpper(code))
	}
	if filter.ASN != nil {
		where += ` AND asn = ?`
		args = append(args, *filter.ASN)
	}
	if op := strings.TrimSpace(filter.Operator); op != "" {
		where += ` AND operator_name = ?`
		args = append(args, op)
	}
	if search := strings.TrimSpace(filter.Search); search != "" {
		like := "%" + escapeSQLLike(search) + "%"
		where += ` AND (cidr LIKE ? OR COALESCE(country_name, '') LIKE ? OR COALESCE(subdivision_name, '') LIKE ? OR COALESCE(city_name, '') LIKE ? OR COALESCE(operator_name, '') LIKE ? OR CAST(asn AS CHAR) LIKE ?)`
		args = append(args, like, like, like, like, like, like)
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM address_base_prefixes`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	query := `SELECT ` + addressBasePrefixColumns + ` FROM address_base_prefixes` + where +
		` ORDER BY family, ip_start, prefix_length, id LIMIT ? OFFSET ?`
	args = append(args, filter.Limit, filter.Offset)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]AddressBasePrefix, 0, filter.Limit)
	for rows.Next() {
		item, err := scanAddressBasePrefix(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

// lookupAddressBasePrefixes returns the prefixes in an import that contain the
// given address, longest-match first, via the BINARY(16) range index.
func (s *Server) lookupAddressBasePrefixes(ctx context.Context, importID, value string, limit int) ([]AddressBasePrefix, error) {
	address, err := netip.ParseAddr(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid lookup IP", errAddressImportInvalid)
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

// --- slot activation --------------------------------------------------------

func scanAddressImportSlot(row rowScanner) (AddressImportSlot, error) {
	var slot AddressImportSlot
	err := row.Scan(&slot.SourceSlot, &slot.ImportID, &slot.RowVersion, &slot.ActivatedBy, &slot.ActivatedAt)
	return slot, err
}

func (s *Server) getAddressImportSlot(ctx context.Context, sourceSlot string) (AddressImportSlot, error) {
	return scanAddressImportSlot(s.db.QueryRowContext(ctx, `
		SELECT source_slot, import_id, row_version, COALESCE(activated_by, ''), activated_at
		FROM address_import_slots WHERE source_slot = ?
	`, sourceSlot))
}

// activateAddressImport points a slot at a ready generation under optimistic
// concurrency: expectedVersion 0 requires the slot to be empty, otherwise it
// must match the stored row_version.
func (s *Server) activateAddressImport(ctx context.Context, importID, actorID string, expectedVersion uint64) (AddressImportSlot, error) {
	if importID == "" {
		return AddressImportSlot{}, fmt.Errorf("%w: import is required", errAddressImportInvalid)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressImportSlot{}, err
	}
	defer tx.Rollback()
	var sourceSlot, status string
	if err := tx.QueryRowContext(ctx,
		`SELECT source_slot, status FROM address_imports WHERE id = ? FOR UPDATE`, importID).Scan(&sourceSlot, &status); err != nil {
		return AddressImportSlot{}, err
	}
	if status != AddressImportStatusReady {
		return AddressImportSlot{}, errAddressImportNotWritable
	}
	var actor any
	if actorID != "" {
		actor = actorID
	}
	var currentVersion uint64
	err = tx.QueryRowContext(ctx,
		`SELECT row_version FROM address_import_slots WHERE source_slot = ? FOR UPDATE`, sourceSlot).Scan(&currentVersion)
	switch {
	case errors.Is(err, sql.ErrNoRows) && expectedVersion != 0:
		return AddressImportSlot{}, errAddressImportVersionConflict
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `
			INSERT INTO address_import_slots (source_slot, import_id, row_version, activated_by)
			VALUES (?, ?, 1, ?)
		`, sourceSlot, importID, actor)
	case err != nil:
		return AddressImportSlot{}, err
	case currentVersion != expectedVersion:
		return AddressImportSlot{}, errAddressImportVersionConflict
	default:
		_, err = tx.ExecContext(ctx, `
			UPDATE address_import_slots SET import_id = ?, row_version = row_version + 1,
				activated_by = ?, activated_at = CURRENT_TIMESTAMP(3)
			WHERE source_slot = ? AND row_version = ?
		`, importID, actor, sourceSlot, expectedVersion)
	}
	if err != nil {
		return AddressImportSlot{}, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE address_imports SET activated_at = CURRENT_TIMESTAMP(3) WHERE id = ?`, importID); err != nil {
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
