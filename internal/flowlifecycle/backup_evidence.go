package flowlifecycle

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

var ErrInvalidBackupEvidence = errors.New("invalid Flow backup restore evidence")

type BackupEvidenceFilter struct {
	StorageKind string
	Status      string
	Search      string
	Sort        string
	Order       string
	Limit       int
	Offset      int
}

const backupEvidenceColumns = `id,storage_kind,covered_from,covered_through,backup_ref,checksum_sha256,status,
	COALESCE(verified_by,''),verified_at,restore_tested_at,restore_test_ref,COALESCE(revoked_by,''),revoked_at,row_version,created_at`

func scanBackupEvidence(row rowScanner) (BackupEvidence, error) {
	var evidence BackupEvidence
	var revokedAt sql.NullTime
	err := row.Scan(&evidence.ID, &evidence.StorageKind, &evidence.CoveredFrom, &evidence.CoveredThrough,
		&evidence.BackupRef, &evidence.ChecksumSHA256, &evidence.Status, &evidence.VerifiedBy,
		&evidence.VerifiedAt, &evidence.RestoreTestedAt, &evidence.RestoreTestRef, &evidence.RevokedBy,
		&revokedAt, &evidence.RowVersion, &evidence.CreatedAt)
	if revokedAt.Valid {
		evidence.RevokedAt = revokedAt.Time
	}
	return evidence, err
}

func NormalizeBackupEvidence(evidence BackupEvidence, now time.Time) (BackupEvidence, error) {
	evidence.ID = strings.TrimSpace(evidence.ID)
	evidence.StorageKind = strings.TrimSpace(evidence.StorageKind)
	evidence.BackupRef = strings.TrimSpace(evidence.BackupRef)
	evidence.ChecksumSHA256 = strings.ToLower(strings.TrimSpace(evidence.ChecksumSHA256))
	evidence.RestoreTestRef = strings.TrimSpace(evidence.RestoreTestRef)
	evidence.CoveredFrom = UTCDate(evidence.CoveredFrom)
	evidence.CoveredThrough = UTCDate(evidence.CoveredThrough)
	evidence.RestoreTestedAt = evidence.RestoreTestedAt.UTC().Truncate(time.Millisecond)
	if evidence.ID == "" {
		evidence.ID = newID()
	}
	if len(evidence.ID) > 26 || (evidence.StorageKind != "raw" && evidence.StorageKind != "archive" && evidence.StorageKind != "all") ||
		evidence.CoveredFrom.IsZero() || !evidence.CoveredThrough.After(evidence.CoveredFrom) ||
		len(evidence.BackupRef) == 0 || len(evidence.BackupRef) > 512 || !validSHA256(evidence.ChecksumSHA256) ||
		evidence.RestoreTestedAt.IsZero() || (!now.IsZero() && evidence.RestoreTestedAt.After(now.UTC())) ||
		len(evidence.RestoreTestRef) == 0 || len(evidence.RestoreTestRef) > 512 {
		return BackupEvidence{}, ErrInvalidBackupEvidence
	}
	return evidence, nil
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func (store *Store) CreateBackupEvidence(ctx context.Context, evidence BackupEvidence, actor string, now time.Time) (BackupEvidence, error) {
	if store == nil || store.db == nil || strings.TrimSpace(actor) == "" || now.IsZero() {
		return BackupEvidence{}, ErrInvalidBackupEvidence
	}
	var err error
	evidence, err = NormalizeBackupEvidence(evidence, now)
	if err != nil {
		return BackupEvidence{}, err
	}
	now = now.UTC().Truncate(time.Millisecond)
	_, err = store.db.ExecContext(ctx, `INSERT INTO flow_backup_restore_evidence
		(id,storage_kind,covered_from,covered_through,backup_ref,checksum_sha256,status,verified_by,verified_at,
		 restore_tested_at,restore_test_ref,row_version)
		VALUES (?,?,?,?,?,?,'verified',?,?,?, ?,1)`, evidence.ID, evidence.StorageKind, evidence.CoveredFrom,
		evidence.CoveredThrough, evidence.BackupRef, evidence.ChecksumSHA256, actor, now,
		evidence.RestoreTestedAt, evidence.RestoreTestRef)
	if err != nil {
		return BackupEvidence{}, err
	}
	return store.GetBackupEvidence(ctx, evidence.ID)
}

func (store *Store) GetBackupEvidence(ctx context.Context, id string) (BackupEvidence, error) {
	if store == nil || store.db == nil || strings.TrimSpace(id) == "" {
		return BackupEvidence{}, ErrInvalidBackupEvidence
	}
	return scanBackupEvidence(store.db.QueryRowContext(ctx, "SELECT "+backupEvidenceColumns+" FROM flow_backup_restore_evidence WHERE id=?", id))
}

func (store *Store) ListBackupEvidence(ctx context.Context, filter BackupEvidenceFilter) ([]BackupEvidence, uint64, error) {
	if store == nil || store.db == nil {
		return nil, 0, ErrInvalidBackupEvidence
	}
	if filter.Limit <= 0 {
		filter.Limit = 25
	}
	if filter.Limit > 200 || filter.Offset < 0 ||
		(filter.StorageKind != "" && filter.StorageKind != "raw" && filter.StorageKind != "archive" && filter.StorageKind != "all") ||
		(filter.Status != "" && filter.Status != "verified" && filter.Status != "revoked") {
		return nil, 0, ErrInvalidBackupEvidence
	}
	sortColumns := map[string]string{"created_at": "created_at", "covered_from": "covered_from", "restore_tested_at": "restore_tested_at", "status": "status"}
	sortColumn := sortColumns[filter.Sort]
	if sortColumn == "" {
		sortColumn = "created_at"
	}
	order := "DESC"
	if strings.EqualFold(filter.Order, "asc") {
		order = "ASC"
	} else if filter.Order != "" && !strings.EqualFold(filter.Order, "desc") {
		return nil, 0, ErrInvalidBackupEvidence
	}
	where := []string{"1=1"}
	args := make([]any, 0, 7)
	if filter.StorageKind != "" {
		where = append(where, "storage_kind=?")
		args = append(args, filter.StorageKind)
	}
	if filter.Status != "" {
		where = append(where, "status=?")
		args = append(args, filter.Status)
	}
	if filter.Search = strings.TrimSpace(filter.Search); filter.Search != "" {
		where = append(where, "(backup_ref LIKE ? OR restore_test_ref LIKE ? OR checksum_sha256 LIKE ?)")
		like := "%" + filter.Search + "%"
		args = append(args, like, like, like)
	}
	clause := strings.Join(where, " AND ")
	var total uint64
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM flow_backup_restore_evidence WHERE "+clause, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	queryArgs := append(append([]any(nil), args...), filter.Limit, filter.Offset)
	rows, err := store.db.QueryContext(ctx, "SELECT "+backupEvidenceColumns+" FROM flow_backup_restore_evidence WHERE "+clause+" ORDER BY "+sortColumn+" "+order+",id "+order+" LIMIT ? OFFSET ?", queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]BackupEvidence, 0)
	for rows.Next() {
		item, err := scanBackupEvidence(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (store *Store) RevokeBackupEvidence(ctx context.Context, id, actor string, expected uint64, now time.Time) (BackupEvidence, error) {
	if store == nil || store.db == nil || strings.TrimSpace(id) == "" || strings.TrimSpace(actor) == "" || expected == 0 || now.IsZero() {
		return BackupEvidence{}, ErrInvalidBackupEvidence
	}
	result, err := store.db.ExecContext(ctx, `UPDATE flow_backup_restore_evidence SET status='revoked',revoked_by=?,revoked_at=?,row_version=row_version+1
		WHERE id=? AND status='verified' AND row_version=?`, actor, now.UTC().Truncate(time.Millisecond), id, expected)
	if err != nil {
		return BackupEvidence{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return BackupEvidence{}, err
	}
	if changed != 1 {
		current, getErr := store.GetBackupEvidence(ctx, id)
		if getErr != nil {
			return BackupEvidence{}, getErr
		}
		if current.RowVersion != expected {
			return BackupEvidence{}, ErrVersionConflict
		}
		return BackupEvidence{}, ErrTransition
	}
	return store.GetBackupEvidence(ctx, id)
}
