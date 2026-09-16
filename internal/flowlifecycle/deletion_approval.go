package flowlifecycle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var ErrInvalidDeletionApproval = errors.New("invalid Flow deletion approval")

type DeletionApproval struct {
	ID                   string           `json:"id"`
	StorageKind          string           `json:"storage_kind"`
	PartitionGranularity string           `json:"partition_granularity"`
	PartitionStart       time.Time        `json:"partition_start"`
	PartitionEnd         time.Time        `json:"partition_end"`
	PolicyID             string           `json:"policy_id"`
	PolicyVersion        uint64           `json:"policy_version"`
	Generation           uint64           `json:"generation"`
	BackupEvidenceID     string           `json:"backup_evidence_id,omitempty"`
	KafkaCoverage        []OffsetCoverage `json:"kafka_coverage"`
	Source               Counters         `json:"source"`
	PhysicalRecords      uint64           `json:"physical_record_count"`
	Archive              Counters         `json:"archive"`
	Status               string           `json:"status"`
	ApprovedBy           string           `json:"approved_by"`
	ApprovedAt           time.Time        `json:"approved_at"`
	RevokedBy            string           `json:"revoked_by,omitempty"`
	RevokedAt            time.Time        `json:"revoked_at,omitzero"`
	RowVersion           uint64           `json:"row_version"`
	CreatedAt            time.Time        `json:"created_at"`
}

type DeletionApprovalFilter struct {
	StorageKind string
	Status      string
	Search      string
	Sort        string
	Order       string
	Limit       int
	Offset      int
}

const deletionApprovalColumns = `id,storage_kind,partition_granularity,partition_start,partition_end,
	policy_id,policy_version,generation,COALESCE(backup_evidence_id,''),kafka_coverage_json,
	source_record_count,source_physical_record_count,source_raw_bytes,source_raw_packets,source_estimated_bytes,source_estimated_packets,source_estimated_valid_records,
	archive_record_count,archive_raw_bytes,archive_raw_packets,archive_estimated_bytes,archive_estimated_packets,archive_estimated_valid_records,
	status,approved_by,approved_at,COALESCE(revoked_by,''),revoked_at,row_version,created_at`

func scanDeletionApproval(row rowScanner) (DeletionApproval, error) {
	var approval DeletionApproval
	var coverage []byte
	var revokedAt sql.NullTime
	err := row.Scan(&approval.ID, &approval.StorageKind, &approval.PartitionGranularity, &approval.PartitionStart, &approval.PartitionEnd,
		&approval.PolicyID, &approval.PolicyVersion, &approval.Generation, &approval.BackupEvidenceID, &coverage,
		&approval.Source.RecordCount, &approval.PhysicalRecords, &approval.Source.RawBytes, &approval.Source.RawPackets, &approval.Source.EstimatedBytes, &approval.Source.EstimatedPackets, &approval.Source.EstimatedValidRecords,
		&approval.Archive.RecordCount, &approval.Archive.RawBytes, &approval.Archive.RawPackets, &approval.Archive.EstimatedBytes, &approval.Archive.EstimatedPackets, &approval.Archive.EstimatedValidRecords,
		&approval.Status, &approval.ApprovedBy, &approval.ApprovedAt, &approval.RevokedBy, &revokedAt, &approval.RowVersion, &approval.CreatedAt)
	if err == nil {
		err = json.Unmarshal(coverage, &approval.KafkaCoverage)
	}
	if revokedAt.Valid {
		approval.RevokedAt = revokedAt.Time
	}
	return approval, err
}

func (store *Store) GetDeletionApproval(ctx context.Context, id string) (DeletionApproval, error) {
	if store == nil || store.db == nil || strings.TrimSpace(id) == "" {
		return DeletionApproval{}, ErrInvalidDeletionApproval
	}
	return scanDeletionApproval(store.db.QueryRowContext(ctx, "SELECT "+deletionApprovalColumns+" FROM flow_deletion_approvals WHERE id=?", id))
}

func (store *Store) ListDeletionApprovals(ctx context.Context, filter DeletionApprovalFilter) ([]DeletionApproval, uint64, error) {
	if store == nil || store.db == nil {
		return nil, 0, ErrInvalidDeletionApproval
	}
	if filter.Limit <= 0 {
		filter.Limit = 25
	}
	if filter.Limit > 200 || filter.Offset < 0 ||
		(filter.StorageKind != "" && filter.StorageKind != "raw" && filter.StorageKind != "archive") ||
		(filter.Status != "" && filter.Status != "approved" && filter.Status != "revoked") {
		return nil, 0, ErrInvalidDeletionApproval
	}
	sortColumns := map[string]string{"approved_at": "approved_at", "partition_start": "partition_start", "status": "status"}
	sortColumn := sortColumns[filter.Sort]
	if sortColumn == "" {
		sortColumn = "approved_at"
	}
	order := "DESC"
	if strings.EqualFold(filter.Order, "asc") {
		order = "ASC"
	} else if filter.Order != "" && !strings.EqualFold(filter.Order, "desc") {
		return nil, 0, ErrInvalidDeletionApproval
	}
	where := []string{"1=1"}
	args := make([]any, 0, 5)
	if filter.StorageKind != "" {
		where, args = append(where, "storage_kind=?"), append(args, filter.StorageKind)
	}
	if filter.Status != "" {
		where, args = append(where, "status=?"), append(args, filter.Status)
	}
	if filter.Search = strings.TrimSpace(filter.Search); filter.Search != "" {
		where = append(where, "(id LIKE ? OR policy_id LIKE ?)")
		like := "%" + filter.Search + "%"
		args = append(args, like, like)
	}
	clause := strings.Join(where, " AND ")
	var total uint64
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM flow_deletion_approvals WHERE "+clause, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	queryArgs := append(append([]any(nil), args...), filter.Limit, filter.Offset)
	rows, err := store.db.QueryContext(ctx, "SELECT "+deletionApprovalColumns+" FROM flow_deletion_approvals WHERE "+clause+" ORDER BY "+sortColumn+" "+order+",id "+order+" LIMIT ? OFFSET ?", queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]DeletionApproval, 0)
	for rows.Next() {
		item, err := scanDeletionApproval(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (store *Store) ApproveRawDayDelete(ctx context.Context, readiness RawDeleteReadiness, expected uint64, actor string, now time.Time) (DeletionApproval, PartitionState, error) {
	if store == nil || store.db == nil {
		return DeletionApproval{}, PartitionState{}, ErrInvalidDeletionApproval
	}
	day, err := validateRawDayApprovalRequest(readiness, expected, actor, now)
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	coverageJSON, err := json.Marshal(readiness.Coverage)
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	now = now.UTC().Truncate(time.Millisecond)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	defer tx.Rollback()
	state, err := scanPartition(tx.QueryRowContext(ctx, "SELECT "+partitionColumns+" FROM flow_retention_partition_states WHERE source_date=? FOR UPDATE", day))
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	if state.RowVersion != expected {
		return DeletionApproval{}, PartitionState{}, ErrVersionConflict
	}
	if state.State != PartitionReconciled || state.DeleteApprovalID != "" || state.DeleteJobID != "" ||
		state.PolicyID != readiness.PolicyID || state.PolicyVersion != readiness.PolicyVersion || state.Generation != readiness.Generation ||
		state.Source != readiness.Source || state.Archive != readiness.Archive || !state.DeleteEligibleAt.Equal(readiness.DeleteEligibleAt) {
		return DeletionApproval{}, PartitionState{}, ErrTransition
	}
	policy, err := scanPolicy(tx.QueryRowContext(ctx, "SELECT "+policyColumns+" FROM flow_retention_policy_revisions WHERE id=? FOR UPDATE", readiness.PolicyID))
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	if policy.Status != PolicyPublished || policy.Version != readiness.PolicyVersion {
		return DeletionApproval{}, PartitionState{}, ErrTransition
	}
	if policy.RequireBackupBeforeDelete {
		backup, err := scanBackupEvidence(tx.QueryRowContext(ctx, "SELECT "+backupEvidenceColumns+" FROM flow_backup_restore_evidence WHERE id=? FOR UPDATE", readiness.BackupEvidenceID))
		if err != nil {
			return DeletionApproval{}, PartitionState{}, err
		}
		if !backup.covers("raw", day, day.Add(24*time.Hour)) {
			return DeletionApproval{}, PartitionState{}, ErrDeleteLocked
		}
	}
	approval := DeletionApproval{
		ID: newID(), StorageKind: "raw", PartitionGranularity: "day", PartitionStart: day, PartitionEnd: day.Add(24 * time.Hour),
		PolicyID: policy.ID, PolicyVersion: policy.Version, Generation: readiness.Generation, BackupEvidenceID: readiness.BackupEvidenceID,
		KafkaCoverage: append([]OffsetCoverage(nil), readiness.Coverage...), Source: readiness.Source, Archive: readiness.Archive,
		Status: "approved", ApprovedBy: actor, ApprovedAt: now,
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO flow_deletion_approvals
		(id,storage_kind,partition_granularity,partition_start,partition_end,policy_id,policy_version,generation,backup_evidence_id,kafka_coverage_json,
		 source_record_count,source_physical_record_count,source_raw_bytes,source_raw_packets,source_estimated_bytes,source_estimated_packets,source_estimated_valid_records,
		 archive_record_count,archive_raw_bytes,archive_raw_packets,archive_estimated_bytes,archive_estimated_packets,archive_estimated_valid_records,
		 status,approved_by,approved_at)
		VALUES (
		 ?,'raw','day',
		 ?,?,?,?,?,
		 NULLIF(?,''),?,
		 ?,?,?,?,?,?,?,
		 ?,?,?,?,?,?,
		 'approved',?,?)`,
		approval.ID, approval.PartitionStart, approval.PartitionEnd, approval.PolicyID, approval.PolicyVersion, approval.Generation,
		approval.BackupEvidenceID, coverageJSON,
		approval.Source.RecordCount, readiness.PhysicalRecords, approval.Source.RawBytes, approval.Source.RawPackets, approval.Source.EstimatedBytes, approval.Source.EstimatedPackets, approval.Source.EstimatedValidRecords,
		approval.Archive.RecordCount, approval.Archive.RawBytes, approval.Archive.RawPackets, approval.Archive.EstimatedBytes, approval.Archive.EstimatedPackets, approval.Archive.EstimatedValidRecords,
		actor, now)
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE flow_retention_partition_states SET state='delete_eligible',delete_approval_id=?,row_version=row_version+1
		WHERE source_date=? AND state='reconciled' AND row_version=?`, approval.ID, day, expected)
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return DeletionApproval{}, PartitionState{}, err
		}
		return DeletionApproval{}, PartitionState{}, ErrVersionConflict
	}
	if err := tx.Commit(); err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	approval, err = store.GetDeletionApproval(ctx, approval.ID)
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	state, err = store.GetPartition(ctx, day)
	return approval, state, err
}

func validateRawDayApprovalRequest(readiness RawDeleteReadiness, expected uint64, actor string, now time.Time) (time.Time, error) {
	day := UTCDate(readiness.SourceDate)
	if !readiness.EvidenceReady || readiness.PartitionState != PartitionReconciled ||
		readiness.PartitionVersion != expected || expected == 0 || day.IsZero() || !readiness.SourceDate.Equal(day) ||
		strings.TrimSpace(readiness.PolicyID) == "" || readiness.PolicyVersion == 0 || readiness.Generation == 0 ||
		strings.TrimSpace(actor) == "" || now.IsZero() || readiness.CheckedAt.IsZero() || readiness.CheckedAt.After(now) ||
		readiness.DeleteEligibleAt.IsZero() || readiness.Source != readiness.Archive ||
		(readiness.Source.RecordCount > 0 && len(readiness.Coverage) == 0) {
		return time.Time{}, ErrInvalidDeletionApproval
	}
	type coverageKey struct {
		stream    string
		partition uint32
	}
	seen := make(map[coverageKey]struct{}, len(readiness.Coverage))
	for _, coverage := range readiness.Coverage {
		if err := coverage.validate(); err != nil {
			return time.Time{}, ErrInvalidDeletionApproval
		}
		key := coverageKey{stream: coverage.SourceStreamID, partition: coverage.KafkaPartition}
		if _, exists := seen[key]; exists {
			return time.Time{}, ErrInvalidDeletionApproval
		}
		seen[key] = struct{}{}
	}
	return day, nil
}

func (store *Store) RevokeRawDayDeleteApproval(ctx context.Context, id string, expected uint64, actor string, now time.Time) (DeletionApproval, PartitionState, error) {
	if store == nil || store.db == nil || strings.TrimSpace(id) == "" || expected == 0 || strings.TrimSpace(actor) == "" || now.IsZero() {
		return DeletionApproval{}, PartitionState{}, ErrInvalidDeletionApproval
	}
	current, err := store.GetDeletionApproval(ctx, id)
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	defer tx.Rollback()
	state, err := scanPartition(tx.QueryRowContext(ctx, "SELECT "+partitionColumns+" FROM flow_retention_partition_states WHERE source_date=? FOR UPDATE", current.PartitionStart))
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	approval, err := scanDeletionApproval(tx.QueryRowContext(ctx, "SELECT "+deletionApprovalColumns+" FROM flow_deletion_approvals WHERE id=? FOR UPDATE", id))
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	if approval.RowVersion != expected {
		return DeletionApproval{}, PartitionState{}, ErrVersionConflict
	}
	if approval.Status != "approved" || approval.StorageKind != "raw" || state.State != PartitionDeleteEligible ||
		state.DeleteApprovalID != approval.ID || state.DeleteJobID != "" || !state.SourceDate.Equal(approval.PartitionStart) {
		return DeletionApproval{}, PartitionState{}, ErrTransition
	}
	now = now.UTC().Truncate(time.Millisecond)
	result, err := tx.ExecContext(ctx, `UPDATE flow_deletion_approvals SET status='revoked',revoked_by=?,revoked_at=?,row_version=row_version+1
		WHERE id=? AND status='approved' AND row_version=?`, actor, now, id, expected)
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return DeletionApproval{}, PartitionState{}, err
		}
		return DeletionApproval{}, PartitionState{}, ErrVersionConflict
	}
	result, err = tx.ExecContext(ctx, `UPDATE flow_retention_partition_states SET state='reconciled',delete_approval_id=NULL,row_version=row_version+1
		WHERE source_date=? AND state='delete_eligible' AND delete_approval_id=?`, approval.PartitionStart, approval.ID)
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return DeletionApproval{}, PartitionState{}, err
		}
		return DeletionApproval{}, PartitionState{}, ErrTransition
	}
	if err := tx.Commit(); err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	approval, err = store.GetDeletionApproval(ctx, id)
	if err != nil {
		return DeletionApproval{}, PartitionState{}, err
	}
	state, err = store.GetPartition(ctx, approval.PartitionStart)
	return approval, state, err
}

func (store *Store) ApproveArchiveMonthDelete(ctx context.Context, readiness ArchiveMonthDeleteReadiness, expectedPolicyVersion uint64, actor string, now time.Time) (DeletionApproval, error) {
	if store == nil || store.db == nil || !readiness.EvidenceReady || readiness.PolicyRowVersion != expectedPolicyVersion ||
		expectedPolicyVersion == 0 || strings.TrimSpace(actor) == "" || now.IsZero() || readiness.CheckedAt.IsZero() ||
		readiness.CheckedAt.After(now) || readiness.Source != readiness.Archive || readiness.PhysicalRecords == 0 || readiness.DeleteApprovalID != "" {
		return DeletionApproval{}, ErrInvalidDeletionApproval
	}
	month, end, err := archiveMonth(readiness.MonthStart)
	if err != nil || !end.Equal(readiness.MonthEnd) {
		return DeletionApproval{}, ErrInvalidDeletionApproval
	}
	now = now.UTC().Truncate(time.Millisecond)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return DeletionApproval{}, err
	}
	defer tx.Rollback()
	policy, err := scanPolicy(tx.QueryRowContext(ctx, "SELECT "+policyColumns+" FROM flow_retention_policy_revisions WHERE status='published' FOR UPDATE"))
	if err != nil {
		return DeletionApproval{}, err
	}
	if policy.ID != readiness.PolicyID || policy.Version != readiness.PolicyVersion || policy.RowVersion != expectedPolicyVersion ||
		policy.ArchiveRetentionSeconds == 0 {
		return DeletionApproval{}, ErrVersionConflict
	}
	expectedGeneration, err := Generation(policy.Version, 1)
	if err != nil || expectedGeneration != readiness.Generation {
		return DeletionApproval{}, ErrTransition
	}
	expectedCounters, deletedDays, complete, err := store.archiveMonthStateEvidence(ctx, tx, month, end, true)
	if err != nil {
		return DeletionApproval{}, err
	}
	if !complete || deletedDays != readiness.ExpectedDays || expectedCounters != readiness.Source {
		return DeletionApproval{}, ErrTransition
	}
	if policy.RequireBackupBeforeDelete {
		backup, err := scanBackupEvidence(tx.QueryRowContext(ctx, "SELECT "+backupEvidenceColumns+" FROM flow_backup_restore_evidence WHERE id=? FOR UPDATE", readiness.BackupEvidenceID))
		if err != nil {
			return DeletionApproval{}, err
		}
		if !backup.covers("archive", month, end) {
			return DeletionApproval{}, ErrDeleteLocked
		}
	}
	var existingID string
	err = tx.QueryRowContext(ctx, `SELECT id FROM flow_deletion_approvals
		WHERE storage_kind='archive' AND partition_start=? AND status='approved' FOR UPDATE`, month).Scan(&existingID)
	if err == nil {
		return DeletionApproval{}, ErrTransition
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return DeletionApproval{}, err
	}
	approval := DeletionApproval{
		ID: newID(), StorageKind: "archive", PartitionGranularity: "month", PartitionStart: month, PartitionEnd: end,
		PolicyID: policy.ID, PolicyVersion: policy.Version, Generation: readiness.Generation, BackupEvidenceID: readiness.BackupEvidenceID,
		KafkaCoverage: []OffsetCoverage{}, Source: readiness.Source, PhysicalRecords: readiness.PhysicalRecords, Archive: readiness.Archive,
		Status: "approved", ApprovedBy: actor, ApprovedAt: now,
	}
	coverageJSON, err := json.Marshal(approval.KafkaCoverage)
	if err != nil {
		return DeletionApproval{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO flow_deletion_approvals
		(id,storage_kind,partition_granularity,partition_start,partition_end,policy_id,policy_version,generation,backup_evidence_id,kafka_coverage_json,
		 source_record_count,source_physical_record_count,source_raw_bytes,source_raw_packets,source_estimated_bytes,source_estimated_packets,source_estimated_valid_records,
		 archive_record_count,archive_raw_bytes,archive_raw_packets,archive_estimated_bytes,archive_estimated_packets,archive_estimated_valid_records,
		 status,approved_by,approved_at)
		VALUES (
		 ?,'archive','month',
		 ?,?,?,?,?,
		 NULLIF(?,''),?,
		 ?,?,?,?,?,?,?,
		 ?,?,?,?,?,?,
		 'approved',?,?)`,
		approval.ID, month, end, policy.ID, policy.Version, approval.Generation, approval.BackupEvidenceID, coverageJSON,
		approval.Source.RecordCount, approval.PhysicalRecords, approval.Source.RawBytes, approval.Source.RawPackets,
		approval.Source.EstimatedBytes, approval.Source.EstimatedPackets, approval.Source.EstimatedValidRecords,
		approval.Archive.RecordCount, approval.Archive.RawBytes, approval.Archive.RawPackets,
		approval.Archive.EstimatedBytes, approval.Archive.EstimatedPackets, approval.Archive.EstimatedValidRecords,
		actor, now)
	if err != nil {
		return DeletionApproval{}, err
	}
	if err := tx.Commit(); err != nil {
		return DeletionApproval{}, err
	}
	return store.GetDeletionApproval(ctx, approval.ID)
}

func (store *Store) RevokeArchiveMonthDeleteApproval(ctx context.Context, id string, expected uint64, actor string, now time.Time) (DeletionApproval, error) {
	if store == nil || store.db == nil || strings.TrimSpace(id) == "" || expected == 0 || strings.TrimSpace(actor) == "" || now.IsZero() {
		return DeletionApproval{}, ErrInvalidDeletionApproval
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return DeletionApproval{}, err
	}
	defer tx.Rollback()
	approval, err := scanDeletionApproval(tx.QueryRowContext(ctx, "SELECT "+deletionApprovalColumns+" FROM flow_deletion_approvals WHERE id=? FOR UPDATE", id))
	if err != nil {
		return DeletionApproval{}, err
	}
	if approval.RowVersion != expected {
		return DeletionApproval{}, ErrVersionConflict
	}
	if approval.Status != "approved" || approval.StorageKind != "archive" || approval.PartitionGranularity != "month" {
		return DeletionApproval{}, ErrTransition
	}
	var receiptStatus string
	err = tx.QueryRowContext(ctx, "SELECT status FROM flow_deletion_receipts WHERE deletion_approval_id=? FOR UPDATE", id).Scan(&receiptStatus)
	if err == nil && receiptStatus != "failed" {
		return DeletionApproval{}, ErrTransition
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return DeletionApproval{}, err
	}
	now = now.UTC().Truncate(time.Millisecond)
	result, err := tx.ExecContext(ctx, `UPDATE flow_deletion_approvals SET status='revoked',revoked_by=?,revoked_at=?,row_version=row_version+1
		WHERE id=? AND status='approved' AND row_version=?`, actor, now, id, expected)
	if err != nil {
		return DeletionApproval{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return DeletionApproval{}, err
		}
		return DeletionApproval{}, ErrVersionConflict
	}
	if err := tx.Commit(); err != nil {
		return DeletionApproval{}, err
	}
	return store.GetDeletionApproval(ctx, id)
}
