// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

const (
	DeleteBlockerInvalidMonth          = "invalid_archive_month"
	DeleteBlockerMonthIncomplete       = "archive_month_incomplete"
	DeleteBlockerArchiveAbsent         = "archive_partition_absent"
	DeleteBlockerArchiveDeleteDisabled = "archive_delete_disabled"
	DeleteBlockerApprovalMismatch      = "approval_evidence_mismatch"
)

type ArchiveMonthEvidenceReader interface {
	ArchiveMonthStorageCounters(context.Context, time.Time) (flowch.StorageCounters, error)
	ArchiveMonthPhysicalRecords(context.Context, time.Time) (uint64, error)
}

// ArchiveMonthDeleteReadiness freezes no state. It explains whether every UTC
// day in a calendar month has completed raw deletion, whether the persisted
// daily archive totals still equal ClickHouse, and whether restore-tested
// backup evidence covers the full month.
type ArchiveMonthDeleteReadiness struct {
	MonthStart       time.Time `json:"month_start"`
	MonthEnd         time.Time `json:"month_end"`
	CheckedAt        time.Time `json:"checked_at"`
	DeleteEligibleAt time.Time `json:"delete_eligible_at,omitzero"`
	PolicyID         string    `json:"policy_id,omitempty"`
	PolicyVersion    uint64    `json:"policy_version,omitempty"`
	PolicyRowVersion uint64    `json:"policy_row_version,omitempty"`
	Generation       uint64    `json:"generation,omitempty"`
	ExpectedDays     uint32    `json:"expected_days"`
	RawDeletedDays   uint32    `json:"raw_deleted_days"`
	DeleteApprovalID string    `json:"delete_approval_id,omitempty"`
	EvidenceReady    bool      `json:"evidence_ready"`
	DeletionReady    bool      `json:"deletion_ready"`
	Blockers         []string  `json:"blockers"`
	Source           Counters  `json:"source"`
	Archive          Counters  `json:"archive"`
	PhysicalRecords  uint64    `json:"physical_record_count"`
	BackupEvidenceID string    `json:"backup_evidence_id,omitempty"`
}

func ArchiveDeleteEligibleAt(monthStart time.Time, policy Policy) (time.Time, error) {
	month, end, err := archiveMonth(monthStart)
	if err != nil {
		return time.Time{}, err
	}
	policy, err = NormalizePolicy(policy)
	if err != nil || policy.ArchiveRetentionSeconds == 0 || policy.BootstrapFrom.After(month) {
		return time.Time{}, ErrInvalidPolicy
	}
	return end.Add(time.Duration(policy.ArchiveRetentionSeconds+uint64(policy.DeleteGraceSeconds)) * time.Second), nil
}

func (store *Store) ArchiveMonthDeleteReadiness(ctx context.Context, monthStart, now time.Time, reader ArchiveMonthEvidenceReader) (ArchiveMonthDeleteReadiness, error) {
	month, end, err := archiveMonth(monthStart)
	if err != nil || store == nil || store.db == nil || reader == nil || now.IsZero() {
		return ArchiveMonthDeleteReadiness{}, ErrInvalidPolicy
	}
	result := ArchiveMonthDeleteReadiness{
		MonthStart: month, MonthEnd: end, CheckedAt: now.UTC(), ExpectedDays: uint32(end.Sub(month) / (24 * time.Hour)),
		Blockers: make([]string, 0),
	}
	policy, err := store.GetPublishedPolicy(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		result.Blockers = append(result.Blockers, DeleteBlockerNoPublishedPolicy)
		return result, nil
	}
	if err != nil {
		return ArchiveMonthDeleteReadiness{}, err
	}
	result.PolicyID, result.PolicyVersion, result.PolicyRowVersion = policy.ID, policy.Version, policy.RowVersion
	result.Generation, err = Generation(policy.Version, 1)
	if err != nil {
		return ArchiveMonthDeleteReadiness{}, err
	}
	result.DeleteEligibleAt, err = ArchiveDeleteEligibleAt(month, policy)
	if err != nil {
		result.Blockers = append(result.Blockers, DeleteBlockerInvalidMonth)
	}

	expected, deletedDays, complete, err := store.archiveMonthStateEvidence(ctx, nil, month, end, false)
	if err != nil {
		return ArchiveMonthDeleteReadiness{}, err
	}
	result.Source, result.RawDeletedDays = expected, deletedDays
	archive, err := reader.ArchiveMonthStorageCounters(ctx, month)
	if err != nil {
		return ArchiveMonthDeleteReadiness{}, err
	}
	result.Archive = lifecycleCounters(archive)
	result.PhysicalRecords, err = reader.ArchiveMonthPhysicalRecords(ctx, month)
	if err != nil {
		return ArchiveMonthDeleteReadiness{}, err
	}

	evidenceBlockers := make([]string, 0)
	if !complete || result.ExpectedDays != result.RawDeletedDays || policy.BootstrapFrom.After(month) {
		evidenceBlockers = append(evidenceBlockers, DeleteBlockerMonthIncomplete)
	}
	if result.Source != result.Archive {
		evidenceBlockers = append(evidenceBlockers, DeleteBlockerCounterMismatch)
	}
	if result.PhysicalRecords == 0 {
		evidenceBlockers = append(evidenceBlockers, DeleteBlockerArchiveAbsent)
	}
	if result.DeleteEligibleAt.IsZero() || now.UTC().Before(result.DeleteEligibleAt) {
		evidenceBlockers = append(evidenceBlockers, DeleteBlockerLateWindowOpen)
	}
	var backup *BackupEvidence
	if policy.RequireBackupBeforeDelete {
		backup, err = store.coveringBackupForKind(ctx, "archive", month, end)
		if err != nil {
			return ArchiveMonthDeleteReadiness{}, err
		}
		if backup == nil {
			evidenceBlockers = append(evidenceBlockers, DeleteBlockerBackupMissing)
		} else {
			result.BackupEvidenceID = backup.ID
		}
	}
	result.EvidenceReady = len(evidenceBlockers) == 0
	result.Blockers = append(result.Blockers, evidenceBlockers...)

	approval, found, err := store.activeArchiveMonthApproval(ctx, month)
	if err != nil {
		return ArchiveMonthDeleteReadiness{}, err
	}
	approvalMatches := found && archiveReadinessMatchesApproval(result, approval)
	if found {
		result.DeleteApprovalID = approval.ID
	}
	if found && !approvalMatches {
		result.Blockers = append(result.Blockers, DeleteBlockerApprovalMismatch)
	}
	if !policy.ArchiveDeleteEnabled {
		result.Blockers = append(result.Blockers, DeleteBlockerArchiveDeleteDisabled)
	}
	if !approvalMatches {
		result.Blockers = append(result.Blockers, DeleteBlockerApprovalRequired)
	}
	result.DeletionReady = result.EvidenceReady && policy.ArchiveDeleteEnabled && approvalMatches
	return result, nil
}

func archiveMonth(value time.Time) (time.Time, time.Time, error) {
	month := value.UTC()
	if value.IsZero() || value.Location() != time.UTC || month.Day() != 1 || month.Hour() != 0 || month.Minute() != 0 || month.Second() != 0 || month.Nanosecond() != 0 {
		return time.Time{}, time.Time{}, ErrInvalidPolicy
	}
	return month, month.AddDate(0, 1, 0), nil
}

func (store *Store) archiveMonthStateEvidence(ctx context.Context, tx *sql.Tx, month, end time.Time, lock bool) (Counters, uint32, bool, error) {
	query := "SELECT " + partitionColumns + " FROM flow_retention_partition_states WHERE source_date>=? AND source_date<? ORDER BY source_date ASC"
	if lock {
		query += " FOR UPDATE"
	}
	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, query, month, end)
	} else {
		rows, err = store.db.QueryContext(ctx, query, month, end)
	}
	if err != nil {
		return Counters{}, 0, false, err
	}
	defer rows.Close()
	var total Counters
	expectedDate := month
	var deleted uint32
	complete := true
	for rows.Next() {
		state, err := scanPartition(rows)
		if err != nil {
			return Counters{}, 0, false, err
		}
		if !state.SourceDate.Equal(expectedDate) || state.State != PartitionRawDeleted || state.Source != state.Archive || state.RawDeletedAt.IsZero() {
			complete = false
		}
		if state.State == PartitionRawDeleted {
			deleted++
		}
		if err := addCounters(&total, state.Archive); err != nil {
			return Counters{}, 0, false, err
		}
		expectedDate = expectedDate.Add(24 * time.Hour)
	}
	if err := rows.Err(); err != nil {
		return Counters{}, 0, false, err
	}
	if !expectedDate.Equal(end) {
		complete = false
	}
	return total, deleted, complete, nil
}

func addCounters(total *Counters, value Counters) error {
	if total == nil {
		return ErrDeleteLocked
	}
	fields := [][2]*uint64{
		{&total.RecordCount, &value.RecordCount}, {&total.RawBytes, &value.RawBytes}, {&total.RawPackets, &value.RawPackets},
		{&total.EstimatedBytes, &value.EstimatedBytes}, {&total.EstimatedPackets, &value.EstimatedPackets},
		{&total.EstimatedValidRecords, &value.EstimatedValidRecords},
	}
	for _, field := range fields {
		if *field[0] > math.MaxUint64-*field[1] {
			return ErrDeleteLocked
		}
		*field[0] += *field[1]
	}
	return nil
}

func (store *Store) activeArchiveMonthApproval(ctx context.Context, month time.Time) (DeletionApproval, bool, error) {
	approval, err := scanDeletionApproval(store.db.QueryRowContext(ctx, "SELECT "+deletionApprovalColumns+" FROM flow_deletion_approvals WHERE storage_kind='archive' AND partition_start=? AND status='approved'", month))
	if errors.Is(err, sql.ErrNoRows) {
		return DeletionApproval{}, false, nil
	}
	return approval, err == nil, err
}

func archiveReadinessMatchesApproval(readiness ArchiveMonthDeleteReadiness, approval DeletionApproval) bool {
	return readiness.EvidenceReady && approval.Status == "approved" && approval.StorageKind == "archive" && approval.PartitionGranularity == "month" &&
		approval.PartitionStart.Equal(readiness.MonthStart) && approval.PartitionEnd.Equal(readiness.MonthEnd) &&
		approval.PolicyID == readiness.PolicyID && approval.PolicyVersion == readiness.PolicyVersion && approval.Generation == readiness.Generation &&
		approval.Source == readiness.Source && approval.Archive == readiness.Archive && approval.PhysicalRecords == readiness.PhysicalRecords &&
		approval.BackupEvidenceID == readiness.BackupEvidenceID && len(approval.KafkaCoverage) == 0
}

func (store *Store) coveringBackupForKind(ctx context.Context, kind string, start, end time.Time) (*BackupEvidence, error) {
	var evidence BackupEvidence
	err := store.db.QueryRowContext(ctx, `SELECT id,storage_kind,covered_from,covered_through,backup_ref,checksum_sha256,status,verified_at,restore_tested_at,restore_test_ref
		FROM flow_backup_restore_evidence
		WHERE status='verified' AND storage_kind IN (?, 'all') AND covered_from<=? AND covered_through>=?
		ORDER BY restore_tested_at DESC,id ASC LIMIT 1`, kind, start, end).
		Scan(&evidence.ID, &evidence.StorageKind, &evidence.CoveredFrom, &evidence.CoveredThrough, &evidence.BackupRef, &evidence.ChecksumSHA256,
			&evidence.Status, &evidence.VerifiedAt, &evidence.RestoreTestedAt, &evidence.RestoreTestRef)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !evidence.covers(kind, start, end) {
		return nil, nil
	}
	return &evidence, nil
}
