// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

const (
	DeleteBlockerNoPublishedPolicy  = "no_published_policy"
	DeleteBlockerNoPartitionState   = "no_partition_state"
	DeleteBlockerNotReconciled      = "partition_not_reconciled"
	DeleteBlockerInvalidGeneration  = "invalid_generation"
	DeleteBlockerCounterMismatch    = "counter_mismatch"
	DeleteBlockerLateWindowOpen     = "late_or_grace_window_open"
	DeleteBlockerCoverageMissing    = "kafka_coverage_missing"
	DeleteBlockerWatermarkMissing   = "watermark_missing"
	DeleteBlockerWatermarkIdentity  = "watermark_identity_mismatch"
	DeleteBlockerWatermarkUnhealthy = "watermark_unhealthy"
	DeleteBlockerWatermarkBehind    = "watermark_behind_partition"
	DeleteBlockerCoverageDuplicate  = "kafka_coverage_duplicate"
	DeleteBlockerBackupMissing      = "backup_restore_evidence_missing"
	DeleteBlockerRawDeleteDisabled  = "raw_delete_disabled"
	DeleteBlockerApprovalRequired   = "approval_required"
)

type RawDayEvidenceReader interface {
	DayStorageCounters(context.Context, time.Time) (flowch.StorageCounters, flowch.StorageCounters, error)
	DayOffsetCoverage(context.Context, time.Time) ([]flowch.DayOffsetCoverage, error)
}

// RawDeleteReadiness is a read-only, reproducible explanation of whether a
// raw UTC-day has all deletion evidence. EvidenceReady excludes the explicit
// feature switch and approval state; DeletionReady is true only when the full
// fail-closed RawDayDeleteGuard also passes. Reading this value never mutates a
// partition and never issues ClickHouse DDL.
type RawDeleteReadiness struct {
	SourceDate       time.Time        `json:"source_date"`
	CheckedAt        time.Time        `json:"checked_at"`
	PolicyID         string           `json:"policy_id,omitempty"`
	PolicyVersion    uint64           `json:"policy_version,omitempty"`
	PartitionState   string           `json:"partition_state,omitempty"`
	Generation       uint64           `json:"generation,omitempty"`
	DeleteEligibleAt time.Time        `json:"delete_eligible_at,omitzero"`
	EvidenceReady    bool             `json:"evidence_ready"`
	DeletionReady    bool             `json:"deletion_ready"`
	Blockers         []string         `json:"blockers"`
	Source           Counters         `json:"source"`
	Archive          Counters         `json:"archive"`
	Coverage         []OffsetCoverage `json:"kafka_coverage"`
	BackupEvidenceID string           `json:"backup_evidence_id,omitempty"`
}

func (store *Store) RawDayDeleteReadiness(ctx context.Context, sourceDate, now time.Time, reader RawDayEvidenceReader) (RawDeleteReadiness, error) {
	day := UTCDate(sourceDate)
	result := RawDeleteReadiness{SourceDate: day, CheckedAt: now.UTC(), Blockers: make([]string, 0), Coverage: make([]OffsetCoverage, 0)}
	if store == nil || store.db == nil || reader == nil || day.IsZero() || !sourceDate.Equal(day) || now.IsZero() {
		return RawDeleteReadiness{}, ErrInvalidPolicy
	}
	policy, err := store.GetPublishedPolicy(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		result.Blockers = append(result.Blockers, DeleteBlockerNoPublishedPolicy)
		return result, nil
	}
	if err != nil {
		return RawDeleteReadiness{}, err
	}
	result.PolicyID, result.PolicyVersion = policy.ID, policy.Version
	state, err := store.GetPartition(ctx, day)
	if errors.Is(err, sql.ErrNoRows) {
		result.Blockers = append(result.Blockers, DeleteBlockerNoPartitionState)
		return result, nil
	}
	if err != nil {
		return RawDeleteReadiness{}, err
	}
	result.PartitionState, result.Generation, result.DeleteEligibleAt = state.State, state.Generation, state.DeleteEligibleAt

	raw, archive, err := reader.DayStorageCounters(ctx, day)
	if err != nil {
		return RawDeleteReadiness{}, err
	}
	result.Source, result.Archive = lifecycleCounters(raw), lifecycleCounters(archive)
	spans, err := reader.DayOffsetCoverage(ctx, day)
	if err != nil {
		return RawDeleteReadiness{}, err
	}

	evidenceBlockers := make([]string, 0)
	if state.State != PartitionReconciled && state.State != PartitionDeleteEligible {
		evidenceBlockers = append(evidenceBlockers, DeleteBlockerNotReconciled)
	}
	version, _, generationErr := SplitGeneration(state.Generation)
	if generationErr != nil || uint64(version) != policy.Version || state.PolicyID != policy.ID || state.PolicyVersion != policy.Version {
		evidenceBlockers = append(evidenceBlockers, DeleteBlockerInvalidGeneration)
	}
	if result.Source != result.Archive || result.Source != state.Source || result.Archive != state.Archive {
		evidenceBlockers = append(evidenceBlockers, DeleteBlockerCounterMismatch)
	}
	earliest, eligibilityErr := RawDeleteEligibleAt(day, policy)
	if eligibilityErr != nil || state.ReconciledAt.IsZero() || state.DeleteEligibleAt.Before(earliest) || now.UTC().Before(state.DeleteEligibleAt.UTC()) {
		evidenceBlockers = append(evidenceBlockers, DeleteBlockerLateWindowOpen)
	}
	if result.Source.RecordCount > 0 && len(spans) == 0 {
		evidenceBlockers = append(evidenceBlockers, DeleteBlockerCoverageMissing)
	}
	seen := make(map[string]struct{}, len(spans))
	for _, span := range spans {
		key := fmt.Sprintf("%s\x00%d", span.SourceStreamID, span.KafkaPartition)
		if _, duplicate := seen[key]; duplicate {
			evidenceBlockers = appendUnique(evidenceBlockers, DeleteBlockerCoverageDuplicate)
			continue
		}
		seen[key] = struct{}{}
		coverage, status, mismatchCount, found, err := store.offsetCoverage(ctx, span)
		if err != nil {
			return RawDeleteReadiness{}, err
		}
		if !found {
			evidenceBlockers = appendUnique(evidenceBlockers, DeleteBlockerWatermarkMissing)
			continue
		}
		result.Coverage = append(result.Coverage, coverage)
		if coverage.KafkaTopic != span.KafkaTopic {
			evidenceBlockers = appendUnique(evidenceBlockers, DeleteBlockerWatermarkIdentity)
		}
		if status != "healthy" || mismatchCount != 0 || coverage.CommittedSnapshotAt.IsZero() || coverage.VerifiedAt.IsZero() {
			evidenceBlockers = appendUnique(evidenceBlockers, DeleteBlockerWatermarkUnhealthy)
		}
		if coverage.LastOffsetExclusive > coverage.CommittedNextOffset || coverage.LastOffsetExclusive > coverage.ReconciledNextOffset {
			evidenceBlockers = appendUnique(evidenceBlockers, DeleteBlockerWatermarkBehind)
		}
	}

	var backup *BackupEvidence
	if policy.RequireBackupBeforeDelete {
		backup, err = store.coveringBackup(ctx, day)
		if err != nil {
			return RawDeleteReadiness{}, err
		}
		if backup == nil {
			evidenceBlockers = append(evidenceBlockers, DeleteBlockerBackupMissing)
		} else {
			result.BackupEvidenceID = backup.ID
		}
	}
	result.EvidenceReady = len(evidenceBlockers) == 0
	result.Blockers = append(result.Blockers, evidenceBlockers...)
	if !policy.RawDeleteEnabled {
		result.Blockers = append(result.Blockers, DeleteBlockerRawDeleteDisabled)
	}
	if state.State != PartitionDeleteEligible {
		result.Blockers = append(result.Blockers, DeleteBlockerApprovalRequired)
	}
	guard := RawDayDeleteGuard{
		Now: now, SourceDate: day, Policy: policy, State: state.State, Generation: state.Generation,
		ReconciledAt: state.ReconciledAt, DeleteEligibleAt: state.DeleteEligibleAt,
		Source: result.Source, Archive: result.Archive, Coverage: result.Coverage, Backup: backup,
	}
	result.DeletionReady = result.EvidenceReady && ValidateRawDayDelete(guard) == nil
	return result, nil
}

func (store *Store) offsetCoverage(ctx context.Context, span flowch.DayOffsetCoverage) (OffsetCoverage, string, uint64, bool, error) {
	var coverage OffsetCoverage
	var topic, group, status string
	var mismatchCount uint64
	var committedAt, verifiedAt sql.NullTime
	err := store.db.QueryRowContext(ctx, `SELECT kafka_topic,consumer_group,bootstrap_offset,committed_next_offset,reconciled_next_offset,
		status,mismatch_count,committed_snapshot_at,last_verified_at
		FROM flow_reconciliation_watermarks WHERE source_stream_id=? AND kafka_partition=?`, span.SourceStreamID, span.KafkaPartition).
		Scan(&topic, &group, &coverage.BootstrapOffset, &coverage.CommittedNextOffset, &coverage.ReconciledNextOffset,
			&status, &mismatchCount, &committedAt, &verifiedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return OffsetCoverage{}, "", 0, false, nil
	}
	if err != nil {
		return OffsetCoverage{}, "", 0, false, err
	}
	if committedAt.Valid {
		coverage.CommittedSnapshotAt = committedAt.Time
	}
	if verifiedAt.Valid {
		coverage.VerifiedAt = verifiedAt.Time
	}
	coverage.SourceStreamID, coverage.KafkaTopic, coverage.ConsumerGroup = span.SourceStreamID, topic, group
	coverage.KafkaPartition, coverage.FirstOffset, coverage.LastOffsetExclusive = span.KafkaPartition, span.FirstOffset, span.LastOffsetExclusive
	return coverage, status, mismatchCount, true, nil
}

func (store *Store) coveringBackup(ctx context.Context, day time.Time) (*BackupEvidence, error) {
	var evidence BackupEvidence
	err := store.db.QueryRowContext(ctx, `SELECT id,storage_kind,covered_from,covered_through,status,verified_at,restore_tested_at
		FROM flow_backup_restore_evidence
		WHERE status='verified' AND storage_kind IN ('raw','all') AND covered_from<=? AND covered_through>=?
		ORDER BY restore_tested_at DESC,id ASC LIMIT 1`, day, day.Add(24*time.Hour)).
		Scan(&evidence.ID, &evidence.StorageKind, &evidence.CoveredFrom, &evidence.CoveredThrough,
			&evidence.Status, &evidence.VerifiedAt, &evidence.RestoreTestedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &evidence, nil
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
