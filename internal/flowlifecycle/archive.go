package flowlifecycle

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/opjob"
)

const (
	ArchiveJobType       = "flow_storage_downsample"
	archivePayloadSchema = 1
)

// PartitionState is the durable UTC-day archive state. Generation is split
// into policy-version and repair-attempt so every repair supersedes the prior
// ClickHouse generation without reading CH during enqueue.
type PartitionState struct {
	SourceDate       time.Time `json:"source_date"`
	PolicyID         string    `json:"policy_id"`
	PolicyVersion    uint64    `json:"policy_version"`
	State            string    `json:"state"`
	Generation       uint64    `json:"generation"`
	RepairAttempt    uint32    `json:"repair_attempt"`
	Source           Counters  `json:"source"`
	Archive          Counters  `json:"archive"`
	ArchiveJobID     string    `json:"archive_job_id,omitempty"`
	DeleteJobID      string    `json:"delete_job_id,omitempty"`
	DeleteApprovalID string    `json:"delete_approval_id,omitempty"`
	ArchivedAt       time.Time `json:"archived_at,omitzero"`
	ReconciledAt     time.Time `json:"reconciled_at,omitzero"`
	LateCheckedAt    time.Time `json:"late_checked_at,omitzero"`
	DeleteEligibleAt time.Time `json:"delete_eligible_at,omitzero"`
	RawDeletedAt     time.Time `json:"raw_deleted_at,omitzero"`
	LastErrorCode    string    `json:"last_error_code,omitempty"`
	LastErrorDetail  string    `json:"last_error_detail,omitempty"`
	RowVersion       uint64    `json:"row_version"`
}

type archivePayload struct {
	SourceDate    string `json:"source_date"`
	PolicyID      string `json:"policy_id"`
	PolicyVersion uint64 `json:"policy_version"`
	RepairAttempt uint32 `json:"repair_attempt"`
	Generation    uint64 `json:"generation"`
	NextHour      uint8  `json:"next_hour"`
}

func NewArchiveOperationJob(policy Policy, sourceDate time.Time, repairAttempt uint32) (opjob.Job, error) {
	policy, err := NormalizePolicy(policy)
	if err != nil || (policy.Status != PolicyPublished && policy.Status != PolicyRetired) {
		return opjob.Job{}, ErrInvalidPolicy
	}
	day := UTCDate(sourceDate)
	if day.IsZero() || day.Before(policy.BootstrapFrom) {
		return opjob.Job{}, ErrInvalidPolicy
	}
	generation, err := Generation(policy.Version, repairAttempt)
	if err != nil {
		return opjob.Job{}, err
	}
	payload, err := opjob.EncodePayload(archivePayloadSchema, archivePayload{
		SourceDate: day.Format(time.DateOnly), PolicyID: policy.ID, PolicyVersion: policy.Version,
		RepairAttempt: repairAttempt, Generation: generation,
	})
	if err != nil {
		return opjob.Job{}, err
	}
	requestHash := archiveRequestHash(payload)
	return opjob.Job{
		JobType: ArchiveJobType, IdempotencyKey: fmt.Sprintf("archive:%s:p%d:a%d", day.Format("20060102"), policy.Version, repairAttempt),
		RequestHash: requestHash, ProgressTotal: 24, CheckpointJSON: payload,
	}, nil
}

// archiveRequestHash is a control-plane idempotency hash. It is evaluated once
// per UTC-day job and is unrelated to the removed per-record ingest hashes.
func archiveRequestHash(payload json.RawMessage) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

type archiveRunner interface {
	Run(context.Context, flowch.RollupRequest) error
	DayStorageCounters(context.Context, time.Time) (flowch.StorageCounters, flowch.StorageCounters, error)
}

type archiveStore interface {
	GetPolicy(context.Context, string) (Policy, error)
	BeginArchive(context.Context, Policy, time.Time, uint64, string) (PartitionState, error)
	MarkArchiveWritten(context.Context, Policy, time.Time, uint64, string, time.Time) error
	CompleteArchive(context.Context, Policy, time.Time, uint64, string, Counters, Counters, time.Time) error
}

func NewArchiveHandler(store archiveStore, runner archiveRunner) opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		if store == nil || runner == nil {
			return "", opjob.TerminalError(errors.New("Flow archive dependencies are not initialized"))
		}
		if job.JobType != ArchiveJobType || strings.TrimSpace(job.ID) == "" || job.CreatedAt.IsZero() {
			return "", opjob.TerminalError(errors.New("Flow archive job identity is invalid"))
		}
		var payload archivePayload
		if err := opjob.DecodePayload(job.CheckpointJSON, archivePayloadSchema, &payload); err != nil {
			return "", err
		}
		day, err := time.Parse(time.DateOnly, payload.SourceDate)
		expectedGeneration, generationErr := Generation(payload.PolicyVersion, payload.RepairAttempt)
		if err != nil || payload.PolicyID == "" || payload.NextHour > 24 || generationErr != nil || payload.Generation != expectedGeneration {
			return "", opjob.TerminalError(ErrInvalidPolicy)
		}
		policy, err := store.GetPolicy(ctx, payload.PolicyID)
		if err != nil {
			return "", err
		}
		if policy.Version != payload.PolicyVersion || (policy.Status != PolicyPublished && policy.Status != PolicyRetired) {
			return "", opjob.TerminalError(ErrTransition)
		}
		state, err := store.BeginArchive(ctx, policy, day, payload.Generation, job.ID)
		if err != nil {
			return "", err
		}
		if state.State == PartitionReconciled && state.Generation == payload.Generation {
			return archiveResultRef(day, payload.PolicyVersion, payload.Generation), nil
		}
		generatedAt := job.CreatedAt.UTC()
		if generatedAt.Before(day.Add(24 * time.Hour)) {
			return "", opjob.TerminalError(errors.New("Flow archive job predates the closed UTC day"))
		}
		for hour := payload.NextHour; hour < 24; hour++ {
			bucket := day.Add(time.Duration(hour) * time.Hour)
			if err := runner.Run(ctx, flowch.RollupRequest{
				Resolution: flowch.RollupOneHour, Bucket: bucket, Generation: payload.Generation, GeneratedAt: generatedAt,
			}); err != nil {
				var permanent *flowch.PermanentError
				if errors.As(err, &permanent) {
					return "", opjob.TerminalError(err)
				}
				return "", err
			}
			payload.NextHour = hour + 1
			checkpoint, err := opjob.EncodePayload(archivePayloadSchema, payload)
			if err != nil {
				return "", err
			}
			if err := opjob.ReporterFromContext(ctx).Report(ctx, uint64(payload.NextHour), checkpoint); err != nil {
				return "", err
			}
		}
		if err := store.MarkArchiveWritten(ctx, policy, day, payload.Generation, job.ID, time.Now()); err != nil {
			return "", err
		}
		raw, archive, err := runner.DayStorageCounters(ctx, day)
		if err != nil {
			return "", err
		}
		sourceCounters, archiveCounters := lifecycleCounters(raw), lifecycleCounters(archive)
		err = store.CompleteArchive(ctx, policy, day, payload.Generation, job.ID, sourceCounters, archiveCounters, time.Now())
		if errors.Is(err, ErrTransition) && sourceCounters != archiveCounters {
			return "", opjob.TerminalError(fmt.Errorf("Flow archive UTC-day conservation failed: raw=%+v archive=%+v", raw, archive))
		}
		if err != nil {
			return "", err
		}
		return archiveResultRef(day, payload.PolicyVersion, payload.Generation), nil
	}
}

func archiveResultRef(day time.Time, policyVersion, generation uint64) string {
	return fmt.Sprintf("clickhouse:flow:1h:%s:p%d:g%d", day.Format(time.DateOnly), policyVersion, generation)
}

func lifecycleCounters(value flowch.StorageCounters) Counters {
	return Counters{value.RecordCount, value.RawBytes, value.RawPackets, value.EstimatedBytes, value.EstimatedPackets, value.EstimatedValidRecords}
}

const partitionColumns = `source_date,policy_id,policy_version,state,generation,repair_attempt,
	source_record_count,source_raw_bytes,source_raw_packets,source_estimated_bytes,source_estimated_packets,source_estimated_valid_records,
	archive_record_count,archive_raw_bytes,archive_raw_packets,archive_estimated_bytes,archive_estimated_packets,archive_estimated_valid_records,
	COALESCE(archive_job_id,''),COALESCE(delete_job_id,''),COALESCE(delete_approval_id,''),archived_at,reconciled_at,late_checked_at,delete_eligible_at,raw_deleted_at,
	COALESCE(last_error_code,''),COALESCE(last_error_detail,''),row_version`

func scanPartition(row rowScanner) (PartitionState, error) {
	var state PartitionState
	var archivedAt, reconciledAt, checkedAt, eligibleAt, deletedAt sql.NullTime
	err := row.Scan(&state.SourceDate, &state.PolicyID, &state.PolicyVersion, &state.State, &state.Generation, &state.RepairAttempt,
		&state.Source.RecordCount, &state.Source.RawBytes, &state.Source.RawPackets, &state.Source.EstimatedBytes, &state.Source.EstimatedPackets, &state.Source.EstimatedValidRecords,
		&state.Archive.RecordCount, &state.Archive.RawBytes, &state.Archive.RawPackets, &state.Archive.EstimatedBytes, &state.Archive.EstimatedPackets, &state.Archive.EstimatedValidRecords,
		&state.ArchiveJobID, &state.DeleteJobID, &state.DeleteApprovalID, &archivedAt, &reconciledAt, &checkedAt, &eligibleAt, &deletedAt,
		&state.LastErrorCode, &state.LastErrorDetail, &state.RowVersion)
	if archivedAt.Valid {
		state.ArchivedAt = archivedAt.Time
	}
	if reconciledAt.Valid {
		state.ReconciledAt = reconciledAt.Time
	}
	if checkedAt.Valid {
		state.LateCheckedAt = checkedAt.Time
	}
	if eligibleAt.Valid {
		state.DeleteEligibleAt = eligibleAt.Time
	}
	if deletedAt.Valid {
		state.RawDeletedAt = deletedAt.Time
	}
	return state, err
}

func (store *Store) GetPublishedPolicy(ctx context.Context) (Policy, error) {
	if store == nil || store.db == nil {
		return Policy{}, errors.New("Flow lifecycle store is not initialized")
	}
	return scanPolicy(store.db.QueryRowContext(ctx, "SELECT "+policyColumns+" FROM flow_retention_policy_revisions WHERE status='published'"))
}

func (store *Store) GetPartition(ctx context.Context, sourceDate time.Time) (PartitionState, error) {
	if store == nil || store.db == nil || UTCDate(sourceDate).IsZero() {
		return PartitionState{}, ErrInvalidPolicy
	}
	return scanPartition(store.db.QueryRowContext(ctx, "SELECT "+partitionColumns+" FROM flow_retention_partition_states WHERE source_date=?", UTCDate(sourceDate)))
}

func (store *Store) BeginArchive(ctx context.Context, policy Policy, sourceDate time.Time, generation uint64, jobID string) (PartitionState, error) {
	if store == nil || store.db == nil {
		return PartitionState{}, errors.New("Flow lifecycle store is not initialized")
	}
	policy, err := NormalizePolicy(policy)
	day := UTCDate(sourceDate)
	version, attempt, generationErr := SplitGeneration(generation)
	if err != nil || generationErr != nil || uint64(version) != policy.Version || day.IsZero() || day.Before(policy.BootstrapFrom) || strings.TrimSpace(jobID) == "" ||
		(policy.Status != PolicyPublished && policy.Status != PolicyRetired) {
		return PartitionState{}, ErrInvalidPolicy
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return PartitionState{}, err
	}
	defer tx.Rollback()
	current, err := scanPartition(tx.QueryRowContext(ctx, "SELECT "+partitionColumns+" FROM flow_retention_partition_states WHERE source_date=? FOR UPDATE", day))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `INSERT INTO flow_retention_partition_states
			(source_date,policy_id,policy_version,state,generation,repair_attempt,archive_job_id)
			VALUES (?,?,?,'sealed',?,?,?)`, day, policy.ID, policy.Version, generation, attempt, jobID)
	case err != nil:
		return PartitionState{}, err
	case current.State == PartitionRawDeleted || current.Generation > generation:
		return PartitionState{}, ErrTransition
	case current.Generation == generation && current.State == PartitionReconciled:
		return current, tx.Commit()
	case current.Generation == generation && current.ArchiveJobID == jobID:
		return current, tx.Commit()
	case current.Generation == generation:
		return PartitionState{}, ErrTransition
	default:
		_, err = tx.ExecContext(ctx, `UPDATE flow_retention_partition_states SET
			policy_id=?,policy_version=?,state='sealed',generation=?,repair_attempt=?,archive_job_id=?,
			source_record_count=0,source_raw_bytes=0,source_raw_packets=0,source_estimated_bytes=0,source_estimated_packets=0,source_estimated_valid_records=0,
			archive_record_count=0,archive_raw_bytes=0,archive_raw_packets=0,archive_estimated_bytes=0,archive_estimated_packets=0,archive_estimated_valid_records=0,
			delete_job_id=NULL,delete_approval_id=NULL,archived_at=NULL,reconciled_at=NULL,late_checked_at=NULL,delete_eligible_at=NULL,
			last_error_code=NULL,last_error_detail=NULL,row_version=row_version+1 WHERE source_date=?`,
			policy.ID, policy.Version, generation, attempt, jobID, day)
	}
	if err != nil {
		return PartitionState{}, err
	}
	if err := tx.Commit(); err != nil {
		return PartitionState{}, err
	}
	return store.GetPartition(ctx, day)
}

func (store *Store) MarkArchiveWritten(ctx context.Context, policy Policy, sourceDate time.Time, generation uint64, jobID string, at time.Time) error {
	if store == nil || store.db == nil || policy.ID == "" || UTCDate(sourceDate).IsZero() || generation == 0 || jobID == "" || at.IsZero() {
		return ErrInvalidPolicy
	}
	result, err := store.db.ExecContext(ctx, `UPDATE flow_retention_partition_states SET state='archive_written',archived_at=?,
		last_error_code=NULL,last_error_detail=NULL,row_version=row_version+1
		WHERE source_date=? AND policy_id=? AND policy_version=? AND generation=? AND archive_job_id=? AND state IN ('sealed','archive_written')`,
		at.UTC().Truncate(time.Millisecond), UTCDate(sourceDate), policy.ID, policy.Version, generation, jobID)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return err
		}
		return ErrTransition
	}
	return nil
}

func (store *Store) CompleteArchive(ctx context.Context, policy Policy, sourceDate time.Time, generation uint64, jobID string,
	source, archive Counters, at time.Time) error {
	day := UTCDate(sourceDate)
	if store == nil || store.db == nil || policy.ID == "" || day.IsZero() || generation == 0 || jobID == "" || at.IsZero() {
		return ErrInvalidPolicy
	}
	state, errorCode, errorDetail := PartitionReconciled, "", ""
	var reconciledAt, checkedAt, eligibleAt any = at.UTC().Truncate(time.Millisecond), at.UTC().Truncate(time.Millisecond), nil
	if source != archive {
		state, errorCode, errorDetail = PartitionFailed, "COUNTER_MISMATCH", "raw and 1h archive conservation counters differ"
		reconciledAt, checkedAt = nil, nil
	} else {
		eligible, err := RawDeleteEligibleAt(day, policy)
		if err != nil {
			return err
		}
		eligibleAt = eligible
	}
	result, err := store.db.ExecContext(ctx, `UPDATE flow_retention_partition_states SET state=?,
		source_record_count=?,source_raw_bytes=?,source_raw_packets=?,source_estimated_bytes=?,source_estimated_packets=?,source_estimated_valid_records=?,
		archive_record_count=?,archive_raw_bytes=?,archive_raw_packets=?,archive_estimated_bytes=?,archive_estimated_packets=?,archive_estimated_valid_records=?,
		reconciled_at=?,late_checked_at=?,delete_eligible_at=?,last_error_code=NULLIF(?,''),last_error_detail=NULLIF(?,''),row_version=row_version+1
		WHERE source_date=? AND policy_id=? AND policy_version=? AND generation=? AND archive_job_id=? AND state='archive_written'`,
		state, source.RecordCount, source.RawBytes, source.RawPackets, source.EstimatedBytes, source.EstimatedPackets, source.EstimatedValidRecords,
		archive.RecordCount, archive.RawBytes, archive.RawPackets, archive.EstimatedBytes, archive.EstimatedPackets, archive.EstimatedValidRecords,
		reconciledAt, checkedAt, eligibleAt, errorCode, errorDetail, day, policy.ID, policy.Version, generation, jobID)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return err
		}
		return ErrTransition
	}
	if source != archive {
		return ErrTransition
	}
	return nil
}

func (store *Store) ListArchiveRepairCandidates(ctx context.Context, limit int) ([]PartitionState, error) {
	if store == nil || store.db == nil || limit < 1 || limit > 366 {
		return nil, ErrInvalidPolicy
	}
	rows, err := store.db.QueryContext(ctx, `SELECT `+partitionColumns+`
		FROM flow_retention_partition_states AS partition_state
		WHERE partition_state.state='failed' OR (
			partition_state.state IN ('sealed','archive_written') AND NOT EXISTS (
				SELECT 1 FROM operation_jobs AS job
				WHERE job.id=partition_state.archive_job_id AND job.status NOT IN ('failed','canceled')
			)
		)
		ORDER BY partition_state.source_date ASC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]PartitionState, 0)
	for rows.Next() {
		item, err := scanPartition(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (store *Store) ListLateCheckCandidates(ctx context.Context, before time.Time, limit int) ([]PartitionState, error) {
	if store == nil || store.db == nil || before.IsZero() || limit < 1 || limit > 366 {
		return nil, ErrInvalidPolicy
	}
	rows, err := store.db.QueryContext(ctx, `SELECT `+partitionColumns+` FROM flow_retention_partition_states
		WHERE state IN ('reconciled','delete_eligible') AND raw_deleted_at IS NULL
		  AND (late_checked_at IS NULL OR late_checked_at<=?)
		ORDER BY COALESCE(late_checked_at,'1970-01-01') ASC,source_date ASC LIMIT ?`, before.UTC().Truncate(time.Millisecond), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]PartitionState, 0)
	for rows.Next() {
		item, err := scanPartition(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (store *Store) RecordLateCheck(ctx context.Context, state PartitionState, source, archive Counters, at time.Time) error {
	if store == nil || store.db == nil || state.SourceDate.IsZero() || state.Generation == 0 || at.IsZero() {
		return ErrInvalidPolicy
	}
	if source == archive {
		result, err := store.db.ExecContext(ctx, `UPDATE flow_retention_partition_states SET late_checked_at=?,
			source_record_count=?,source_raw_bytes=?,source_raw_packets=?,source_estimated_bytes=?,source_estimated_packets=?,source_estimated_valid_records=?,
			archive_record_count=?,archive_raw_bytes=?,archive_raw_packets=?,archive_estimated_bytes=?,archive_estimated_packets=?,archive_estimated_valid_records=?,
			row_version=row_version+1 WHERE source_date=? AND generation=? AND state IN ('reconciled','delete_eligible')`,
			at.UTC().Truncate(time.Millisecond), source.RecordCount, source.RawBytes, source.RawPackets, source.EstimatedBytes, source.EstimatedPackets, source.EstimatedValidRecords,
			archive.RecordCount, archive.RawBytes, archive.RawPackets, archive.EstimatedBytes, archive.EstimatedPackets, archive.EstimatedValidRecords,
			UTCDate(state.SourceDate), state.Generation)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return ErrTransition
		}
		return nil
	}
	result, err := store.db.ExecContext(ctx, `UPDATE flow_retention_partition_states SET state='failed',late_checked_at=?,
		source_record_count=?,source_raw_bytes=?,source_raw_packets=?,source_estimated_bytes=?,source_estimated_packets=?,source_estimated_valid_records=?,
		archive_record_count=?,archive_raw_bytes=?,archive_raw_packets=?,archive_estimated_bytes=?,archive_estimated_packets=?,archive_estimated_valid_records=?,
		last_error_code='LATE_ARRIVAL',last_error_detail='raw and 1h archive counters changed after reconciliation',row_version=row_version+1
		WHERE source_date=? AND generation=? AND state IN ('reconciled','delete_eligible')`,
		at.UTC().Truncate(time.Millisecond), source.RecordCount, source.RawBytes, source.RawPackets, source.EstimatedBytes, source.EstimatedPackets, source.EstimatedValidRecords,
		archive.RecordCount, archive.RawBytes, archive.RawPackets, archive.EstimatedBytes, archive.EstimatedPackets, archive.EstimatedValidRecords,
		UTCDate(state.SourceDate), state.Generation)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return err
		}
		return ErrTransition
	}
	return nil
}

// ArchiveThrough returns the largest continuous archived boundary in [from,to].
// A gap or failed day stops the boundary; callers then read raw facts from that
// point onward and never mix an incomplete archive prefix into a query.
func (store *Store) ArchiveThrough(ctx context.Context, from, to time.Time) (time.Time, error) {
	from, to = from.UTC(), to.UTC()
	if store == nil || store.db == nil || from.IsZero() || to.IsZero() || !to.After(from) || from.Truncate(time.Hour) != from || to.Truncate(time.Hour) != to {
		return time.Time{}, ErrInvalidPolicy
	}
	firstDay := UTCDate(from)
	endDay := UTCDate(to)
	if to.After(endDay) {
		endDay = endDay.Add(24 * time.Hour)
	}
	rows, err := store.db.QueryContext(ctx, `SELECT source_date,state FROM flow_retention_partition_states
		WHERE source_date>=? AND source_date<? ORDER BY source_date ASC`, firstDay, endDay)
	if err != nil {
		return time.Time{}, err
	}
	defer rows.Close()
	expected := firstDay
	for rows.Next() {
		var day time.Time
		var state string
		if err := rows.Scan(&day, &state); err != nil {
			return time.Time{}, err
		}
		day = UTCDate(day)
		if day.Before(expected) {
			continue
		}
		if !day.Equal(expected) || (state != PartitionReconciled && state != PartitionDeleteEligible && state != PartitionRawDeleted) {
			break
		}
		expected = expected.Add(24 * time.Hour)
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, err
	}
	if expected.Equal(firstDay) {
		return from, nil
	}
	if expected.After(to) {
		return to, nil
	}
	return expected, nil
}
