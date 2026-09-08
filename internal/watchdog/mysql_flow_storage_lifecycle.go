package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const flowStoragePolicyColumns = `
	id, tenant_id, policy_version, status, bootstrap_from,
	raw_retention_seconds, downsample_resolution_seconds, archive_retention_seconds,
	late_arrival_seconds, delete_grace_seconds, max_partitions_per_run, raw_delete_enabled,
	row_version, COALESCE(created_by, ''), COALESCE(published_by, ''), COALESCE(retired_by, ''),
	created_at, published_at, retired_at`

func (s *MySQLStore) ListFlowStoragePolicies(ctx context.Context, tenantID ID) ([]FlowStoragePolicy, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+flowStoragePolicyColumns+`
		FROM flow_storage_policy_revisions WHERE tenant_id = ?
		ORDER BY policy_version DESC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]FlowStoragePolicy, 0)
	for rows.Next() {
		item, err := scanFlowStoragePolicy(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *MySQLStore) ListPublishedFlowStoragePolicies(ctx context.Context, afterTenant ID, limit int) ([]FlowStoragePolicy, ID, error) {
	if limit < 1 || limit > 1000 {
		return nil, "", ErrFlowStorageInvalid
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+flowStoragePolicyColumns+`
		FROM flow_storage_policy_revisions
		WHERE status = 'published' AND tenant_id > ?
		ORDER BY tenant_id LIMIT ?`, afterTenant, limit)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	items := make([]FlowStoragePolicy, 0)
	var next ID
	for rows.Next() {
		item, err := scanFlowStoragePolicy(rows)
		if err != nil {
			return nil, "", err
		}
		items = append(items, item)
		next = item.TenantID
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(items) < limit {
		next = ""
	}
	return items, next, nil
}

func (s *MySQLStore) GetFlowStoragePolicy(ctx context.Context, tenantID, policyID ID) (FlowStoragePolicy, error) {
	return scanFlowStoragePolicy(s.db.QueryRowContext(ctx, `SELECT `+flowStoragePolicyColumns+`
		FROM flow_storage_policy_revisions WHERE tenant_id = ? AND id = ?`, tenantID, policyID))
}

func (s *MySQLStore) CreateFlowStoragePolicyDraft(ctx context.Context, policy FlowStoragePolicy) (FlowStoragePolicy, error) {
	policy, err := normalizeFlowStoragePolicy(policy)
	if err != nil {
		return FlowStoragePolicy{}, err
	}
	if policy.RawDeleteEnabled {
		return FlowStoragePolicy{}, ErrFlowStorageRawDeleteLocked
	}
	if policy.ID == "" {
		policy.ID, err = newManagementID()
		if err != nil {
			return FlowStoragePolicy{}, err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FlowStoragePolicy{}, err
	}
	defer tx.Rollback()
	// Serialize revision allocation on the tenant row. MAX()+1 alone races.
	var tenantLock ID
	if err := tx.QueryRowContext(ctx, `SELECT id FROM tenants WHERE id = ? FOR UPDATE`, policy.TenantID).Scan(&tenantLock); err != nil {
		return FlowStoragePolicy{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(policy_version), 0) + 1
		FROM flow_storage_policy_revisions WHERE tenant_id = ?`, policy.TenantID).Scan(&policy.PolicyVersion); err != nil {
		return FlowStoragePolicy{}, err
	}
	if policy.PolicyVersion > uint64(^uint32(0)) {
		return FlowStoragePolicy{}, ErrFlowStorageInvalid
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO flow_storage_policy_revisions (
		id, tenant_id, policy_version, status, bootstrap_from,
		raw_retention_seconds, downsample_resolution_seconds, archive_retention_seconds,
		late_arrival_seconds, delete_grace_seconds, max_partitions_per_run, raw_delete_enabled, created_by
	) VALUES (?, ?, ?, 'draft', ?, ?, ?, ?, ?, ?, ?, 0, NULLIF(?, ''))`,
		policy.ID, policy.TenantID, policy.PolicyVersion, policy.BootstrapFrom,
		policy.RawRetentionSeconds, policy.DownsampleResolutionSeconds, policy.ArchiveRetentionSeconds,
		policy.LateArrivalSeconds, policy.DeleteGraceSeconds, policy.MaxPartitionsPerRun, policy.CreatedBy); err != nil {
		return FlowStoragePolicy{}, err
	}
	if err := tx.Commit(); err != nil {
		return FlowStoragePolicy{}, err
	}
	return s.GetFlowStoragePolicy(ctx, policy.TenantID, policy.ID)
}

func (s *MySQLStore) UpdateFlowStoragePolicyDraft(ctx context.Context, policy FlowStoragePolicy, expected uint64) (FlowStoragePolicy, error) {
	policy, err := normalizeFlowStoragePolicy(policy)
	if err != nil {
		return FlowStoragePolicy{}, err
	}
	if policy.RawDeleteEnabled {
		return FlowStoragePolicy{}, ErrFlowStorageRawDeleteLocked
	}
	result, err := s.db.ExecContext(ctx, `UPDATE flow_storage_policy_revisions SET
		bootstrap_from = ?, raw_retention_seconds = ?, downsample_resolution_seconds = ?,
		archive_retention_seconds = ?, late_arrival_seconds = ?, delete_grace_seconds = ?,
		max_partitions_per_run = ?, raw_delete_enabled = 0, row_version = row_version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'draft' AND row_version = ?`,
		policy.BootstrapFrom, policy.RawRetentionSeconds, policy.DownsampleResolutionSeconds,
		policy.ArchiveRetentionSeconds, policy.LateArrivalSeconds, policy.DeleteGraceSeconds,
		policy.MaxPartitionsPerRun, policy.TenantID, policy.ID, expected)
	if err != nil {
		return FlowStoragePolicy{}, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return FlowStoragePolicy{}, err
	}
	if count != 1 {
		return FlowStoragePolicy{}, s.flowStorageMutationError(ctx, policy.TenantID, policy.ID, expected, true)
	}
	return s.GetFlowStoragePolicy(ctx, policy.TenantID, policy.ID)
}

func (s *MySQLStore) DeleteFlowStoragePolicyDraft(ctx context.Context, tenantID, policyID ID, expected uint64) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM flow_storage_policy_revisions
		WHERE tenant_id = ? AND id = ? AND status = 'draft' AND row_version = ?`, tenantID, policyID, expected)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return s.flowStorageMutationError(ctx, tenantID, policyID, expected, true)
	}
	return nil
}

func (s *MySQLStore) PublishFlowStoragePolicy(ctx context.Context, tenantID, policyID, actorID ID, expected uint64, now time.Time) (FlowStoragePolicy, error) {
	if tenantID == "" || policyID == "" || actorID == "" || expected == 0 || now.IsZero() {
		return FlowStoragePolicy{}, ErrFlowStorageInvalid
	}
	now = now.UTC().Truncate(time.Millisecond)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FlowStoragePolicy{}, err
	}
	defer tx.Rollback()
	policy, err := scanFlowStoragePolicy(tx.QueryRowContext(ctx, `SELECT `+flowStoragePolicyColumns+`
		FROM flow_storage_policy_revisions WHERE tenant_id = ? AND id = ? FOR UPDATE`, tenantID, policyID))
	if err != nil {
		return FlowStoragePolicy{}, err
	}
	if policy.Status != FlowStoragePolicyDraft {
		return FlowStoragePolicy{}, ErrFlowStorageTransition
	}
	if policy.RowVersion != expected {
		return FlowStoragePolicy{}, ErrFlowStorageVersionConflict
	}
	if policy.RawDeleteEnabled {
		return FlowStoragePolicy{}, ErrFlowStorageRawDeleteLocked
	}
	if _, err := tx.ExecContext(ctx, `UPDATE flow_storage_policy_revisions SET
		status = 'retired', retired_by = ?, retired_at = ?, row_version = row_version + 1
		WHERE tenant_id = ? AND status = 'published'`, actorID, now, tenantID); err != nil {
		return FlowStoragePolicy{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE flow_storage_policy_revisions SET
		status = 'published', published_by = ?, published_at = ?, row_version = row_version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'draft' AND row_version = ?`, actorID, now, tenantID, policyID, expected)
	if err != nil {
		return FlowStoragePolicy{}, err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		if err != nil {
			return FlowStoragePolicy{}, err
		}
		return FlowStoragePolicy{}, ErrFlowStorageVersionConflict
	}
	if err := tx.Commit(); err != nil {
		return FlowStoragePolicy{}, err
	}
	return s.GetFlowStoragePolicy(ctx, tenantID, policyID)
}

func (s *MySQLStore) flowStorageMutationError(ctx context.Context, tenantID, policyID ID, expected uint64, draftOnly bool) error {
	item, err := s.GetFlowStoragePolicy(ctx, tenantID, policyID)
	if err != nil {
		return err
	}
	if item.RowVersion != expected {
		return ErrFlowStorageVersionConflict
	}
	if draftOnly && item.Status != FlowStoragePolicyDraft {
		return ErrFlowStorageTransition
	}
	return ErrFlowStorageVersionConflict
}

func scanFlowStoragePolicy(row rowScanner) (FlowStoragePolicy, error) {
	var item FlowStoragePolicy
	var bootstrap mysqlUTCDate
	var publishedAt, retiredAt sql.NullTime
	if err := row.Scan(&item.ID, &item.TenantID, &item.PolicyVersion, &item.Status, &bootstrap,
		&item.RawRetentionSeconds, &item.DownsampleResolutionSeconds, &item.ArchiveRetentionSeconds,
		&item.LateArrivalSeconds, &item.DeleteGraceSeconds, &item.MaxPartitionsPerRun, &item.RawDeleteEnabled,
		&item.RowVersion, &item.CreatedBy, &item.PublishedBy, &item.RetiredBy,
		&item.CreatedAt, &publishedAt, &retiredAt); err != nil {
		return FlowStoragePolicy{}, err
	}
	item.BootstrapFrom = time.Time(bootstrap)
	if publishedAt.Valid {
		item.PublishedAt = publishedAt.Time
	}
	if retiredAt.Valid {
		item.RetiredAt = retiredAt.Time
	}
	return item, nil
}

const flowStoragePartitionColumns = `
	tenant_id, source_date, policy_id, policy_version, state, generation,
	source_record_count, source_raw_bytes, source_raw_packets, source_estimated_bytes, source_estimated_packets, source_estimated_valid_records,
	archive_record_count, archive_raw_bytes, archive_raw_packets, archive_estimated_bytes, archive_estimated_packets, archive_estimated_valid_records,
	COALESCE(downsample_job_id, ''), COALESCE(delete_job_id, ''),
	downsampled_at, reconciled_at, delete_eligible_at, raw_deleted_at,
	COALESCE(last_error_code, ''), COALESCE(last_error_detail, ''), row_version, created_at, updated_at`

const flowStoragePartitionStateColumns = `
	state.tenant_id, state.source_date, state.policy_id, state.policy_version, state.state, state.generation,
	state.source_record_count, state.source_raw_bytes, state.source_raw_packets, state.source_estimated_bytes, state.source_estimated_packets, state.source_estimated_valid_records,
	state.archive_record_count, state.archive_raw_bytes, state.archive_raw_packets, state.archive_estimated_bytes, state.archive_estimated_packets, state.archive_estimated_valid_records,
	COALESCE(state.downsample_job_id, ''), COALESCE(state.delete_job_id, ''),
	state.downsampled_at, state.reconciled_at, state.delete_eligible_at, state.raw_deleted_at,
	COALESCE(state.last_error_code, ''), COALESCE(state.last_error_detail, ''), state.row_version, state.created_at, state.updated_at`

func (s *MySQLStore) ListFlowStoragePartitions(ctx context.Context, tenantID ID, filter FlowStoragePartitionFilter) ([]FlowStoragePartitionState, int, error) {
	if tenantID == "" || filter.Limit < 1 || filter.Limit > 500 || filter.Offset < 0 ||
		(filter.State != "" && !validFlowStoragePartitionState(filter.State)) ||
		(!filter.From.IsZero() && !filter.To.IsZero() && !utcDate(filter.From).Before(utcDate(filter.To))) {
		return nil, 0, ErrFlowStorageInvalid
	}
	where := []string{"tenant_id = ?"}
	args := []any{tenantID}
	if filter.State != "" {
		where = append(where, "state = ?")
		args = append(args, filter.State)
	}
	if !filter.From.IsZero() {
		where = append(where, "source_date >= ?")
		args = append(args, utcDate(filter.From))
	}
	if !filter.To.IsZero() {
		where = append(where, "source_date < ?")
		args = append(args, utcDate(filter.To))
	}
	clause := strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_storage_partition_states WHERE `+clause, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	queryArgs := append(append([]any(nil), args...), filter.Limit, filter.Offset)
	rows, err := s.db.QueryContext(ctx, `SELECT `+flowStoragePartitionColumns+`
		FROM flow_storage_partition_states WHERE `+clause+`
		ORDER BY source_date DESC, policy_version DESC LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]FlowStoragePartitionState, 0)
	for rows.Next() {
		item, err := scanFlowStoragePartition(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *MySQLStore) FlowStorageArchiveThrough(ctx context.Context, tenantID ID, from, to time.Time) (time.Time, error) {
	from, to = from.UTC(), to.UTC()
	if tenantID == "" || from.IsZero() || to.IsZero() || !to.After(from) || from.Truncate(time.Hour) != from || to.Truncate(time.Hour) != to {
		return time.Time{}, ErrFlowStorageInvalid
	}
	firstDay := utcDate(from)
	endDay := utcDate(to)
	if to.After(endDay) {
		endDay = endDay.Add(24 * time.Hour)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT state.source_date, state.state
		FROM flow_storage_partition_states AS state
		INNER JOIN (
			SELECT source_date, MAX(generation) AS generation
			FROM flow_storage_partition_states
			WHERE tenant_id = ? AND source_date >= ? AND source_date < ?
			GROUP BY source_date
		) AS latest USING (source_date, generation)
		WHERE state.tenant_id = ?
		ORDER BY state.source_date ASC`, tenantID, firstDay, endDay, tenantID)
	if err != nil {
		return time.Time{}, err
	}
	defer rows.Close()
	expected := firstDay
	for rows.Next() {
		var sourceDate mysqlUTCDate
		var state string
		if err := rows.Scan(&sourceDate, &state); err != nil {
			return time.Time{}, err
		}
		day := time.Time(sourceDate)
		if day.Before(expected) {
			continue
		}
		if !day.Equal(expected) || (state != FlowStoragePartitionReconciled && state != FlowStoragePartitionDeleteEligible && state != FlowStoragePartitionRawDeleted) {
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

func (s *MySQLStore) BeginFlowStoragePartition(ctx context.Context, policy FlowStoragePolicy, sourceDate time.Time, generation uint64, jobID ID) (FlowStoragePartitionState, error) {
	day := utcDate(sourceDate)
	if policy.ID == "" || policy.TenantID == "" || policy.PolicyVersion == 0 ||
		(policy.Status != FlowStoragePolicyPublished && policy.Status != FlowStoragePolicyRetired) ||
		day.IsZero() || generation == 0 || jobID == "" {
		return FlowStoragePartitionState{}, ErrFlowStorageInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FlowStoragePartitionState{}, err
	}
	defer tx.Rollback()
	current, err := scanFlowStoragePartition(tx.QueryRowContext(ctx, `SELECT `+flowStoragePartitionColumns+`
		FROM flow_storage_partition_states
		WHERE tenant_id = ? AND source_date = ? AND policy_version = ? FOR UPDATE`,
		policy.TenantID, day, policy.PolicyVersion))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `INSERT INTO flow_storage_partition_states (
			tenant_id, source_date, policy_id, policy_version, state, generation, downsample_job_id
		) VALUES (?, ?, ?, ?, 'sealed', ?, ?)`, policy.TenantID, day, policy.ID, policy.PolicyVersion, generation, jobID)
		if err != nil {
			return FlowStoragePartitionState{}, err
		}
	case err != nil:
		return FlowStoragePartitionState{}, err
	case current.PolicyID != policy.ID || current.Generation > generation || current.State == FlowStoragePartitionRawDeleted:
		return FlowStoragePartitionState{}, ErrFlowStorageTransition
	case current.Generation == generation && current.State == FlowStoragePartitionReconciled:
		return current, nil
	default:
		_, err = tx.ExecContext(ctx, `UPDATE flow_storage_partition_states SET
			state = 'sealed', generation = ?, downsample_job_id = ?,
			source_record_count = NULL, source_raw_bytes = NULL, source_raw_packets = NULL,
			source_estimated_bytes = NULL, source_estimated_packets = NULL, source_estimated_valid_records = NULL,
			archive_record_count = NULL, archive_raw_bytes = NULL, archive_raw_packets = NULL,
			archive_estimated_bytes = NULL, archive_estimated_packets = NULL, archive_estimated_valid_records = NULL,
			downsampled_at = NULL, reconciled_at = NULL, delete_eligible_at = NULL,
			last_error_code = NULL, last_error_detail = NULL, row_version = row_version + 1
			WHERE tenant_id = ? AND source_date = ? AND policy_version = ?`,
			generation, jobID, policy.TenantID, day, policy.PolicyVersion)
		if err != nil {
			return FlowStoragePartitionState{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return FlowStoragePartitionState{}, err
	}
	return s.getFlowStoragePartition(ctx, policy.TenantID, day, policy.PolicyVersion)
}

func (s *MySQLStore) ListFlowStorageRepairCandidates(ctx context.Context, policy FlowStoragePolicy, limit int) ([]FlowStoragePartitionState, error) {
	if policy.ID == "" || policy.TenantID == "" || policy.PolicyVersion == 0 || limit < 1 || limit > 366 {
		return nil, ErrFlowStorageInvalid
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+flowStoragePartitionStateColumns+`
		FROM flow_storage_partition_states AS state
		LEFT JOIN operation_jobs AS job ON job.id = state.downsample_job_id
		WHERE state.tenant_id = ? AND state.policy_id = ? AND state.policy_version = ?
		  AND (state.state = 'failed' OR (state.state IN ('sealed', 'downsample_written')
		       AND (job.id IS NULL OR job.status IN ('failed', 'canceled'))))
		ORDER BY state.source_date ASC LIMIT ?`, policy.TenantID, policy.ID, policy.PolicyVersion, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]FlowStoragePartitionState, 0)
	for rows.Next() {
		item, err := scanFlowStoragePartition(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *MySQLStore) MarkFlowStoragePartitionDownsampleWritten(ctx context.Context, policy FlowStoragePolicy, sourceDate time.Time, generation uint64, jobID ID, now time.Time) (FlowStoragePartitionState, error) {
	day := utcDate(sourceDate)
	if policy.ID == "" || policy.TenantID == "" || policy.PolicyVersion == 0 || day.IsZero() || generation == 0 || jobID == "" || now.IsZero() {
		return FlowStoragePartitionState{}, ErrFlowStorageInvalid
	}
	result, err := s.db.ExecContext(ctx, `UPDATE flow_storage_partition_states SET
		state = 'downsample_written', downsample_job_id = ?, downsampled_at = ?,
		last_error_code = NULL, last_error_detail = NULL, row_version = row_version + 1
		WHERE tenant_id = ? AND source_date = ? AND policy_id = ? AND policy_version = ?
		  AND generation = ? AND state IN ('sealed', 'downsample_written')`,
		jobID, now.UTC().Truncate(time.Millisecond), policy.TenantID, day, policy.ID, policy.PolicyVersion, generation)
	if err != nil {
		return FlowStoragePartitionState{}, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return FlowStoragePartitionState{}, err
	}
	if count != 1 {
		return FlowStoragePartitionState{}, ErrFlowStorageTransition
	}
	return s.getFlowStoragePartition(ctx, policy.TenantID, day, policy.PolicyVersion)
}

func (s *MySQLStore) CompleteFlowStoragePartition(ctx context.Context, policy FlowStoragePolicy, sourceDate time.Time, generation uint64, jobID ID, source, archive FlowStoragePartitionCounters, now time.Time) (FlowStoragePartitionState, error) {
	day := utcDate(sourceDate)
	if policy.ID == "" || policy.TenantID == "" || policy.PolicyVersion == 0 || day.IsZero() || generation == 0 || jobID == "" || now.IsZero() {
		return FlowStoragePartitionState{}, ErrFlowStorageInvalid
	}
	now = now.UTC().Truncate(time.Millisecond)
	matched := source == archive
	state, errorCode, errorDetail := FlowStoragePartitionReconciled, "", ""
	var reconciledAt, deleteEligibleAt any = now, now.Add(time.Duration(policy.DeleteGraceSeconds) * time.Second)
	if !matched {
		state, errorCode, errorDetail = FlowStoragePartitionFailed, "COUNTER_MISMATCH", "raw and 1h archive conservation counters differ"
		reconciledAt, deleteEligibleAt = nil, nil
	}
	result, err := s.db.ExecContext(ctx, `UPDATE flow_storage_partition_states SET
		state = ?, source_record_count = ?, source_raw_bytes = ?, source_raw_packets = ?,
		source_estimated_bytes = ?, source_estimated_packets = ?, source_estimated_valid_records = ?,
		archive_record_count = ?, archive_raw_bytes = ?, archive_raw_packets = ?,
		archive_estimated_bytes = ?, archive_estimated_packets = ?, archive_estimated_valid_records = ?,
		downsample_job_id = ?, downsampled_at = ?, reconciled_at = ?, delete_eligible_at = ?,
		last_error_code = NULLIF(?, ''), last_error_detail = NULLIF(?, ''), row_version = row_version + 1
		WHERE tenant_id = ? AND source_date = ? AND policy_id = ? AND policy_version = ?
		  AND generation = ? AND state IN ('sealed', 'downsample_written', 'failed')`,
		state, source.RecordCount, source.RawBytes, source.RawPackets, source.EstimatedBytes, source.EstimatedPackets, source.EstimatedValidRecords,
		archive.RecordCount, archive.RawBytes, archive.RawPackets, archive.EstimatedBytes, archive.EstimatedPackets, archive.EstimatedValidRecords,
		jobID, now, reconciledAt, deleteEligibleAt, errorCode, errorDetail,
		policy.TenantID, day, policy.ID, policy.PolicyVersion, generation)
	if err != nil {
		return FlowStoragePartitionState{}, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return FlowStoragePartitionState{}, err
	}
	if count != 1 {
		return FlowStoragePartitionState{}, ErrFlowStorageTransition
	}
	item, err := s.getFlowStoragePartition(ctx, policy.TenantID, day, policy.PolicyVersion)
	if err != nil {
		return FlowStoragePartitionState{}, err
	}
	if !matched {
		return item, ErrFlowStorageTransition
	}
	return item, nil
}

func (s *MySQLStore) getFlowStoragePartition(ctx context.Context, tenantID ID, sourceDate time.Time, policyVersion uint64) (FlowStoragePartitionState, error) {
	return scanFlowStoragePartition(s.db.QueryRowContext(ctx, `SELECT `+flowStoragePartitionColumns+`
		FROM flow_storage_partition_states
		WHERE tenant_id = ? AND source_date = ? AND policy_version = ?`, tenantID, utcDate(sourceDate), policyVersion))
}

func scanFlowStoragePartition(row rowScanner) (FlowStoragePartitionState, error) {
	var item FlowStoragePartitionState
	var sourceDate mysqlUTCDate
	var source, archive [6]sql.NullInt64
	var downsampled, reconciled, eligible, deleted sql.NullTime
	if err := row.Scan(&item.TenantID, &sourceDate, &item.PolicyID, &item.PolicyVersion, &item.State, &item.Generation,
		&source[0], &source[1], &source[2], &source[3], &source[4], &source[5],
		&archive[0], &archive[1], &archive[2], &archive[3], &archive[4], &archive[5],
		&item.DownsampleJobID, &item.DeleteJobID, &downsampled, &reconciled, &eligible, &deleted,
		&item.LastErrorCode, &item.LastErrorDetail, &item.RowVersion, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return FlowStoragePartitionState{}, err
	}
	item.SourceDate = time.Time(sourceDate)
	item.Source = nullableFlowStorageCounters(source)
	item.Archive = nullableFlowStorageCounters(archive)
	if downsampled.Valid {
		item.DownsampledAt = downsampled.Time
	}
	if reconciled.Valid {
		item.ReconciledAt = reconciled.Time
	}
	if eligible.Valid {
		item.DeleteEligibleAt = eligible.Time
	}
	if deleted.Valid {
		item.RawDeletedAt = deleted.Time
	}
	return item, nil
}

func nullableFlowStorageCounters(values [6]sql.NullInt64) *FlowStoragePartitionCounters {
	if !values[0].Valid {
		return nil
	}
	return &FlowStoragePartitionCounters{RecordCount: uint64(values[0].Int64), RawBytes: uint64(values[1].Int64),
		RawPackets: uint64(values[2].Int64), EstimatedBytes: uint64(values[3].Int64),
		EstimatedPackets: uint64(values[4].Int64), EstimatedValidRecords: uint64(values[5].Int64)}
}

func validFlowStoragePartitionState(state string) bool {
	switch state {
	case FlowStoragePartitionSealed, FlowStoragePartitionDownsampleWritten, FlowStoragePartitionReconciled,
		FlowStoragePartitionDeleteEligible, FlowStoragePartitionRawDeleted, FlowStoragePartitionFailed:
		return true
	default:
		return false
	}
}

var _ FlowStorageLifecycleRepository = (*MySQLStore)(nil)
var _ FlowStorageJobRepository = (*MySQLStore)(nil)
var _ FlowStorageArchiveBoundaryRepository = (*MySQLStore)(nil)

type mysqlUTCDate time.Time

func (value *mysqlUTCDate) Scan(source any) error {
	var parsed time.Time
	var err error
	switch typed := source.(type) {
	case time.Time:
		parsed = typed
	case []byte:
		parsed, err = time.Parse("2006-01-02", string(typed))
	case string:
		parsed, err = time.Parse("2006-01-02", typed)
	default:
		return fmt.Errorf("unsupported MySQL DATE type %T", source)
	}
	if err != nil {
		return err
	}
	*value = mysqlUTCDate(utcDate(parsed))
	return nil
}
