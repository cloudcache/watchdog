package watchdog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

const operationJobScheduleColumns = `
	id, tenant_id, scope_type, name, job_type, partition_key, cron_expression,
	timezone, payload_json, enabled, max_inflight, next_run_at, last_enqueued_at,
	COALESCE(last_error_detail, ''), row_version, COALESCE(created_by, ''), created_at, updated_at`

func scanOperationJobSchedule(row rowScanner) (OperationJobSchedule, error) {
	var item OperationJobSchedule
	var tenantID sql.NullString
	var payload []byte
	var lastEnqueued sql.NullTime
	if err := row.Scan(&item.ID, &tenantID, &item.ScopeType, &item.Name, &item.JobType,
		&item.PartitionKey, &item.CronExpression, &item.Timezone, &payload, &item.Enabled,
		&item.MaxInflight, &item.NextRunAt, &lastEnqueued, &item.LastErrorDetail,
		&item.RowVersion, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return OperationJobSchedule{}, err
	}
	item.TenantID = ID(tenantID.String)
	item.PayloadJSON = payload
	if lastEnqueued.Valid {
		item.LastEnqueuedAt = lastEnqueued.Time
	}
	return item, nil
}

func normalizeOperationJobScheduleFilter(filter OperationJobScheduleFilter) (OperationJobScheduleFilter, error) {
	filter.Search = strings.TrimSpace(filter.Search)
	filter.JobType = strings.TrimSpace(filter.JobType)
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Limit > 200 {
		filter.Limit = 200
	}
	if filter.Offset < 0 {
		return OperationJobScheduleFilter{}, errors.New("schedule offset must not be negative")
	}
	if filter.Sort == "" {
		filter.Sort = "name"
	}
	if _, ok := map[string]bool{"name": true, "job_type": true, "enabled": true, "next_run_at": true, "updated_at": true}[filter.Sort]; !ok {
		return OperationJobScheduleFilter{}, errors.New("unsupported schedule sort")
	}
	return filter, nil
}

func (s *MySQLStore) ListOperationJobSchedules(ctx context.Context, tenantID ID, filter OperationJobScheduleFilter) ([]OperationJobSchedule, int64, error) {
	if tenantID == "" {
		return nil, 0, errors.New("schedule tenant is required")
	}
	filter, err := normalizeOperationJobScheduleFilter(filter)
	if err != nil {
		return nil, 0, err
	}
	where := ` FROM operation_job_schedules WHERE tenant_id = ? AND scope_type = 'tenant'`
	args := []any{tenantID}
	if filter.Search != "" {
		like := "%" + escapeSQLLike(filter.Search) + "%"
		where += ` AND (name LIKE ? OR job_type LIKE ? OR partition_key LIKE ?)`
		args = append(args, like, like, like)
	}
	if filter.JobType != "" {
		where += ` AND job_type = ?`
		args = append(args, filter.JobType)
	}
	if filter.Enabled != nil {
		where += ` AND enabled = ?`
		args = append(args, *filter.Enabled)
	}
	var total int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*)`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	direction := "ASC"
	if filter.Desc {
		direction = "DESC"
	}
	queryArgs := append(append([]any(nil), args...), filter.Limit, filter.Offset)
	rows, err := s.db.QueryContext(ctx, `SELECT `+operationJobScheduleColumns+where+
		` ORDER BY `+filter.Sort+` `+direction+`, id `+direction+` LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]OperationJobSchedule, 0)
	for rows.Next() {
		item, err := scanOperationJobSchedule(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *MySQLStore) GetOperationJobSchedule(ctx context.Context, tenantID, scheduleID ID) (OperationJobSchedule, error) {
	return scanOperationJobSchedule(s.db.QueryRowContext(ctx, `
		SELECT `+operationJobScheduleColumns+`
		FROM operation_job_schedules
		WHERE tenant_id = ? AND scope_type = 'tenant' AND id = ?
	`, tenantID, scheduleID))
}

func (s *MySQLStore) CreateOperationJobSchedule(ctx context.Context, item OperationJobSchedule) (OperationJobSchedule, error) {
	item, err := normalizeOperationJobSchedule(item, time.Now())
	if err != nil {
		return OperationJobSchedule{}, err
	}
	if item.ID == "" {
		item.ID, err = newIdentityID()
		if err != nil {
			return OperationJobSchedule{}, err
		}
	}
	var tenantArg any
	if item.TenantID != "" {
		tenantArg = item.TenantID
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO operation_job_schedules (
			id, tenant_id, scope_type, name, job_type, partition_key, cron_expression,
			timezone, payload_json, enabled, max_inflight, next_run_at, created_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''))
	`, item.ID, tenantArg, item.ScopeType, item.Name, item.JobType, item.PartitionKey,
		item.CronExpression, item.Timezone, string(item.PayloadJSON), item.Enabled,
		item.MaxInflight, item.NextRunAt.UTC(), item.CreatedBy)
	if err != nil {
		return OperationJobSchedule{}, normalizeOperationJobScheduleWriteError(err)
	}
	return getOperationJobScheduleByScope(ctx, s.db, item.ScopeType, item.TenantID, item.ID)
}

// EnsureSystemOperationJobSchedule creates or converges one runtime-owned
// system schedule. It preserves next_run_at when the desired definition is
// unchanged, so a hub restart cannot postpone already due work.
func (s *MySQLStore) EnsureSystemOperationJobSchedule(ctx context.Context, desired OperationJobSchedule) (OperationJobSchedule, error) {
	desired.ScopeType, desired.TenantID = OperationJobScopeSystem, ""
	var err error
	desired, err = normalizeOperationJobSchedule(desired, time.Now())
	if err != nil {
		return OperationJobSchedule{}, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		current, err := scanOperationJobSchedule(s.db.QueryRowContext(ctx, `
			SELECT `+operationJobScheduleColumns+`
			FROM operation_job_schedules
			WHERE tenant_id IS NULL AND scope_type = 'system' AND job_type = ? AND partition_key = ?
		`, desired.JobType, desired.PartitionKey))
		if errors.Is(err, sql.ErrNoRows) {
			created, createErr := s.CreateOperationJobSchedule(ctx, desired)
			if errors.Is(createErr, ErrOperationJobScheduleExists) {
				continue
			}
			return created, createErr
		}
		if err != nil {
			return OperationJobSchedule{}, err
		}
		if current.Name == desired.Name && current.CronExpression == desired.CronExpression && current.Timezone == desired.Timezone &&
			operationJobSchedulePayloadEqual(current.PayloadJSON, desired.PayloadJSON) && current.Enabled == desired.Enabled && current.MaxInflight == desired.MaxInflight {
			return current, nil
		}
		desired.ID = current.ID
		updated, updateErr := s.UpdateOperationJobSchedule(ctx, desired, current.RowVersion)
		if errors.Is(updateErr, ErrOperationJobScheduleConflict) {
			continue
		}
		return updated, updateErr
	}
	return OperationJobSchedule{}, ErrOperationJobScheduleConflict
}

func operationJobSchedulePayloadEqual(left, right json.RawMessage) bool {
	decode := func(raw json.RawMessage) (any, error) {
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		return value, nil
	}
	leftValue, leftErr := decode(left)
	rightValue, rightErr := decode(right)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(leftValue, rightValue)
}

// DisableSystemOperationJobSchedulesExcept retires stale runtime-owned system
// schedules after a stream/config replacement. Passing an empty keep ID
// disables the job type entirely.
func (s *MySQLStore) DisableSystemOperationJobSchedulesExcept(ctx context.Context, jobType string, keep ID) error {
	jobType = strings.TrimSpace(jobType)
	if jobType == "" {
		return errors.New("system operation schedule job type is required")
	}
	query := `UPDATE operation_job_schedules
		SET enabled = 0, last_error_detail = 'disabled by runtime configuration',
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id IS NULL AND scope_type = 'system' AND job_type = ? AND enabled = 1`
	args := []any{jobType}
	if keep != "" {
		query += ` AND id <> ?`
		args = append(args, keep)
	}
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *MySQLStore) UpdateOperationJobSchedule(ctx context.Context, item OperationJobSchedule, expectedVersion uint64) (OperationJobSchedule, error) {
	if expectedVersion == 0 {
		return OperationJobSchedule{}, ErrOperationJobScheduleConflict
	}
	item, err := normalizeOperationJobSchedule(item, time.Now())
	if err != nil {
		return OperationJobSchedule{}, err
	}
	var tenantArg any
	if item.TenantID != "" {
		tenantArg = item.TenantID
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE operation_job_schedules
		SET name = ?, job_type = ?, partition_key = ?, cron_expression = ?, timezone = ?,
			payload_json = ?, enabled = ?, max_inflight = ?, next_run_at = ?,
			last_error_detail = NULL, row_version = row_version + 1,
			updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND tenant_id <=> ? AND scope_type = ? AND row_version = ?
	`, item.Name, item.JobType, item.PartitionKey, item.CronExpression, item.Timezone,
		string(item.PayloadJSON), item.Enabled, item.MaxInflight, item.NextRunAt.UTC(),
		item.ID, tenantArg, item.ScopeType, expectedVersion)
	if err != nil {
		return OperationJobSchedule{}, normalizeOperationJobScheduleWriteError(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return OperationJobSchedule{}, err
	}
	if affected == 0 {
		if _, getErr := getOperationJobScheduleByScope(ctx, s.db, item.ScopeType, item.TenantID, item.ID); errors.Is(getErr, sql.ErrNoRows) {
			return OperationJobSchedule{}, sql.ErrNoRows
		} else if getErr != nil {
			return OperationJobSchedule{}, getErr
		}
		return OperationJobSchedule{}, ErrOperationJobScheduleConflict
	}
	return getOperationJobScheduleByScope(ctx, s.db, item.ScopeType, item.TenantID, item.ID)
}

func (s *MySQLStore) DeleteOperationJobSchedule(ctx context.Context, tenantID, scheduleID ID, expectedVersion uint64) error {
	if expectedVersion == 0 {
		return ErrOperationJobScheduleConflict
	}
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM operation_job_schedules
		WHERE tenant_id = ? AND scope_type = 'tenant' AND id = ? AND row_version = ?
	`, tenantID, scheduleID, expectedVersion)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return nil
	}
	if _, getErr := s.GetOperationJobSchedule(ctx, tenantID, scheduleID); errors.Is(getErr, sql.ErrNoRows) {
		return sql.ErrNoRows
	} else if getErr != nil {
		return getErr
	}
	return ErrOperationJobScheduleConflict
}

func getOperationJobScheduleByScope(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, scope string, tenantID, scheduleID ID) (OperationJobSchedule, error) {
	var tenantArg any
	if tenantID != "" {
		tenantArg = tenantID
	}
	return scanOperationJobSchedule(queryer.QueryRowContext(ctx, `
		SELECT `+operationJobScheduleColumns+`
		FROM operation_job_schedules
		WHERE tenant_id <=> ? AND scope_type = ? AND id = ?
	`, tenantArg, scope, scheduleID))
}

func normalizeOperationJobScheduleWriteError(err error) error {
	var mysqlErr *mysqldriver.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return ErrOperationJobScheduleExists
	}
	return err
}

func (s *MySQLStore) DispatchDueOperationJobSchedules(ctx context.Context, scannerKey string, now time.Time, scanLimit, dispatchLimit int, typeBudgets map[string]int) (OperationJobScheduleDispatchResult, error) {
	var result OperationJobScheduleDispatchResult
	if strings.TrimSpace(scannerKey) == "" || len(scannerKey) > 64 || scanLimit <= 0 || scanLimit > 1000 || dispatchLimit <= 0 || dispatchLimit > scanLimit {
		return result, errors.New("invalid operation schedule dispatch bounds")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO operation_job_scheduler_state (scanner_key) VALUES (?)
		ON DUPLICATE KEY UPDATE scanner_key = scanner_key
	`, scannerKey); err != nil {
		return result, err
	}
	var cursorDue sql.NullTime
	var cursorID sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT cursor_due_at, cursor_schedule_id
		FROM operation_job_scheduler_state WHERE scanner_key = ? FOR UPDATE
	`, scannerKey).Scan(&cursorDue, &cursorID); err != nil {
		return result, err
	}
	items, err := selectDueOperationJobSchedules(ctx, tx, now, scanLimit, cursorDue, cursorID)
	if err != nil {
		return result, err
	}
	if len(items) == 0 && cursorDue.Valid {
		items, err = selectDueOperationJobSchedules(ctx, tx, now, scanLimit, sql.NullTime{}, sql.NullString{})
		if err != nil {
			return result, err
		}
	}
	typeInflight := make(map[string]int)
	var last *OperationJobSchedule
	for i := range items {
		if result.Enqueued >= dispatchLimit {
			break
		}
		item := items[i]
		last = &item
		result.Scanned++
		budget := typeBudgets[item.JobType]
		if budget <= 0 {
			result.Unregistered++
			if err := setOperationJobScheduleError(ctx, tx, item.ID, "job type is not registered"); err != nil {
				return result, err
			}
			continue
		}
		inflight, ok := typeInflight[item.JobType]
		if !ok {
			if err := tx.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM operation_jobs
				WHERE job_type = ? AND status IN ('queued','running','paused','validating','cancel_requested')
			`, item.JobType).Scan(&inflight); err != nil {
				return result, err
			}
			typeInflight[item.JobType] = inflight
		}
		var scheduleInflight int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM operation_jobs
			WHERE schedule_id = ? AND status IN ('queued','running','paused','validating','cancel_requested')
		`, item.ID).Scan(&scheduleInflight); err != nil {
			return result, err
		}
		if inflight >= budget || scheduleInflight >= int(item.MaxInflight) {
			result.Backpressured++
			if err := setOperationJobScheduleError(ctx, tx, item.ID, "dispatch deferred by inflight budget"); err != nil {
				return result, err
			}
			continue
		}
		next, err := nextOperationJobScheduleTime(item.CronExpression, item.Timezone, now)
		if err != nil {
			result.Unregistered++
			if _, updateErr := tx.ExecContext(ctx, `
				UPDATE operation_job_schedules
				SET enabled = 0, last_error_detail = LEFT(?, 1024), row_version = row_version + 1
				WHERE id = ?
			`, err.Error(), item.ID); updateErr != nil {
				return result, updateErr
			}
			continue
		}
		if err := enqueueScheduledOperationJob(ctx, tx, item, now); err != nil {
			return result, err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE operation_job_schedules
			SET next_run_at = ?, last_enqueued_at = ?, last_error_detail = NULL,
				row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
			WHERE id = ?
		`, next.UTC(), now.UTC(), item.ID); err != nil {
			return result, err
		}
		typeInflight[item.JobType] = inflight + 1
		result.Enqueued++
	}
	if last != nil {
		if _, err := tx.ExecContext(ctx, `
			UPDATE operation_job_scheduler_state
			SET cursor_due_at = ?, cursor_schedule_id = ?, row_version = row_version + 1
			WHERE scanner_key = ?
		`, last.NextRunAt.UTC(), last.ID, scannerKey); err != nil {
			return result, err
		}
	}
	if err := tx.Commit(); err != nil {
		return result, err
	}
	return result, nil
}

func selectDueOperationJobSchedules(ctx context.Context, tx *sql.Tx, now time.Time, limit int, cursorDue sql.NullTime, cursorID sql.NullString) ([]OperationJobSchedule, error) {
	query := `SELECT ` + operationJobScheduleColumns + `
		FROM operation_job_schedules
		WHERE enabled = 1 AND next_run_at <= ?`
	args := []any{now.UTC()}
	if cursorDue.Valid {
		query += ` AND (next_run_at > ? OR (next_run_at = ? AND id > ?))`
		args = append(args, cursorDue.Time.UTC(), cursorDue.Time.UTC(), cursorID.String)
	}
	query += ` ORDER BY next_run_at, id LIMIT ? FOR UPDATE`
	args = append(args, limit)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	items := make([]OperationJobSchedule, 0, limit)
	for rows.Next() {
		item, err := scanOperationJobSchedule(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	return items, rows.Close()
}

func setOperationJobScheduleError(ctx context.Context, tx *sql.Tx, scheduleID ID, detail string) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE operation_job_schedules
		SET last_error_detail = LEFT(?, 1024), row_version = row_version + 1,
			updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND COALESCE(last_error_detail, '') <> LEFT(?, 1024)
	`, detail, scheduleID, detail)
	return err
}

func enqueueScheduledOperationJob(ctx context.Context, tx *sql.Tx, schedule OperationJobSchedule, scheduledAt time.Time) error {
	jobID, err := newIdentityID()
	if err != nil {
		return err
	}
	idempotencyKey := fmt.Sprintf("schedule:%s:%d", schedule.ID, schedule.NextRunAt.UTC().Unix())
	requestHash, err := scheduledOperationJobRequestHash(schedule)
	if err != nil {
		return err
	}
	var tenantArg any
	if schedule.TenantID != "" {
		tenantArg = schedule.TenantID
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO operation_jobs (
			id, tenant_id, scope_type, schedule_id, job_type, status, idempotency_key,
			request_hash, checkpoint_json, created_by, next_attempt_at
		) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?, NULLIF(?, ''), ?)
		ON DUPLICATE KEY UPDATE id = id
	`, jobID, tenantArg, schedule.ScopeType, schedule.ID, schedule.JobType,
		idempotencyKey, requestHash, string(schedule.PayloadJSON), schedule.CreatedBy, scheduledAt.UTC())
	if err != nil {
		return err
	}
	var storedHash string
	if err := tx.QueryRowContext(ctx, `
		SELECT request_hash FROM operation_jobs
		WHERE tenant_id <=> ? AND job_type = ? AND idempotency_key = ?
	`, tenantArg, schedule.JobType, idempotencyKey).Scan(&storedHash); err != nil {
		return err
	}
	if storedHash != requestHash {
		return ErrOperationJobHashMismatch
	}
	return nil
}

func scheduledOperationJobRequestHash(schedule OperationJobSchedule) (string, error) {
	canonical, err := json.Marshal(struct {
		Version      int             `json:"version"`
		ScheduleID   ID              `json:"schedule_id"`
		ScheduledAt  string          `json:"scheduled_at"`
		JobType      string          `json:"job_type"`
		PartitionKey string          `json:"partition_key"`
		Payload      json.RawMessage `json:"payload"`
	}{1, schedule.ID, schedule.NextRunAt.UTC().Format(time.RFC3339Nano), schedule.JobType, schedule.PartitionKey, schedule.PayloadJSON})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func (s *MySQLStore) GetSystemOperationJobWatermark(ctx context.Context, jobType, partitionKey string) (uint64, error) {
	value, _, err := s.LookupSystemOperationJobWatermark(ctx, jobType, partitionKey)
	return value, err
}

// LookupSystemOperationJobWatermark distinguishes a persisted zero from no
// watermark. Reconciliation needs that distinction: silently treating a
// missing cutover as offset zero creates false loss alarms for an established
// consumer group.
func (s *MySQLStore) LookupSystemOperationJobWatermark(ctx context.Context, jobType, partitionKey string) (uint64, bool, error) {
	if strings.TrimSpace(jobType) == "" || strings.TrimSpace(partitionKey) == "" {
		return 0, false, errors.New("system operation job watermark type and partition are required")
	}
	var value uint64
	err := s.db.QueryRowContext(ctx, `
		SELECT watermark_value FROM operation_job_system_watermarks
		WHERE job_type = ? AND partition_key = ?
	`, jobType, partitionKey).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	return value, err == nil, err
}

func (s *MySQLStore) AdvanceSystemOperationJobWatermark(ctx context.Context, jobType, partitionKey string, value uint64) error {
	if strings.TrimSpace(jobType) == "" || strings.TrimSpace(partitionKey) == "" {
		return errors.New("system operation job watermark type and partition are required")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO operation_job_system_watermarks (job_type, partition_key, watermark_value)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE watermark_value = GREATEST(watermark_value, VALUES(watermark_value)),
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
	`, jobType, partitionKey, value)
	return err
}
