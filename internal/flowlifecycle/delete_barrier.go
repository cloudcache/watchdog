// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowlifecycle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowtombstone"
)

var ErrDeleteBarrierPending = errors.New("raw Flow deletion barrier is not installed by every required worker")

type DeleteBarrierStatus struct {
	Barrier          flowtombstone.Barrier `json:"barrier"`
	RequiredWorkers  uint64                `json:"required_workers"`
	InstalledWorkers uint64                `json:"installed_workers"`
	FailedWorkers    uint64                `json:"failed_workers"`
	Ready            bool                  `json:"ready"`
}

func (store *Store) LatestRawDeleteBarrier(ctx context.Context) (flowtombstone.Barrier, error) {
	if store == nil || store.db == nil {
		return flowtombstone.Barrier{}, ErrInvalidPolicy
	}
	return scanRawDeleteBarrier(store.db.QueryRowContext(ctx, `SELECT id,schema_version,revision,deleted_through,exception_days_json,published_at
		FROM flow_raw_delete_barriers ORDER BY revision DESC LIMIT 1`))
}

func scanRawDeleteBarrier(row rowScanner) (flowtombstone.Barrier, error) {
	var barrier flowtombstone.Barrier
	var through time.Time
	var exceptions []byte
	if err := row.Scan(&barrier.ID, &barrier.SchemaVersion, &barrier.Revision, &through, &exceptions, &barrier.PublishedAt); err != nil {
		return flowtombstone.Barrier{}, err
	}
	barrier.DeletedThrough = through.UTC().Format(time.DateOnly)
	if err := json.Unmarshal(exceptions, &barrier.ExceptionDays); err != nil {
		return flowtombstone.Barrier{}, err
	}
	if err := barrier.Validate(); err != nil {
		return flowtombstone.Barrier{}, err
	}
	return barrier, nil
}

// EnsureRawDeleteBarrierForApproval publishes (or reuses) the tombstone that
// covers an approved UTC day. Active flow workers are snapshotted as required
// ACK targets in the same transaction.
func (store *Store) EnsureRawDeleteBarrierForApproval(ctx context.Context, approvalID string, expectedVersion uint64, actor string, now time.Time) (DeleteBarrierStatus, error) {
	if store == nil || store.db == nil || strings.TrimSpace(approvalID) == "" || expectedVersion == 0 || strings.TrimSpace(actor) == "" || now.IsZero() {
		return DeleteBarrierStatus{}, ErrInvalidDeletionApproval
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return DeleteBarrierStatus{}, err
	}
	defer tx.Rollback()
	approval, err := scanDeletionApproval(tx.QueryRowContext(ctx, "SELECT "+deletionApprovalColumns+" FROM flow_deletion_approvals WHERE id=? FOR UPDATE", approvalID))
	if err != nil {
		return DeleteBarrierStatus{}, err
	}
	if approval.RowVersion != expectedVersion {
		return DeleteBarrierStatus{}, ErrVersionConflict
	}
	if approval.Status != "approved" || approval.StorageKind != "raw" || approval.PartitionGranularity != "day" {
		return DeleteBarrierStatus{}, ErrDeleteLocked
	}
	current, currentErr := scanRawDeleteBarrier(tx.QueryRowContext(ctx, `SELECT id,schema_version,revision,deleted_through,exception_days_json,published_at
		FROM flow_raw_delete_barriers ORDER BY revision DESC LIMIT 1 FOR UPDATE`))
	if currentErr != nil && !errors.Is(currentErr, sql.ErrNoRows) {
		return DeleteBarrierStatus{}, currentErr
	}
	if currentErr == nil {
		if _, covered := current.Covers(approval.PartitionStart); covered {
			if err := tx.Commit(); err != nil {
				return DeleteBarrierStatus{}, err
			}
			return store.RawDeleteBarrierStatus(ctx, current.ID)
		}
	}
	revision := uint64(1)
	var previous *flowtombstone.Barrier
	if currentErr == nil {
		revision = current.Revision + 1
		previous = &current
	}
	barrier, err := flowtombstone.Advance(previous, UTCDate(approval.PartitionStart), newID(), revision, now)
	if err != nil {
		return DeleteBarrierStatus{}, err
	}
	exceptions, err := json.Marshal(barrier.ExceptionDays)
	if err != nil {
		return DeleteBarrierStatus{}, err
	}
	through, _ := time.Parse(time.DateOnly, barrier.DeletedThrough)
	if _, err := tx.ExecContext(ctx, `INSERT INTO flow_raw_delete_barriers
		(id,schema_version,revision,deleted_through,exception_days_json,published_by,published_at)
		VALUES (?,?,?,?,CAST(? AS JSON),?,?)`, barrier.ID, barrier.SchemaVersion, barrier.Revision, through, exceptions, actor, barrier.PublishedAt); err != nil {
		return DeleteBarrierStatus{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO flow_raw_delete_barrier_acks (barrier_id,worker_id,required,state)
		SELECT ?,id,1,'pending' FROM agents WHERE kind='flow_worker' AND status='active'`, barrier.ID); err != nil {
		return DeleteBarrierStatus{}, err
	}
	if err := tx.Commit(); err != nil {
		return DeleteBarrierStatus{}, err
	}
	return store.RawDeleteBarrierStatus(ctx, barrier.ID)
}

func (store *Store) RawDeleteBarrierStatus(ctx context.Context, barrierID string) (DeleteBarrierStatus, error) {
	if store == nil || store.db == nil || strings.TrimSpace(barrierID) == "" {
		return DeleteBarrierStatus{}, ErrInvalidPolicy
	}
	barrier, err := scanRawDeleteBarrier(store.db.QueryRowContext(ctx, `SELECT id,schema_version,revision,deleted_through,exception_days_json,published_at
		FROM flow_raw_delete_barriers WHERE id=?`, barrierID))
	if err != nil {
		return DeleteBarrierStatus{}, err
	}
	status := DeleteBarrierStatus{Barrier: barrier}
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(a.state='installed'),0),COALESCE(SUM(a.state='failed'),0)
		FROM agents w LEFT JOIN flow_raw_delete_barrier_acks a ON a.barrier_id=? AND a.worker_id=w.id
		WHERE w.kind='flow_worker' AND w.status='active'`, barrierID).Scan(&status.RequiredWorkers, &status.InstalledWorkers, &status.FailedWorkers); err != nil {
		return DeleteBarrierStatus{}, err
	}
	status.Ready = status.InstalledWorkers == status.RequiredWorkers && status.FailedWorkers == 0
	return status, nil
}

func rawDeleteBarrierReadyTx(ctx context.Context, tx *sql.Tx, day time.Time) error {
	barrier, err := scanRawDeleteBarrier(tx.QueryRowContext(ctx, `SELECT id,schema_version,revision,deleted_through,exception_days_json,published_at
		FROM flow_raw_delete_barriers ORDER BY revision DESC LIMIT 1 FOR UPDATE`))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrDeleteBarrierPending
		}
		return err
	}
	if _, covered := barrier.Covers(UTCDate(day)); !covered {
		return ErrDeleteBarrierPending
	}
	var required, installed, failed uint64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(a.state='installed'),0),COALESCE(SUM(a.state='failed'),0)
		FROM agents w LEFT JOIN flow_raw_delete_barrier_acks a ON a.barrier_id=? AND a.worker_id=w.id
		WHERE w.kind='flow_worker' AND w.status='active'`, barrier.ID).Scan(&required, &installed, &failed); err != nil {
		return err
	}
	if installed != required || failed != 0 {
		return ErrDeleteBarrierPending
	}
	return nil
}

func (store *Store) RawDeleteBarrierReadyForDay(ctx context.Context, day time.Time) (DeleteBarrierStatus, error) {
	barrier, err := store.LatestRawDeleteBarrier(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DeleteBarrierStatus{}, ErrDeleteBarrierPending
		}
		return DeleteBarrierStatus{}, err
	}
	if _, covered := barrier.Covers(UTCDate(day)); !covered {
		return DeleteBarrierStatus{Barrier: barrier}, ErrDeleteBarrierPending
	}
	status, err := store.RawDeleteBarrierStatus(ctx, barrier.ID)
	if err != nil {
		return DeleteBarrierStatus{}, err
	}
	if !status.Ready {
		return status, ErrDeleteBarrierPending
	}
	return status, nil
}

func (store *Store) AcknowledgeRawDeleteBarrier(ctx context.Context, barrierID, workerID, bootID, softwareVersion, state, errorCode, errorMessage string, revision uint64, now time.Time) error {
	if store == nil || store.db == nil || barrierID == "" || workerID == "" || bootID == "" || softwareVersion == "" || revision == 0 || now.IsZero() || (state != "installed" && state != "failed") {
		return flowtombstone.ErrInvalidBarrier
	}
	if state == "failed" && strings.TrimSpace(errorCode) == "" || state == "installed" && (errorCode != "" || errorMessage != "") {
		return flowtombstone.ErrInvalidBarrier
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var storedRevision uint64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM flow_raw_delete_barriers WHERE id=? FOR UPDATE`, barrierID).Scan(&storedRevision); err != nil {
		return err
	}
	if storedRevision != revision {
		return flowtombstone.ErrInvalidBarrier
	}
	var existingState string
	existingErr := tx.QueryRowContext(ctx, `SELECT state FROM flow_raw_delete_barrier_acks WHERE barrier_id=? AND worker_id=? FOR UPDATE`, barrierID, workerID).Scan(&existingState)
	if existingErr != nil && !errors.Is(existingErr, sql.ErrNoRows) {
		return existingErr
	}
	if existingState == "installed" && state == "failed" {
		return flowtombstone.ErrInvalidBarrier
	}
	installedAt := any(nil)
	if state == "installed" {
		installedAt = now.UTC().Truncate(time.Millisecond)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO flow_raw_delete_barrier_acks
		(barrier_id,worker_id,required,state,boot_id,software_version,attempted_at,installed_at,error_code,error_message)
		VALUES (?,?,0,?,?,?,?,?,NULLIF(?,''),NULLIF(?,''))
		ON DUPLICATE KEY UPDATE state=VALUES(state),boot_id=VALUES(boot_id),software_version=VALUES(software_version),
		attempted_at=VALUES(attempted_at),installed_at=VALUES(installed_at),error_code=VALUES(error_code),error_message=VALUES(error_message),row_version=row_version+1`,
		barrierID, workerID, state, bootID, softwareVersion, now.UTC().Truncate(time.Millisecond), installedAt, errorCode, errorMessage)
	if err != nil {
		return err
	}
	return tx.Commit()
}
