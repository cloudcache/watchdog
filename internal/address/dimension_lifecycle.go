package address

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

const (
	DimensionPublicationActivationPublish  = "publish"
	DimensionPublicationActivationRollback = "rollback"
	DimensionPublicationAckDownloaded      = "downloaded"
	DimensionPublicationAckInstalled       = "installed"
	DimensionPublicationAckFailed          = "failed"

	AddressDimensionActivationPublish  = DimensionPublicationActivationPublish
	AddressDimensionActivationRollback = DimensionPublicationActivationRollback
	AddressDimensionAckDownloaded      = DimensionPublicationAckDownloaded
	AddressDimensionAckInstalled       = DimensionPublicationAckInstalled
	AddressDimensionAckFailed          = DimensionPublicationAckFailed
)

var ErrAddressDimensionInvalidTransition = errors.New("address dimension lifecycle transition is invalid")

type DimensionPublicationActivationRequest struct {
	SnapshotID         ID
	EffectiveFrom      time.Time
	ExpectedRowVersion uint64
}

type AddressDimensionActivationRequest = DimensionPublicationActivationRequest

type DimensionPublicationRollbackRequest struct {
	SnapshotID         ID
	EffectiveFrom      time.Time
	ExpectedRowVersion uint64
}

type AddressDimensionRollbackRequest = DimensionPublicationRollbackRequest

type DimensionPublicationRetireRequest struct {
	SnapshotID         ID
	ExpectedRowVersion uint64
	Reason             string
}

type AddressDimensionRetireRequest = DimensionPublicationRetireRequest

// ApproveDimensionPublication moves a pending publication to approved. The
// checksum-only build has no signature envelope; consumers authenticate the
// object by its stored checksum.
func (p *Publisher) ApproveDimensionPublication(ctx context.Context, actorID, snapshotID ID, expectedRowVersion uint64) (DimensionPublicationSnapshot, error) {
	if p == nil || p.store == nil || actorID == "" || snapshotID == "" || expectedRowVersion == 0 {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	decidedAt := p.now().UTC()
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, snapshotID, true)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if snapshot.RowVersion != expectedRowVersion {
		return AddressDimensionSnapshot{}, ErrAddressDimensionConflict
	}
	if snapshot.Status != AddressDimensionStatusActive || snapshot.ApprovalState != AddressDimensionApprovalPending || snapshot.ObjectDeletedAt != nil {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalidTransition
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE dimension_snapshots
		SET approval_state = 'approved', decided_by = ?, decided_at = ?, decision_reason = NULL, row_version = row_version + 1
		WHERE module_key = ? AND dimension_key = ? AND id = ? AND row_version = ?
	`, actorID, decidedAt, p.scope.ModuleKey, p.scope.DimensionKey, snapshotID, expectedRowVersion)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, actorID, snapshot.ID, "dimension.snapshot.approved", map[string]any{
		"version": snapshot.Version, "checksum": snapshot.Checksum,
	}); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	return p.GetDimensionPublicationSnapshot(ctx, snapshot.ID)
}

func (p *Publisher) RejectDimensionPublication(ctx context.Context, actorID, snapshotID ID, expectedRowVersion uint64, reason string) (DimensionPublicationSnapshot, error) {
	reason = strings.TrimSpace(reason)
	if p == nil || p.store == nil || actorID == "" || snapshotID == "" || expectedRowVersion == 0 || reason == "" || len(reason) > 512 {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	decidedAt := p.now().UTC()
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, snapshotID, true)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if snapshot.RowVersion != expectedRowVersion {
		return AddressDimensionSnapshot{}, ErrAddressDimensionConflict
	}
	if snapshot.Status != AddressDimensionStatusActive || snapshot.ApprovalState != AddressDimensionApprovalPending || snapshot.ObjectDeletedAt != nil {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalidTransition
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE dimension_snapshots
		SET approval_state = 'rejected', decided_by = ?, decided_at = ?, decision_reason = ?, row_version = row_version + 1
		WHERE module_key = ? AND dimension_key = ? AND id = ? AND row_version = ?
	`, actorID, decidedAt, reason, p.scope.ModuleKey, p.scope.DimensionKey, snapshotID, expectedRowVersion)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, actorID, snapshotID, "dimension.snapshot.rejected", map[string]any{"reason": reason}); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	return p.GetDimensionPublicationSnapshot(ctx, snapshotID)
}

func (p *Publisher) ActivateDimensionPublication(ctx context.Context, actorID ID, request DimensionPublicationActivationRequest) (DimensionPublicationActivation, error) {
	if p == nil || p.store == nil || actorID == "" || request.SnapshotID == "" || request.ExpectedRowVersion == 0 || !isUTCMinute(request.EffectiveFrom) {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublication(ctx, tx); err != nil {
		return AddressDimensionActivation{}, err
	}
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, request.SnapshotID, true)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	if snapshot.RowVersion != request.ExpectedRowVersion {
		return AddressDimensionActivation{}, ErrAddressDimensionConflict
	}
	if snapshot.Status != AddressDimensionStatusActive || snapshot.ApprovalState != AddressDimensionApprovalApproved || snapshot.ObjectDeletedAt != nil || !snapshot.EffectiveFrom.Equal(request.EffectiveFrom) {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalidTransition
	}
	activation, err := insertDimensionPublicationActivation(ctx, tx, snapshot, actorID, request.EffectiveFrom, AddressDimensionActivationPublish, "")
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE dimension_snapshots SET row_version = row_version + 1
		WHERE module_key = ? AND dimension_key = ? AND id = ? AND row_version = ?`,
		p.scope.ModuleKey, p.scope.DimensionKey, snapshot.ID, request.ExpectedRowVersion)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionActivation{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, actorID, snapshot.ID, "dimension.snapshot.activated", map[string]any{
		"activation_id": activation.ID, "effective_from": activation.EffectiveFrom, "version": snapshot.Version,
	}); err != nil {
		return AddressDimensionActivation{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionActivation{}, err
	}
	return activation, nil
}

func (p *Publisher) RollbackDimensionPublication(ctx context.Context, actorID ID, request DimensionPublicationRollbackRequest) (DimensionPublicationActivation, error) {
	if p == nil || p.store == nil || actorID == "" || request.SnapshotID == "" || request.ExpectedRowVersion == 0 || !isUTCMinute(request.EffectiveFrom) {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublication(ctx, tx); err != nil {
		return AddressDimensionActivation{}, err
	}
	target, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, request.SnapshotID, true)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	if target.RowVersion != request.ExpectedRowVersion {
		return AddressDimensionActivation{}, ErrAddressDimensionConflict
	}
	now := p.now().UTC()
	if target.ApprovalState != AddressDimensionApprovalApproved || target.ObjectDeletedAt != nil ||
		(target.Status == AddressDimensionStatusRetired && target.RetentionUntil != nil && !now.Before(*target.RetentionUntil)) ||
		request.EffectiveFrom.Before(target.EffectiveFrom) {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalidTransition
	}
	current, err := getDimensionPublicationActivationBeforeTx(ctx, tx, p.scope, request.EffectiveFrom)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	if current.SnapshotID == target.ID {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalidTransition
	}
	activation, err := insertDimensionPublicationActivation(ctx, tx, target, actorID, request.EffectiveFrom, AddressDimensionActivationRollback, current.SnapshotID)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE dimension_snapshots
		SET status = 'active', retired_by = NULL, retired_at = NULL, retention_until = NULL, row_version = row_version + 1
		WHERE module_key = ? AND dimension_key = ? AND id = ? AND row_version = ?
	`, p.scope.ModuleKey, p.scope.DimensionKey, target.ID, request.ExpectedRowVersion)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionActivation{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, actorID, target.ID, "dimension.snapshot.rolled_back", map[string]any{
		"activation_id": activation.ID, "rollback_of_snapshot_id": current.SnapshotID,
		"effective_from": activation.EffectiveFrom, "version": target.Version,
	}); err != nil {
		return AddressDimensionActivation{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionActivation{}, err
	}
	return activation, nil
}

func (p *Publisher) RetireDimensionPublication(ctx context.Context, actorID ID, request DimensionPublicationRetireRequest) (DimensionPublicationSnapshot, error) {
	request.Reason = strings.TrimSpace(request.Reason)
	if p == nil || p.store == nil || actorID == "" || request.SnapshotID == "" || request.ExpectedRowVersion == 0 || len(request.Reason) > 512 {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublication(ctx, tx); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, request.SnapshotID, true)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if snapshot.RowVersion != request.ExpectedRowVersion {
		return AddressDimensionSnapshot{}, ErrAddressDimensionConflict
	}
	if snapshot.Status != AddressDimensionStatusActive {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalidTransition
	}
	latest, err := getLatestDimensionPublicationActivationTx(ctx, tx, p.scope)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return AddressDimensionSnapshot{}, err
	}
	if err == nil && latest.SnapshotID == snapshot.ID {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalidTransition
	}
	retiredAt := p.now().UTC().Truncate(time.Millisecond)
	retentionUntil := retiredAt.Add(p.objectRetention).UTC().Truncate(time.Millisecond)
	var latestReference sql.NullTime
	if err := tx.QueryRowContext(ctx, `
		SELECT MAX(retain_until) FROM dimension_snapshot_references WHERE snapshot_id = ?
	`, snapshot.ID).Scan(&latestReference); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if latestReference.Valid && latestReference.Time.After(retentionUntil) {
		retentionUntil = latestReference.Time.UTC()
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE dimension_snapshots
		SET status = 'retired', retired_by = ?, retired_at = ?, retention_until = ?, row_version = row_version + 1
		WHERE module_key = ? AND dimension_key = ? AND id = ? AND row_version = ?
	`, actorID, retiredAt, retentionUntil, p.scope.ModuleKey, p.scope.DimensionKey, snapshot.ID, request.ExpectedRowVersion)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, actorID, snapshot.ID, "dimension.snapshot.retired", map[string]any{
		"reason": request.Reason, "version": snapshot.Version, "retention_until": retentionUntil,
	}); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	return p.GetDimensionPublicationSnapshot(ctx, snapshot.ID)
}

func (p *Publisher) GetDimensionPublicationActivationAt(ctx context.Context, eventTime time.Time) (DimensionPublicationActivation, error) {
	if p == nil || p.store == nil || eventTime.IsZero() {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalid
	}
	return scanDimensionPublicationActivation(p.store.db.QueryRowContext(ctx, `
		SELECT id, module_key, dimension_key, snapshot_id, effective_from,
		       reason, COALESCE(rollback_of_snapshot_id, ''), COALESCE(created_by, ''), created_at
		FROM dimension_snapshot_activations
		WHERE module_key = ? AND dimension_key = ? AND effective_from <= ?
		ORDER BY effective_from DESC LIMIT 1
	`, p.scope.ModuleKey, p.scope.DimensionKey, eventTime.UTC()))
}

func (p *Publisher) ReportDimensionPublicationAcknowledgement(ctx context.Context, ack DimensionPublicationAcknowledgement) (DimensionPublicationAcknowledgement, error) {
	ack.WorkerID = strings.TrimSpace(ack.WorkerID)
	ack.BootID = strings.TrimSpace(ack.BootID)
	ack.SoftwareVersion = strings.TrimSpace(ack.SoftwareVersion)
	ack.ErrorCode = strings.TrimSpace(ack.ErrorCode)
	ack.ErrorMessage = strings.TrimSpace(ack.ErrorMessage)
	if p == nil || p.store == nil || ack.SnapshotID == "" || ack.WorkerID == "" || len(ack.WorkerID) > 128 || ack.BootID == "" || len(ack.BootID) > 128 || ack.SoftwareVersion == "" || len(ack.SoftwareVersion) > 64 || len(ack.ErrorCode) > 64 || len(ack.ErrorMessage) > 512 || !validAddressDimensionAckState(ack.State) ||
		(ack.State == AddressDimensionAckFailed && ack.ErrorCode == "") ||
		(ack.State != AddressDimensionAckFailed && (ack.ErrorCode != "" || ack.ErrorMessage != "")) {
		return AddressDimensionAcknowledgement{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionAcknowledgement{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublication(ctx, tx); err != nil {
		return AddressDimensionAcknowledgement{}, err
	}
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, ack.SnapshotID, true)
	if err != nil {
		return AddressDimensionAcknowledgement{}, err
	}
	attemptedAt := p.now().UTC().Truncate(time.Millisecond)
	if ack.Checksum != snapshot.Checksum || snapshot.ObjectDeletedAt != nil ||
		(snapshot.Status == AddressDimensionStatusRetired && snapshot.RetentionUntil != nil && !attemptedAt.Before(*snapshot.RetentionUntil)) {
		return AddressDimensionAcknowledgement{}, ErrAddressDimensionInvalid
	}
	var installedAt any
	if ack.State == AddressDimensionAckInstalled {
		installedAt = attemptedAt
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_acks (
			snapshot_id, worker_id, boot_id, software_version, checksum,
			state, attempted_at, installed_at, error_code, error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''))
		ON DUPLICATE KEY UPDATE
			boot_id = VALUES(boot_id), software_version = VALUES(software_version), checksum = VALUES(checksum),
			state = VALUES(state), attempted_at = VALUES(attempted_at), installed_at = VALUES(installed_at),
			error_code = VALUES(error_code), error_message = VALUES(error_message), row_version = row_version + 1
	`, ack.SnapshotID, ack.WorkerID, ack.BootID, ack.SoftwareVersion, ack.Checksum,
		ack.State, attemptedAt, installedAt, ack.ErrorCode, ack.ErrorMessage); err != nil {
		return AddressDimensionAcknowledgement{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionAcknowledgement{}, err
	}
	return p.getDimensionPublicationAcknowledgement(ctx, ack.SnapshotID, ack.WorkerID)
}

func (p *Publisher) ReportDimensionPublicationReference(ctx context.Context, reference DimensionPublicationReference) (DimensionPublicationReference, error) {
	reference.ConsumerKind = strings.TrimSpace(reference.ConsumerKind)
	reference.ConsumerID = strings.TrimSpace(reference.ConsumerID)
	if p == nil || p.store == nil || reference.SnapshotID == "" || reference.ConsumerKind == "" || len(reference.ConsumerKind) > 32 || reference.ConsumerID == "" || len(reference.ConsumerID) > 190 || reference.MinEventTime.IsZero() || reference.MaxEventTime.Before(reference.MinEventTime) || reference.RetainUntil.Before(reference.MaxEventTime) {
		return AddressDimensionReference{}, ErrAddressDimensionInvalid
	}
	observedAt := p.now().UTC().Truncate(time.Millisecond)
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionReference{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublication(ctx, tx); err != nil {
		return AddressDimensionReference{}, err
	}
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, reference.SnapshotID, true)
	if err != nil {
		return AddressDimensionReference{}, err
	}
	if snapshot.ObjectDeletedAt != nil ||
		(snapshot.Status == AddressDimensionStatusRetired && snapshot.RetentionUntil != nil &&
			(!observedAt.Before(*snapshot.RetentionUntil) || reference.RetainUntil.After(*snapshot.RetentionUntil))) {
		return AddressDimensionReference{}, ErrAddressDimensionInvalid
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_references (
			snapshot_id, consumer_kind, consumer_id, min_event_time, max_event_time, retain_until, last_observed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			min_event_time = LEAST(min_event_time, VALUES(min_event_time)),
			max_event_time = GREATEST(max_event_time, VALUES(max_event_time)),
			retain_until = GREATEST(retain_until, VALUES(retain_until)),
			last_observed_at = VALUES(last_observed_at), row_version = row_version + 1
	`, reference.SnapshotID, reference.ConsumerKind, reference.ConsumerID,
		reference.MinEventTime.UTC(), reference.MaxEventTime.UTC(), reference.RetainUntil.UTC(), observedAt); err != nil {
		return AddressDimensionReference{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionReference{}, err
	}
	return p.getDimensionPublicationReference(ctx, reference.SnapshotID, reference.ConsumerKind, reference.ConsumerID)
}

func getDimensionPublicationSnapshotTx(ctx context.Context, tx *sql.Tx, scope DimensionPublicationScope, snapshotID ID, lock bool) (DimensionPublicationSnapshot, error) {
	if scope.validate() != nil {
		return DimensionPublicationSnapshot{}, ErrAddressDimensionInvalid
	}
	query := `SELECT ` + addressDimensionSnapshotColumns + ` FROM dimension_snapshots
		WHERE module_key = ? AND dimension_key = ? AND id = ?`
	if lock {
		query += ` FOR UPDATE`
	}
	return scanAddressDimensionSnapshot(tx.QueryRowContext(ctx, query, scope.ModuleKey, scope.DimensionKey, snapshotID))
}

func insertDimensionPublicationActivation(ctx context.Context, tx *sql.Tx, snapshot DimensionPublicationSnapshot, actorID ID, effectiveFrom time.Time, reason string, rollbackOf ID) (DimensionPublicationActivation, error) {
	id, err := newManagementID()
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	activation := AddressDimensionActivation{
		ID: id, ModuleKey: snapshot.ModuleKey, DimensionKey: snapshot.DimensionKey,
		SnapshotID: snapshot.ID, EffectiveFrom: effectiveFrom.UTC(), Reason: reason,
		RollbackOfSnapshotID: rollbackOf, CreatedBy: actorID, CreatedAt: time.Now().UTC(),
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_activations (
			id, module_key, dimension_key, snapshot_id, effective_from,
			reason, rollback_of_snapshot_id, created_by, created_at
		) VALUES (?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?)
	`, activation.ID, activation.ModuleKey, activation.DimensionKey,
		activation.SnapshotID, activation.EffectiveFrom, activation.Reason,
		activation.RollbackOfSnapshotID, activation.CreatedBy, activation.CreatedAt)
	if err != nil {
		var mysqlErr *mysqldriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return AddressDimensionActivation{}, ErrAddressDimensionConflict
		}
		return AddressDimensionActivation{}, err
	}
	return activation, nil
}

func getDimensionPublicationActivationBeforeTx(ctx context.Context, tx *sql.Tx, scope DimensionPublicationScope, effectiveFrom time.Time) (DimensionPublicationActivation, error) {
	return scanDimensionPublicationActivation(tx.QueryRowContext(ctx, `
		SELECT id, module_key, dimension_key, snapshot_id, effective_from,
		       reason, COALESCE(rollback_of_snapshot_id, ''), COALESCE(created_by, ''), created_at
		FROM dimension_snapshot_activations
		WHERE module_key = ? AND dimension_key = ? AND effective_from < ?
		ORDER BY effective_from DESC LIMIT 1 FOR UPDATE
	`, scope.ModuleKey, scope.DimensionKey, effectiveFrom.UTC()))
}

func getLatestDimensionPublicationActivationTx(ctx context.Context, tx *sql.Tx, scope DimensionPublicationScope) (DimensionPublicationActivation, error) {
	return scanDimensionPublicationActivation(tx.QueryRowContext(ctx, `
		SELECT id, module_key, dimension_key, snapshot_id, effective_from,
		       reason, COALESCE(rollback_of_snapshot_id, ''), COALESCE(created_by, ''), created_at
		FROM dimension_snapshot_activations
		WHERE module_key = ? AND dimension_key = ?
		ORDER BY effective_from DESC LIMIT 1 FOR UPDATE
	`, scope.ModuleKey, scope.DimensionKey))
}

func scanDimensionPublicationActivation(row rowScanner) (DimensionPublicationActivation, error) {
	var activation DimensionPublicationActivation
	err := row.Scan(&activation.ID, &activation.ModuleKey, &activation.DimensionKey,
		&activation.SnapshotID, &activation.EffectiveFrom, &activation.Reason,
		&activation.RollbackOfSnapshotID, &activation.CreatedBy, &activation.CreatedAt)
	return activation, err
}

func validAddressDimensionAckState(state string) bool {
	return state == AddressDimensionAckDownloaded || state == AddressDimensionAckInstalled || state == AddressDimensionAckFailed
}

func (p *Publisher) getDimensionPublicationAcknowledgement(ctx context.Context, snapshotID ID, workerID string) (DimensionPublicationAcknowledgement, error) {
	var item AddressDimensionAcknowledgement
	err := p.store.db.QueryRowContext(ctx, `
		SELECT ack.snapshot_id, ack.worker_id, ack.boot_id, ack.software_version, ack.checksum,
		       ack.state, ack.attempted_at, ack.installed_at,
		       COALESCE(ack.error_code, ''), COALESCE(ack.error_message, ''), ack.row_version
		FROM dimension_snapshot_acks AS ack
		JOIN dimension_snapshots AS snapshot ON snapshot.id = ack.snapshot_id
		WHERE ack.snapshot_id = ? AND ack.worker_id = ?
		  AND snapshot.module_key = ? AND snapshot.dimension_key = ?
	`, snapshotID, workerID, p.scope.ModuleKey, p.scope.DimensionKey).Scan(&item.SnapshotID, &item.WorkerID,
		&item.BootID, &item.SoftwareVersion, &item.Checksum, &item.State, &item.AttemptedAt,
		&item.InstalledAt, &item.ErrorCode, &item.ErrorMessage, &item.RowVersion)
	return item, err
}

func (p *Publisher) getDimensionPublicationReference(ctx context.Context, snapshotID ID, consumerKind, consumerID string) (DimensionPublicationReference, error) {
	var item AddressDimensionReference
	err := p.store.db.QueryRowContext(ctx, `
		SELECT reference.snapshot_id, reference.consumer_kind, reference.consumer_id,
		       reference.min_event_time, reference.max_event_time, reference.retain_until,
		       reference.last_observed_at, reference.row_version
		FROM dimension_snapshot_references AS reference
		JOIN dimension_snapshots AS snapshot ON snapshot.id = reference.snapshot_id
		WHERE reference.snapshot_id = ? AND reference.consumer_kind = ? AND reference.consumer_id = ?
		  AND snapshot.module_key = ? AND snapshot.dimension_key = ?
	`, snapshotID, consumerKind, consumerID, p.scope.ModuleKey, p.scope.DimensionKey).Scan(&item.SnapshotID,
		&item.ConsumerKind, &item.ConsumerID, &item.MinEventTime, &item.MaxEventTime,
		&item.RetainUntil, &item.LastObservedAt, &item.RowVersion)
	return item, err
}

func requireOneAddressDimensionRow(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrAddressDimensionConflict
	}
	return nil
}
