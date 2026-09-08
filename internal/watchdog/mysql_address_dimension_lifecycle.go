package watchdog

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

func (p *MySQLDimensionPublicationStore) ApproveDimensionPublication(ctx context.Context, tenantID, actorID ID, expectedRowVersion uint64, approval DimensionPublicationApproval) (DimensionPublicationSnapshot, error) {
	if p == nil || p.store == nil || tenantID == "" || actorID == "" || expectedRowVersion == 0 || approval.SnapshotID == "" {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, tenantID, approval.SnapshotID, true)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if snapshot.RowVersion != expectedRowVersion {
		return AddressDimensionSnapshot{}, ErrAddressDimensionConflict
	}
	if snapshot.Status != AddressDimensionStatusActive || snapshot.ApprovalState != AddressDimensionApprovalPending || snapshot.ObjectDeletedAt != nil {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalidTransition
	}
	if err := validateVerifiedDimensionPublicationApproval(snapshot, approval); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE dimension_snapshots
		SET approval_state = 'approved', decided_by = ?, decided_at = ?,
		    decision_reason = NULL, signature_algorithm = ?, signing_key_id = ?,
		    signature = ?, signed_at = ?, row_version = row_version + 1
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ? AND id = ? AND row_version = ?
	`, actorID, approval.SignedAt, AddressDimensionSignatureAlgorithm, approval.SigningKeyID,
		approval.Signature, approval.SignedAt, tenantID, p.scope.ModuleKey, p.scope.DimensionKey, approval.SnapshotID, expectedRowVersion)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, tenantID, actorID, snapshot.ID, "dimension.snapshot.approved", map[string]any{
		"version": snapshot.Version, "checksum": snapshot.Checksum, "signing_key_id": approval.SigningKeyID,
	}); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	return p.GetDimensionPublicationSnapshot(ctx, tenantID, snapshot.ID)
}

func (p *MySQLDimensionPublicationStore) RejectDimensionPublication(ctx context.Context, tenantID, actorID, snapshotID ID, expectedRowVersion uint64, reason string) (DimensionPublicationSnapshot, error) {
	reason = strings.TrimSpace(reason)
	if p == nil || p.store == nil || tenantID == "" || actorID == "" || snapshotID == "" || expectedRowVersion == 0 || reason == "" || len(reason) > 512 {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	decidedAt := time.Now().UTC()
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, tenantID, snapshotID, true)
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
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ? AND id = ? AND row_version = ?
	`, actorID, decidedAt, reason, tenantID, p.scope.ModuleKey, p.scope.DimensionKey, snapshotID, expectedRowVersion)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, tenantID, actorID, snapshotID, "dimension.snapshot.rejected", map[string]any{"reason": reason}); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	return p.GetDimensionPublicationSnapshot(ctx, tenantID, snapshotID)
}

func (p *MySQLDimensionPublicationStore) ActivateDimensionPublication(ctx context.Context, tenantID, actorID ID, request DimensionPublicationActivationRequest) (DimensionPublicationActivation, error) {
	if p == nil || p.store == nil || tenantID == "" || actorID == "" || request.SnapshotID == "" || request.ExpectedRowVersion == 0 || !isUTCMinute(request.EffectiveFrom) {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublicationTenant(ctx, tx, tenantID); err != nil {
		return AddressDimensionActivation{}, err
	}
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, tenantID, request.SnapshotID, true)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	if snapshot.RowVersion != request.ExpectedRowVersion {
		return AddressDimensionActivation{}, ErrAddressDimensionConflict
	}
	if snapshot.Status != AddressDimensionStatusActive || snapshot.ApprovalState != AddressDimensionApprovalApproved || snapshot.ObjectDeletedAt != nil || !snapshot.EffectiveFrom.Equal(request.EffectiveFrom) || !dimensionPublicationSnapshotHasTrustedApproval(snapshot) {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalidTransition
	}
	activation, err := insertDimensionPublicationActivation(ctx, tx, snapshot, actorID, request.EffectiveFrom, AddressDimensionActivationPublish, "")
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE dimension_snapshots SET row_version = row_version + 1
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ? AND id = ? AND row_version = ?`,
		tenantID, p.scope.ModuleKey, p.scope.DimensionKey, snapshot.ID, request.ExpectedRowVersion)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionActivation{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, tenantID, actorID, snapshot.ID, "dimension.snapshot.activated", map[string]any{
		"activation_id": activation.ID, "effective_from": activation.EffectiveFrom, "version": snapshot.Version,
	}); err != nil {
		return AddressDimensionActivation{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionActivation{}, err
	}
	return activation, nil
}

func (p *MySQLDimensionPublicationStore) RollbackDimensionPublication(ctx context.Context, tenantID, actorID ID, request DimensionPublicationRollbackRequest) (DimensionPublicationActivation, error) {
	if p == nil || p.store == nil || tenantID == "" || actorID == "" || request.SnapshotID == "" || request.ExpectedRowVersion == 0 || !isUTCMinute(request.EffectiveFrom) {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublicationTenant(ctx, tx, tenantID); err != nil {
		return AddressDimensionActivation{}, err
	}
	target, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, tenantID, request.SnapshotID, true)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	if target.RowVersion != request.ExpectedRowVersion {
		return AddressDimensionActivation{}, ErrAddressDimensionConflict
	}
	now := p.now().UTC()
	if target.ApprovalState != AddressDimensionApprovalApproved || target.ObjectDeletedAt != nil ||
		(target.Status == AddressDimensionStatusRetired && target.RetentionUntil != nil && !now.Before(*target.RetentionUntil)) ||
		request.EffectiveFrom.Before(target.EffectiveFrom) || !dimensionPublicationSnapshotHasTrustedApproval(target) {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalidTransition
	}
	current, err := getDimensionPublicationActivationBeforeTx(ctx, tx, p.scope, tenantID, request.EffectiveFrom)
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
		SET status = 'active', retired_by = NULL, retired_at = NULL,
		    retention_until = NULL, row_version = row_version + 1
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ? AND id = ? AND row_version = ?
	`, tenantID, p.scope.ModuleKey, p.scope.DimensionKey, target.ID, request.ExpectedRowVersion)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionActivation{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, tenantID, actorID, target.ID, "dimension.snapshot.rolled_back", map[string]any{
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

func (p *MySQLDimensionPublicationStore) RetireDimensionPublication(ctx context.Context, tenantID, actorID ID, request DimensionPublicationRetireRequest) (DimensionPublicationSnapshot, error) {
	request.Reason = strings.TrimSpace(request.Reason)
	if p == nil || p.store == nil || tenantID == "" || actorID == "" || request.SnapshotID == "" || request.ExpectedRowVersion == 0 || len(request.Reason) > 512 {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublicationTenant(ctx, tx, tenantID); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, tenantID, request.SnapshotID, true)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if snapshot.RowVersion != request.ExpectedRowVersion {
		return AddressDimensionSnapshot{}, ErrAddressDimensionConflict
	}
	if snapshot.Status != AddressDimensionStatusActive {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalidTransition
	}
	latest, err := getLatestDimensionPublicationActivationTx(ctx, tx, p.scope, tenantID)
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
		SELECT MAX(retain_until) FROM dimension_snapshot_references
		WHERE tenant_id = ? AND snapshot_id = ?
	`, tenantID, snapshot.ID).Scan(&latestReference); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if latestReference.Valid && latestReference.Time.After(retentionUntil) {
		retentionUntil = latestReference.Time.UTC()
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE dimension_snapshots
		SET status = 'retired', retired_by = ?, retired_at = ?, retention_until = ?, row_version = row_version + 1
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ? AND id = ? AND row_version = ?
	`, actorID, retiredAt, retentionUntil, tenantID, p.scope.ModuleKey, p.scope.DimensionKey, snapshot.ID, request.ExpectedRowVersion)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, tenantID, actorID, snapshot.ID, "dimension.snapshot.retired", map[string]any{
		"reason": request.Reason, "version": snapshot.Version, "retention_until": retentionUntil,
	}); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	return p.GetDimensionPublicationSnapshot(ctx, tenantID, snapshot.ID)
}

func (p *MySQLDimensionPublicationStore) GetDimensionPublicationActivationAt(ctx context.Context, tenantID ID, eventTime time.Time) (DimensionPublicationActivation, error) {
	if p == nil || p.store == nil || tenantID == "" || eventTime.IsZero() {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalid
	}
	return scanDimensionPublicationActivation(p.store.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, module_key, dimension_key, snapshot_id, effective_from,
		       reason, COALESCE(rollback_of_snapshot_id, ''), COALESCE(created_by, ''), created_at
		FROM dimension_snapshot_activations
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ? AND effective_from <= ?
		ORDER BY effective_from DESC LIMIT 1
	`, tenantID, p.scope.ModuleKey, p.scope.DimensionKey, eventTime.UTC()))
}

func (p *MySQLDimensionPublicationStore) ReportDimensionPublicationAcknowledgement(ctx context.Context, acknowledgement DimensionPublicationAcknowledgement) (DimensionPublicationAcknowledgement, error) {
	acknowledgement.WorkerID = strings.TrimSpace(acknowledgement.WorkerID)
	acknowledgement.BootID = strings.TrimSpace(acknowledgement.BootID)
	acknowledgement.SoftwareVersion = strings.TrimSpace(acknowledgement.SoftwareVersion)
	acknowledgement.ErrorCode = strings.TrimSpace(acknowledgement.ErrorCode)
	acknowledgement.ErrorMessage = strings.TrimSpace(acknowledgement.ErrorMessage)
	if p == nil || p.store == nil || acknowledgement.TenantID == "" || acknowledgement.SnapshotID == "" || acknowledgement.WorkerID == "" || len(acknowledgement.WorkerID) > 128 || acknowledgement.BootID == "" || len(acknowledgement.BootID) > 128 || acknowledgement.SoftwareVersion == "" || len(acknowledgement.SoftwareVersion) > 64 || len(acknowledgement.ErrorCode) > 64 || len(acknowledgement.ErrorMessage) > 512 || !validAddressDimensionAckState(acknowledgement.State) ||
		(acknowledgement.State == AddressDimensionAckFailed && acknowledgement.ErrorCode == "") ||
		(acknowledgement.State != AddressDimensionAckFailed && (acknowledgement.ErrorCode != "" || acknowledgement.ErrorMessage != "")) {
		return AddressDimensionAcknowledgement{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionAcknowledgement{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublicationTenant(ctx, tx, acknowledgement.TenantID); err != nil {
		return AddressDimensionAcknowledgement{}, err
	}
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, acknowledgement.TenantID, acknowledgement.SnapshotID, true)
	if err != nil {
		return AddressDimensionAcknowledgement{}, err
	}
	attemptedAt := p.now().UTC().Truncate(time.Millisecond)
	if acknowledgement.Checksum != snapshot.Checksum || snapshot.ObjectDeletedAt != nil ||
		(snapshot.Status == AddressDimensionStatusRetired && snapshot.RetentionUntil != nil && !attemptedAt.Before(*snapshot.RetentionUntil)) {
		return AddressDimensionAcknowledgement{}, ErrAddressDimensionInvalid
	}
	var installedAt any
	if acknowledgement.State == AddressDimensionAckInstalled {
		installedAt = attemptedAt
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_acks (
			tenant_id, snapshot_id, worker_id, boot_id, software_version, checksum,
			state, attempted_at, installed_at, error_code, error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''))
		ON DUPLICATE KEY UPDATE
			boot_id = VALUES(boot_id), software_version = VALUES(software_version), checksum = VALUES(checksum),
			state = VALUES(state), attempted_at = VALUES(attempted_at), installed_at = VALUES(installed_at),
			error_code = VALUES(error_code), error_message = VALUES(error_message), row_version = row_version + 1
	`, acknowledgement.TenantID, acknowledgement.SnapshotID, acknowledgement.WorkerID,
		acknowledgement.BootID, acknowledgement.SoftwareVersion, acknowledgement.Checksum,
		acknowledgement.State, attemptedAt, installedAt, acknowledgement.ErrorCode, acknowledgement.ErrorMessage)
	if err != nil {
		return AddressDimensionAcknowledgement{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionAcknowledgement{}, err
	}
	return p.getDimensionPublicationAcknowledgement(ctx, acknowledgement.TenantID, acknowledgement.SnapshotID, acknowledgement.WorkerID)
}

func (p *MySQLDimensionPublicationStore) ReportDimensionPublicationReference(ctx context.Context, reference DimensionPublicationReference) (DimensionPublicationReference, error) {
	reference.ConsumerKind = strings.TrimSpace(reference.ConsumerKind)
	reference.ConsumerID = strings.TrimSpace(reference.ConsumerID)
	if p == nil || p.store == nil || reference.TenantID == "" || reference.SnapshotID == "" || reference.ConsumerKind == "" || len(reference.ConsumerKind) > 32 || reference.ConsumerID == "" || len(reference.ConsumerID) > 190 || reference.MinEventTime.IsZero() || reference.MaxEventTime.Before(reference.MinEventTime) || reference.RetainUntil.Before(reference.MaxEventTime) {
		return AddressDimensionReference{}, ErrAddressDimensionInvalid
	}
	observedAt := p.now().UTC().Truncate(time.Millisecond)
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionReference{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublicationTenant(ctx, tx, reference.TenantID); err != nil {
		return AddressDimensionReference{}, err
	}
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, reference.TenantID, reference.SnapshotID, true)
	if err != nil {
		return AddressDimensionReference{}, err
	}
	if snapshot.ObjectDeletedAt != nil ||
		(snapshot.Status == AddressDimensionStatusRetired && snapshot.RetentionUntil != nil &&
			(!observedAt.Before(*snapshot.RetentionUntil) || reference.RetainUntil.After(*snapshot.RetentionUntil))) {
		return AddressDimensionReference{}, ErrAddressDimensionInvalid
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_references (
			tenant_id, snapshot_id, consumer_kind, consumer_id, min_event_time,
			max_event_time, retain_until, last_observed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			min_event_time = LEAST(min_event_time, VALUES(min_event_time)),
			max_event_time = GREATEST(max_event_time, VALUES(max_event_time)),
			retain_until = GREATEST(retain_until, VALUES(retain_until)),
			last_observed_at = VALUES(last_observed_at), row_version = row_version + 1
	`, reference.TenantID, reference.SnapshotID, reference.ConsumerKind, reference.ConsumerID,
		reference.MinEventTime.UTC(), reference.MaxEventTime.UTC(), reference.RetainUntil.UTC(), observedAt)
	if err != nil {
		return AddressDimensionReference{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionReference{}, err
	}
	return p.getDimensionPublicationReference(ctx, reference.TenantID, reference.SnapshotID, reference.ConsumerKind, reference.ConsumerID)
}

func getDimensionPublicationSnapshotTx(ctx context.Context, tx *sql.Tx, scope DimensionPublicationScope, tenantID, snapshotID ID, lock bool) (DimensionPublicationSnapshot, error) {
	if scope.validate() != nil {
		return DimensionPublicationSnapshot{}, ErrAddressDimensionInvalid
	}
	query := `SELECT ` + addressDimensionSnapshotColumns + ` FROM dimension_snapshots
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ? AND id = ?`
	if lock {
		query += ` FOR UPDATE`
	}
	return scanAddressDimensionSnapshot(tx.QueryRowContext(ctx, query,
		tenantID, scope.ModuleKey, scope.DimensionKey, snapshotID))
}

func lockDimensionPublicationTenant(ctx context.Context, tx *sql.Tx, tenantID ID) error {
	var locked ID
	return tx.QueryRowContext(ctx, `SELECT id FROM tenants WHERE id = ? FOR UPDATE`, tenantID).Scan(&locked)
}

func insertDimensionPublicationActivation(ctx context.Context, tx *sql.Tx, snapshot DimensionPublicationSnapshot, actorID ID, effectiveFrom time.Time, reason string, rollbackOf ID) (DimensionPublicationActivation, error) {
	id, err := newManagementID()
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	activation := AddressDimensionActivation{
		ID: id, TenantID: snapshot.TenantID, ModuleKey: snapshot.ModuleKey, DimensionKey: snapshot.DimensionKey,
		SnapshotID: snapshot.ID, EffectiveFrom: effectiveFrom.UTC(), Reason: reason,
		RollbackOfSnapshotID: rollbackOf, CreatedBy: actorID, CreatedAt: time.Now().UTC(),
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_activations (
			id, tenant_id, module_key, dimension_key, snapshot_id, effective_from,
			reason, rollback_of_snapshot_id, created_by, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?)
	`, activation.ID, activation.TenantID, activation.ModuleKey, activation.DimensionKey,
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

func getDimensionPublicationActivationBeforeTx(ctx context.Context, tx *sql.Tx, scope DimensionPublicationScope, tenantID ID, effectiveFrom time.Time) (DimensionPublicationActivation, error) {
	return scanDimensionPublicationActivation(tx.QueryRowContext(ctx, `
		SELECT id, tenant_id, module_key, dimension_key, snapshot_id, effective_from,
		       reason, COALESCE(rollback_of_snapshot_id, ''), COALESCE(created_by, ''), created_at
		FROM dimension_snapshot_activations
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ? AND effective_from < ?
		ORDER BY effective_from DESC LIMIT 1 FOR UPDATE
	`, tenantID, scope.ModuleKey, scope.DimensionKey, effectiveFrom.UTC()))
}

func getLatestDimensionPublicationActivationTx(ctx context.Context, tx *sql.Tx, scope DimensionPublicationScope, tenantID ID) (DimensionPublicationActivation, error) {
	return scanDimensionPublicationActivation(tx.QueryRowContext(ctx, `
		SELECT id, tenant_id, module_key, dimension_key, snapshot_id, effective_from,
		       reason, COALESCE(rollback_of_snapshot_id, ''), COALESCE(created_by, ''), created_at
		FROM dimension_snapshot_activations
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ?
		ORDER BY effective_from DESC LIMIT 1 FOR UPDATE
	`, tenantID, scope.ModuleKey, scope.DimensionKey))
}

func scanDimensionPublicationActivation(row rowScanner) (DimensionPublicationActivation, error) {
	var activation DimensionPublicationActivation
	err := row.Scan(&activation.ID, &activation.TenantID, &activation.ModuleKey, &activation.DimensionKey,
		&activation.SnapshotID, &activation.EffectiveFrom, &activation.Reason,
		&activation.RollbackOfSnapshotID, &activation.CreatedBy, &activation.CreatedAt)
	return activation, err
}

func dimensionPublicationSnapshotHasTrustedApproval(snapshot DimensionPublicationSnapshot) bool {
	return snapshot.ApprovalState == AddressDimensionApprovalApproved &&
		((snapshot.DecidedAt == nil && len(snapshot.Signature) == 0) ||
			(snapshot.DecidedAt != nil && snapshot.SignedAt != nil && snapshot.SignatureAlgorithm == AddressDimensionSignatureAlgorithm && snapshot.SigningKeyID != "" && len(snapshot.Signature) == ed25519.SignatureSize))
}

func validAddressDimensionAckState(state string) bool {
	return state == AddressDimensionAckDownloaded || state == AddressDimensionAckInstalled || state == AddressDimensionAckFailed
}

func (p *MySQLDimensionPublicationStore) getDimensionPublicationAcknowledgement(ctx context.Context, tenantID, snapshotID ID, workerID string) (DimensionPublicationAcknowledgement, error) {
	var item AddressDimensionAcknowledgement
	err := p.store.db.QueryRowContext(ctx, `
		SELECT acknowledgement.tenant_id, acknowledgement.snapshot_id, acknowledgement.worker_id,
		       acknowledgement.boot_id, acknowledgement.software_version, acknowledgement.checksum,
		       acknowledgement.state, acknowledgement.attempted_at, acknowledgement.installed_at,
		       COALESCE(acknowledgement.error_code, ''), COALESCE(acknowledgement.error_message, ''), acknowledgement.row_version
		FROM dimension_snapshot_acks AS acknowledgement
		JOIN dimension_snapshots AS snapshot
		  ON snapshot.tenant_id = acknowledgement.tenant_id AND snapshot.id = acknowledgement.snapshot_id
		WHERE acknowledgement.tenant_id = ? AND acknowledgement.snapshot_id = ? AND acknowledgement.worker_id = ?
		  AND snapshot.module_key = ? AND snapshot.dimension_key = ?
	`, tenantID, snapshotID, workerID, p.scope.ModuleKey, p.scope.DimensionKey).Scan(&item.TenantID, &item.SnapshotID, &item.WorkerID,
		&item.BootID, &item.SoftwareVersion, &item.Checksum, &item.State, &item.AttemptedAt,
		&item.InstalledAt, &item.ErrorCode, &item.ErrorMessage, &item.RowVersion)
	return item, err
}

func (p *MySQLDimensionPublicationStore) getDimensionPublicationReference(ctx context.Context, tenantID, snapshotID ID, consumerKind, consumerID string) (DimensionPublicationReference, error) {
	var item AddressDimensionReference
	err := p.store.db.QueryRowContext(ctx, `
		SELECT reference.tenant_id, reference.snapshot_id, reference.consumer_kind, reference.consumer_id,
		       reference.min_event_time, reference.max_event_time, reference.retain_until,
		       reference.last_observed_at, reference.row_version
		FROM dimension_snapshot_references AS reference
		JOIN dimension_snapshots AS snapshot
		  ON snapshot.tenant_id = reference.tenant_id AND snapshot.id = reference.snapshot_id
		WHERE reference.tenant_id = ? AND reference.snapshot_id = ? AND reference.consumer_kind = ? AND reference.consumer_id = ?
		  AND snapshot.module_key = ? AND snapshot.dimension_key = ?
	`, tenantID, snapshotID, consumerKind, consumerID, p.scope.ModuleKey, p.scope.DimensionKey).Scan(&item.TenantID, &item.SnapshotID,
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

func insertAddressDimensionAudit(ctx context.Context, tx *sql.Tx, tenantID, actorID, snapshotID ID, action string, detail map[string]any) error {
	payload, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	id, err := newManagementID()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO audit_logs (id, tenant_id, actor_id, action, resource_type, resource_id, detail_json, created_at)
		VALUES (?, ?, ?, ?, 'dimension_snapshot', ?, ?, ?)
	`, id, tenantID, actorID, action, snapshotID, payload, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("insert address dimension audit: %w", err)
	}
	return nil
}
