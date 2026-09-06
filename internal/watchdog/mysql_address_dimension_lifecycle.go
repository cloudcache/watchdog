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

func (p *MySQLAddressDimensionPublisher) ApproveAddressDimension(ctx context.Context, tenantID, actorID ID, expectedRowVersion uint64, approval AddressDimensionApproval) (AddressDimensionSnapshot, error) {
	if p == nil || p.store == nil || tenantID == "" || actorID == "" || expectedRowVersion == 0 || approval.SnapshotID == "" {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, err := getAddressDimensionSnapshotTx(ctx, tx, tenantID, approval.SnapshotID, true)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if snapshot.RowVersion != expectedRowVersion {
		return AddressDimensionSnapshot{}, ErrAddressDimensionConflict
	}
	if snapshot.Status != AddressDimensionStatusActive || snapshot.ApprovalState != AddressDimensionApprovalPending || snapshot.ObjectDeletedAt != nil {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalidTransition
	}
	if err := validateVerifiedAddressDimensionApproval(snapshot, approval); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE dimension_snapshots
		SET approval_state = 'approved', decided_by = ?, decided_at = ?,
		    decision_reason = NULL, signature_algorithm = ?, signing_key_id = ?,
		    signature = ?, signed_at = ?, row_version = row_version + 1
		WHERE tenant_id = ? AND id = ? AND row_version = ?
	`, actorID, approval.SignedAt, AddressDimensionSignatureAlgorithm, approval.SigningKeyID,
		approval.Signature, approval.SignedAt, tenantID, approval.SnapshotID, expectedRowVersion)
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
	return p.GetAddressDimensionSnapshot(ctx, tenantID, snapshot.ID)
}

func (p *MySQLAddressDimensionPublisher) RejectAddressDimension(ctx context.Context, tenantID, actorID, snapshotID ID, expectedRowVersion uint64, reason string) (AddressDimensionSnapshot, error) {
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
	snapshot, err := getAddressDimensionSnapshotTx(ctx, tx, tenantID, snapshotID, true)
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
		WHERE tenant_id = ? AND id = ? AND row_version = ?
	`, actorID, decidedAt, reason, tenantID, snapshotID, expectedRowVersion)
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
	return p.GetAddressDimensionSnapshot(ctx, tenantID, snapshotID)
}

func (p *MySQLAddressDimensionPublisher) ActivateAddressDimension(ctx context.Context, tenantID, actorID ID, request AddressDimensionActivationRequest) (AddressDimensionActivation, error) {
	if p == nil || p.store == nil || tenantID == "" || actorID == "" || request.SnapshotID == "" || request.ExpectedRowVersion == 0 || !isUTCMinute(request.EffectiveFrom) {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	defer tx.Rollback()
	if err := lockAddressDimensionTenant(ctx, tx, tenantID); err != nil {
		return AddressDimensionActivation{}, err
	}
	snapshot, err := getAddressDimensionSnapshotTx(ctx, tx, tenantID, request.SnapshotID, true)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	if snapshot.RowVersion != request.ExpectedRowVersion {
		return AddressDimensionActivation{}, ErrAddressDimensionConflict
	}
	if snapshot.Status != AddressDimensionStatusActive || snapshot.ApprovalState != AddressDimensionApprovalApproved || snapshot.ObjectDeletedAt != nil || !snapshot.EffectiveFrom.Equal(request.EffectiveFrom) || !addressDimensionSnapshotHasTrustedApproval(snapshot) {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalidTransition
	}
	activation, err := insertAddressDimensionActivation(ctx, tx, snapshot, actorID, request.EffectiveFrom, AddressDimensionActivationPublish, "")
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE dimension_snapshots SET row_version = row_version + 1 WHERE tenant_id = ? AND id = ? AND row_version = ?`, tenantID, snapshot.ID, request.ExpectedRowVersion)
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

func (p *MySQLAddressDimensionPublisher) RollbackAddressDimension(ctx context.Context, tenantID, actorID ID, request AddressDimensionRollbackRequest) (AddressDimensionActivation, error) {
	if p == nil || p.store == nil || tenantID == "" || actorID == "" || request.SnapshotID == "" || request.ExpectedRowVersion == 0 || !isUTCMinute(request.EffectiveFrom) {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	defer tx.Rollback()
	if err := lockAddressDimensionTenant(ctx, tx, tenantID); err != nil {
		return AddressDimensionActivation{}, err
	}
	target, err := getAddressDimensionSnapshotTx(ctx, tx, tenantID, request.SnapshotID, true)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	if target.RowVersion != request.ExpectedRowVersion {
		return AddressDimensionActivation{}, ErrAddressDimensionConflict
	}
	if target.ApprovalState != AddressDimensionApprovalApproved || target.ObjectDeletedAt != nil || request.EffectiveFrom.Before(target.EffectiveFrom) || !addressDimensionSnapshotHasTrustedApproval(target) {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalidTransition
	}
	current, err := getAddressDimensionActivationBeforeTx(ctx, tx, tenantID, request.EffectiveFrom)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	if current.SnapshotID == target.ID {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalidTransition
	}
	activation, err := insertAddressDimensionActivation(ctx, tx, target, actorID, request.EffectiveFrom, AddressDimensionActivationRollback, current.SnapshotID)
	if err != nil {
		return AddressDimensionActivation{}, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE dimension_snapshots
		SET status = 'active', retired_by = NULL, retired_at = NULL, row_version = row_version + 1
		WHERE tenant_id = ? AND id = ? AND row_version = ?
	`, tenantID, target.ID, request.ExpectedRowVersion)
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

func (p *MySQLAddressDimensionPublisher) RetireAddressDimension(ctx context.Context, tenantID, actorID ID, request AddressDimensionRetireRequest) (AddressDimensionSnapshot, error) {
	request.Reason = strings.TrimSpace(request.Reason)
	if p == nil || p.store == nil || tenantID == "" || actorID == "" || request.SnapshotID == "" || request.ExpectedRowVersion == 0 || len(request.Reason) > 512 {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	defer tx.Rollback()
	if err := lockAddressDimensionTenant(ctx, tx, tenantID); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	snapshot, err := getAddressDimensionSnapshotTx(ctx, tx, tenantID, request.SnapshotID, true)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if snapshot.RowVersion != request.ExpectedRowVersion {
		return AddressDimensionSnapshot{}, ErrAddressDimensionConflict
	}
	if snapshot.Status != AddressDimensionStatusActive {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalidTransition
	}
	latest, err := getLatestAddressDimensionActivationTx(ctx, tx, tenantID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return AddressDimensionSnapshot{}, err
	}
	if err == nil && latest.SnapshotID == snapshot.ID {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalidTransition
	}
	retiredAt := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE dimension_snapshots
		SET status = 'retired', retired_by = ?, retired_at = ?, row_version = row_version + 1
		WHERE tenant_id = ? AND id = ? AND row_version = ?
	`, actorID, retiredAt, tenantID, snapshot.ID, request.ExpectedRowVersion)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, tenantID, actorID, snapshot.ID, "dimension.snapshot.retired", map[string]any{"reason": request.Reason, "version": snapshot.Version}); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	return p.GetAddressDimensionSnapshot(ctx, tenantID, snapshot.ID)
}

func (p *MySQLAddressDimensionPublisher) GetAddressDimensionActivationAt(ctx context.Context, tenantID ID, eventTime time.Time) (AddressDimensionActivation, error) {
	if p == nil || p.store == nil || tenantID == "" || eventTime.IsZero() {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalid
	}
	return scanAddressDimensionActivation(p.store.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, module_key, dimension_key, snapshot_id, effective_from,
		       reason, COALESCE(rollback_of_snapshot_id, ''), COALESCE(created_by, ''), created_at
		FROM dimension_snapshot_activations
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ? AND effective_from <= ?
		ORDER BY effective_from DESC LIMIT 1
	`, tenantID, AddressDimensionModuleKey, AddressDimensionKey, eventTime.UTC()))
}

func (p *MySQLAddressDimensionPublisher) ReportAddressDimensionAcknowledgement(ctx context.Context, acknowledgement AddressDimensionAcknowledgement) (AddressDimensionAcknowledgement, error) {
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
	snapshot, err := p.GetAddressDimensionSnapshot(ctx, acknowledgement.TenantID, acknowledgement.SnapshotID)
	if err != nil {
		return AddressDimensionAcknowledgement{}, err
	}
	if acknowledgement.Checksum != snapshot.Checksum {
		return AddressDimensionAcknowledgement{}, ErrAddressDimensionInvalid
	}
	attemptedAt := time.Now().UTC()
	var installedAt any
	if acknowledgement.State == AddressDimensionAckInstalled {
		installedAt = attemptedAt
	}
	_, err = p.store.db.ExecContext(ctx, `
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
	return p.getAddressDimensionAcknowledgement(ctx, acknowledgement.TenantID, acknowledgement.SnapshotID, acknowledgement.WorkerID)
}

func (p *MySQLAddressDimensionPublisher) ReportAddressDimensionReference(ctx context.Context, reference AddressDimensionReference) (AddressDimensionReference, error) {
	reference.ConsumerKind = strings.TrimSpace(reference.ConsumerKind)
	reference.ConsumerID = strings.TrimSpace(reference.ConsumerID)
	if p == nil || p.store == nil || reference.TenantID == "" || reference.SnapshotID == "" || reference.ConsumerKind == "" || len(reference.ConsumerKind) > 32 || reference.ConsumerID == "" || len(reference.ConsumerID) > 190 || reference.MinEventTime.IsZero() || reference.MaxEventTime.Before(reference.MinEventTime) || reference.RetainUntil.Before(reference.MaxEventTime) {
		return AddressDimensionReference{}, ErrAddressDimensionInvalid
	}
	observedAt := time.Now().UTC()
	_, err := p.store.db.ExecContext(ctx, `
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
	return p.getAddressDimensionReference(ctx, reference.TenantID, reference.SnapshotID, reference.ConsumerKind, reference.ConsumerID)
}

func getAddressDimensionSnapshotTx(ctx context.Context, tx *sql.Tx, tenantID, snapshotID ID, lock bool) (AddressDimensionSnapshot, error) {
	query := `SELECT ` + addressDimensionSnapshotColumns + ` FROM dimension_snapshots WHERE tenant_id = ? AND id = ?`
	if lock {
		query += ` FOR UPDATE`
	}
	return scanAddressDimensionSnapshot(tx.QueryRowContext(ctx, query, tenantID, snapshotID))
}

func lockAddressDimensionTenant(ctx context.Context, tx *sql.Tx, tenantID ID) error {
	var locked ID
	return tx.QueryRowContext(ctx, `SELECT id FROM tenants WHERE id = ? FOR UPDATE`, tenantID).Scan(&locked)
}

func insertAddressDimensionActivation(ctx context.Context, tx *sql.Tx, snapshot AddressDimensionSnapshot, actorID ID, effectiveFrom time.Time, reason string, rollbackOf ID) (AddressDimensionActivation, error) {
	id, err := newIdentityID()
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

func getAddressDimensionActivationBeforeTx(ctx context.Context, tx *sql.Tx, tenantID ID, effectiveFrom time.Time) (AddressDimensionActivation, error) {
	return scanAddressDimensionActivation(tx.QueryRowContext(ctx, `
		SELECT id, tenant_id, module_key, dimension_key, snapshot_id, effective_from,
		       reason, COALESCE(rollback_of_snapshot_id, ''), COALESCE(created_by, ''), created_at
		FROM dimension_snapshot_activations
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ? AND effective_from < ?
		ORDER BY effective_from DESC LIMIT 1 FOR UPDATE
	`, tenantID, AddressDimensionModuleKey, AddressDimensionKey, effectiveFrom.UTC()))
}

func getLatestAddressDimensionActivationTx(ctx context.Context, tx *sql.Tx, tenantID ID) (AddressDimensionActivation, error) {
	return scanAddressDimensionActivation(tx.QueryRowContext(ctx, `
		SELECT id, tenant_id, module_key, dimension_key, snapshot_id, effective_from,
		       reason, COALESCE(rollback_of_snapshot_id, ''), COALESCE(created_by, ''), created_at
		FROM dimension_snapshot_activations
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ?
		ORDER BY effective_from DESC LIMIT 1 FOR UPDATE
	`, tenantID, AddressDimensionModuleKey, AddressDimensionKey))
}

func scanAddressDimensionActivation(row rowScanner) (AddressDimensionActivation, error) {
	var activation AddressDimensionActivation
	err := row.Scan(&activation.ID, &activation.TenantID, &activation.ModuleKey, &activation.DimensionKey,
		&activation.SnapshotID, &activation.EffectiveFrom, &activation.Reason,
		&activation.RollbackOfSnapshotID, &activation.CreatedBy, &activation.CreatedAt)
	return activation, err
}

func addressDimensionSnapshotHasTrustedApproval(snapshot AddressDimensionSnapshot) bool {
	return snapshot.ApprovalState == AddressDimensionApprovalApproved &&
		((snapshot.DecidedAt == nil && len(snapshot.Signature) == 0) ||
			(snapshot.DecidedAt != nil && snapshot.SignedAt != nil && snapshot.SignatureAlgorithm == AddressDimensionSignatureAlgorithm && snapshot.SigningKeyID != "" && len(snapshot.Signature) == ed25519.SignatureSize))
}

func validAddressDimensionAckState(state string) bool {
	return state == AddressDimensionAckDownloaded || state == AddressDimensionAckInstalled || state == AddressDimensionAckFailed
}

func (p *MySQLAddressDimensionPublisher) getAddressDimensionAcknowledgement(ctx context.Context, tenantID, snapshotID ID, workerID string) (AddressDimensionAcknowledgement, error) {
	var item AddressDimensionAcknowledgement
	err := p.store.db.QueryRowContext(ctx, `
		SELECT tenant_id, snapshot_id, worker_id, boot_id, software_version, checksum,
		       state, attempted_at, installed_at, COALESCE(error_code, ''), COALESCE(error_message, ''), row_version
		FROM dimension_snapshot_acks WHERE tenant_id = ? AND snapshot_id = ? AND worker_id = ?
	`, tenantID, snapshotID, workerID).Scan(&item.TenantID, &item.SnapshotID, &item.WorkerID,
		&item.BootID, &item.SoftwareVersion, &item.Checksum, &item.State, &item.AttemptedAt,
		&item.InstalledAt, &item.ErrorCode, &item.ErrorMessage, &item.RowVersion)
	return item, err
}

func (p *MySQLAddressDimensionPublisher) getAddressDimensionReference(ctx context.Context, tenantID, snapshotID ID, consumerKind, consumerID string) (AddressDimensionReference, error) {
	var item AddressDimensionReference
	err := p.store.db.QueryRowContext(ctx, `
		SELECT tenant_id, snapshot_id, consumer_kind, consumer_id, min_event_time,
		       max_event_time, retain_until, last_observed_at, row_version
		FROM dimension_snapshot_references
		WHERE tenant_id = ? AND snapshot_id = ? AND consumer_kind = ? AND consumer_id = ?
	`, tenantID, snapshotID, consumerKind, consumerID).Scan(&item.TenantID, &item.SnapshotID,
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
	id, err := newIdentityID()
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

var _ AddressDimensionLifecycle = (*MySQLAddressDimensionPublisher)(nil)
