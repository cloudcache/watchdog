package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

type MySQLFlowStateCleanupEvidenceProvider struct {
	db *sql.DB
}

func NewMySQLFlowStateCleanupEvidenceProvider(db *sql.DB) (*MySQLFlowStateCleanupEvidenceProvider, error) {
	if db == nil {
		return nil, errors.New("flow state-cleanup evidence database is required")
	}
	return &MySQLFlowStateCleanupEvidenceProvider{db: db}, nil
}

func (p *MySQLFlowStateCleanupEvidenceProvider) OwnershipFence(ctx context.Context, snapshot flowcollect.StateCleanupSnapshot) (flowcollect.OwnershipFenceEvidence, error) {
	if p == nil || p.db == nil || ctx == nil {
		return flowcollect.OwnershipFenceEvidence{}, errors.New("flow state-cleanup evidence provider is unavailable")
	}
	if _, err := flowcollect.RestoreStateCleanupJob(snapshot); err != nil {
		return flowcollect.OwnershipFenceEvidence{}, fmt.Errorf("%w: %v", ErrFlowStateCleanupInvalidEvidence, err)
	}
	var evidence flowcollect.OwnershipFenceEvidence
	var oldPlanStatus, revokePlanStatus, newPlanStatus, principalStatus string
	var oldPlanExpiresAt time.Time
	var oldPlanRevokedAt, revokePlanActivatedAt, newPlanActivatedAt sql.NullTime
	var oldOwnerDrainedAt, principalRevokedAt sql.NullTime
	var drainHash, revokeHash, grantHash, principalRef sql.NullString
	var oldRevokeRevision, maxClockSkewMS, aclPropagationMS uint64
	var drainConfigVersion sql.NullInt64
	err := p.db.QueryRowContext(ctx, `
		SELECT
			transfer.new_collector_id,
			transfer.old_plan_revision,
			transfer.old_revoke_plan_revision,
			transfer.new_plan_revision,
			transfer.old_ownership_epoch,
			transfer.new_ownership_epoch,
			old_plan.status,
			old_plan.retired_at,
			old_plan.expires_at,
			revoke_plan.status,
			revoke_plan.activated_at,
			new_plan.status,
			new_plan.activated_at,
			transfer.old_owner_drained_at,
			transfer.old_owner_drain_config_version,
			transfer.drain_receipt_sha256,
			principal.status,
			principal.write_revoked_at,
			principal.revoke_receipt_sha256,
			principal.grant_receipt_sha256,
			principal.principal_ref,
			transfer.max_clock_skew_ms,
			principal.acl_propagation_delay_ms
		FROM collector_ownership_transfers AS transfer
		JOIN collector_plan_revisions AS old_plan
		  ON old_plan.tenant_id = transfer.tenant_id
		 AND old_plan.collector_id = transfer.old_collector_id
		 AND old_plan.config_version = transfer.old_plan_revision
		JOIN collector_plan_revisions AS revoke_plan
		  ON revoke_plan.tenant_id = transfer.tenant_id
		 AND revoke_plan.collector_id = transfer.old_collector_id
		 AND revoke_plan.config_version = transfer.old_revoke_plan_revision
		JOIN collector_plan_revisions AS new_plan
		  ON new_plan.tenant_id = transfer.tenant_id
		 AND new_plan.collector_id = transfer.new_collector_id
		 AND new_plan.config_version = transfer.new_plan_revision
		JOIN collector_service_principals AS principal
		  ON principal.tenant_id = transfer.tenant_id
		 AND principal.id = transfer.old_principal_id
		 AND principal.collector_id = transfer.old_collector_id
		 AND principal.service_type = 'kafka'
		WHERE transfer.tenant_id = ? AND transfer.exporter_id = ?
		  AND transfer.old_collector_id = ? AND transfer.old_plan_revision = ?
		  AND transfer.old_ownership_epoch = ?
	`, snapshot.Old.TenantID, snapshot.Old.ExporterID, snapshot.Old.CollectorID,
		snapshot.Old.RegistryVersion, snapshot.Old.OwnershipEpoch).Scan(
		&evidence.NewCollectorID, &evidence.OldPlanRevision, &oldRevokeRevision,
		&evidence.NewPlanRevision, &evidence.OldOwnershipEpoch, &evidence.NewOwnershipEpoch,
		&oldPlanStatus, &oldPlanRevokedAt, &oldPlanExpiresAt,
		&revokePlanStatus, &revokePlanActivatedAt,
		&newPlanStatus, &newPlanActivatedAt,
		&oldOwnerDrainedAt, &drainConfigVersion, &drainHash,
		&principalStatus, &principalRevokedAt, &revokeHash, &grantHash, &principalRef,
		&maxClockSkewMS, &aclPropagationMS,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return flowcollect.OwnershipFenceEvidence{}, ErrCollectorEvidenceNotReady
	}
	if err != nil {
		return flowcollect.OwnershipFenceEvidence{}, err
	}
	evidence.OldCollectorID = snapshot.Old.CollectorID
	maxClockSkew, ok := evidenceMilliseconds(maxClockSkewMS)
	if !ok {
		return flowcollect.OwnershipFenceEvidence{}, ErrFlowStateCleanupInvalidEvidence
	}
	aclPropagation, ok := evidenceMilliseconds(aclPropagationMS)
	if !ok {
		return flowcollect.OwnershipFenceEvidence{}, ErrFlowStateCleanupInvalidEvidence
	}
	if !oldPlanRevokedAt.Valid || !revokePlanActivatedAt.Valid || !newPlanActivatedAt.Valid || !oldOwnerDrainedAt.Valid || !drainConfigVersion.Valid || !principalRevokedAt.Valid || !drainHash.Valid || !revokeHash.Valid {
		return flowcollect.OwnershipFenceEvidence{}, ErrCollectorEvidenceNotReady
	}
	if oldPlanStatus != string(CollectorPlanRetired) || (revokePlanStatus != string(CollectorPlanActive) && revokePlanStatus != string(CollectorPlanRetired)) || (newPlanStatus != string(CollectorPlanActive) && newPlanStatus != string(CollectorPlanRetired)) || oldRevokeRevision != uint64(drainConfigVersion.Int64) || oldPlanExpiresAt.IsZero() || principalStatus != "revoked" || !validSHA256Hex(drainHash.String) || !validSHA256Hex(revokeHash.String) || !validSHA256Hex(grantHash.String) || principalRef.String == "" || oldOwnerDrainedAt.Time.Before(revokePlanActivatedAt.Time) {
		return flowcollect.OwnershipFenceEvidence{}, ErrFlowStateCleanupInvalidEvidence
	}
	evidence.OldPlanRevokedAt = oldPlanRevokedAt.Time
	evidence.OldPlanExpiresAt = oldPlanExpiresAt
	evidence.OldOwnerDrainedAt = oldOwnerDrainedAt.Time
	evidence.OldPrincipalWriteRevokedAt = principalRevokedAt.Time
	evidence.NewPlanActivatedAt = newPlanActivatedAt.Time
	evidence.MaxClockSkew = maxClockSkew
	evidence.ACLPropagationDelay = aclPropagation
	evidence.UniqueOldPrincipal = true
	return evidence, nil
}

func (p *MySQLFlowStateCleanupEvidenceProvider) ReplacementRestoreProof(ctx context.Context, snapshot flowcollect.StateCleanupSnapshot) (FlowStateCleanupRestoreProof, error) {
	if p == nil || p.db == nil || ctx == nil {
		return FlowStateCleanupRestoreProof{}, errors.New("flow state-cleanup evidence provider is unavailable")
	}
	if _, err := flowcollect.RestoreStateCleanupJob(snapshot); err != nil {
		return FlowStateCleanupRestoreProof{}, fmt.Errorf("%w: %v", ErrFlowStateCleanupInvalidEvidence, err)
	}
	var proof FlowStateCleanupRestoreProof
	var newPlanRevision, restoreConfigVersion, newOwnershipEpoch uint64
	var receiptHash string
	var reportedAt, newPlanActivatedAt time.Time
	err := p.db.QueryRowContext(ctx, `
		SELECT
			transfer.new_plan_revision,
			transfer.new_ownership_epoch,
			receipt.restore_config_version,
			receipt.restored_old_ownership_epoch,
			receipt.restored_old_generation,
			receipt.new_epoch_baseline_generation,
			receipt.receipt_sha256,
			receipt.reported_at,
			new_plan.activated_at
		FROM collector_ownership_transfers AS transfer
		JOIN collector_state_restore_receipts AS receipt
		  ON receipt.tenant_id = transfer.tenant_id
		 AND receipt.transfer_id = transfer.id
		 AND receipt.state_kind = ?
		 AND receipt.state_identity_key = ?
		JOIN collector_plan_revisions AS new_plan
		  ON new_plan.tenant_id = transfer.tenant_id
		 AND new_plan.collector_id = transfer.new_collector_id
		 AND new_plan.config_version = transfer.new_plan_revision
		WHERE transfer.tenant_id = ? AND transfer.exporter_id = ?
		  AND transfer.old_collector_id = ? AND transfer.old_plan_revision = ?
		  AND transfer.old_ownership_epoch = ?
	`, snapshot.Old.Kind, snapshot.Old.IdentityKey, snapshot.Old.TenantID,
		snapshot.Old.ExporterID, snapshot.Old.CollectorID,
		snapshot.Old.RegistryVersion, snapshot.Old.OwnershipEpoch).Scan(
		&newPlanRevision, &newOwnershipEpoch, &restoreConfigVersion,
		&proof.RestoredOldOwnershipEpoch, &proof.RestoredOldGeneration,
		&proof.NewEpochBaselineGeneration, &receiptHash, &reportedAt, &newPlanActivatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return FlowStateCleanupRestoreProof{}, ErrCollectorEvidenceNotReady
	}
	if err != nil {
		return FlowStateCleanupRestoreProof{}, err
	}
	if restoreConfigVersion != newPlanRevision || newOwnershipEpoch <= snapshot.Old.OwnershipEpoch || proof.RestoredOldOwnershipEpoch != snapshot.Old.OwnershipEpoch || proof.RestoredOldGeneration != snapshot.Old.StateGeneration || !validSHA256Hex(receiptHash) || reportedAt.Before(newPlanActivatedAt) {
		return FlowStateCleanupRestoreProof{}, ErrFlowStateCleanupInvalidEvidence
	}
	return proof, nil
}

func evidenceMilliseconds(value uint64) (time.Duration, bool) {
	if value > uint64((24*time.Hour)/time.Millisecond) {
		return 0, false
	}
	return time.Duration(value) * time.Millisecond, true
}

var _ FlowStateCleanupEvidenceProvider = (*MySQLFlowStateCleanupEvidenceProvider)(nil)
