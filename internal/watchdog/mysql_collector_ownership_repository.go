package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
)

func (s *MySQLStore) CreateCollectorServicePrincipal(ctx context.Context, grant CollectorServicePrincipalGrant) error {
	if err := validateCollectorServicePrincipalGrant(grant); err != nil {
		return err
	}
	grantHash, err := evidencePayloadSHA256(grant.GrantReceipt)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var existingID, existingTenant, existingCollector ID
	var existingService, existingPrincipalRef, existingSecretRef, existingProvider, existingRequestHash, existingReceiptRef, existingReceiptHash string
	err = tx.QueryRowContext(ctx, `
		SELECT id, tenant_id, collector_id, service_type, principal_ref,
			credential_secret_ref, provider, grant_request_hash,
			grant_receipt_ref, grant_receipt_sha256
		FROM collector_service_principals
		WHERE grant_operation_key = ? FOR UPDATE
	`, grant.GrantOperationKey).Scan(
		&existingID, &existingTenant, &existingCollector, &existingService,
		&existingPrincipalRef, &existingSecretRef, &existingProvider,
		&existingRequestHash, &existingReceiptRef, &existingReceiptHash,
	)
	if err == nil {
		if existingID == grant.ID && existingTenant == grant.TenantID && existingCollector == grant.CollectorID && existingService == grant.ServiceType && existingPrincipalRef == grant.PrincipalRef && existingSecretRef == grant.CredentialSecretRef && existingProvider == grant.Provider && existingRequestHash == grant.GrantRequestHash && existingReceiptRef == grant.GrantReceiptRef && existingReceiptHash == grantHash {
			return tx.Commit()
		}
		return ErrCollectorEvidenceConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var collectorStatus string
	if err := tx.QueryRowContext(ctx, `
		SELECT status FROM collector_agents
		WHERE tenant_id = ? AND id = ? AND deleted_at IS NULL
		FOR UPDATE
	`, grant.TenantID, grant.CollectorID).Scan(&collectorStatus); err != nil {
		return err
	}
	if collectorStatus == "revoked" || collectorStatus == "deleted" {
		return ErrCollectorPlanInvalidTransition
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO collector_service_principals (
			id, tenant_id, collector_id, service_type, principal_ref,
			credential_secret_ref, provider, grant_operation_key, grant_request_hash,
			status, grant_receipt_ref,
			grant_receipt_sha256, acl_propagation_delay_ms, created_by, updated_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)
	`, grant.ID, grant.TenantID, grant.CollectorID, grant.ServiceType, grant.PrincipalRef,
		grant.CredentialSecretRef, grant.Provider, grant.GrantOperationKey, grant.GrantRequestHash, grant.GrantReceiptRef, grantHash,
		uint64(grant.ACLPropagationDelay/time.Millisecond), grant.ActorID, grant.ActorID); err != nil {
		return err
	}
	if err := insertCollectorEvidenceAudit(ctx, tx, grant.TenantID, grant.ActorID, "collector_principal", grant.ID, "collector.principal.granted", map[string]any{
		"collector_id": grant.CollectorID, "service_type": grant.ServiceType,
		"principal_ref": grant.PrincipalRef, "provider": grant.Provider,
		"grant_operation_key": grant.GrantOperationKey,
		"grant_request_hash":  grant.GrantRequestHash,
		"grant_receipt_ref":   grant.GrantReceiptRef, "grant_receipt_sha256": grantHash,
	}); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *MySQLStore) RevokeCollectorServicePrincipal(ctx context.Context, revocation CollectorPrincipalRevocation) error {
	if revocation.TenantID == "" || len(revocation.TenantID) > 26 || revocation.PrincipalID == "" || len(revocation.PrincipalID) > 26 || revocation.ExpectedRowVersion == 0 || revocation.ActorID == "" || len(revocation.ActorID) > 26 || revocation.Provider == "" || len(revocation.Provider) > 64 || !isPrintableASCII(revocation.Provider) || !validSHA256Hex(revocation.OperationKey) || revocation.RevokeReceiptRef == "" || len(revocation.RevokeReceiptRef) > 512 || !isPrintableASCII(revocation.RevokeReceiptRef) {
		return errors.New("collector principal revocation is incomplete")
	}
	receiptHash, err := evidencePayloadSHA256(revocation.RevokeReceipt)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status, provider string
	var rowVersion uint64
	var storedOperation, storedRef, storedHash sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT status, provider, row_version, revoke_operation_key,
			revoke_receipt_ref, revoke_receipt_sha256
		FROM collector_service_principals
		WHERE tenant_id = ? AND id = ?
		FOR UPDATE
	`, revocation.TenantID, revocation.PrincipalID).Scan(&status, &provider, &rowVersion, &storedOperation, &storedRef, &storedHash); err != nil {
		return err
	}
	if status == "revoked" {
		if provider == revocation.Provider && storedOperation.String == revocation.OperationKey && storedRef.String == revocation.RevokeReceiptRef && storedHash.String == receiptHash {
			return tx.Commit()
		}
		return ErrCollectorEvidenceConflict
	}
	if status != "active" || provider != revocation.Provider || rowVersion != revocation.ExpectedRowVersion {
		return ErrCollectorEvidenceConflict
	}
	revokedAt := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE collector_service_principals
		SET status = 'revoked', write_revoked_at = ?, revoke_operation_key = ?, revoke_receipt_ref = ?,
			revoke_receipt_sha256 = ?, updated_by = ?, row_version = row_version + 1,
			updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND status = 'active' AND row_version = ?
	`, revokedAt, revocation.OperationKey, revocation.RevokeReceiptRef, receiptHash, revocation.ActorID,
		revocation.TenantID, revocation.PrincipalID, revocation.ExpectedRowVersion)
	if err != nil {
		return err
	}
	if err := requireOneCollectorPlanRow(result); err != nil {
		return ErrCollectorEvidenceConflict
	}
	if err := insertCollectorEvidenceAudit(ctx, tx, revocation.TenantID, revocation.ActorID, "collector_principal", revocation.PrincipalID, "collector.principal.write_revoked", map[string]any{
		"provider": revocation.Provider, "operation_key": revocation.OperationKey, "receipt_ref": revocation.RevokeReceiptRef,
		"receipt_sha256": receiptHash, "write_revoked_at": revokedAt,
	}); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *MySQLStore) CreateCollectorOwnershipTransfer(ctx context.Context, transfer CollectorOwnershipTransfer) error {
	if err := validateCollectorOwnershipTransfer(transfer); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	type collectorHead struct {
		rowVersion, configVersion, acknowledgedVersion, lastGoodVersion uint64
		status, agentType                                               string
	}
	heads := make(map[ID]collectorHead, 2)
	rows, err := tx.QueryContext(ctx, `
		SELECT id, row_version, config_version, acknowledged_config_version,
			last_good_config_version, status, agent_type
		FROM collector_agents
		WHERE tenant_id = ? AND id IN (?, ?) AND deleted_at IS NULL
		ORDER BY id FOR UPDATE
	`, transfer.TenantID, transfer.OldCollectorID, transfer.NewCollectorID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id ID
		var head collectorHead
		if err := rows.Scan(&id, &head.rowVersion, &head.configVersion, &head.acknowledgedVersion, &head.lastGoodVersion, &head.status, &head.agentType); err != nil {
			rows.Close()
			return err
		}
		heads[id] = head
	}
	if err := rows.Close(); err != nil {
		return err
	}
	oldHead, oldOK := heads[transfer.OldCollectorID]
	newHead, newOK := heads[transfer.NewCollectorID]
	if !oldOK || !newOK || oldHead.rowVersion != transfer.ExpectedOldCollectorRow || newHead.rowVersion != transfer.ExpectedNewCollectorRow || oldHead.agentType != "flow_collect" || newHead.agentType != "flow_collect" || oldHead.status != "active" || (newHead.status != "pending" && newHead.status != "active") || oldHead.configVersion != transfer.OldPlanRevision || oldHead.acknowledgedVersion < transfer.OldPlanRevision || oldHead.lastGoodVersion < transfer.OldPlanRevision {
		return ErrCollectorEvidenceConflict
	}
	oldPlan, err := getCollectorPlanRevisionTx(ctx, tx, transfer.TenantID, transfer.OldCollectorID, transfer.OldPlanRevision, true)
	if err != nil {
		return err
	}
	oldRevokePlan, err := getCollectorPlanRevisionTx(ctx, tx, transfer.TenantID, transfer.OldCollectorID, transfer.OldRevokePlanRevision, true)
	if err != nil {
		return err
	}
	newPlan, err := getCollectorPlanRevisionTx(ctx, tx, transfer.TenantID, transfer.NewCollectorID, transfer.NewPlanRevision, true)
	if err != nil {
		return err
	}
	if oldPlan.Status != CollectorPlanActive || oldRevokePlan.Status != CollectorPlanValidated || oldRevokePlan.SupersedesConfigVersion != transfer.OldPlanRevision || newPlan.Status != CollectorPlanValidated || newPlan.SupersedesConfigVersion != newHead.configVersion {
		return ErrCollectorPlanInvalidTransition
	}
	if err := validateFlowCollectorOwnershipPlanTransfer(transfer, oldPlan, oldRevokePlan, newPlan); err != nil {
		return err
	}
	var principalCollector ID
	var principalService, principalStatus string
	if err := tx.QueryRowContext(ctx, `
		SELECT collector_id, service_type, status
		FROM collector_service_principals
		WHERE tenant_id = ? AND id = ?
		FOR UPDATE
	`, transfer.TenantID, transfer.OldPrincipalID).Scan(&principalCollector, &principalService, &principalStatus); err != nil {
		return err
	}
	if principalCollector != transfer.OldCollectorID || principalService != "kafka" || principalStatus != "active" {
		return ErrCollectorEvidenceConflict
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO collector_ownership_transfers (
			id, tenant_id, exporter_id, old_collector_id, new_collector_id,
			old_plan_revision, old_revoke_plan_revision, new_plan_revision,
			old_ownership_epoch, new_ownership_epoch, old_principal_id,
			max_clock_skew_ms, approval_id, requested_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, transfer.ID, transfer.TenantID, transfer.ExporterID,
		transfer.OldCollectorID, transfer.NewCollectorID,
		transfer.OldPlanRevision, transfer.OldRevokePlanRevision, transfer.NewPlanRevision,
		transfer.OldOwnershipEpoch, transfer.NewOwnershipEpoch, transfer.OldPrincipalID,
		uint64(transfer.MaxClockSkew/time.Millisecond), transfer.ApprovalID, transfer.RequestedBy); err != nil {
		return err
	}
	if err := insertCollectorEvidenceAudit(ctx, tx, transfer.TenantID, transfer.RequestedBy, "collector_transfer", transfer.ID, "collector.ownership_transfer.created", map[string]any{
		"exporter_id": transfer.ExporterID, "old_collector_id": transfer.OldCollectorID,
		"new_collector_id": transfer.NewCollectorID, "old_plan_revision": transfer.OldPlanRevision,
		"old_revoke_plan_revision": transfer.OldRevokePlanRevision,
		"new_plan_revision":        transfer.NewPlanRevision, "old_ownership_epoch": transfer.OldOwnershipEpoch,
		"new_ownership_epoch": transfer.NewOwnershipEpoch, "approval_id": transfer.ApprovalID,
	}); err != nil {
		return err
	}
	return tx.Commit()
}

func validateFlowCollectorOwnershipPlanTransfer(transfer CollectorOwnershipTransfer, oldRevision, revokeRevision, newRevision CollectorPlanRevision) error {
	oldPlan, err := flowPlanFromCollectorRevision(oldRevision)
	if err != nil {
		return fmt.Errorf("%w: old flow plan: %v", ErrCollectorPlanInvalidTransition, err)
	}
	revokePlan, err := flowPlanFromCollectorRevision(revokeRevision)
	if err != nil {
		return fmt.Errorf("%w: old-owner revoke flow plan: %v", ErrCollectorPlanInvalidTransition, err)
	}
	newPlan, err := flowPlanFromCollectorRevision(newRevision)
	if err != nil {
		return fmt.Errorf("%w: new-owner flow plan: %v", ErrCollectorPlanInvalidTransition, err)
	}
	oldSelectors, err := flowExporterSelectors(oldPlan, string(transfer.TenantID), string(transfer.ExporterID), transfer.OldOwnershipEpoch)
	if err != nil || len(oldSelectors) == 0 {
		return fmt.Errorf("%w: old flow plan does not exclusively own the exporter at the old epoch", ErrCollectorPlanInvalidTransition)
	}
	revokedSelectors, err := flowExporterSelectors(revokePlan, string(transfer.TenantID), string(transfer.ExporterID), 0)
	if err != nil || len(revokedSelectors) != 0 {
		return fmt.Errorf("%w: old-owner successor plan still admits the exporter", ErrCollectorPlanInvalidTransition)
	}
	newSelectors, err := flowExporterSelectors(newPlan, string(transfer.TenantID), string(transfer.ExporterID), transfer.NewOwnershipEpoch)
	if err != nil || len(newSelectors) != len(oldSelectors) {
		return fmt.Errorf("%w: new flow plan does not own the complete exporter selector set at the new epoch", ErrCollectorPlanInvalidTransition)
	}
	for selector := range oldSelectors {
		if _, ok := newSelectors[selector]; !ok {
			return fmt.Errorf("%w: exporter selector %s is missing from the new flow plan", ErrCollectorPlanInvalidTransition, selector)
		}
	}
	return nil
}

func flowPlanFromCollectorRevision(revision CollectorPlanRevision) (flowplan.Plan, error) {
	var plan flowplan.Plan
	if err := json.Unmarshal(revision.SpecJSON, &plan); err != nil {
		return flowplan.Plan{}, err
	}
	if uint16(plan.SchemaVersion) != revision.PlanSchemaVersion || plan.Revision != revision.ConfigVersion || plan.CollectorID != string(revision.CollectorID) || !plan.NotBefore.Equal(revision.NotBefore) || !plan.ExpiresAt.Equal(revision.ExpiresAt) {
		return flowplan.Plan{}, errors.New("signed plan payload does not match its revision envelope")
	}
	validationTime := plan.ExpiresAt.Add(-time.Millisecond)
	if !plan.NotBefore.IsZero() {
		validationTime = plan.NotBefore
	}
	if _, err := flowplan.CompilePlan(plan, validationTime); err != nil {
		return flowplan.Plan{}, err
	}
	return plan, nil
}

func flowExporterSelectors(plan flowplan.Plan, tenantID, exporterID string, requiredEpoch uint64) (map[string]struct{}, error) {
	selectors := make(map[string]struct{})
	for _, source := range plan.Sources {
		if !source.Enabled || source.ExporterID != exporterID {
			continue
		}
		if requiredEpoch != 0 && source.EffectiveOwnershipEpoch() != requiredEpoch {
			return nil, errors.New("exporter binding uses an unexpected ownership epoch")
		}
		domain := "*"
		if source.ObservationDomainID != nil {
			domain = fmt.Sprint(*source.ObservationDomainID)
		}
		prefix, err := netip.ParsePrefix(source.SourcePrefix)
		if err != nil {
			return nil, err
		}
		selectors[fmt.Sprintf("%d|%s|%s", source.Protocol, prefix.Masked(), domain)] = struct{}{}
	}
	return selectors, nil
}

func (s *MySQLStore) RecordCollectorDrain(ctx context.Context, receipt CollectorDrainReceipt) error {
	if receipt.TenantID == "" || len(receipt.TenantID) > 26 || receipt.TransferID == "" || len(receipt.TransferID) > 26 || receipt.AuthenticatedCollectorID == "" || len(receipt.AuthenticatedCollectorID) > 26 || receipt.BootID == "" || len(receipt.BootID) > 64 || receipt.AppliedConfigVersion == 0 || receipt.ReceiptNonce == "" || len(receipt.ReceiptNonce) > 128 {
		return errors.New("collector drain receipt is incomplete")
	}
	receiptHash, err := hashCollectorReceipt(struct {
		Type          string `json:"type"`
		TenantID      ID     `json:"tenant_id"`
		TransferID    ID     `json:"transfer_id"`
		CollectorID   ID     `json:"collector_id"`
		BootID        string `json:"boot_id"`
		ConfigVersion uint64 `json:"config_version"`
		Nonce         string `json:"nonce"`
	}{"drain", receipt.TenantID, receipt.TransferID, receipt.AuthenticatedCollectorID, receipt.BootID, receipt.AppliedConfigVersion, receipt.ReceiptNonce})
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var oldCollectorID ID
	var revokeRevision uint64
	var storedBoot, storedHash sql.NullString
	var storedConfig sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT old_collector_id, old_revoke_plan_revision,
			old_owner_boot_id, old_owner_drain_config_version, drain_receipt_sha256
		FROM collector_ownership_transfers
		WHERE tenant_id = ? AND id = ? FOR UPDATE
	`, receipt.TenantID, receipt.TransferID).Scan(&oldCollectorID, &revokeRevision, &storedBoot, &storedConfig, &storedHash); err != nil {
		return err
	}
	if oldCollectorID != receipt.AuthenticatedCollectorID || revokeRevision != receipt.AppliedConfigVersion {
		return ErrCollectorEvidenceConflict
	}
	if storedHash.Valid {
		if storedBoot.String == receipt.BootID && uint64(storedConfig.Int64) == receipt.AppliedConfigVersion && storedHash.String == receiptHash {
			return tx.Commit()
		}
		return ErrCollectorEvidenceConflict
	}
	var configVersion, acknowledgedVersion, lastGoodVersion uint64
	var bootID, status string
	if err := tx.QueryRowContext(ctx, `
		SELECT config_version, acknowledged_config_version, last_good_config_version, boot_id, status
		FROM collector_agents
		WHERE tenant_id = ? AND id = ? AND deleted_at IS NULL FOR UPDATE
	`, receipt.TenantID, oldCollectorID).Scan(&configVersion, &acknowledgedVersion, &lastGoodVersion, &bootID, &status); err != nil {
		return err
	}
	if status != "active" || configVersion < revokeRevision || acknowledgedVersion < revokeRevision || lastGoodVersion < revokeRevision || bootID != receipt.BootID {
		return ErrCollectorEvidenceNotReady
	}
	var revokeActivatedAt sql.NullTime
	err = tx.QueryRowContext(ctx, `
		SELECT activated_at FROM collector_plan_revisions
		WHERE tenant_id = ? AND collector_id = ? AND config_version = ?
		  AND status IN ('active','retired') FOR UPDATE
	`, receipt.TenantID, oldCollectorID, revokeRevision).Scan(&revokeActivatedAt)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !revokeActivatedAt.Valid) {
		return ErrCollectorEvidenceNotReady
	}
	if err != nil {
		return err
	}
	drainedAt := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE collector_ownership_transfers
		SET old_owner_boot_id = ?, old_owner_drain_config_version = ?,
			old_owner_drained_at = ?, drain_receipt_sha256 = ?,
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND old_owner_drained_at IS NULL
	`, receipt.BootID, receipt.AppliedConfigVersion, drainedAt, receiptHash, receipt.TenantID, receipt.TransferID)
	if err != nil {
		return err
	}
	if err := requireOneCollectorPlanRow(result); err != nil {
		return ErrCollectorEvidenceConflict
	}
	if err := insertCollectorEvidenceAudit(ctx, tx, receipt.TenantID, "", "collector_transfer", receipt.TransferID, "collector.ownership_transfer.drained", map[string]any{
		"collector_id": oldCollectorID, "boot_id": receipt.BootID,
		"config_version": receipt.AppliedConfigVersion, "receipt_sha256": receiptHash,
		"drained_at": drainedAt,
	}); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *MySQLStore) GetCollectorServicePrincipal(ctx context.Context, tenantID, collectorID, principalID ID) (CollectorServicePrincipal, error) {
	if s == nil || s.db == nil || ctx == nil || !validCollectorEvidenceID(tenantID) || !validCollectorEvidenceID(collectorID) || !validCollectorEvidenceID(principalID) {
		return CollectorServicePrincipal{}, errors.New("collector service principal identity is required")
	}
	return scanCollectorServicePrincipal(s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, collector_id, service_type, principal_ref,
			credential_secret_ref, provider, grant_operation_key, grant_request_hash,
			status, COALESCE(revoke_operation_key, ''), acl_propagation_delay_ms,
			row_version, created_at, updated_at
		FROM collector_service_principals
		WHERE tenant_id = ? AND collector_id = ? AND id = ?
	`, tenantID, collectorID, principalID))
}

func (s *MySQLStore) GetCollectorServicePrincipalByGrantOperation(ctx context.Context, tenantID ID, operationKey string) (CollectorServicePrincipal, error) {
	if s == nil || s.db == nil || ctx == nil || !validCollectorEvidenceID(tenantID) || !validSHA256Hex(operationKey) {
		return CollectorServicePrincipal{}, errors.New("collector service principal grant operation is required")
	}
	return scanCollectorServicePrincipal(s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, collector_id, service_type, principal_ref,
			credential_secret_ref, provider, grant_operation_key, grant_request_hash,
			status, COALESCE(revoke_operation_key, ''), acl_propagation_delay_ms,
			row_version, created_at, updated_at
		FROM collector_service_principals
		WHERE tenant_id = ? AND grant_operation_key = ?
	`, tenantID, operationKey))
}

type collectorServicePrincipalRow interface {
	Scan(...any) error
}

func scanCollectorServicePrincipal(row collectorServicePrincipalRow) (CollectorServicePrincipal, error) {
	var principal CollectorServicePrincipal
	var propagationDelayMS uint64
	err := row.Scan(
		&principal.ID, &principal.TenantID, &principal.CollectorID,
		&principal.ServiceType, &principal.PrincipalRef, &principal.CredentialSecretRef,
		&principal.Provider, &principal.GrantOperationKey, &principal.GrantRequestHash,
		&principal.Status, &principal.RevokeOperationKey, &propagationDelayMS,
		&principal.RowVersion,
		&principal.CreatedAt, &principal.UpdatedAt,
	)
	if err != nil {
		return CollectorServicePrincipal{}, err
	}
	if propagationDelayMS > uint64((24*time.Hour)/time.Millisecond) {
		return CollectorServicePrincipal{}, errors.New("stored collector service principal propagation delay is invalid")
	}
	principal.ACLPropagationDelay = time.Duration(propagationDelayMS) * time.Millisecond
	if !validCollectorEvidenceID(principal.ID) || !validCollectorEvidenceID(principal.TenantID) || !validCollectorEvidenceID(principal.CollectorID) || principal.ServiceType != "kafka" || principal.PrincipalRef == "" || len(principal.PrincipalRef) > 190 || !isPrintableASCII(principal.PrincipalRef) || principal.CredentialSecretRef == "" || len(principal.CredentialSecretRef) > 255 || !isPrintableASCII(principal.CredentialSecretRef) || principal.Provider == "" || len(principal.Provider) > 64 || !isPrintableASCII(principal.Provider) || !validSHA256Hex(principal.GrantOperationKey) || !validSHA256Hex(principal.GrantRequestHash) || !validEvidenceDuration(principal.ACLPropagationDelay) || principal.RowVersion == 0 || principal.CreatedAt.IsZero() || principal.UpdatedAt.Before(principal.CreatedAt) {
		return CollectorServicePrincipal{}, errors.New("stored collector service principal is invalid")
	}
	if (principal.Status == "active" && principal.RevokeOperationKey != "") || (principal.Status == "revoked" && !validSHA256Hex(principal.RevokeOperationKey)) || (principal.Status != "active" && principal.Status != "revoked") {
		return CollectorServicePrincipal{}, errors.New("stored collector service principal lifecycle is invalid")
	}
	return principal, nil
}

func hashCollectorReceipt(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return evidencePayloadSHA256(payload)
}

func evidenceIdentityAuditHash(identity []byte) string {
	hash, _ := evidencePayloadSHA256(identity)
	return hash
}

func insertCollectorEvidenceAudit(ctx context.Context, tx *sql.Tx, tenantID, actorID ID, resourceType string, resourceID ID, action string, detail map[string]any) error {
	payload, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	createdAt := time.Now().UTC()
	var actor any
	if actorID != "" {
		actor = actorID
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO audit_logs (
			id, tenant_id, actor_id, action, resource_type, resource_id, detail_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, stableID("audit", string(resourceID), action, createdAt.Format(time.RFC3339Nano)),
		tenantID, actor, action, resourceType, resourceID, payload, createdAt)
	if err != nil {
		return fmt.Errorf("insert collector ownership evidence audit: %w", err)
	}
	return nil
}

var _ CollectorOwnershipRepository = (*MySQLStore)(nil)
var _ CollectorPrincipalOperationRepository = (*MySQLStore)(nil)
