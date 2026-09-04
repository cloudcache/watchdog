package watchdog

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

func TestMySQLCollectorOwnershipEvidenceLifecycle(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_COLLECTOR_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_COLLECTOR_MYSQL_TEST_DSN to run collector ownership evidence integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := ApplyMySQLMigrations(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	tenantID := ID("tenant_transfer_test_001")
	userID := ID("user_transfer_test_00001")
	oldCollectorID, newCollectorID := ID("collector_transfer_old"), ID("collector_transfer_new")
	principalID, transferID := ID("principal_transfer_old"), ID("transfer_exporter_000001")
	exporterID := ID("exporter_transfer_test")
	if err := cleanupCollectorOwnershipEvidenceFixture(ctx, db, tenantID); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cleanupCollectorOwnershipEvidenceFixture(context.Background(), db, tenantID) }()
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'Ownership Transfer Test', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO users (id, tenant_id, email, name, status) VALUES (?, ?, 'ownership-transfer@watchdog.local', 'Ownership Transfer', 'active')", userID, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_agents (
			id, tenant_id, module_key, name, agent_type, mode, status,
			observed_health, auth_type, token_hash, plan_schema_min,
			plan_schema_max, created_by, updated_by
		) VALUES
			(?, ?, 'flow', 'Old Flow Collector', 'flow_collect', 'listen', 'pending',
			 'unknown', 'token', 'old-transfer-token', 2, 2, ?, ?),
			(?, ?, 'flow', 'New Flow Collector', 'flow_collect', 'listen', 'pending',
			 'unknown', 'token', 'new-transfer-token', 2, 2, ?, ?)
	`, oldCollectorID, tenantID, userID, userID, newCollectorID, tenantID, userID, userID); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)

	base := time.Now().UTC().Truncate(time.Millisecond)
	domainID := uint64(42)
	oldSources := []flowcollect.SourceBinding{{
		Protocol: flowcollect.ProtocolNetFlow9, SourcePrefix: "192.0.2.31/32",
		ObservationDomainID: &domainID, TenantID: string(tenantID), ExporterID: string(exporterID),
		TargetID: "target-flow-transfer", OwnershipEpoch: 5,
		SamplingMode: flowcollect.SamplingModeSampled, Enabled: true,
	}}
	newSources := append([]flowcollect.SourceBinding(nil), oldSources...)
	newSources[0].OwnershipEpoch = 6
	oldPlan1 := flowCollectorPlanFixture(t, "plan_transfer_old_00001", tenantID, oldCollectorID, userID, 1, 0, base, oldSources)
	oldPlan1, err = store.CreateCollectorPlanRevision(ctx, oldPlan1)
	if err != nil {
		t.Fatal(err)
	}
	oldPlan1, err = store.ActivateCollectorPlanRevision(ctx, CollectorPlanActivation{
		TenantID: tenantID, CollectorID: oldCollectorID, ConfigVersion: 1,
		ExpectedCollectorRowVersion: 1, ExpectedPlanRowVersion: oldPlan1.RowVersion, ActorID: userID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeCollectorPlan(ctx, CollectorPlanAcknowledgement{
		TenantID: tenantID, CollectorID: oldCollectorID, ConfigVersion: 1,
		SpecHash: oldPlan1.SpecHash, BootID: "boot-old-1", SoftwareVersion: "1.0.0",
	}); err != nil {
		t.Fatal(err)
	}
	oldRevokePlan := flowCollectorPlanFixture(t, "plan_transfer_old_00002", tenantID, oldCollectorID, userID, 2, 1, base.Add(time.Second), nil)
	oldRevokePlan, err = store.CreateCollectorPlanRevision(ctx, oldRevokePlan)
	if err != nil {
		t.Fatal(err)
	}
	newPlan := flowCollectorPlanFixture(t, "plan_transfer_new_00001", tenantID, newCollectorID, userID, 1, 0, base.Add(time.Second), newSources)
	newPlan, err = store.CreateCollectorPlanRevision(ctx, newPlan)
	if err != nil {
		t.Fatal(err)
	}

	grant := CollectorServicePrincipalGrant{
		ID: principalID, TenantID: tenantID, CollectorID: oldCollectorID,
		ServiceType: "kafka", PrincipalRef: "User:flow-old-transfer-test",
		CredentialSecretRef: "secret://flow/old-transfer", Provider: "kafka-admin",
		GrantReceiptRef: "kafka://acl/grant/old-transfer", GrantReceipt: []byte("verified grant response"),
		ACLPropagationDelay: 2 * time.Second, ActorID: userID,
	}
	if err := store.CreateCollectorServicePrincipal(ctx, grant); err != nil {
		t.Fatal(err)
	}
	duplicatePrincipal := grant
	duplicatePrincipal.ID = "principal_transfer_dup"
	duplicatePrincipal.CollectorID = newCollectorID
	if err := store.CreateCollectorServicePrincipal(ctx, duplicatePrincipal); err == nil {
		t.Fatal("globally reused Kafka principal was accepted")
	}

	transfer := CollectorOwnershipTransfer{
		ID: transferID, TenantID: tenantID, ExporterID: exporterID,
		OldCollectorID: oldCollectorID, NewCollectorID: newCollectorID,
		OldPlanRevision: 1, OldRevokePlanRevision: 2, NewPlanRevision: 1,
		OldOwnershipEpoch: 5, NewOwnershipEpoch: 6, OldPrincipalID: principalID,
		MaxClockSkew: 500 * time.Millisecond, ApprovalID: "approval_transfer_00001",
		RequestedBy: userID, ExpectedOldCollectorRow: 2, ExpectedNewCollectorRow: 1,
	}
	if err := store.CreateCollectorOwnershipTransfer(ctx, transfer); err != nil {
		t.Fatal(err)
	}
	authority, err := store.GetFlowStateCleanupAuthority(ctx, tenantID, transferID)
	if err != nil {
		t.Fatal(err)
	}
	if authority.TransferID != transferID || authority.TenantID != tenantID || authority.ExporterID != transfer.ExporterID || authority.OldCollectorID != oldCollectorID || authority.OldPlanRevision != transfer.OldPlanRevision || authority.OldOwnershipEpoch != transfer.OldOwnershipEpoch || authority.ApprovalID != transfer.ApprovalID {
		t.Fatalf("cleanup authority=%+v", authority)
	}
	evidenceProvider, err := NewMySQLFlowStateCleanupEvidenceProvider(db)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := ownershipTransferCleanupSnapshot(t, tenantID, userID, exporterID, oldCollectorID)
	if _, err := evidenceProvider.OwnershipFence(ctx, snapshot); !errors.Is(err, ErrCollectorEvidenceNotReady) {
		t.Fatalf("early ownership fence error=%v", err)
	}
	if err := store.RecordCollectorDrain(ctx, CollectorDrainReceipt{
		TenantID: tenantID, TransferID: transferID, AuthenticatedCollectorID: oldCollectorID,
		BootID: "boot-old-1", AppliedConfigVersion: 2, ReceiptNonce: "drain-before-revoke-plan",
	}); !errors.Is(err, ErrCollectorEvidenceNotReady) {
		t.Fatalf("premature drain error=%v", err)
	}

	oldRevokePlan, err = store.ActivateCollectorPlanRevision(ctx, CollectorPlanActivation{
		TenantID: tenantID, CollectorID: oldCollectorID, ConfigVersion: 2,
		ExpectedCollectorRowVersion: 2, ExpectedPlanRowVersion: oldRevokePlan.RowVersion, ActorID: userID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeCollectorPlan(ctx, CollectorPlanAcknowledgement{
		TenantID: tenantID, CollectorID: oldCollectorID, ConfigVersion: 2,
		SpecHash: oldRevokePlan.SpecHash, BootID: "boot-old-2", SoftwareVersion: "1.0.0",
	}); err != nil {
		t.Fatal(err)
	}
	newPlan, err = store.ActivateCollectorPlanRevision(ctx, CollectorPlanActivation{
		TenantID: tenantID, CollectorID: newCollectorID, ConfigVersion: 1,
		ExpectedCollectorRowVersion: 1, ExpectedPlanRowVersion: newPlan.RowVersion, ActorID: userID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeCollectorPlan(ctx, CollectorPlanAcknowledgement{
		TenantID: tenantID, CollectorID: newCollectorID, ConfigVersion: 1,
		SpecHash: newPlan.SpecHash, BootID: "boot-new-1", SoftwareVersion: "1.0.0",
	}); err != nil {
		t.Fatal(err)
	}
	drain := CollectorDrainReceipt{
		TenantID: tenantID, TransferID: transferID, AuthenticatedCollectorID: oldCollectorID,
		BootID: "boot-old-2", AppliedConfigVersion: 2, ReceiptNonce: "drain-receipt-1",
	}
	if err := store.RecordCollectorDrain(ctx, drain); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCollectorDrain(ctx, drain); err != nil {
		t.Fatalf("idempotent drain receipt: %v", err)
	}
	conflictingDrain := drain
	conflictingDrain.ReceiptNonce = "different-drain"
	if err := store.RecordCollectorDrain(ctx, conflictingDrain); !errors.Is(err, ErrCollectorEvidenceConflict) {
		t.Fatalf("conflicting drain receipt error=%v", err)
	}
	revocation := CollectorPrincipalRevocation{
		TenantID: tenantID, PrincipalID: principalID, ExpectedRowVersion: 1,
		Provider: "kafka-admin", RevokeReceiptRef: "kafka://acl/revoke/old-transfer",
		RevokeReceipt: []byte("verified revoke response"), ActorID: userID,
	}
	if err := store.RevokeCollectorServicePrincipal(ctx, revocation); err != nil {
		t.Fatal(err)
	}
	if err := store.RevokeCollectorServicePrincipal(ctx, revocation); err != nil {
		t.Fatalf("idempotent principal revocation: %v", err)
	}

	fence, err := evidenceProvider.OwnershipFence(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if fence.OldCollectorID != string(oldCollectorID) || fence.NewCollectorID != string(newCollectorID) || fence.OldPlanRevision != 1 || fence.NewPlanRevision != 1 || fence.OldOwnershipEpoch != 5 || fence.NewOwnershipEpoch != 6 || !fence.UniqueOldPrincipal || fence.MaxClockSkew != 500*time.Millisecond || fence.ACLPropagationDelay != 2*time.Second {
		t.Fatalf("ownership fence=%+v", fence)
	}
	if !fence.OldOwnerDrainedAt.After(fence.OldPlanRevokedAt) && !fence.OldOwnerDrainedAt.Equal(fence.OldPlanRevokedAt) {
		t.Fatalf("drain precedes old plan revocation: %+v", fence)
	}
	if _, err := evidenceProvider.ReplacementRestoreProof(ctx, snapshot); !errors.Is(err, ErrCollectorEvidenceNotReady) {
		t.Fatalf("early restore proof error=%v", err)
	}
	restore := CollectorStateRestoreReceipt{
		TenantID: tenantID, TransferID: transferID, AuthenticatedCollectorID: newCollectorID,
		Kind: snapshot.Old.Kind, StateIdentityKey: snapshot.Old.IdentityKey,
		BootID: "boot-new-1", AppliedConfigVersion: 1,
		RestoredOldOwnershipEpoch: 5, RestoredOldGeneration: snapshot.Old.StateGeneration,
		NewEpochBaselineGeneration: 3, ReceiptNonce: "restore-receipt-1",
	}
	if err := store.RecordCollectorStateRestore(ctx, restore); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCollectorStateRestore(ctx, restore); err != nil {
		t.Fatalf("idempotent restore receipt: %v", err)
	}
	conflictingRestore := restore
	conflictingRestore.RestoredOldGeneration++
	if err := store.RecordCollectorStateRestore(ctx, conflictingRestore); !errors.Is(err, ErrCollectorEvidenceConflict) {
		t.Fatalf("conflicting restore receipt error=%v", err)
	}
	proof, err := evidenceProvider.ReplacementRestoreProof(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if proof.RestoredOldOwnershipEpoch != 5 || proof.RestoredOldGeneration != snapshot.Old.StateGeneration || proof.NewEpochBaselineGeneration != 3 {
		t.Fatalf("restore proof=%+v", proof)
	}
	if _, err := db.ExecContext(ctx, "UPDATE collector_state_restore_receipts SET receipt_sha256 = ? WHERE transfer_id = ?", strings.Repeat("F", 64), transferID); err != nil {
		t.Fatal(err)
	}
	if _, err := evidenceProvider.ReplacementRestoreProof(ctx, snapshot); !errors.Is(err, ErrFlowStateCleanupInvalidEvidence) {
		t.Fatalf("tampered restore proof error=%v", err)
	}
}

func TestValidateFlowCollectorOwnershipPlanTransferRejectsSemanticGaps(t *testing.T) {
	at := time.Unix(1_900_000_000, 0).UTC()
	tenantID, userID := ID("tenant_semantic_plan_001"), ID("user_semantic_plan_00001")
	oldCollectorID, newCollectorID := ID("collector_semantic_old"), ID("collector_semantic_new")
	domainID := uint64(42)
	oldSource := flowcollect.SourceBinding{
		Protocol: flowcollect.ProtocolIPFIX, SourcePrefix: "192.0.2.8/32", ObservationDomainID: &domainID,
		TenantID: string(tenantID), ExporterID: "exporter-semantic", TargetID: "target-semantic",
		OwnershipEpoch: 7, SamplingMode: flowcollect.SamplingModeSampled, Enabled: true,
	}
	newSource := oldSource
	newSource.OwnershipEpoch = 8
	oldPlan := flowCollectorPlanFixture(t, "plan_semantic_old_000001", tenantID, oldCollectorID, userID, 1, 0, at, []flowcollect.SourceBinding{oldSource})
	revokePlan := flowCollectorPlanFixture(t, "plan_semantic_old_000002", tenantID, oldCollectorID, userID, 2, 1, at, nil)
	newPlan := flowCollectorPlanFixture(t, "plan_semantic_new_000001", tenantID, newCollectorID, userID, 1, 0, at, []flowcollect.SourceBinding{newSource})
	transfer := CollectorOwnershipTransfer{TenantID: tenantID, ExporterID: "exporter-semantic", OldOwnershipEpoch: 7, NewOwnershipEpoch: 8}
	if err := validateFlowCollectorOwnershipPlanTransfer(transfer, oldPlan, revokePlan, newPlan); err != nil {
		t.Fatalf("valid semantic transfer: %v", err)
	}

	tests := []struct {
		name   string
		revoke CollectorPlanRevision
		new    CollectorPlanRevision
	}{
		{name: "old successor still admits exporter", revoke: flowCollectorPlanFixture(t, "plan_semantic_bad_revoke", tenantID, oldCollectorID, userID, 2, 1, at, []flowcollect.SourceBinding{oldSource}), new: newPlan},
		{name: "new owner reuses old epoch", revoke: revokePlan, new: flowCollectorPlanFixture(t, "plan_semantic_bad_epoch", tenantID, newCollectorID, userID, 1, 0, at, []flowcollect.SourceBinding{oldSource})},
		{name: "new owner changes selector", revoke: revokePlan, new: flowCollectorPlanFixture(t, "plan_semantic_bad_source", tenantID, newCollectorID, userID, 1, 0, at, []flowcollect.SourceBinding{func() flowcollect.SourceBinding {
			changed := newSource
			changed.SourcePrefix = "192.0.2.9/32"
			return changed
		}()})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateFlowCollectorOwnershipPlanTransfer(transfer, oldPlan, test.revoke, test.new); !errors.Is(err, ErrCollectorPlanInvalidTransition) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func flowCollectorPlanFixture(t *testing.T, id string, tenantID, collectorID, userID ID, version, supersedes uint64, at time.Time, sources []flowcollect.SourceBinding) CollectorPlanRevision {
	t.Helper()
	notBefore := at.Add(-time.Minute)
	expiresAt := at.Add(time.Hour)
	spec, err := json.Marshal(flowcollect.Plan{
		SchemaVersion: 2, Revision: version, CollectorID: string(collectorID),
		NotBefore: notBefore, ExpiresAt: expiresAt,
		PartitionMapVersion: 1, PartitionMap: make([]uint32, flowcollect.VirtualShardCount),
		Sources: sources,
	})
	if err != nil {
		t.Fatal(err)
	}
	canonical, hash, err := CanonicalCollectorPlanJSON(spec)
	if err != nil {
		t.Fatal(err)
	}
	plan := CollectorPlanRevision{
		ID: ID(id), TenantID: tenantID, CollectorID: collectorID,
		ConfigVersion: version, PlanSchemaVersion: 2, Status: CollectorPlanValidated,
		SpecJSON: canonical, SpecHash: hash, SigningKeyID: "collector-plan-test-key",
		ValidationJSON: []byte(`{"valid":true}`), NotBefore: notBefore, ExpiresAt: expiresAt,
		SupersedesConfigVersion: supersedes, CreatedBy: userID,
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := CollectorPlanSigningPayload(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.Signature = ed25519.Sign(privateKey, payload)
	verified, err := VerifyCollectorPlanRevisionSignature(plan, publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return verified
}

func cleanupCollectorOwnershipEvidenceFixture(ctx context.Context, db *sql.DB, tenantID ID) error {
	tables := []string{
		"collector_state_restore_receipts",
		"collector_ownership_transfers",
		"collector_service_principals",
		"collector_plan_revisions",
		"collector_bindings",
		"agent_run_history",
		"collector_agents",
		"target_agents",
		"audit_logs",
		"targets",
		"users",
	}
	for _, table := range tables {
		if _, err := db.ExecContext(ctx, "DELETE FROM "+table+" WHERE tenant_id = ?", tenantID); err != nil {
			return err
		}
	}
	_, err := db.ExecContext(ctx, "DELETE FROM tenants WHERE id = ?", tenantID)
	return err
}

func ownershipTransferCleanupSnapshot(t *testing.T, tenantID, userID, exporterID, collectorID ID) flowcollect.StateCleanupSnapshot {
	t.Helper()
	snapshot := flowStateCleanupSnapshotFixture(t)
	snapshot.Old.TenantID = string(tenantID)
	snapshot.Old.ExporterID = string(exporterID)
	snapshot.Old.CollectorID = string(collectorID)
	snapshot.Old.RegistryVersion = 1
	snapshot.Old.OwnershipEpoch = 5
	snapshot.RequestedBy = string(userID)
	snapshot.Old.StateGeneration = 9
	if _, err := flowcollect.RestoreStateCleanupJob(snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}
