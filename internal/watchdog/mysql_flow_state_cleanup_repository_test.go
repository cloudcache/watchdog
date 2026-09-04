package watchdog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

func TestOperationJobsMigrationHasLeaseAndAuditSafetyContract(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "017_operation_jobs.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, fragment := range []string{
		"create table if not exists operation_jobs",
		"unique key uq_operation_jobs_idempotency",
		"lease_token varchar(64)",
		"lease_expires_at datetime(3)",
		"progress_total bigint unsigned",
		"progress_done bigint unsigned",
		"checkpoint_json json not null",
		"result_ref varchar(1024)",
		"cancel_requested_at datetime(3)",
		"expires_at datetime(3)",
		"row_version bigint unsigned not null",
		"check ((status in ('running','validating','cancel_requested')) =",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Fatalf("operation job migration missing %q", fragment)
		}
	}
}

func TestScanFlowStateCleanupJobRestoresValidatedCheckpoint(t *testing.T) {
	snapshot := flowStateCleanupSnapshotFixture(t)
	checkpointJSON, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	base := snapshot.CreatedAt
	job, err := scanFlowStateCleanupJob(fakeRow{values: []any{
		ID(snapshot.JobID), ID(snapshot.Old.TenantID), OperationJobQueued,
		"cleanup-request-1", strings.Repeat("a", 64), checkpointJSON,
		nil, nil, nil, base.Add(time.Minute), uint32(0),
		nil, nil, uint64(1), sql.NullString{String: snapshot.RequestedBy, Valid: true}, base,
		nil, nil, nil, base,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != ID(snapshot.JobID) || job.Status != OperationJobQueued || job.RowVersion != 1 || job.Snapshot.Phase != flowcollect.StateCleanupAwaitingFence || job.LeaseToken != "" {
		t.Fatalf("unexpected scanned cleanup job: %+v", job)
	}

	tampered := snapshot
	tampered.JobID = "other-job"
	tamperedJSON, err := json.Marshal(tampered)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scanFlowStateCleanupJob(fakeRow{values: []any{
		ID(snapshot.JobID), ID(snapshot.Old.TenantID), OperationJobQueued,
		"cleanup-request-1", strings.Repeat("a", 64), tamperedJSON,
		nil, nil, nil, base.Add(time.Minute), uint32(0),
		nil, nil, uint64(1), sql.NullString{String: snapshot.RequestedBy, Valid: true}, base,
		nil, nil, nil, base,
	}}); err == nil {
		t.Fatal("stored job/checkpoint identity mismatch was accepted")
	}
}

func TestFlowStateCleanupCheckpointAdvanceIsStrictAndImmutable(t *testing.T) {
	current := flowStateCleanupSnapshotFixture(t)
	job, err := flowcollect.RestoreStateCleanupJob(current)
	if err != nil {
		t.Fatal(err)
	}
	base := current.CreatedAt
	fence := flowcollect.OwnershipFenceEvidence{
		OldCollectorID: current.Old.CollectorID, NewCollectorID: "collector-b",
		OldPlanRevision: current.Old.RegistryVersion, NewPlanRevision: current.Old.RegistryVersion + 1,
		OldOwnershipEpoch: current.Old.OwnershipEpoch, NewOwnershipEpoch: current.Old.OwnershipEpoch + 1,
		OldPlanRevokedAt: base.Add(time.Second), OldPlanExpiresAt: base.Add(2 * time.Second),
		OldOwnerDrainedAt: base.Add(2 * time.Second), OldPrincipalWriteRevokedAt: base.Add(time.Second),
		NewPlanActivatedAt: base.Add(2 * time.Second), UniqueOldPrincipal: true,
	}
	if err := job.ConfirmFence(fence, base.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	next := job.Snapshot()
	if !validFlowStateCleanupAdvance(current, next) {
		t.Fatal("valid single-phase checkpoint advance was rejected")
	}
	if validFlowStateCleanupAdvance(current, current) {
		t.Fatal("same-phase checkpoint rewrite was accepted")
	}
	next.RequestedBy = "different-user"
	if validFlowStateCleanupAdvance(current, next) {
		t.Fatal("cleanup requester mutation was accepted")
	}
}

func TestValidateNewFlowStateCleanupJob(t *testing.T) {
	snapshot := flowStateCleanupSnapshotFixture(t)
	job := FlowStateCleanupJob{
		ID: ID(snapshot.JobID), TenantID: ID(snapshot.Old.TenantID), CreatedBy: ID(snapshot.RequestedBy),
		IdempotencyKey: "cleanup-request-1", RequestHash: mustFlowStateCleanupRequestHash(t, snapshot), Snapshot: snapshot,
	}
	if err := validateNewFlowStateCleanupJob(job); err != nil {
		t.Fatal(err)
	}
	job.RequestHash = strings.Repeat("A", 64)
	if err := validateNewFlowStateCleanupJob(job); err == nil {
		t.Fatal("uppercase request hash was accepted")
	}
}

func TestMySQLFlowStateCleanupRepositoryLifecycle(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_FLOW_CLEANUP_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_FLOW_CLEANUP_MYSQL_TEST_DSN to run MySQL flow state-cleanup integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := ApplyMySQLMigrations(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	snapshot := flowStateCleanupSnapshotFixture(t)
	tenantID, userID := ID(snapshot.Old.TenantID), ID(snapshot.RequestedBy)
	if _, err := db.Exec("DELETE FROM tenants WHERE id = ?", tenantID); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DELETE FROM tenants WHERE id = ?", tenantID)
	if _, err := db.Exec("INSERT INTO tenants (id, name, status) VALUES (?, 'Flow Cleanup Repository Check', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO users (id, tenant_id, email, name, status) VALUES (?, ?, 'flow-cleanup-check@watchdog.local', 'Flow Cleanup Check', 'active')", userID, tenantID); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)
	request := FlowStateCleanupJob{
		ID: ID(snapshot.JobID), TenantID: tenantID, CreatedBy: userID,
		IdempotencyKey: "cleanup-request-1", RequestHash: mustFlowStateCleanupRequestHash(t, snapshot), Snapshot: snapshot,
		NextAttemptAt: time.Unix(1, 0).UTC(),
	}
	created, err := store.CreateFlowStateCleanupJob(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != OperationJobQueued || created.RowVersion != 1 {
		t.Fatalf("created job=%+v", created)
	}
	replayed, err := store.CreateFlowStateCleanupJob(context.Background(), request)
	if err != nil || replayed.ID != created.ID || replayed.RowVersion != created.RowVersion {
		t.Fatalf("idempotent create=%+v err=%v", replayed, err)
	}
	conflict := request
	conflict.ID = "flow_cleanup_job_0000002"
	conflict.Snapshot.JobID = string(conflict.ID)
	conflict.Snapshot.ApprovalID = "approval-2"
	conflict.RequestHash = mustFlowStateCleanupRequestHash(t, conflict.Snapshot)
	if _, err := store.CreateFlowStateCleanupJob(context.Background(), conflict); !errors.Is(err, ErrFlowStateCleanupIdempotencyConflict) {
		t.Fatalf("idempotency conflict error=%v", err)
	}

	claimed, ok, err := store.ClaimFlowStateCleanupJob(context.Background(), "reconciler-a", "lease-1", time.Minute)
	if err != nil || !ok || claimed.ID != created.ID || claimed.Status != OperationJobRunning || claimed.RowVersion != 2 || claimed.AttemptCount != 1 {
		t.Fatalf("claimed=%+v ok=%t err=%v", claimed, ok, err)
	}
	if err := store.RenewFlowStateCleanupLease(context.Background(), claimed.ID, claimed.LeaseToken, time.Minute); err != nil {
		t.Fatal(err)
	}
	stateMachine, err := flowcollect.RestoreStateCleanupJob(claimed.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	base := claimed.Snapshot.CreatedAt
	fence := flowcollect.OwnershipFenceEvidence{
		OldCollectorID: claimed.Snapshot.Old.CollectorID, NewCollectorID: "collector-b",
		OldPlanRevision: claimed.Snapshot.Old.RegistryVersion, NewPlanRevision: claimed.Snapshot.Old.RegistryVersion + 1,
		OldOwnershipEpoch: claimed.Snapshot.Old.OwnershipEpoch, NewOwnershipEpoch: claimed.Snapshot.Old.OwnershipEpoch + 1,
		OldPlanRevokedAt: base.Add(time.Second), OldPlanExpiresAt: base.Add(2 * time.Second),
		OldOwnerDrainedAt: base.Add(2 * time.Second), OldPrincipalWriteRevokedAt: base.Add(time.Second),
		NewPlanActivatedAt: base.Add(2 * time.Second), UniqueOldPrincipal: true,
	}
	if err := stateMachine.ConfirmFence(fence, base.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	checkpointed, err := store.SaveFlowStateCleanupCheckpoint(context.Background(), claimed.ID, claimed.LeaseToken, claimed.RowVersion, stateMachine.Snapshot(), time.Unix(1, 0).UTC())
	if err != nil || checkpointed.RowVersion != 3 || checkpointed.Snapshot.Phase != flowcollect.StateCleanupAwaitingReplacement {
		t.Fatalf("checkpointed=%+v err=%v", checkpointed, err)
	}
	requeued, err := store.RequeueFlowStateCleanupJob(context.Background(), checkpointed.ID, checkpointed.LeaseToken, checkpointed.RowVersion, "KAFKA_UNAVAILABLE", "injected retry", time.Unix(1, 0).UTC())
	if err != nil || requeued.Status != OperationJobQueued || requeued.RowVersion != 4 || requeued.LeaseToken != "" {
		t.Fatalf("requeued=%+v err=%v", requeued, err)
	}
	if err := store.RenewFlowStateCleanupLease(context.Background(), requeued.ID, "lease-1", time.Minute); !errors.Is(err, ErrFlowStateCleanupConflict) {
		t.Fatalf("stale lease renewal error=%v", err)
	}
	claimedAgain, ok, err := store.ClaimFlowStateCleanupJob(context.Background(), "reconciler-b", "lease-2", time.Minute)
	if err != nil || !ok || claimedAgain.ID != created.ID || claimedAgain.RowVersion != 5 || claimedAgain.AttemptCount != 2 {
		t.Fatalf("claimed again=%+v ok=%t err=%v", claimedAgain, ok, err)
	}
	failed, err := store.FailFlowStateCleanupJob(context.Background(), claimedAgain.ID, claimedAgain.LeaseToken, claimedAgain.RowVersion, "INVALID_EVIDENCE", "injected terminal failure")
	if err != nil || failed.Status != OperationJobFailed || failed.RowVersion != 6 || failed.FinishedAt.IsZero() {
		t.Fatalf("failed=%+v err=%v", failed, err)
	}
	var auditCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM audit_logs WHERE tenant_id = ? AND resource_type = 'operation_job' AND resource_id = ?", tenantID, created.ID).Scan(&auditCount); err != nil || auditCount != 6 {
		t.Fatalf("audit count=%d err=%v", auditCount, err)
	}
}

func flowStateCleanupSnapshotFixture(t *testing.T) flowcollect.StateCleanupSnapshot {
	t.Helper()
	base := time.Unix(1_000_000, 0).UTC()
	identity := sha256.Sum256([]byte("state-cleanup-repository-identity"))
	stateKey := testCollectStatePartitionKey(identity, 5)
	payload := sha256.Sum256([]byte("validated-old-checkpoint"))
	snapshot := flowcollect.StateCleanupSnapshot{
		SchemaVersion: 1,
		JobID:         "flow_cleanup_job_0000001",
		ApprovalID:    "approval-1",
		RequestedBy:   "user_cleanup_00000000001",
		Phase:         flowcollect.StateCleanupAwaitingFence,
		Old: flowcollect.StateCleanupCheckpoint{
			Kind: flowcollect.StateCheckpointDecoder, KafkaKey: stateKey, IdentityKey: identity[:], PayloadSHA256: payload[:],
			TenantID: "tenant_cleanup_000000001", ExporterID: "exporter-a", CollectorID: "collector-a",
			RegistryVersion: 20, OwnershipEpoch: 5, StateGeneration: 9, CheckpointAt: base,
		},
		CreatedAt: base.Add(time.Second), UpdatedAt: base.Add(time.Second),
	}
	if _, err := flowcollect.RestoreStateCleanupJob(snapshot); err != nil {
		t.Fatalf("invalid cleanup repository fixture: %v", err)
	}
	return snapshot
}

func mustFlowStateCleanupRequestHash(t *testing.T, snapshot flowcollect.StateCleanupSnapshot) string {
	t.Helper()
	hash, err := FlowStateCleanupRequestHash(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func testCollectStatePartitionKey(identity [sha256.Size]byte, epoch uint64) []byte {
	hash := sha256.New()
	testWriteHashField(hash, []byte("watchdog.flow.collect-state.partition.v2"))
	testWriteHashField(hash, identity[:])
	var epochBytes [8]byte
	binary.BigEndian.PutUint64(epochBytes[:], epoch)
	testWriteHashField(hash, epochBytes[:])
	return hash.Sum(nil)
}

func testWriteHashField(hash interface{ Write([]byte) (int, error) }, value []byte) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	_, _ = hash.Write(size[:])
	_, _ = hash.Write(value)
}
