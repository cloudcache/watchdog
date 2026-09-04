package watchdog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

func TestFlowStateCleanupReconcilerPersistsEveryPhaseAndCompletes(t *testing.T) {
	base := time.Unix(2_000_000, 0).UTC()
	repository := newFakeFlowStateCleanupRepository(flowStateCleanupJobFixture(t))
	evidence := cleanupEvidenceFixture(base)
	reader := cleanupReaderFixture(base)
	writer := &fakeFlowStateCleanupWriter{receipt: flowcollect.StateTombstoneReceipt{
		Position: flowcollect.KafkaRecordPosition{Partition: 0, Offset: 20}, AcknowledgedAt: base.Add(12 * time.Second),
	}}
	reconciler := newTestFlowStateCleanupReconciler(t, repository, evidence, reader, writer, base.Add(10*time.Second))

	worked, err := reconciler.ReconcileOne(context.Background())
	if err != nil || !worked {
		t.Fatalf("worked=%t err=%v", worked, err)
	}
	job := repository.snapshot()
	if job.Status != OperationJobSucceeded || job.Snapshot.Phase != flowcollect.StateCleanupComplete || job.LeaseToken != "" || job.RowVersion != 6 {
		t.Fatalf("completed job=%+v", job)
	}
	wantPhases := []flowcollect.StateCleanupPhase{
		flowcollect.StateCleanupAwaitingReplacement,
		flowcollect.StateCleanupReadyToTombstone,
		flowcollect.StateCleanupAwaitingVerification,
		flowcollect.StateCleanupComplete,
	}
	if got := repository.savedPhasesSnapshot(); len(got) != len(wantPhases) {
		t.Fatalf("saved phases=%v", got)
	} else {
		for index := range wantPhases {
			if got[index] != wantPhases[index] {
				t.Fatalf("saved phases=%v", got)
			}
		}
	}
	if reader.replacementCalls != 1 || reader.verificationCalls != 1 || writer.calls != 1 {
		t.Fatalf("reader replacement=%d verification=%d writer=%d", reader.replacementCalls, reader.verificationCalls, writer.calls)
	}
	if worked, err := reconciler.ReconcileOne(context.Background()); err != nil || worked {
		t.Fatalf("completed job remained claimable: worked=%t err=%v", worked, err)
	}
}

func TestFlowStateCleanupReconcilerCheckpointsBeforeRetry(t *testing.T) {
	base := time.Unix(2_100_000, 0).UTC()
	repository := newFakeFlowStateCleanupRepository(flowStateCleanupJobFixture(t))
	reader := cleanupReaderFixture(base)
	reader.replacementErr = ErrFlowStateCleanupReplacementNotObserved
	reconciler := newTestFlowStateCleanupReconciler(t, repository, cleanupEvidenceFixture(base), reader, &fakeFlowStateCleanupWriter{}, base.Add(10*time.Second))

	worked, err := reconciler.ReconcileOne(context.Background())
	if err != nil || !worked {
		t.Fatalf("worked=%t err=%v", worked, err)
	}
	job := repository.snapshot()
	if job.Status != OperationJobQueued || job.Snapshot.Phase != flowcollect.StateCleanupAwaitingReplacement || job.LastErrorCode != "REPLACEMENT_NOT_OBSERVED" || job.LeaseToken != "" {
		t.Fatalf("requeued job=%+v", job)
	}
	if got := repository.savedPhasesSnapshot(); len(got) != 1 || got[0] != flowcollect.StateCleanupAwaitingReplacement {
		t.Fatalf("saved phases before retry=%v", got)
	}

	reader.replacementErr = nil
	restarted := newTestFlowStateCleanupReconciler(t, repository, cleanupEvidenceFixture(base), reader, &fakeFlowStateCleanupWriter{receipt: flowcollect.StateTombstoneReceipt{
		Position: flowcollect.KafkaRecordPosition{Partition: 0, Offset: 20}, AcknowledgedAt: base.Add(12 * time.Second),
	}}, base.Add(10*time.Second))
	worked, err = restarted.ReconcileOne(context.Background())
	if err != nil || !worked {
		t.Fatalf("restarted worked=%t err=%v", worked, err)
	}
	job = repository.snapshot()
	if job.Status != OperationJobSucceeded || job.Snapshot.Phase != flowcollect.StateCleanupComplete || job.AttemptCount != 2 {
		t.Fatalf("restarted job=%+v", job)
	}
}

func TestFlowStateCleanupReconcilerClassifiesManagementEvidenceFailures(t *testing.T) {
	base := time.Unix(2_150_000, 0).UTC()
	tests := []struct {
		name       string
		evidence   *fakeFlowStateCleanupEvidence
		wantStatus OperationJobStatus
		wantCode   string
		wantRetry  int
		wantFail   int
	}{
		{
			name:       "missing fence remains retryable",
			evidence:   &fakeFlowStateCleanupEvidence{fenceErr: ErrCollectorEvidenceNotReady},
			wantStatus: OperationJobQueued,
			wantCode:   "FENCE_EVIDENCE_UNAVAILABLE",
			wantRetry:  1,
		},
		{
			name:       "invalid fence fails closed",
			evidence:   &fakeFlowStateCleanupEvidence{fenceErr: ErrFlowStateCleanupInvalidEvidence},
			wantStatus: OperationJobFailed,
			wantCode:   "INVALID_FENCE_EVIDENCE",
			wantFail:   1,
		},
		{
			name: "invalid restore proof fails closed",
			evidence: &fakeFlowStateCleanupEvidence{
				fence:    cleanupEvidenceFixture(base).fence,
				proofErr: ErrFlowStateCleanupInvalidEvidence,
			},
			wantStatus: OperationJobFailed,
			wantCode:   "INVALID_RESTORE_PROOF",
			wantFail:   1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := newFakeFlowStateCleanupRepository(flowStateCleanupJobFixture(t))
			reconciler := newTestFlowStateCleanupReconciler(t, repository, test.evidence, cleanupReaderFixture(base), &fakeFlowStateCleanupWriter{}, base.Add(10*time.Second))
			worked, err := reconciler.ReconcileOne(context.Background())
			if err != nil || !worked {
				t.Fatalf("worked=%t err=%v", worked, err)
			}
			job := repository.snapshot()
			if job.Status != test.wantStatus || job.LastErrorCode != test.wantCode || repository.requeues != test.wantRetry || repository.failures != test.wantFail {
				t.Fatalf("job=%+v requeues=%d failures=%d", job, repository.requeues, repository.failures)
			}
		})
	}
}

func TestFlowStateCleanupReconcilerFailsWhenOldStateReappears(t *testing.T) {
	base := time.Unix(2_200_000, 0).UTC()
	repository := newFakeFlowStateCleanupRepository(flowStateCleanupJobFixture(t))
	reader := cleanupReaderFixture(base)
	reader.verificationErr = ErrFlowStateCleanupOldStatePresent
	reconciler := newTestFlowStateCleanupReconciler(t, repository, cleanupEvidenceFixture(base), reader, &fakeFlowStateCleanupWriter{receipt: flowcollect.StateTombstoneReceipt{
		Position: flowcollect.KafkaRecordPosition{Partition: 0, Offset: 20}, AcknowledgedAt: base.Add(12 * time.Second),
	}}, base.Add(10*time.Second))

	worked, err := reconciler.ReconcileOne(context.Background())
	if err != nil || !worked {
		t.Fatalf("worked=%t err=%v", worked, err)
	}
	job := repository.snapshot()
	if job.Status != OperationJobFailed || job.Snapshot.Phase != flowcollect.StateCleanupAwaitingVerification || job.LastErrorCode != "OLD_STATE_REAPPEARED" || job.FinishedAt.IsZero() {
		t.Fatalf("failed job=%+v", job)
	}
}

func TestFlowStateCleanupReconcilerStopsOnLeaseRenewalFailure(t *testing.T) {
	base := time.Unix(2_300_000, 0).UTC()
	repository := newFakeFlowStateCleanupRepository(flowStateCleanupJobFixture(t))
	repository.renewErr = ErrFlowStateCleanupConflict
	evidence := cleanupEvidenceFixture(base)
	evidence.fenceWait = true
	config := DefaultFlowStateCleanupReconcilerConfig("worker-a")
	config.LeaseDuration = 30 * time.Millisecond
	config.HeartbeatInterval = 5 * time.Millisecond
	config.StepTimeout = time.Second
	config.PollInterval = time.Millisecond
	config.RetryMin = time.Millisecond
	config.RetryMax = time.Second
	reconciler, err := NewFlowStateCleanupReconciler(repository, evidence, cleanupReaderFixture(base), &fakeFlowStateCleanupWriter{}, config)
	if err != nil {
		t.Fatal(err)
	}
	reconciler.newToken = func() (string, error) { return "lease-token", nil }
	reconciler.now = func() time.Time { return base.Add(10 * time.Second) }

	worked, err := reconciler.ReconcileOne(context.Background())
	if !worked || !errors.Is(err, ErrFlowStateCleanupConflict) {
		t.Fatalf("worked=%t err=%v", worked, err)
	}
	job := repository.snapshot()
	if job.Status != OperationJobRunning || job.Snapshot.Phase != flowcollect.StateCleanupAwaitingFence || len(repository.savedPhasesSnapshot()) != 0 || repository.requeues != 0 || repository.failures != 0 {
		t.Fatalf("lease-lost job was mutated: %+v", job)
	}
}

func TestFlowStateCleanupErrorDetailIsBoundedAndSingleLine(t *testing.T) {
	detail := flowStateCleanupErrorDetail(errors.New("secret\n\t" + string(bytes.Repeat([]byte("界"), 1100))))
	if len([]rune(detail)) != 1024 || bytes.ContainsAny([]byte(detail), "\n\t") {
		t.Fatalf("unsafe error detail length=%d value=%q", len([]rune(detail)), detail[:20])
	}
}

func TestMySQLFlowStateCleanupReconcilerLifecycle(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_FLOW_CLEANUP_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_FLOW_CLEANUP_MYSQL_TEST_DSN to run MySQL flow state-cleanup reconciler integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := ApplyMySQLMigrations(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	request := flowStateCleanupJobFixture(t)
	if _, err := db.Exec("DELETE FROM tenants WHERE id = ?", request.TenantID); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DELETE FROM tenants WHERE id = ?", request.TenantID)
	if _, err := db.Exec("INSERT INTO tenants (id, name, status) VALUES (?, 'Flow Cleanup Reconciler Check', 'active')", request.TenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO users (id, tenant_id, email, name, status) VALUES (?, ?, 'flow-cleanup-reconciler@watchdog.local', 'Flow Cleanup Reconciler', 'active')", request.CreatedBy, request.TenantID); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)
	if _, err := store.CreateFlowStateCleanupJob(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	base := request.Snapshot.CreatedAt
	reconciler := newTestFlowStateCleanupReconciler(t, store, cleanupEvidenceFixture(base), cleanupReaderFixture(base), &fakeFlowStateCleanupWriter{receipt: flowcollect.StateTombstoneReceipt{
		Position: flowcollect.KafkaRecordPosition{Partition: 0, Offset: 20}, AcknowledgedAt: base.Add(12 * time.Second),
	}}, base.Add(10*time.Second))
	worked, err := reconciler.ReconcileOne(context.Background())
	if err != nil || !worked {
		t.Fatalf("worked=%t err=%v", worked, err)
	}
	stored, err := store.GetFlowStateCleanupJob(context.Background(), request.TenantID, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != OperationJobSucceeded || stored.Snapshot.Phase != flowcollect.StateCleanupComplete || stored.RowVersion != 6 || stored.AttemptCount != 1 {
		t.Fatalf("stored reconciled job=%+v", stored)
	}
	var auditCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM audit_logs WHERE tenant_id = ? AND resource_type = 'operation_job' AND resource_id = ?", request.TenantID, request.ID).Scan(&auditCount); err != nil || auditCount != 6 {
		t.Fatalf("audit count=%d err=%v", auditCount, err)
	}
}

func flowStateCleanupJobFixture(t *testing.T) FlowStateCleanupJob {
	t.Helper()
	snapshot := flowStateCleanupSnapshotFixture(t)
	return FlowStateCleanupJob{
		ID: ID(snapshot.JobID), TenantID: ID(snapshot.Old.TenantID), Status: OperationJobQueued,
		IdempotencyKey: "cleanup-request-1", RequestHash: mustFlowStateCleanupRequestHash(t, snapshot),
		Snapshot: snapshot, NextAttemptAt: time.Unix(1, 0).UTC(), RowVersion: 1,
		CreatedBy: ID(snapshot.RequestedBy), CreatedAt: snapshot.CreatedAt, UpdatedAt: snapshot.CreatedAt,
	}
}

func cleanupEvidenceFixture(base time.Time) *fakeFlowStateCleanupEvidence {
	return &fakeFlowStateCleanupEvidence{
		fence: flowcollect.OwnershipFenceEvidence{
			OldCollectorID: "collector-a", NewCollectorID: "collector-b",
			OldPlanRevision: 20, NewPlanRevision: 21, OldOwnershipEpoch: 5, NewOwnershipEpoch: 6,
			OldPlanRevokedAt: base.Add(time.Second), OldPlanExpiresAt: base.Add(4 * time.Second),
			OldOwnerDrainedAt: base.Add(2 * time.Second), OldPrincipalWriteRevokedAt: base.Add(2 * time.Second),
			NewPlanActivatedAt: base.Add(2 * time.Second), MaxClockSkew: time.Second,
			ACLPropagationDelay: 2 * time.Second, UniqueOldPrincipal: true,
		},
		proof: FlowStateCleanupRestoreProof{RestoredOldOwnershipEpoch: 5, RestoredOldGeneration: 9, NewEpochBaselineGeneration: 9},
	}
}

func cleanupReaderFixture(base time.Time) *fakeFlowStateCleanupReader {
	identity := sha256.Sum256([]byte("state-cleanup-repository-identity"))
	return &fakeFlowStateCleanupReader{
		replacement: flowcollect.StateCleanupCheckpoint{
			Kind: flowcollect.StateCheckpointDecoder, IdentityKey: identity[:], PayloadSHA256: bytes.Repeat([]byte{3}, 32),
			TenantID: "tenant_cleanup_000000001", ExporterID: "exporter-a", CollectorID: "collector-b",
			RegistryVersion: 21, OwnershipEpoch: 6, StateGeneration: 10, CheckpointAt: base.Add(10 * time.Second),
		},
		replacementObservation: flowcollect.FrozenReplacementObservation{
			CapturedAt: base.Add(11 * time.Second), Position: flowcollect.KafkaRecordPosition{Partition: 0, Offset: 10}, HighWatermark: 11,
			RestoredOldOwnershipEpoch: 5, RestoredOldGeneration: 9, NewEpochBaselineGeneration: 9,
		},
		verification: flowcollect.FrozenTombstoneVerification{CapturedAt: base.Add(13 * time.Second), Partition: 0, HighWatermark: 21, KeyAbsent: true},
	}
}

func newTestFlowStateCleanupReconciler(t *testing.T, repository FlowStateCleanupRepository, evidence FlowStateCleanupEvidenceProvider, reader FlowStateCleanupStateReader, writer FlowStateCleanupTombstoneWriter, now time.Time) *FlowStateCleanupReconciler {
	t.Helper()
	config := DefaultFlowStateCleanupReconcilerConfig("worker-a")
	config.LeaseDuration = 3 * time.Hour
	config.HeartbeatInterval = time.Hour
	config.StepTimeout = time.Second
	config.PollInterval = time.Millisecond
	config.RetryMin = time.Second
	config.RetryMax = time.Minute
	reconciler, err := NewFlowStateCleanupReconciler(repository, evidence, reader, writer, config)
	if err != nil {
		t.Fatal(err)
	}
	reconciler.newToken = func() (string, error) { return "lease-token", nil }
	reconciler.now = func() time.Time { return now }
	return reconciler
}

type fakeFlowStateCleanupRepository struct {
	mu          sync.Mutex
	job         FlowStateCleanupJob
	savedPhases []flowcollect.StateCleanupPhase
	renewErr    error
	requeues    int
	failures    int
}

func newFakeFlowStateCleanupRepository(job FlowStateCleanupJob) *fakeFlowStateCleanupRepository {
	return &fakeFlowStateCleanupRepository{job: job}
}

func (r *fakeFlowStateCleanupRepository) CreateFlowStateCleanupJob(context.Context, FlowStateCleanupJob) (FlowStateCleanupJob, error) {
	return FlowStateCleanupJob{}, errors.New("not implemented")
}

func (r *fakeFlowStateCleanupRepository) GetFlowStateCleanupJob(context.Context, ID, ID) (FlowStateCleanupJob, error) {
	return FlowStateCleanupJob{}, errors.New("not implemented")
}

func (r *fakeFlowStateCleanupRepository) ClaimFlowStateCleanupJob(_ context.Context, owner, token string, duration time.Duration) (FlowStateCleanupJob, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.job.Status != OperationJobQueued {
		return FlowStateCleanupJob{}, false, nil
	}
	r.job.Status = OperationJobRunning
	r.job.LeaseOwner = owner
	r.job.LeaseToken = token
	r.job.LeaseExpiresAt = time.Now().Add(duration)
	r.job.AttemptCount++
	r.job.RowVersion++
	return r.job, true, nil
}

func (r *fakeFlowStateCleanupRepository) RenewFlowStateCleanupLease(context.Context, ID, string, time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.renewErr
}

func (r *fakeFlowStateCleanupRepository) SaveFlowStateCleanupCheckpoint(_ context.Context, _ ID, token string, version uint64, snapshot flowcollect.StateCleanupSnapshot, _ time.Time) (FlowStateCleanupJob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.job.Status != OperationJobRunning || r.job.LeaseToken != token || r.job.RowVersion != version {
		return FlowStateCleanupJob{}, ErrFlowStateCleanupConflict
	}
	r.job.Snapshot = snapshot
	r.job.RowVersion++
	r.savedPhases = append(r.savedPhases, snapshot.Phase)
	if snapshot.Phase == flowcollect.StateCleanupComplete {
		r.job.Status = OperationJobSucceeded
		r.job.LeaseOwner = ""
		r.job.LeaseToken = ""
		r.job.LeaseExpiresAt = time.Time{}
		r.job.FinishedAt = time.Now()
	}
	return r.job, nil
}

func (r *fakeFlowStateCleanupRepository) RequeueFlowStateCleanupJob(_ context.Context, _ ID, token string, version uint64, code, detail string, next time.Time) (FlowStateCleanupJob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.job.Status != OperationJobRunning || r.job.LeaseToken != token || r.job.RowVersion != version {
		return FlowStateCleanupJob{}, ErrFlowStateCleanupConflict
	}
	r.job.Status = OperationJobQueued
	r.job.LeaseOwner = ""
	r.job.LeaseToken = ""
	r.job.LeaseExpiresAt = time.Time{}
	r.job.LastErrorCode = code
	r.job.LastErrorDetail = detail
	r.job.NextAttemptAt = next
	r.job.RowVersion++
	r.requeues++
	return r.job, nil
}

func (r *fakeFlowStateCleanupRepository) FailFlowStateCleanupJob(_ context.Context, _ ID, token string, version uint64, code, detail string) (FlowStateCleanupJob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.job.Status != OperationJobRunning || r.job.LeaseToken != token || r.job.RowVersion != version {
		return FlowStateCleanupJob{}, ErrFlowStateCleanupConflict
	}
	r.job.Status = OperationJobFailed
	r.job.LeaseOwner = ""
	r.job.LeaseToken = ""
	r.job.LeaseExpiresAt = time.Time{}
	r.job.LastErrorCode = code
	r.job.LastErrorDetail = detail
	r.job.FinishedAt = time.Now()
	r.job.RowVersion++
	r.failures++
	return r.job, nil
}

func (r *fakeFlowStateCleanupRepository) snapshot() FlowStateCleanupJob {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.job
}

func (r *fakeFlowStateCleanupRepository) savedPhasesSnapshot() []flowcollect.StateCleanupPhase {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]flowcollect.StateCleanupPhase(nil), r.savedPhases...)
}

type fakeFlowStateCleanupEvidence struct {
	fence     flowcollect.OwnershipFenceEvidence
	proof     FlowStateCleanupRestoreProof
	fenceErr  error
	proofErr  error
	fenceWait bool
}

func (e *fakeFlowStateCleanupEvidence) OwnershipFence(ctx context.Context, _ flowcollect.StateCleanupSnapshot) (flowcollect.OwnershipFenceEvidence, error) {
	if e.fenceWait {
		<-ctx.Done()
		return flowcollect.OwnershipFenceEvidence{}, ctx.Err()
	}
	return e.fence, e.fenceErr
}

func (e *fakeFlowStateCleanupEvidence) ReplacementRestoreProof(context.Context, flowcollect.StateCleanupSnapshot) (FlowStateCleanupRestoreProof, error) {
	return e.proof, e.proofErr
}

type fakeFlowStateCleanupReader struct {
	replacement            flowcollect.StateCleanupCheckpoint
	replacementObservation flowcollect.FrozenReplacementObservation
	verification           flowcollect.FrozenTombstoneVerification
	replacementErr         error
	verificationErr        error
	replacementCalls       int
	verificationCalls      int
}

func (r *fakeFlowStateCleanupReader) ObserveReplacement(_ context.Context, key []byte, _ flowcollect.StateCheckpointKind, _ FlowStateCleanupRestoreProof) (flowcollect.StateCleanupCheckpoint, flowcollect.FrozenReplacementObservation, error) {
	r.replacementCalls++
	checkpoint := r.replacement
	checkpoint.KafkaKey = bytes.Clone(key)
	return checkpoint, r.replacementObservation, r.replacementErr
}

func (r *fakeFlowStateCleanupReader) VerifyTombstone(context.Context, []byte, flowcollect.StateTombstoneReceipt) (flowcollect.FrozenTombstoneVerification, error) {
	r.verificationCalls++
	return r.verification, r.verificationErr
}

type fakeFlowStateCleanupWriter struct {
	receipt flowcollect.StateTombstoneReceipt
	err     error
	calls   int
}

func (w *fakeFlowStateCleanupWriter) Publish(context.Context, flowcollect.StateTombstone, time.Time) (flowcollect.StateTombstoneReceipt, error) {
	w.calls++
	return w.receipt, w.err
}
