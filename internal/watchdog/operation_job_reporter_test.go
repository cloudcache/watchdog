package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// capturingHeartbeatRepo records the last progress/checkpoint a reporter
// flushed, and can pretend the lease was lost. Only HeartbeatOperationJob and
// the success finish are exercised; the rest of the interface stays nil.
type capturingHeartbeatRepo struct {
	OperationJobRepository
	mu         sync.Mutex
	progress   uint64
	checkpoint json.RawMessage
	calls      int
	lost       bool
}

func (r *capturingHeartbeatRepo) HeartbeatOperationJob(_ context.Context, _ ID, _ string, _ time.Duration, progress uint64, checkpoint json.RawMessage) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.lost {
		return false, ErrOperationJobLeaseLost
	}
	r.progress = progress
	r.checkpoint = checkpoint
	return false, nil
}

func (r *capturingHeartbeatRepo) CompleteOperationJobSucceeded(context.Context, ID, string, string) error {
	return nil
}

func TestOperationJobReporterMonotonicAndCheckpointPreserve(t *testing.T) {
	repo := &capturingHeartbeatRepo{}
	rep := &OperationJobReporter{repo: repo, jobID: "j1", leaseToken: "tok", leaseFor: time.Minute}
	ctx := context.Background()
	report := func(done uint64, cp string) {
		var c json.RawMessage
		if cp != "" {
			c = json.RawMessage(cp)
		}
		if err := rep.Report(ctx, done, c); err != nil {
			t.Fatalf("report(%d,%q): %v", done, cp, err)
		}
	}
	report(2, `{"p":2}`)
	report(1, "") // lower progress is ignored; nil checkpoint preserves the stored one
	report(5, "") // higher progress advances; checkpoint still preserved

	repo.mu.Lock()
	gotProgress, gotCP := repo.progress, string(repo.checkpoint)
	repo.mu.Unlock()
	if gotProgress != 5 {
		t.Fatalf("progress = %d, want 5 (monotonic)", gotProgress)
	}
	if gotCP != `{"p":2}` {
		t.Fatalf("checkpoint = %s, want {\"p\":2} preserved", gotCP)
	}
}

func TestOperationJobReporterLeaseLostAndNilSafe(t *testing.T) {
	// A nil reporter (handler invoked directly, no worker installed one) is a
	// safe no-op, and a bare context yields no reporter.
	var nilReporter *OperationJobReporter
	if err := nilReporter.Report(context.Background(), 1, nil); err != nil {
		t.Fatalf("nil reporter report = %v", err)
	}
	if OperationJobReporterFromContext(context.Background()) != nil {
		t.Fatal("expected nil reporter from bare context")
	}

	// A lost lease surfaces to the handler so it can abandon the attempt.
	repo := &capturingHeartbeatRepo{lost: true}
	rep := &OperationJobReporter{repo: repo, jobID: "j1", leaseToken: "tok", leaseFor: time.Minute}
	if err := rep.Report(context.Background(), 1, nil); !errors.Is(err, ErrOperationJobLeaseLost) {
		t.Fatalf("report = %v, want lease lost", err)
	}
}

// TestOperationJobWorkerInstallsReporter proves the worker hands the handler an
// attempt-scoped reporter seeded with the leased job's progress floor.
func TestOperationJobWorkerInstallsReporter(t *testing.T) {
	repo := &capturingHeartbeatRepo{}
	var got *OperationJobReporter
	w := &OperationJobWorker{
		Repo: repo, JobType: "t", Owner: "o",
		Handler: func(ctx context.Context, _ OperationJob) (string, error) {
			got = OperationJobReporterFromContext(ctx)
			return "", nil
		},
	}
	w.runAttempt(context.Background(), OperationJob{ID: "j1", LeaseToken: "tok", ProgressDone: 7})
	if got == nil {
		t.Fatal("handler did not receive a reporter")
	}
	if got.progress != 7 {
		t.Fatalf("reporter progress floor = %d, want 7 (resume from leased progress)", got.progress)
	}
}

// compactJSON strips insignificant whitespace so checkpoint assertions are
// stable across MySQL JSON re-spacing (`{"page":3}` stored as `{"page": 3}`).
func compactJSON(raw json.RawMessage) string {
	return strings.ReplaceAll(string(raw), " ", "")
}

// TestMySQLOperationJobReporterPersistsAcrossTakeover is the end-to-end proof:
// a reporter publishes fenced progress + checkpoint, a takeover resumes from
// that checkpoint, the superseded attempt can no longer write, and the new
// owner continues forward.
func TestMySQLOperationJobReporterPersistsAcrossTakeover(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()

	job, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenant, JobType: "reporter_job", IdempotencyKey: "rep-1", RequestHash: strings.Repeat("a", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.LeaseNextOperationJob(ctx, "reporter_job", "worker-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	rep1 := &OperationJobReporter{repo: store, jobID: job.ID, leaseToken: first.LeaseToken, leaseFor: time.Minute, progress: first.ProgressDone, checkpoint: first.CheckpointJSON}
	if err := rep1.Report(ctx, 3, json.RawMessage(`{"page":3}`)); err != nil {
		t.Fatalf("report: %v", err)
	}
	// A lower value and a nil checkpoint must not regress the persisted state.
	if err := rep1.Report(ctx, 1, nil); err != nil {
		t.Fatalf("monotonic report: %v", err)
	}
	mid, err := store.GetOperationJob(ctx, tenant, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if mid.ProgressDone != 3 || !strings.Contains(compactJSON(mid.CheckpointJSON), `"page":3`) {
		t.Fatalf("mid progress=%d checkpoint=%s", mid.ProgressDone, mid.CheckpointJSON)
	}

	// Expire worker-1's lease so worker-2 takes over.
	if _, err := db.ExecContext(ctx, "UPDATE operation_jobs SET lease_expires_at = ? WHERE id = ?",
		time.Now().UTC().Add(-time.Second), job.ID); err != nil {
		t.Fatal(err)
	}
	second, err := store.LeaseNextOperationJob(ctx, "reporter_job", "worker-2", time.Minute)
	if err != nil || second.LeaseToken == first.LeaseToken {
		t.Fatalf("takeover = %+v, err = %v", second, err)
	}
	// Takeover resumes from the persisted checkpoint, not from zero.
	if second.ProgressDone != 3 || !strings.Contains(compactJSON(second.CheckpointJSON), `"page":3`) {
		t.Fatalf("takeover did not resume: progress=%d checkpoint=%s", second.ProgressDone, second.CheckpointJSON)
	}

	// Fencing: the superseded attempt's reporter can no longer write.
	if err := rep1.Report(ctx, 99, json.RawMessage(`{"page":99}`)); !errors.Is(err, ErrOperationJobLeaseLost) {
		t.Fatalf("stale reporter write = %v, want lease lost", err)
	}
	afterStale, err := store.GetOperationJob(ctx, tenant, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterStale.ProgressDone != 3 || strings.Contains(compactJSON(afterStale.CheckpointJSON), "99") {
		t.Fatalf("stale write leaked: progress=%d checkpoint=%s", afterStale.ProgressDone, afterStale.CheckpointJSON)
	}

	// The new owner continues forward from the resumed state.
	rep2 := &OperationJobReporter{repo: store, jobID: job.ID, leaseToken: second.LeaseToken, leaseFor: time.Minute, progress: second.ProgressDone, checkpoint: second.CheckpointJSON}
	if err := rep2.Report(ctx, 5, json.RawMessage(`{"page":5}`)); err != nil {
		t.Fatalf("new owner report: %v", err)
	}
	final, err := store.GetOperationJob(ctx, tenant, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.ProgressDone != 5 || !strings.Contains(compactJSON(final.CheckpointJSON), `"page":5`) {
		t.Fatalf("final progress=%d checkpoint=%s", final.ProgressDone, final.CheckpointJSON)
	}
}
