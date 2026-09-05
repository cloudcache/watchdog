package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func operationJobTestDB(t *testing.T) (*sql.DB, ID) {
	t.Helper()
	dsn := os.Getenv("WATCHDOG_COLLECTOR_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_COLLECTOR_MYSQL_TEST_DSN to run operation job integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	tenant := ID("tenant_opjob_" + t.Name()[len(t.Name())-min(8, len(t.Name())):])
	if _, err := db.ExecContext(ctx, "DELETE FROM tenants WHERE id = ?", tenant); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM tenants WHERE id = ?", tenant)
	})
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'OpJob', 'active')", tenant); err != nil {
		t.Fatal(err)
	}
	return db, tenant
}

type fakeOperationJobRepository struct {
	filter OperationJobFilter
	jobs   []OperationJob
	next   string
}

func (f *fakeOperationJobRepository) EnqueueOperationJob(context.Context, OperationJob) (OperationJob, error) {
	return OperationJob{}, nil
}
func (f *fakeOperationJobRepository) GetOperationJob(context.Context, ID, ID) (OperationJob, error) {
	return OperationJob{}, sql.ErrNoRows
}
func (f *fakeOperationJobRepository) ListOperationJobs(_ context.Context, _ ID, filter OperationJobFilter) ([]OperationJob, string, error) {
	f.filter = filter
	return f.jobs, f.next, nil
}
func (f *fakeOperationJobRepository) LeaseNextOperationJob(context.Context, string, string, time.Duration) (OperationJob, error) {
	return OperationJob{}, sql.ErrNoRows
}
func (f *fakeOperationJobRepository) HeartbeatOperationJob(context.Context, ID, string, time.Duration, uint64, json.RawMessage) (bool, error) {
	return false, nil
}
func (f *fakeOperationJobRepository) CompleteOperationJobSucceeded(context.Context, ID, string, string) error {
	return nil
}
func (f *fakeOperationJobRepository) CompleteOperationJobCanceled(context.Context, ID, string) error {
	return nil
}
func (f *fakeOperationJobRepository) CompleteOperationJobFailed(context.Context, ID, string, string, string, bool, time.Time) error {
	return nil
}
func (f *fakeOperationJobRepository) RequestOperationJobCancel(context.Context, ID, ID) error {
	return nil
}

func TestOperationJobListEndpoint(t *testing.T) {
	repo := &fakeOperationJobRepository{
		jobs: []OperationJob{{ID: "job-1", TenantID: "tenant-a", JobType: "target_delete", Status: "succeeded"}},
		next: "cursor-2",
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(true), OperationJobs: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/api/v1/operation-jobs?job_type=target_delete&status=succeeded&limit=25&cursor=abc", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.filter.JobType != "target_delete" || repo.filter.Status != "succeeded" ||
		repo.filter.Limit != 25 || repo.filter.Cursor != "abc" {
		t.Fatalf("filter = %+v", repo.filter)
	}
	var body struct {
		Items      []OperationJob `json:"items"`
		NextCursor string         `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Items) != 1 || body.NextCursor != "cursor-2" {
		t.Fatalf("body = %+v", body)
	}

	bad := httptest.NewRecorder()
	router.ServeHTTP(bad, httptest.NewRequest(http.MethodGet, "/api/v1/operation-jobs?limit=0", nil))
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("limit=0 status = %d", bad.Code)
	}
}

func TestMySQLOperationJobLifecycle(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()

	payload := json.RawMessage(`{"target_id":"tgt-1"}`)
	hash := strings.Repeat("a", 64)
	job, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenant, JobType: "test_job", IdempotencyKey: "key-1", RequestHash: hash, CheckpointJSON: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != OperationJobStatusQueued || job.ID == "" {
		t.Fatalf("enqueued job = %+v", job)
	}

	// Idempotent re-enqueue returns the same job; different hash conflicts.
	again, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenant, JobType: "test_job", IdempotencyKey: "key-1", RequestHash: hash,
	})
	if err != nil || again.ID != job.ID {
		t.Fatalf("re-enqueue job = %+v, err = %v", again, err)
	}
	if _, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenant, JobType: "test_job", IdempotencyKey: "key-1", RequestHash: strings.Repeat("b", 64),
	}); !errors.Is(err, ErrOperationJobHashMismatch) {
		t.Fatalf("hash mismatch error = %v", err)
	}

	// First claim wins; a second worker finds nothing while the lease holds.
	leased, err := store.LeaseNextOperationJob(ctx, "test_job", "worker-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if leased.ID != job.ID || leased.Status != OperationJobStatusRunning || leased.AttemptCount != 1 || leased.LeaseToken == "" {
		t.Fatalf("leased job = %+v", leased)
	}
	if _, err := store.LeaseNextOperationJob(ctx, "test_job", "worker-2", time.Minute); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("second claim error = %v", err)
	}

	// Heartbeat extends the lease; a wrong token is a lost lease.
	if cancelRequested, err := store.HeartbeatOperationJob(ctx, leased.ID, leased.LeaseToken, time.Minute, 5, json.RawMessage(`{"step":2}`)); err != nil || cancelRequested {
		t.Fatalf("heartbeat cancel=%v err=%v", cancelRequested, err)
	}
	if _, err := store.HeartbeatOperationJob(ctx, leased.ID, "bogus-token", time.Minute, 0, nil); !errors.Is(err, ErrOperationJobLeaseLost) {
		t.Fatalf("bogus heartbeat error = %v", err)
	}

	// Failed with retry requeues with backoff, then a later claim succeeds and
	// finishes terminally.
	if err := store.CompleteOperationJobFailed(ctx, leased.ID, leased.LeaseToken, "BOOM", "first failure", true, time.Now().UTC().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	requeued, err := store.GetOperationJob(ctx, tenant, job.ID)
	if err != nil || requeued.Status != OperationJobStatusQueued || requeued.LastErrorCode != "BOOM" {
		t.Fatalf("requeued job = %+v, err = %v", requeued, err)
	}
	leased2, err := store.LeaseNextOperationJob(ctx, "test_job", "worker-2", time.Minute)
	if err != nil || leased2.AttemptCount != 2 {
		t.Fatalf("second attempt = %+v, err = %v", leased2, err)
	}
	if err := store.CompleteOperationJobSucceeded(ctx, leased2.ID, leased2.LeaseToken, "done-ref"); err != nil {
		t.Fatal(err)
	}
	finished, err := store.GetOperationJob(ctx, tenant, job.ID)
	if err != nil || finished.Status != OperationJobStatusSucceeded || finished.ResultRef != "done-ref" || finished.FinishedAt.IsZero() {
		t.Fatalf("finished job = %+v, err = %v", finished, err)
	}
	// A stale token cannot double-finish.
	if err := store.CompleteOperationJobSucceeded(ctx, leased2.ID, leased2.LeaseToken, "again"); !errors.Is(err, ErrOperationJobLeaseLost) {
		t.Fatalf("double finish error = %v", err)
	}

	// List with pagination and filters over a second enqueued job.
	other, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenant, JobType: "other_job", IdempotencyKey: "key-other", RequestHash: strings.Repeat("f", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	page1, cursor, err := store.ListOperationJobs(ctx, tenant, OperationJobFilter{Limit: 1})
	if err != nil || len(page1) != 1 || cursor == "" {
		t.Fatalf("page1 = %d cursor=%q err=%v", len(page1), cursor, err)
	}
	page2, cursor2, err := store.ListOperationJobs(ctx, tenant, OperationJobFilter{Limit: 1, Cursor: cursor})
	if err != nil || len(page2) != 1 || cursor2 != "" || page2[0].ID == page1[0].ID {
		t.Fatalf("page2 = %+v cursor=%q err=%v", page2, cursor2, err)
	}
	byType, _, err := store.ListOperationJobs(ctx, tenant, OperationJobFilter{JobType: "other_job"})
	if err != nil || len(byType) != 1 || byType[0].ID != other.ID {
		t.Fatalf("type filter = %+v err=%v", byType, err)
	}
	succeeded, _, err := store.ListOperationJobs(ctx, tenant, OperationJobFilter{Status: OperationJobStatusSucceeded})
	if err != nil || len(succeeded) != 1 || succeeded[0].ID != job.ID {
		t.Fatalf("status filter = %+v err=%v", succeeded, err)
	}
}

func TestMySQLOperationJobTakeoverAndCancel(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()

	// Expired lease is taken over by another owner with a fresh token.
	job, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenant, JobType: "takeover_job", IdempotencyKey: "key-t", RequestHash: strings.Repeat("c", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.LeaseNextOperationJob(ctx, "takeover_job", "worker-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE operation_jobs SET lease_expires_at = ? WHERE id = ?",
		time.Now().UTC().Add(-time.Second), job.ID); err != nil {
		t.Fatal(err)
	}
	second, err := store.LeaseNextOperationJob(ctx, "takeover_job", "worker-2", time.Minute)
	if err != nil || second.ID != job.ID || second.AttemptCount != 2 || second.LeaseToken == first.LeaseToken {
		t.Fatalf("takeover = %+v, err = %v", second, err)
	}
	// The dead worker's token can no longer finish the job.
	if err := store.CompleteOperationJobSucceeded(ctx, job.ID, first.LeaseToken, "stale"); !errors.Is(err, ErrOperationJobLeaseLost) {
		t.Fatalf("stale owner finish error = %v", err)
	}

	// Cancel of a running job flags it; the worker sees the flag on heartbeat
	// and finishes canceled.
	if err := store.RequestOperationJobCancel(ctx, tenant, job.ID); err != nil {
		t.Fatal(err)
	}
	cancelRequested, err := store.HeartbeatOperationJob(ctx, job.ID, second.LeaseToken, time.Minute, 0, nil)
	if err != nil || !cancelRequested {
		t.Fatalf("cancel heartbeat = %v, err = %v", cancelRequested, err)
	}
	if err := store.CompleteOperationJobCanceled(ctx, job.ID, second.LeaseToken); err != nil {
		t.Fatal(err)
	}
	canceled, err := store.GetOperationJob(ctx, tenant, job.ID)
	if err != nil || canceled.Status != OperationJobStatusCanceled || canceled.FinishedAt.IsZero() {
		t.Fatalf("canceled job = %+v, err = %v", canceled, err)
	}

	// Cancel of a queued job is immediate; canceling again is a no-op.
	queued, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenant, JobType: "takeover_job", IdempotencyKey: "key-q", RequestHash: strings.Repeat("d", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RequestOperationJobCancel(ctx, tenant, queued.ID); err != nil {
		t.Fatal(err)
	}
	direct, err := store.GetOperationJob(ctx, tenant, queued.ID)
	if err != nil || direct.Status != OperationJobStatusCanceled {
		t.Fatalf("direct cancel = %+v, err = %v", direct, err)
	}
	if err := store.RequestOperationJobCancel(ctx, tenant, queued.ID); err != nil {
		t.Fatalf("repeat cancel err = %v", err)
	}
	if err := store.RequestOperationJobCancel(ctx, tenant, "missing-job"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing cancel err = %v", err)
	}
}

// TestMySQLOperationJobWorkerEndToEnd runs the real worker loop against MySQL:
// a failing first attempt retries, the second succeeds, and async target
// deletion through the API removes the target row.
func TestMySQLOperationJobWorkerEndToEnd(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var attempts atomic.Uint32
	worker := &OperationJobWorker{
		Repo: store, JobType: "e2e_job", Owner: "worker-e2e",
		PollInterval: 50 * time.Millisecond, LeaseFor: 3 * time.Second,
		MaxAttempts: 5, RetryBase: time.Nanosecond,
		Handler: func(ctx context.Context, job OperationJob) (string, error) {
			if attempts.Add(1) == 1 {
				return "", errors.New("transient")
			}
			return "ok", nil
		},
	}
	go worker.Run(ctx)

	job, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenant, JobType: "e2e_job", IdempotencyKey: "e2e-1", RequestHash: strings.Repeat("e", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForOperationJob(t, store, tenant, job.ID, OperationJobStatusSucceeded)
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}

	// Async target deletion end to end through the API surface.
	targetID := ID("target_opjob_del_01")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO targets (id, tenant_id, name, kind, host, status)
		VALUES (?, ?, 'OpJob Del', 'network', '192.0.2.150', 'pending')
	`, targetID, tenant); err != nil {
		t.Fatal(err)
	}
	deleteWorker := &OperationJobWorker{
		Repo: store, JobType: TargetDeleteJobType, Owner: "worker-e2e",
		PollInterval: 50 * time.Millisecond, LeaseFor: 3 * time.Second,
		Handler: NewTargetDeleteJobHandler(store, nil),
	}
	go deleteWorker.Run(ctx)

	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: tenant, UserID: "", IsAdmin: true}, nil
		},
		Targets:       store,
		OperationJobs: store,
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/targets/"+string(targetID), nil))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("delete status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var accepted struct {
		JobID ID `json:"job_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &accepted); err != nil || accepted.JobID == "" {
		t.Fatalf("delete response = %s, err = %v", rec.Body.String(), err)
	}
	waitForOperationJob(t, store, tenant, accepted.JobID, OperationJobStatusSucceeded)
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM targets WHERE id = ?", targetID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("target count after async delete = %d, err = %v", count, err)
	}

	// Repeating the DELETE reuses the finished job idempotently.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/targets/"+string(targetID), nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("repeat delete of removed target status = %d", rec.Code)
	}
}

func waitForOperationJob(t *testing.T, store *MySQLStore, tenant, jobID ID, wantStatus string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		job, err := store.GetOperationJob(context.Background(), tenant, jobID)
		if err == nil && job.Status == wantStatus {
			return
		}
		if err == nil && job.Terminal() && job.Status != wantStatus {
			t.Fatalf("job %s finished as %s (%s %s), want %s", jobID, job.Status, job.LastErrorCode, job.LastErrorDetail, wantStatus)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s in time", jobID, wantStatus)
}
