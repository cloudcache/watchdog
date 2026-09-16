package server

import (
	"bytes"
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

	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowlifecycle"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestFlowStoragePolicyLifecycleIntegration(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.DBName = "watchdog_flow_lifecycle_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	if err := ensureDatabase(dsn); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ApplyMySQLSchema(ctx, db, schema.MySQL); err != nil {
		t.Fatal(err)
	}
	userID := newID()
	if _, err := db.Exec(`INSERT INTO users (id,username,status) VALUES (?,?,'active')`, userID, "flow-lifecycle-admin"); err != nil {
		t.Fatal(err)
	}
	s := &Server{db: db}
	admin := &principal{UserID: userID, Username: "flow-lifecycle-admin", IsAdmin: true}

	input := map[string]any{
		"bootstrap_from": "2026-01-01", "raw_retention_seconds": 86400,
		"archive_retention_seconds": 2592000, "late_arrival_seconds": 3600,
		"delete_grace_seconds": 3600, "max_partitions_per_run": 7,
		"require_backup_before_delete": true,
	}
	created := flowLifecycleRequest(t, s.createFlowRetentionPolicy, admin, http.MethodPost, "/api/v1/flow/storage/policies", "", "", input, "")
	if created.Code != http.StatusCreated || created.Header().Get("ETag") != `"1"` {
		t.Fatalf("create: status=%d etag=%q body=%s", created.Code, created.Header().Get("ETag"), created.Body.String())
	}
	var first flowlifecycle.Policy
	if err := json.Unmarshal(created.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	input["late_arrival_seconds"] = 7200
	updated := flowLifecycleRequest(t, s.updateFlowRetentionPolicy, admin, http.MethodPatch, "/api/v1/flow/storage/policies/"+first.ID, "id", first.ID, input, `"1"`)
	if updated.Code != http.StatusOK || updated.Header().Get("ETag") != `"2"` {
		t.Fatalf("update: status=%d etag=%q body=%s", updated.Code, updated.Header().Get("ETag"), updated.Body.String())
	}
	stale := flowLifecycleRequest(t, s.updateFlowRetentionPolicy, admin, http.MethodPatch, "/api/v1/flow/storage/policies/"+first.ID, "id", first.ID, input, `"1"`)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale update: status=%d body=%s", stale.Code, stale.Body.String())
	}
	published := flowLifecycleRequest(t, s.publishFlowRetentionPolicy, admin, http.MethodPost, "/api/v1/flow/storage/policies/"+first.ID+"/actions/publish", "id", first.ID, nil, `"2"`)
	if published.Code != http.StatusOK || !strings.Contains(published.Body.String(), `"status":"published"`) {
		t.Fatalf("publish: status=%d body=%s", published.Code, published.Body.String())
	}

	unsafeInput := cloneFlowLifecycleMap(input)
	unsafeInput["raw_delete_enabled"] = true
	unsafe := flowLifecycleRequest(t, s.createFlowRetentionPolicy, admin, http.MethodPost, "/api/v1/flow/storage/policies", "", "", unsafeInput, "")
	if unsafe.Code != http.StatusConflict || !strings.Contains(unsafe.Body.String(), "flow_deletion_locked") {
		t.Fatalf("unsafe create: status=%d body=%s", unsafe.Code, unsafe.Body.String())
	}

	secondCreated := flowLifecycleRequest(t, s.createFlowRetentionPolicy, admin, http.MethodPost, "/api/v1/flow/storage/policies", "", "", input, "")
	var second flowlifecycle.Policy
	if secondCreated.Code != http.StatusCreated || json.Unmarshal(secondCreated.Body.Bytes(), &second) != nil {
		t.Fatalf("second create: status=%d body=%s", secondCreated.Code, secondCreated.Body.String())
	}
	secondPublished := flowLifecycleRequest(t, s.publishFlowRetentionPolicy, admin, http.MethodPost, "/api/v1/flow/storage/policies/"+second.ID+"/actions/publish", "id", second.ID, nil, `"1"`)
	if secondPublished.Code != http.StatusOK {
		t.Fatalf("second publish: status=%d body=%s", secondPublished.Code, secondPublished.Body.String())
	}
	var activeCount, retiredCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM flow_retention_policy_revisions WHERE status='published'`).Scan(&activeCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM flow_retention_policy_revisions WHERE status='retired'`).Scan(&retiredCount); err != nil {
		t.Fatal(err)
	}
	if activeCount != 1 || retiredCount != 1 {
		t.Fatalf("published=%d retired=%d", activeCount, retiredCount)
	}

	listed := flowLifecycleRequest(t, s.listFlowRetentionPolicies, admin, http.MethodGet, "/api/v1/flow/storage/policies?sort=version&order=desc&status=published", "", "", nil, "")
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), `"total":1`) || !strings.Contains(listed.Body.String(), second.ID) {
		t.Fatalf("list: status=%d body=%s", listed.Code, listed.Body.String())
	}
	for name, test := range map[string]struct {
		handler gin.HandlerFunc
		path    string
	}{
		"partitions": {s.listFlowRetentionPartitions, "/api/v1/flow/storage/partitions"},
		"watermarks": {s.listFlowReconciliationWatermarks, "/api/v1/flow/storage/watermarks"},
		"receipts":   {s.listFlowDeletionReceipts, "/api/v1/flow/storage/deletion-receipts"},
	} {
		t.Run(name, func(t *testing.T) {
			response := flowLifecycleRequest(t, test.handler, admin, http.MethodGet, test.path, "", "", nil, "")
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"items":[]`) {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	var auditCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_logs WHERE actor_id=? AND resource='flow_retention_policy'`, userID).Scan(&auditCount); err != nil || auditCount != 5 {
		t.Fatalf("audit count=%d error=%v", auditCount, err)
	}
}

func TestFlowReconciliationWatermarkIntegration(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.DBName = "watchdog_flow_reconciliation_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	if err := ensureDatabase(dsn); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ApplyMySQLSchema(ctx, db, schema.MySQL); err != nil {
		t.Fatal(err)
	}
	store := flowlifecycle.NewStore(db)
	config := flowlifecycle.ReconciliationConfig{
		SourceStreamID: "site-a:raw-v2:boot-1", KafkaTopic: "watchdog.flow.raw", ConsumerGroup: "watchdog-flow-worker",
		BootstrapOffset: map[uint32]uint64{0: 10}, MaxBatches: 100, MaxFactRows: 10_000, MaxReadBytes: 1 << 20,
	}
	if _, err := store.FreezeReconciliation(ctx, config, 0, nil, 20, time.Now()); !errors.Is(err, flowlifecycle.ErrBootstrapRequired) {
		t.Fatalf("missing bootstrap error=%v", err)
	}
	bootstrap := uint64(10)
	watermark, err := store.FreezeReconciliation(ctx, config, 0, &bootstrap, 20, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if watermark.BootstrapOffset != 10 || watermark.ReconciledNextOffset != 10 || watermark.CommittedNextOffset != 20 {
		t.Fatalf("initial watermark=%#v", watermark)
	}
	if _, err := store.FreezeReconciliation(ctx, config, 0, &bootstrap, 19, time.Now()); !errors.Is(err, flowlifecycle.ErrOffsetRegression) {
		t.Fatalf("committed regression error=%v", err)
	}
	if err := store.CompleteReconciliation(ctx, config, 0, 20, 15, 2, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertFlowWatermark(t, db, config.SourceStreamID, 0, 20, 15, "mismatch", 2)

	watermark, err = store.FreezeReconciliation(ctx, config, 0, nil, 25, time.Now())
	if err != nil || watermark.ReconciledNextOffset != 15 {
		t.Fatalf("resume watermark=%#v error=%v", watermark, err)
	}
	if err := store.CompleteReconciliation(ctx, config, 0, 25, 25, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertFlowWatermark(t, db, config.SourceStreamID, 0, 25, 25, "healthy", 0)
	// A stale overlapping job may finish after a newer clean window; it is a
	// no-op and must never move the durable next offset backwards.
	if err := store.CompleteReconciliation(ctx, config, 0, 20, 20, 0, time.Now()); err != nil {
		t.Fatalf("stale overlapping completion: %v", err)
	}
	assertFlowWatermark(t, db, config.SourceStreamID, 0, 25, 25, "healthy", 0)
}

type testReconciliationOffsets struct{ calls atomic.Int32 }

func (reader *testReconciliationOffsets) CommittedOffsets(context.Context, string, string) ([]flowstream.CommittedPartitionOffset, error) {
	reader.calls.Add(1)
	return []flowstream.CommittedPartitionOffset{{Partition: 0, NextOffset: 103}}, nil
}

type testRetryReconciliationScanner struct{ calls atomic.Int32 }

func (scanner *testRetryReconciliationScanner) Scan(_ context.Context, request flowch.ReconciliationScanRequest) (flowch.ReconciliationScanResult, error) {
	if scanner.calls.Add(1) == 1 {
		return flowch.ReconciliationScanResult{}, errors.New("temporary ClickHouse read failure")
	}
	next := request.Cursor
	next.NextOffset = request.CloseOffset
	return flowch.ReconciliationScanResult{
		NextCursor: next, Complete: true,
		Comparison: flowch.IngestReconciliationComparison{Batches: 3, Facts: 6},
	}, nil
}

func TestFlowReconciliationOperationJobRetriesFromCheckpointIntegration(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.DBName = "watchdog_flow_reconciliation_job_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	if err := ensureDatabase(dsn); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ApplyMySQLSchema(ctx, db, schema.MySQL); err != nil {
		t.Fatal(err)
	}

	cfg := defaultConfig()
	cfg.Flow.Reconciliation.Enabled = true
	cfg.Flow.Reconciliation.SourceStreamID = "site-a:raw-v2:boot-2"
	cfg.Flow.Reconciliation.BootstrapOffsets = map[uint32]uint64{0: 100}
	queued, err := flowReconciliationOperationJob(cfg, time.Unix(1_800_000_000, 0))
	if err != nil {
		t.Fatal(err)
	}
	jobs := opjob.NewStore(db)
	queued, err = jobs.Enqueue(ctx, queued)
	if err != nil {
		t.Fatal(err)
	}
	offsets := &testReconciliationOffsets{}
	scanner := &testRetryReconciliationScanner{}
	worker := &opjob.Worker{
		Repo: jobs, JobType: flowlifecycle.ReconciliationJobType, Owner: "flow-reconciliation-integration",
		Handler:      flowlifecycle.NewReconciliationHandler(offsets, scanner, flowlifecycle.NewStore(db)),
		PollInterval: 5 * time.Millisecond, LeaseFor: 300 * time.Millisecond, RetryBase: 5 * time.Millisecond, MaxAttempts: 3,
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	go worker.Run(workerCtx)

	var completed opjob.Job
	for {
		completed, err = jobs.Get(ctx, queued.ID)
		if err != nil {
			t.Fatal(err)
		}
		if completed.Terminal() {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("operation job did not finish: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if completed.Status != opjob.StatusSucceeded || completed.AttemptCount != 2 || completed.ProgressDone != 3 {
		t.Fatalf("completed job=%+v", completed)
	}
	if offsets.calls.Load() != 1 || scanner.calls.Load() != 2 || !bytes.Contains(completed.CheckpointJSON, []byte(`"frozen": true`)) {
		t.Fatalf("offset calls=%d scanner calls=%d checkpoint=%s", offsets.calls.Load(), scanner.calls.Load(), completed.CheckpointJSON)
	}
	assertFlowWatermark(t, db, cfg.Flow.Reconciliation.SourceStreamID, 0, 103, 103, "healthy", 0)
}

func assertFlowWatermark(t *testing.T, db *sql.DB, stream string, partition uint32, committed, reconciled uint64, status string, mismatches uint64) {
	t.Helper()
	var gotCommitted, gotReconciled, gotMismatches uint64
	var gotStatus string
	if err := db.QueryRow(`SELECT committed_next_offset,reconciled_next_offset,status,mismatch_count
		FROM flow_reconciliation_watermarks WHERE source_stream_id=? AND kafka_partition=?`, stream, partition).
		Scan(&gotCommitted, &gotReconciled, &gotStatus, &gotMismatches); err != nil {
		t.Fatal(err)
	}
	if gotCommitted != committed || gotReconciled != reconciled || gotStatus != status || gotMismatches != mismatches {
		t.Fatalf("watermark committed=%d reconciled=%d status=%s mismatches=%d", gotCommitted, gotReconciled, gotStatus, gotMismatches)
	}
}

func flowLifecycleRequest(t *testing.T, handler gin.HandlerFunc, principal *principal, method, target, paramName, paramValue string, body any, ifMatch string) *httptest.ResponseRecorder {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, bytes.NewReader(encoded))
	c.Request.Header.Set("Content-Type", "application/json")
	if ifMatch != "" {
		c.Request.Header.Set("If-Match", ifMatch)
	}
	c.Set(principalKey, principal)
	if paramName != "" {
		c.Params = gin.Params{{Key: paramName, Value: paramValue}}
	}
	handler(c)
	return recorder
}

func cloneFlowLifecycleMap(source map[string]any) map[string]any {
	copy := make(map[string]any, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}
