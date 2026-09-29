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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowlifecycle"
	"github.com/cloudcache/watchdog/internal/flowquery"
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
	unsafeInput["archive_delete_enabled"] = true
	unsafe := flowLifecycleRequest(t, s.createFlowRetentionPolicy, admin, http.MethodPost, "/api/v1/flow/storage/policies", "", "", unsafeInput, "")
	var archivePolicy flowlifecycle.Policy
	if unsafe.Code != http.StatusCreated || json.Unmarshal(unsafe.Body.Bytes(), &archivePolicy) != nil || !archivePolicy.ArchiveDeleteEnabled {
		t.Fatalf("archive-delete policy create: status=%d body=%s", unsafe.Code, unsafe.Body.String())
	}
	archivePublished := flowLifecycleRequest(t, s.publishFlowRetentionPolicy, admin, http.MethodPost,
		"/api/v1/flow/storage/policies/"+archivePolicy.ID+"/actions/publish", "id", archivePolicy.ID, nil, `"1"`)
	if archivePublished.Code != http.StatusOK || !strings.Contains(archivePublished.Body.String(), `"archive_delete_enabled":true`) {
		t.Fatalf("archive-delete policy publish: status=%d body=%s", archivePublished.Code, archivePublished.Body.String())
	}

	rawDeleteInput := cloneFlowLifecycleMap(input)
	rawDeleteInput["raw_delete_enabled"] = true
	secondCreated := flowLifecycleRequest(t, s.createFlowRetentionPolicy, admin, http.MethodPost, "/api/v1/flow/storage/policies", "", "", rawDeleteInput, "")
	var second flowlifecycle.Policy
	if secondCreated.Code != http.StatusCreated || json.Unmarshal(secondCreated.Body.Bytes(), &second) != nil || !second.RawDeleteEnabled || second.ArchiveDeleteEnabled {
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
	if activeCount != 1 || retiredCount != 2 {
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
		"approvals":  {s.listFlowDeletionApprovals, "/api/v1/flow/storage/deletion-approvals"},
	} {
		t.Run(name, func(t *testing.T) {
			response := flowLifecycleRequest(t, test.handler, admin, http.MethodGet, test.path, "", "", nil, "")
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"items":[]`) {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	backupInput := map[string]any{
		"storage_kind": "raw", "covered_from": "2026-01-01", "covered_through": "2026-01-02",
		"backup_ref": "s3://backups/flow/raw/2026-01-01", "checksum_sha256": strings.Repeat("a", 64),
		"restore_tested_at": time.Now().UTC().Add(-time.Hour).Format(time.RFC3339),
		"restore_test_ref":  "restore-run/2026-09-16/flow-raw-2026-01-01",
	}
	backupCreated := flowLifecycleRequest(t, s.createFlowBackupEvidence, admin, http.MethodPost, "/api/v1/flow/storage/backup-evidence", "", "", backupInput, "")
	if backupCreated.Code != http.StatusCreated || backupCreated.Header().Get("ETag") != `"1"` {
		t.Fatalf("create backup evidence: status=%d etag=%q body=%s", backupCreated.Code, backupCreated.Header().Get("ETag"), backupCreated.Body.String())
	}
	var backup flowlifecycle.BackupEvidence
	if err := json.Unmarshal(backupCreated.Body.Bytes(), &backup); err != nil {
		t.Fatal(err)
	}
	backupListed := flowLifecycleRequest(t, s.listFlowBackupEvidence, admin, http.MethodGet, "/api/v1/flow/storage/backup-evidence?status=verified&q=restore-run", "", "", nil, "")
	if backupListed.Code != http.StatusOK || !strings.Contains(backupListed.Body.String(), `"total":1`) || !strings.Contains(backupListed.Body.String(), backup.ID) {
		t.Fatalf("list backup evidence: status=%d body=%s", backupListed.Code, backupListed.Body.String())
	}
	backupGet := flowLifecycleRequest(t, s.getFlowBackupEvidence, admin, http.MethodGet, "/api/v1/flow/storage/backup-evidence/"+backup.ID, "id", backup.ID, nil, "")
	if backupGet.Code != http.StatusOK || backupGet.Header().Get("ETag") != `"1"` {
		t.Fatalf("get backup evidence: status=%d etag=%q body=%s", backupGet.Code, backupGet.Header().Get("ETag"), backupGet.Body.String())
	}
	backupStale := flowLifecycleRequest(t, s.revokeFlowBackupEvidence, admin, http.MethodPost, "/api/v1/flow/storage/backup-evidence/"+backup.ID+"/actions/revoke", "id", backup.ID, nil, `"2"`)
	if backupStale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale revoke backup evidence: status=%d body=%s", backupStale.Code, backupStale.Body.String())
	}
	backupRevoked := flowLifecycleRequest(t, s.revokeFlowBackupEvidence, admin, http.MethodPost, "/api/v1/flow/storage/backup-evidence/"+backup.ID+"/actions/revoke", "id", backup.ID, nil, `"1"`)
	if backupRevoked.Code != http.StatusOK || backupRevoked.Header().Get("ETag") != `"2"` || !strings.Contains(backupRevoked.Body.String(), `"status":"revoked"`) {
		t.Fatalf("revoke backup evidence: status=%d etag=%q body=%s", backupRevoked.Code, backupRevoked.Header().Get("ETag"), backupRevoked.Body.String())
	}
	var auditCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_logs WHERE actor_id=? AND resource='flow_retention_policy'`, userID).Scan(&auditCount); err != nil || auditCount != 7 {
		t.Fatalf("audit count=%d error=%v", auditCount, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_logs WHERE actor_id=? AND resource='flow_backup_evidence'`, userID).Scan(&auditCount); err != nil || auditCount != 2 {
		t.Fatalf("backup audit count=%d error=%v", auditCount, err)
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

type testRetryArchiveRunner struct{ calls atomic.Int32 }

func (runner *testRetryArchiveRunner) Run(context.Context, flowch.RollupRequest) error {
	if runner.calls.Add(1) == 4 {
		return errors.New("temporary ClickHouse rollup failure")
	}
	return nil
}

func (runner *testRetryArchiveRunner) DayStorageCounters(context.Context, time.Time) (flowch.StorageCounters, flowch.StorageCounters, error) {
	counters := flowch.StorageCounters{RecordCount: 7, RawBytes: 100, RawPackets: 10, EstimatedBytes: 900, EstimatedPackets: 90, EstimatedValidRecords: 6}
	return counters, counters, nil
}

type testRawDeleteEvidence struct {
	mu        sync.Mutex
	counters  flowch.StorageCounters
	physical  uint64
	coverage  []flowch.DayOffsetCoverage
	dropped   bool
	dropCalls int
}

func (reader *testRawDeleteEvidence) DayStorageCounters(context.Context, time.Time) (flowch.StorageCounters, flowch.StorageCounters, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.dropped {
		return flowch.StorageCounters{}, reader.counters, nil
	}
	return reader.counters, reader.counters, nil
}

func (reader *testRawDeleteEvidence) RawDayPhysicalRecords(context.Context, time.Time) (uint64, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.dropped {
		return 0, nil
	}
	return reader.physical, nil
}

func (reader *testRawDeleteEvidence) DayOffsetCoverage(context.Context, time.Time) ([]flowch.DayOffsetCoverage, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return append([]flowch.DayOffsetCoverage(nil), reader.coverage...), nil
}

func (reader *testRawDeleteEvidence) DropRawDay(_ context.Context, day time.Time, queryID string) error {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if !day.Equal(day.UTC().Truncate(24*time.Hour)) || queryID == "" {
		return errors.New("invalid raw deletion request")
	}
	reader.dropCalls++
	reader.dropped = true
	return nil
}

type testArchiveDeleteEvidence struct {
	mu        sync.Mutex
	counters  flowch.StorageCounters
	physical  uint64
	dropped   bool
	dropCalls int
}

func (reader *testArchiveDeleteEvidence) ArchiveMonthStorageCounters(context.Context, time.Time) (flowch.StorageCounters, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.dropped {
		return flowch.StorageCounters{}, nil
	}
	return reader.counters, nil
}

func (reader *testArchiveDeleteEvidence) ArchiveMonthPhysicalRecords(context.Context, time.Time) (uint64, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.dropped {
		return 0, nil
	}
	return reader.physical, nil
}

func (reader *testArchiveDeleteEvidence) DropArchiveMonth(_ context.Context, month time.Time, queryID string) error {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if month.Location() != time.UTC || month.Day() != 1 || queryID == "" {
		return errors.New("invalid archive deletion request")
	}
	reader.dropCalls++
	reader.dropped = true
	return nil
}

func TestFlowRawDeleteReadinessUsesMySQLEvidenceWithoutMutatingState(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.DBName = "watchdog_flow_delete_readiness_it"
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
	userID := "01JDELETEADMIN000000000000"
	if _, err := db.ExecContext(ctx, `INSERT INTO users (id,username,status) VALUES (?,?,'active')`, userID, "delete-approval-admin"); err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := day.Add(5 * 24 * time.Hour)
	policyID := "01JDELETEPOLICY0000000000"
	generation, err := flowlifecycle.Generation(3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO flow_retention_policy_revisions
		(id,policy_version,status,bootstrap_from,raw_retention_seconds,archive_resolution_seconds,
		 archive_retention_seconds,late_arrival_seconds,delete_grace_seconds,max_partitions_per_run,
		 raw_delete_enabled,archive_delete_enabled,require_backup_before_delete,published_at)
		VALUES (?,3,'published',?,86400,3600,0,3600,3600,7,1,0,1,?)`, policyID, day, day); err != nil {
		t.Fatal(err)
	}
	counters := flowch.StorageCounters{RecordCount: 7, RawBytes: 100, RawPackets: 10, EstimatedBytes: 900, EstimatedPackets: 90, EstimatedValidRecords: 6}
	eligible := day.Add(49 * time.Hour)
	if _, err := db.ExecContext(ctx, `INSERT INTO flow_retention_partition_states
		(source_date,policy_id,policy_version,state,generation,repair_attempt,
		 source_record_count,source_raw_bytes,source_raw_packets,source_estimated_bytes,source_estimated_packets,source_estimated_valid_records,
		 archive_record_count,archive_raw_bytes,archive_raw_packets,archive_estimated_bytes,archive_estimated_packets,archive_estimated_valid_records,
		 archived_at,reconciled_at,late_checked_at,delete_eligible_at)
		VALUES (?,?,3,'reconciled',?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, day, policyID, generation,
		counters.RecordCount, counters.RawBytes, counters.RawPackets, counters.EstimatedBytes, counters.EstimatedPackets, counters.EstimatedValidRecords,
		counters.RecordCount, counters.RawBytes, counters.RawPackets, counters.EstimatedBytes, counters.EstimatedPackets, counters.EstimatedValidRecords,
		day.Add(48*time.Hour), day.Add(48*time.Hour), day.Add(48*time.Hour), eligible); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO flow_reconciliation_watermarks
		(source_stream_id,kafka_topic,consumer_group,kafka_partition,bootstrap_offset,committed_next_offset,reconciled_next_offset,
		 status,mismatch_count,committed_snapshot_at,last_verified_at)
		VALUES ('site-a:boot-1','watchdog.flow.raw','watchdog-flow-worker',0,0,30,30,'healthy',0,?,?)`, day.Add(3*24*time.Hour), day.Add(3*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	backupID := "01JDELETEBACKUP0000000000"
	if _, err := db.ExecContext(ctx, `INSERT INTO flow_backup_restore_evidence
		(id,storage_kind,covered_from,covered_through,backup_ref,checksum_sha256,status,verified_at,restore_tested_at,restore_test_ref)
		VALUES (?,'raw',?,?,?,REPEAT('a',64),'verified',?,?,?)`, backupID, day, day.Add(24*time.Hour), "s3://backup/flow/2026-01-01", day.Add(3*24*time.Hour), day.Add(4*24*time.Hour), "restore-run/2026-01-05"); err != nil {
		t.Fatal(err)
	}
	reader := &testRawDeleteEvidence{counters: counters, physical: 9, coverage: []flowch.DayOffsetCoverage{{
		SourceStreamID: "site-a:boot-1", KafkaTopic: "watchdog.flow.raw", KafkaPartition: 0,
		FirstOffset: 10, LastOffsetExclusive: 21,
	}}}
	store := flowlifecycle.NewStore(db)
	readiness, err := store.RawDayDeleteReadiness(ctx, day, now, reader)
	if err != nil {
		t.Fatal(err)
	}
	if !readiness.EvidenceReady || readiness.DeletionReady || !containsString(readiness.Blockers, flowlifecycle.DeleteBlockerApprovalRequired) || readiness.BackupEvidenceID != backupID {
		t.Fatalf("ready evidence=%+v", readiness)
	}
	admin := &principal{UserID: userID, Username: "delete-approval-admin", IsAdmin: true}
	s := &Server{db: db, flowLifecycle: store, flowDeleteEvidence: reader}
	response := flowLifecycleRequest(t, s.getFlowRawDeleteReadiness, admin, http.MethodGet,
		"/api/v1/flow/storage/partitions/2026-01-01/delete-readiness", "date", "2026-01-01", nil, "")
	if response.Code != http.StatusOK || response.Header().Get("ETag") != `"1"` || !strings.Contains(response.Body.String(), `"evidence_ready":true`) || !strings.Contains(response.Body.String(), `"deletion_ready":false`) {
		t.Fatalf("readiness API status=%d body=%s", response.Code, response.Body.String())
	}
	var state string
	var rowVersion uint64
	if err := db.QueryRowContext(ctx, `SELECT state,row_version FROM flow_retention_partition_states WHERE source_date=?`, day).Scan(&state, &rowVersion); err != nil {
		t.Fatal(err)
	}
	if state != flowlifecycle.PartitionReconciled || rowVersion != 1 {
		t.Fatalf("readiness check mutated partition state=%q row_version=%d", state, rowVersion)
	}
	approvedResponse := flowLifecycleRequest(t, s.approveFlowRawDelete, admin, http.MethodPost,
		"/api/v1/flow/storage/partitions/2026-01-01/actions/approve-delete", "date", "2026-01-01", nil, `"1"`)
	if approvedResponse.Code != http.StatusCreated || approvedResponse.Header().Get("ETag") != `"1"` || !strings.Contains(approvedResponse.Body.String(), `"state":"delete_eligible"`) {
		t.Fatalf("approve delete: status=%d etag=%q body=%s", approvedResponse.Code, approvedResponse.Header().Get("ETag"), approvedResponse.Body.String())
	}
	var approved struct {
		Approval flowlifecycle.DeletionApproval `json:"approval"`
	}
	if err := json.Unmarshal(approvedResponse.Body.Bytes(), &approved); err != nil {
		t.Fatal(err)
	}
	expectedCounters := flowlifecycle.Counters{RecordCount: 7, RawBytes: 100, RawPackets: 10, EstimatedBytes: 900, EstimatedPackets: 90, EstimatedValidRecords: 6}
	if approved.Approval.PolicyID != policyID || approved.Approval.Generation != generation || approved.Approval.BackupEvidenceID != backupID ||
		approved.Approval.Source != expectedCounters || approved.Approval.PhysicalRecords != 9 ||
		len(approved.Approval.KafkaCoverage) != 1 || approved.Approval.KafkaCoverage[0].LastOffsetExclusive != 21 {
		t.Fatalf("approval did not freeze evidence: %+v", approved.Approval)
	}
	listed := flowLifecycleRequest(t, s.listFlowDeletionApprovals, admin, http.MethodGet,
		"/api/v1/flow/storage/deletion-approvals?status=approved&storage_kind=raw", "", "", nil, "")
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), `"total":1`) || !strings.Contains(listed.Body.String(), approved.Approval.ID) {
		t.Fatalf("list deletion approvals: status=%d body=%s", listed.Code, listed.Body.String())
	}
	gotApproval := flowLifecycleRequest(t, s.getFlowDeletionApproval, admin, http.MethodGet,
		"/api/v1/flow/storage/deletion-approvals/"+approved.Approval.ID, "id", approved.Approval.ID, nil, "")
	if gotApproval.Code != http.StatusOK || gotApproval.Header().Get("ETag") != `"1"` {
		t.Fatalf("get deletion approval: status=%d etag=%q body=%s", gotApproval.Code, gotApproval.Header().Get("ETag"), gotApproval.Body.String())
	}
	staleRevoke := flowLifecycleRequest(t, s.revokeFlowDeletionApproval, admin, http.MethodPost,
		"/api/v1/flow/storage/deletion-approvals/"+approved.Approval.ID+"/actions/revoke", "id", approved.Approval.ID, nil, `"2"`)
	if staleRevoke.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale revoke approval: status=%d body=%s", staleRevoke.Code, staleRevoke.Body.String())
	}
	revokedResponse := flowLifecycleRequest(t, s.revokeFlowDeletionApproval, admin, http.MethodPost,
		"/api/v1/flow/storage/deletion-approvals/"+approved.Approval.ID+"/actions/revoke", "id", approved.Approval.ID, nil, `"1"`)
	if revokedResponse.Code != http.StatusOK || revokedResponse.Header().Get("ETag") != `"2"` ||
		!strings.Contains(revokedResponse.Body.String(), `"status":"revoked"`) || !strings.Contains(revokedResponse.Body.String(), `"state":"reconciled"`) {
		t.Fatalf("revoke approval: status=%d etag=%q body=%s", revokedResponse.Code, revokedResponse.Header().Get("ETag"), revokedResponse.Body.String())
	}
	readiness, err = store.RawDayDeleteReadiness(ctx, day, now, reader)
	if err != nil {
		t.Fatal(err)
	}
	if !readiness.EvidenceReady || readiness.DeletionReady || !containsString(readiness.Blockers, flowlifecycle.DeleteBlockerApprovalRequired) {
		t.Fatalf("revoked approval must relock deletion: %+v", readiness)
	}
	var approvalAudits int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_logs WHERE actor_id=? AND resource='flow_deletion_approval'`, userID).Scan(&approvalAudits); err != nil || approvalAudits != 2 {
		t.Fatalf("approval audit count=%d error=%v", approvalAudits, err)
	}
	missingWatermarkReader := &testRawDeleteEvidence{counters: counters, coverage: append(append([]flowch.DayOffsetCoverage(nil), reader.coverage...), flowch.DayOffsetCoverage{
		SourceStreamID: "site-b:boot-1", KafkaTopic: "watchdog.flow.raw", KafkaPartition: 1,
		FirstOffset: 40, LastOffsetExclusive: 42,
	})}
	readiness, err = store.RawDayDeleteReadiness(ctx, day, now, missingWatermarkReader)
	if err != nil {
		t.Fatal(err)
	}
	if readiness.EvidenceReady || readiness.DeletionReady || !containsString(readiness.Blockers, flowlifecycle.DeleteBlockerWatermarkMissing) {
		t.Fatalf("partially proven Kafka coverage was accepted: %+v", readiness)
	}
	if _, err := db.ExecContext(ctx, `UPDATE flow_reconciliation_watermarks SET reconciled_next_offset=20,status='mismatch',mismatch_count=1 WHERE source_stream_id='site-a:boot-1' AND kafka_partition=0`); err != nil {
		t.Fatal(err)
	}
	readiness, err = store.RawDayDeleteReadiness(ctx, day, now, reader)
	if err != nil {
		t.Fatal(err)
	}
	if readiness.EvidenceReady || readiness.DeletionReady || !containsString(readiness.Blockers, flowlifecycle.DeleteBlockerWatermarkUnhealthy) || !containsString(readiness.Blockers, flowlifecycle.DeleteBlockerWatermarkBehind) {
		t.Fatalf("unsafe evidence was accepted: %+v", readiness)
	}

	// Restore the Kafka proof, approve a new immutable attempt, and drive the
	// canonical operation_jobs worker through the destructive transition.
	if _, err := db.ExecContext(ctx, `UPDATE flow_reconciliation_watermarks SET reconciled_next_offset=30,status='healthy',mismatch_count=0 WHERE source_stream_id='site-a:boot-1' AND kafka_partition=0`); err != nil {
		t.Fatal(err)
	}
	readiness, err = store.RawDayDeleteReadiness(ctx, day, now, reader)
	if err != nil {
		t.Fatal(err)
	}
	if !readiness.EvidenceReady || readiness.PartitionVersion == 0 {
		t.Fatalf("restored evidence is not approvable: %+v", readiness)
	}
	reapprovedResponse := flowLifecycleRequest(t, s.approveFlowRawDelete, admin, http.MethodPost,
		"/api/v1/flow/storage/partitions/2026-01-01/actions/approve-delete", "date", "2026-01-01", nil, etag(readiness.PartitionVersion))
	if reapprovedResponse.Code != http.StatusCreated {
		t.Fatalf("reapprove delete: status=%d body=%s", reapprovedResponse.Code, reapprovedResponse.Body.String())
	}
	var reapproved struct {
		Approval flowlifecycle.DeletionApproval `json:"approval"`
	}
	if err := json.Unmarshal(reapprovedResponse.Body.Bytes(), &reapproved); err != nil {
		t.Fatal(err)
	}
	s.jobs = opjob.NewStore(db)
	workerID, workerToken := "flow_worker_delete_gate", "wda_flow_delete_gate_secret"
	// Flow workers authenticate with the installation shared token.
	s.cfg.Agents.SharedToken = workerToken
	if _, err := db.ExecContext(ctx, `INSERT INTO agents (id,name,kind,status,health,api_version,capabilities_json)
		VALUES (?,?,'flow_worker','active','ok','v1',JSON_ARRAY('flow.write.clickhouse/v1'))`, workerID, "Flow delete gate worker"); err != nil {
		t.Fatal(err)
	}
	pendingResponse := flowLifecycleRequest(t, s.executeFlowRawDelete, admin, http.MethodPost,
		"/api/v1/flow/storage/deletion-approvals/"+reapproved.Approval.ID+"/actions/execute", "id", reapproved.Approval.ID, nil, `"1"`)
	if pendingResponse.Code != http.StatusConflict || !strings.Contains(pendingResponse.Body.String(), `"error":"flow_delete_barrier_pending"`) {
		t.Fatalf("unacknowledged worker did not block delete: status=%d body=%s", pendingResponse.Code, pendingResponse.Body.String())
	}
	var pending struct {
		Barrier flowlifecycle.DeleteBarrierStatus `json:"barrier"`
	}
	if err := json.Unmarshal(pendingResponse.Body.Bytes(), &pending); err != nil {
		t.Fatal(err)
	}
	if pending.Barrier.Ready || pending.Barrier.RequiredWorkers != 1 || pending.Barrier.InstalledWorkers != 0 {
		t.Fatalf("unexpected pending barrier status: %+v", pending.Barrier)
	}
	s.installed.Store(true)
	s.engine = s.newRouter()
	fetchedBarrier := machineRequest(t, s, http.MethodGet, "/api/v1/flow-workers/"+workerID+"/raw-delete-barrier", workerToken, nil)
	if fetchedBarrier.Code != http.StatusOK || !strings.Contains(fetchedBarrier.Body.String(), pending.Barrier.Barrier.ID) {
		t.Fatalf("fetch raw-delete barrier: status=%d body=%s", fetchedBarrier.Code, fetchedBarrier.Body.String())
	}
	acknowledged := machineRequest(t, s, http.MethodPost,
		"/api/v1/flow-workers/"+workerID+"/raw-delete-barriers/"+pending.Barrier.Barrier.ID+"/ack", workerToken, map[string]any{
			"revision": pending.Barrier.Barrier.Revision, "boot_id": "delete-gate-boot", "software_version": "integration-test", "state": "installed",
		})
	if acknowledged.Code != http.StatusAccepted {
		t.Fatalf("acknowledge raw-delete barrier: status=%d body=%s", acknowledged.Code, acknowledged.Body.String())
	}
	executeResponse := flowLifecycleRequest(t, s.executeFlowRawDelete, admin, http.MethodPost,
		"/api/v1/flow/storage/deletion-approvals/"+reapproved.Approval.ID+"/actions/execute", "id", reapproved.Approval.ID, nil, `"1"`)
	if executeResponse.Code != http.StatusAccepted {
		t.Fatalf("schedule raw delete: status=%d body=%s", executeResponse.Code, executeResponse.Body.String())
	}
	var scheduled struct {
		Job     opjob.Job                     `json:"job"`
		Receipt flowlifecycle.DeletionReceipt `json:"receipt"`
	}
	if err := json.Unmarshal(executeResponse.Body.Bytes(), &scheduled); err != nil {
		t.Fatal(err)
	}
	if scheduled.Job.Status != opjob.StatusQueued || scheduled.Receipt.Status != "requested" ||
		scheduled.Receipt.PhysicalRecords != reader.physical || scheduled.Receipt.OperationJobID != scheduled.Job.ID {
		t.Fatalf("scheduled job/receipt mismatch: %+v", scheduled)
	}
	cancelResponse := flowLifecycleRequest(t, s.cancelOperationJob, admin, http.MethodPost,
		"/api/v1/operation-jobs/"+scheduled.Job.ID+"/actions/cancel", "id", scheduled.Job.ID, nil, "")
	if cancelResponse.Code != http.StatusConflict || !strings.Contains(cancelResponse.Body.String(), "destructive_job_not_cancelable") {
		t.Fatalf("scheduled destructive cancel: status=%d body=%s", cancelResponse.Code, cancelResponse.Body.String())
	}
	workerContext, stopWorker := context.WithCancel(ctx)
	worker := &opjob.Worker{
		Repo: s.jobs, JobType: flowlifecycle.RawDeleteJobType, Owner: "raw-delete-integration",
		Handler: flowlifecycle.NewRawDeleteHandler(store, reader), PollInterval: time.Millisecond,
		LeaseFor: time.Second, RetryBase: time.Millisecond,
	}
	go worker.Run(workerContext)
	deadline := time.Now().Add(5 * time.Second)
	var finished opjob.Job
	for time.Now().Before(deadline) {
		finished, err = s.jobs.Get(ctx, scheduled.Job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if finished.Terminal() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopWorker()
	if finished.Status != opjob.StatusSucceeded {
		t.Fatalf("raw deletion job did not succeed: %+v", finished)
	}
	receipt, err := store.GetDeletionReceiptByJob(ctx, scheduled.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.GetPartition(ctx, day)
	if err != nil {
		t.Fatal(err)
	}
	reader.mu.Lock()
	dropCalls := reader.dropCalls
	reader.mu.Unlock()
	if receipt.Status != "succeeded" || receipt.PostDeleteRecordCount != 0 || partition.State != flowlifecycle.PartitionRawDeleted || dropCalls != 1 {
		t.Fatalf("raw deletion did not converge: job=%+v receipt=%+v partition=%+v drops=%d", finished, receipt, partition, dropCalls)
	}
}

func TestFlowArchiveMonthDeleteLifecycleIntegration(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.DBName = "watchdog_flow_archive_delete_it"
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
	userID := "01JARCHDELETEADMIN00000000"
	if _, err := db.ExecContext(ctx, `INSERT INTO users (id,username,status) VALUES (?,?,'active')`, userID, "archive-delete-admin"); err != nil {
		t.Fatal(err)
	}
	month := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	monthEnd := month.AddDate(0, 1, 0)
	now := monthEnd.Add(40 * 24 * time.Hour)
	policyID := "01JARCHDELETEPOLICY0000000"
	if _, err := db.ExecContext(ctx, `INSERT INTO flow_retention_policy_revisions
		(id,policy_version,status,bootstrap_from,raw_retention_seconds,archive_resolution_seconds,
		 archive_retention_seconds,late_arrival_seconds,delete_grace_seconds,max_partitions_per_run,
		 raw_delete_enabled,archive_delete_enabled,require_backup_before_delete,published_at)
		VALUES (?,4,'published',?,86400,3600,2592000,3600,3600,7,1,1,1,?)`, policyID, month, month); err != nil {
		t.Fatal(err)
	}
	dayCounters := flowlifecycle.Counters{RecordCount: 1, RawBytes: 100, RawPackets: 10, EstimatedBytes: 1_000, EstimatedPackets: 100, EstimatedValidRecords: 1}
	for day := month; day.Before(monthEnd.Add(-24 * time.Hour)); day = day.Add(24 * time.Hour) {
		insertArchiveDeletedDay(t, ctx, db, day, policyID, 4, dayCounters)
	}
	reader := &testArchiveDeleteEvidence{
		counters: flowch.StorageCounters{RecordCount: 31, RawBytes: 3_100, RawPackets: 310, EstimatedBytes: 31_000, EstimatedPackets: 3_100, EstimatedValidRecords: 31},
		physical: 744,
	}
	store := flowlifecycle.NewStore(db)
	readiness, err := store.ArchiveMonthDeleteReadiness(ctx, month, now, reader)
	if err != nil {
		t.Fatal(err)
	}
	if readiness.EvidenceReady || !containsString(readiness.Blockers, flowlifecycle.DeleteBlockerMonthIncomplete) || readiness.RawDeletedDays != 30 {
		t.Fatalf("incomplete month was accepted: %+v", readiness)
	}
	insertArchiveDeletedDay(t, ctx, db, monthEnd.Add(-24*time.Hour), policyID, 4, dayCounters)
	backupID := "01JARCHDELETEBACKUP0000000"
	if _, err := db.ExecContext(ctx, `INSERT INTO flow_backup_restore_evidence
		(id,storage_kind,covered_from,covered_through,backup_ref,checksum_sha256,status,verified_at,restore_tested_at,restore_test_ref)
		VALUES (?,'archive',?,?,?,REPEAT('b',64),'verified',?,?,?)`, backupID, month, monthEnd,
		"s3://backup/flow/archive/2026-01", now.Add(-48*time.Hour), now.Add(-24*time.Hour), "restore-run/archive-2026-01"); err != nil {
		t.Fatal(err)
	}
	readiness, err = store.ArchiveMonthDeleteReadiness(ctx, month, now, reader)
	if err != nil {
		t.Fatal(err)
	}
	if !readiness.EvidenceReady || readiness.DeletionReady || readiness.PolicyRowVersion != 1 || readiness.BackupEvidenceID != backupID ||
		!containsString(readiness.Blockers, flowlifecycle.DeleteBlockerApprovalRequired) {
		t.Fatalf("complete month was not approvable: %+v", readiness)
	}
	admin := &principal{UserID: userID, Username: "archive-delete-admin", IsAdmin: true}
	s := &Server{db: db, flowLifecycle: store, flowArchiveDeleteEvidence: reader, jobs: opjob.NewStore(db)}
	readinessResponse := flowLifecycleRequest(t, s.getFlowArchiveDeleteReadiness, admin, http.MethodGet,
		"/api/v1/flow/storage/archive-months/2026-01/delete-readiness", "month", "2026-01", nil, "")
	if readinessResponse.Code != http.StatusOK || readinessResponse.Header().Get("ETag") != `"1"` ||
		!strings.Contains(readinessResponse.Body.String(), `"evidence_ready":true`) {
		t.Fatalf("archive readiness API: status=%d etag=%q body=%s", readinessResponse.Code, readinessResponse.Header().Get("ETag"), readinessResponse.Body.String())
	}
	approvedResponse := flowLifecycleRequest(t, s.approveFlowArchiveDelete, admin, http.MethodPost,
		"/api/v1/flow/storage/archive-months/2026-01/actions/approve-delete", "month", "2026-01", nil, `"1"`)
	if approvedResponse.Code != http.StatusCreated || approvedResponse.Header().Get("ETag") != `"1"` {
		t.Fatalf("approve archive delete: status=%d body=%s", approvedResponse.Code, approvedResponse.Body.String())
	}
	var approved struct {
		Approval flowlifecycle.DeletionApproval `json:"approval"`
	}
	if err := json.Unmarshal(approvedResponse.Body.Bytes(), &approved); err != nil {
		t.Fatal(err)
	}
	if approved.Approval.StorageKind != "archive" || approved.Approval.PartitionGranularity != "month" ||
		approved.Approval.Source.RecordCount != 31 || approved.Approval.Archive != approved.Approval.Source ||
		approved.Approval.PhysicalRecords != reader.physical || len(approved.Approval.KafkaCoverage) != 0 {
		t.Fatalf("archive approval did not freeze month evidence: %+v", approved.Approval)
	}
	executeResponse := flowLifecycleRequest(t, s.executeFlowRawDelete, admin, http.MethodPost,
		"/api/v1/flow/storage/deletion-approvals/"+approved.Approval.ID+"/actions/execute", "id", approved.Approval.ID, nil, `"1"`)
	if executeResponse.Code != http.StatusAccepted {
		t.Fatalf("schedule archive delete: status=%d body=%s", executeResponse.Code, executeResponse.Body.String())
	}
	var scheduled struct {
		Job     opjob.Job                     `json:"job"`
		Receipt flowlifecycle.DeletionReceipt `json:"receipt"`
	}
	if err := json.Unmarshal(executeResponse.Body.Bytes(), &scheduled); err != nil {
		t.Fatal(err)
	}
	if scheduled.Job.JobType != flowlifecycle.ArchiveDeleteJobType || scheduled.Receipt.StorageKind != "archive" ||
		scheduled.Receipt.PartitionGranularity != "month" || scheduled.Receipt.PhysicalRecords != reader.physical {
		t.Fatalf("archive schedule mismatch: %+v", scheduled)
	}
	cancelResponse := flowLifecycleRequest(t, s.cancelOperationJob, admin, http.MethodPost,
		"/api/v1/operation-jobs/"+scheduled.Job.ID+"/actions/cancel", "id", scheduled.Job.ID, nil, "")
	if cancelResponse.Code != http.StatusConflict || !strings.Contains(cancelResponse.Body.String(), "destructive_job_not_cancelable") {
		t.Fatalf("archive destructive cancel: status=%d body=%s", cancelResponse.Code, cancelResponse.Body.String())
	}
	workerContext, stopWorker := context.WithCancel(ctx)
	worker := &opjob.Worker{
		Repo: s.jobs, JobType: flowlifecycle.ArchiveDeleteJobType, Owner: "archive-delete-integration",
		Handler: flowlifecycle.NewArchiveDeleteHandler(store, reader), PollInterval: time.Millisecond,
		LeaseFor: time.Second, RetryBase: time.Millisecond,
	}
	go worker.Run(workerContext)
	deadline := time.Now().Add(5 * time.Second)
	var finished opjob.Job
	for time.Now().Before(deadline) {
		finished, err = s.jobs.Get(ctx, scheduled.Job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if finished.Terminal() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopWorker()
	if finished.Status != opjob.StatusSucceeded {
		t.Fatalf("archive deletion job did not succeed: %+v", finished)
	}
	receipt, err := store.GetDeletionReceiptByJob(ctx, scheduled.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	reader.mu.Lock()
	dropCalls := reader.dropCalls
	reader.mu.Unlock()
	if receipt.Status != "succeeded" || receipt.PostDeleteRecordCount != 0 || dropCalls != 1 {
		t.Fatalf("archive deletion did not converge: receipt=%+v drops=%d", receipt, dropCalls)
	}
	revokeAfterSuccess := flowLifecycleRequest(t, s.revokeFlowDeletionApproval, admin, http.MethodPost,
		"/api/v1/flow/storage/deletion-approvals/"+approved.Approval.ID+"/actions/revoke", "id", approved.Approval.ID, nil, `"1"`)
	if revokeAfterSuccess.Code != http.StatusConflict {
		t.Fatalf("successful archive approval was revoked: status=%d body=%s", revokeAfterSuccess.Code, revokeAfterSuccess.Body.String())
	}
}

func insertArchiveDeletedDay(t testing.TB, ctx context.Context, db *sql.DB, day time.Time, policyID string, policyVersion uint64, counters flowlifecycle.Counters) {
	t.Helper()
	generation, err := flowlifecycle.Generation(policyVersion, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO flow_retention_partition_states
		(source_date,policy_id,policy_version,state,generation,repair_attempt,
		 source_record_count,source_raw_bytes,source_raw_packets,source_estimated_bytes,source_estimated_packets,source_estimated_valid_records,
		 archive_record_count,archive_raw_bytes,archive_raw_packets,archive_estimated_bytes,archive_estimated_packets,archive_estimated_valid_records,
		 archived_at,reconciled_at,late_checked_at,delete_eligible_at,raw_deleted_at)
		VALUES (?,?,?,'raw_deleted',?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, day, policyID, policyVersion, generation,
		counters.RecordCount, counters.RawBytes, counters.RawPackets, counters.EstimatedBytes, counters.EstimatedPackets, counters.EstimatedValidRecords,
		counters.RecordCount, counters.RawBytes, counters.RawPackets, counters.EstimatedBytes, counters.EstimatedPackets, counters.EstimatedValidRecords,
		day.Add(48*time.Hour), day.Add(48*time.Hour), day.Add(48*time.Hour), day.Add(49*time.Hour), day.Add(50*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
}

func TestFlowArchiveOperationJobRetriesFromCheckpointIntegration(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.DBName = "watchdog_flow_archive_job_it"
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
	policy := flowlifecycle.Policy{
		ID: "01JARCHIVEPOLICY000000000", Version: 1, Status: flowlifecycle.PolicyPublished,
		BootstrapFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), RawRetentionSeconds: 86400,
		ArchiveResolutionSeconds: 3600, ArchiveRetentionSeconds: 0, LateArrivalSeconds: 3600,
		DeleteGraceSeconds: 3600, MaxPartitionsPerRun: 7, RequireBackupBeforeDelete: true,
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO flow_retention_policy_revisions
		(id,policy_version,status,bootstrap_from,raw_retention_seconds,archive_resolution_seconds,
		 archive_retention_seconds,late_arrival_seconds,delete_grace_seconds,max_partitions_per_run,
		 raw_delete_enabled,archive_delete_enabled,require_backup_before_delete,published_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,0,0,1,?)`, policy.ID, policy.Version, policy.Status, policy.BootstrapFrom,
		policy.RawRetentionSeconds, policy.ArchiveResolutionSeconds, policy.ArchiveRetentionSeconds,
		policy.LateArrivalSeconds, policy.DeleteGraceSeconds, policy.MaxPartitionsPerRun, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	job, err := flowlifecycle.NewArchiveOperationJob(policy, policy.BootstrapFrom, 1)
	if err != nil {
		t.Fatal(err)
	}
	jobs := opjob.NewStore(db)
	job, err = jobs.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	runner := &testRetryArchiveRunner{}
	worker := &opjob.Worker{
		Repo: jobs, JobType: flowlifecycle.ArchiveJobType, Owner: "flow-archive-integration",
		Handler:      flowlifecycle.NewArchiveHandler(flowlifecycle.NewStore(db), runner),
		PollInterval: 5 * time.Millisecond, LeaseFor: 300 * time.Millisecond, RetryBase: 5 * time.Millisecond, MaxAttempts: 3,
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	go worker.Run(workerCtx)

	var completed opjob.Job
	for {
		completed, err = jobs.Get(ctx, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if completed.Terminal() {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("archive operation job did not finish: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	// 24 hourly rollups + the injected failure at call 4 (retried from the hour
	// checkpoint) + the daily 1d rollup the archive handler runs since 00be2cdb5.
	if completed.Status != opjob.StatusSucceeded || completed.AttemptCount != 2 || completed.ProgressDone != 24 || runner.calls.Load() != 26 {
		t.Fatalf("completed=%+v rollup_calls=%d", completed, runner.calls.Load())
	}
	var state, errorCode string
	var lateCheckedAt sql.NullTime
	if err := db.QueryRowContext(ctx, `SELECT state,COALESCE(last_error_code,''),late_checked_at
		FROM flow_retention_partition_states WHERE source_date=?`, policy.BootstrapFrom).Scan(&state, &errorCode, &lateCheckedAt); err != nil {
		t.Fatal(err)
	}
	if state != flowlifecycle.PartitionReconciled || errorCode != "" || !lateCheckedAt.Valid {
		t.Fatalf("partition state=%q error=%q late_checked=%v", state, errorCode, lateCheckedAt)
	}
	boundary, err := flowlifecycle.NewStore(db).ArchiveThrough(ctx, policy.BootstrapFrom, policy.BootstrapFrom.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if want := policy.BootstrapFrom.Add(24 * time.Hour); !boundary.Equal(want) {
		t.Fatalf("continuous archive boundary=%v want=%v", boundary, want)
	}
	s := &Server{flowLifecycle: flowlifecycle.NewStore(db)}
	request := flowquery.Request{From: policy.BootstrapFrom.Add(12 * time.Hour), To: policy.BootstrapFrom.Add(36 * time.Hour), Bucket: flowquery.BucketOneHour}
	if err := s.applyFlowStorageBoundary(ctx, &request); err != nil {
		t.Fatal(err)
	}
	if !request.StorageV2 || !request.ArchiveThrough.Equal(policy.BootstrapFrom.Add(24*time.Hour)) {
		t.Fatalf("hourly query boundary=%+v", request)
	}
	minuteRequest := flowquery.Request{From: policy.BootstrapFrom.Add(12 * time.Hour), To: policy.BootstrapFrom.Add(13 * time.Hour), Bucket: flowquery.BucketOneMinute}
	if err := s.applyFlowStorageBoundary(ctx, &minuteRequest); err != nil {
		t.Fatal(err)
	}
	if !minuteRequest.StorageV2 || !minuteRequest.ArchiveThrough.Equal(minuteRequest.From) {
		t.Fatalf("minute query must stay raw-only: %+v", minuteRequest)
	}
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
