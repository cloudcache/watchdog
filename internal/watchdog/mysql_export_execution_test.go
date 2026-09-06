package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestMySQLExportExecutionAtomicCreateCancelRetryAndProjection(t *testing.T) {
	db, tenantID := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	userID, targetID := ID("user_export_v1"), ID("target_export_v1")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status)
		VALUES (?, ?, 'export-v1@example.test', 'Export V1', 'active')
	`, userID, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO targets (id, tenant_id, name, kind, host, status)
		VALUES (?, ?, 'Export V1', 'network', 'export-v1.example.test', 'up')
	`, targetID, tenantID); err != nil {
		t.Fatal(err)
	}

	policy := exportGatewayPolicy(QueryValueRaw)
	policy.TenantID = tenantID
	gateway := newExportGatewayFixture(t, policy, &queryProviderStub{})
	auth := exportGatewayAuth(QueryValueRaw)
	auth.TenantID, auth.UserID = tenantID, userID
	for index := range auth.Grants {
		auth.Grants[index].TenantID = tenantID
		auth.Grants[index].SubjectID = userID
		if auth.Grants[index].ResourceType == ResourceTenant {
			auth.Grants[index].ResourceID = tenantID
		} else {
			auth.Grants[index].ResourceID = targetID
		}
	}
	start := time.Date(2026, 9, 6, 1, 0, 0, 0, time.UTC)
	task, err := prepareExportExecutionTask(ctx, gateway, nil, auth, MetricSNMPIfInBps, time.Minute, ExportTask{
		ID: "export_contract_v1", TenantID: tenantID, CreatedBy: userID, TargetID: targetID,
		RangeStart: start, RangeEnd: start.Add(time.Hour), Step: 5 * time.Minute,
		Aggregation: AggregationP95FiveMinute, ValueMode: ExportValueRaw, ValueLayer: QueryValueRaw,
		Format: ExportFormatCSV,
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateExportTask(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	if created.OperationJobID == "" || created.Status != ExportStatusPending || created.ContractVersion != ExportExecutionContractVersion {
		t.Fatalf("created = %+v", created)
	}
	firstJob, err := store.GetOperationJob(ctx, tenantID, created.OperationJobID)
	if err != nil {
		t.Fatal(err)
	}
	if firstJob.CreatedBy != userID || firstJob.JobType != ExportExecutionJobType || firstJob.Status != OperationJobStatusQueued {
		t.Fatalf("first job = %+v", firstJob)
	}
	if pending, err := store.ListPendingExportTasks(ctx, 10); err != nil || len(pending) != 0 {
		t.Fatalf("legacy pending = %+v, err=%v", pending, err)
	}

	if err := store.RequestOperationJobCancel(ctx, tenantID, firstJob.ID); err != nil {
		t.Fatal(err)
	}
	canceled, err := store.GetExportTask(ctx, tenantID, task.ID)
	if err != nil || canceled.Status != ExportStatusCanceled {
		t.Fatalf("canceled = %+v, err=%v", canceled, err)
	}
	if err := store.RetryExportTask(ctx, tenantID, task.ID); err != nil {
		t.Fatal(err)
	}
	retried, err := store.GetExportTask(ctx, tenantID, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.OperationJobID == firstJob.ID || retried.Status != ExportStatusPending || retried.QueryHash != task.QueryHash {
		t.Fatalf("retried = %+v", retried)
	}
	secondJob, err := store.GetOperationJob(ctx, tenantID, retried.OperationJobID)
	if err != nil {
		t.Fatal(err)
	}
	if secondJob.CreatedBy != userID || secondJob.Status != OperationJobStatusQueued {
		t.Fatalf("second job = %+v", secondJob)
	}

	leased, err := store.LeaseNextOperationJob(ctx, ExportExecutionJobType, "export-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if leased.ID != secondJob.ID {
		t.Fatalf("leased = %+v", leased)
	}
	artifact := ExportArtifact{
		FileRef:   "exports/export_contract_v1.csv",
		Checksum:  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SizeBytes: 100, SchemaVersion: ExportArtifactSchemaVersion,
		ContentType: "text/csv; charset=utf-8", RowCount: 5,
	}
	if err := store.MarkExportComplete(ctx, tenantID, task.ID, artifact, time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteOperationJobSucceeded(ctx, leased.ID, leased.LeaseToken, artifact.FileRef); err != nil {
		t.Fatal(err)
	}
	completed, err := store.GetExportTask(ctx, tenantID, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != ExportStatusComplete || completed.FileRef != artifact.FileRef || completed.RowCount != 5 || completed.ContentType != artifact.ContentType {
		t.Fatalf("completed = %+v", completed)
	}
	page, total, err := store.ListExportTasksPage(ctx, tenantID, ExportTaskListFilter{
		CreatedBy: userID, Search: "target_export", Status: ExportStatusComplete,
		ValueLayer: QueryValueRaw, Format: ExportFormatCSV, SortBy: "status", SortDirection: "asc",
		Limit: 25,
	})
	if err != nil || total != 1 || len(page) != 1 || page[0].ID != task.ID {
		t.Fatalf("filtered export page=%+v total=%d err=%v", page, total, err)
	}

	// Expiry only enqueues the same durable deletion lifecycle used by the API;
	// it does not delete the row behind the artifact store's back.
	if _, err := db.ExecContext(ctx, `UPDATE export_tasks SET expires_at = ? WHERE tenant_id = ? AND id = ?`, time.Now().UTC().Add(-time.Minute), tenantID, task.ID); err != nil {
		t.Fatal(err)
	}
	if queued, err := store.QueueExpiredExportDeletes(ctx, time.Now().UTC(), 100); err != nil || queued != 1 {
		t.Fatalf("QueueExpiredExportDeletes() queued=%d err=%v", queued, err)
	}
	if queued, err := store.QueueExpiredExportDeletes(ctx, time.Now().UTC(), 100); err != nil || queued != 0 {
		t.Fatalf("duplicate expiry scan queued=%d err=%v", queued, err)
	}
	deleteJob, err := store.LeaseNextOperationJob(ctx, ExportDeleteJobType, "export-delete-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	files := &CSVExportWriter{Files: map[string][]byte{artifact.FileRef: []byte("artifact")}}
	result, err := NewExportDeleteJobHandler(store, files, store)(ctx, deleteJob)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteOperationJobSucceeded(ctx, deleteJob.ID, deleteJob.LeaseToken, result); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetExportTask(ctx, tenantID, task.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("deleted export lookup error = %v", err)
	}
	if _, exists := files.Files[artifact.FileRef]; exists {
		t.Fatal("expired export artifact still exists")
	}
	var receiptCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM audit_logs
		WHERE tenant_id = ? AND action = 'export_task.destroyed' AND resource_id = ?
	`, tenantID, task.ID).Scan(&receiptCount); err != nil || receiptCount != 1 {
		t.Fatalf("destruction receipts=%d err=%v", receiptCount, err)
	}
}

func TestValidateExportTaskListFilter(t *testing.T) {
	for _, filter := range []ExportTaskListFilter{
		{Limit: 101}, {Limit: 25, Offset: -1}, {Limit: 25, Status: "bogus"},
		{Limit: 25, ValueLayer: "bogus"}, {Limit: 25, Format: "json"},
		{Limit: 25, SortBy: "tenant_id"}, {Limit: 25, SortDirection: "sideways"},
	} {
		if err := validateExportTaskListFilter(&filter); err == nil {
			t.Fatalf("expected invalid filter: %+v", filter)
		}
	}
}
