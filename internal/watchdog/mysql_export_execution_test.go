package watchdog

import (
	"context"
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
}
