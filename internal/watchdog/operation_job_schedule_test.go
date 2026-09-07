package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestNextOperationJobScheduleTimeUsesRequestedTimezone(t *testing.T) {
	after := time.Date(2024, time.January, 1, 23, 59, 30, 0, time.UTC)
	next, err := nextOperationJobScheduleTime("0 9 * * *", "Asia/Shanghai", after)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2024, time.January, 2, 1, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
	if _, err := nextOperationJobScheduleTime("bad", "UTC", after); err == nil {
		t.Fatal("invalid cron expression was accepted")
	}
	if _, err := nextOperationJobScheduleTime("@hourly", "Mars/Olympus", after); err == nil {
		t.Fatal("invalid timezone was accepted")
	}
}

func TestNormalizeOperationJobScheduleRejectsInvalidScopeAndPayload(t *testing.T) {
	base := OperationJobSchedule{
		TenantID: "tenant-a", Name: "Nightly", JobType: "cleanup", PartitionKey: "default",
		CronExpression: "@daily", Timezone: "UTC", PayloadJSON: json.RawMessage(`{"schema_version":1}`),
		Enabled: true,
	}
	got, err := normalizeOperationJobSchedule(base, time.Now())
	if err != nil || got.ScopeType != OperationJobScopeTenant || got.MaxInflight != 1 || got.NextRunAt.IsZero() {
		t.Fatalf("normalized = %#v, err = %v", got, err)
	}
	badScope := base
	badScope.ScopeType = OperationJobScopeSystem
	if _, err := normalizeOperationJobSchedule(badScope, time.Now()); err == nil {
		t.Fatal("system schedule carrying a tenant was accepted")
	}
	badPayload := base
	badPayload.PayloadJSON = json.RawMessage(`[]`)
	if _, err := normalizeOperationJobSchedule(badPayload, time.Now()); err == nil {
		t.Fatal("array payload was accepted")
	}
}

func TestOperationJobSchedulePayloadEqualIgnoresMySQLJSONFormatting(t *testing.T) {
	stored := json.RawMessage(`{"payload": {"version": 1}, "schema_version": 1}`)
	desired := json.RawMessage(`{"schema_version":1,"payload":{"version":1}}`)
	if !operationJobSchedulePayloadEqual(stored, desired) {
		t.Fatal("semantically equal JSON payloads were treated as different")
	}
	if operationJobSchedulePayloadEqual(stored, json.RawMessage(`{"schema_version":1,"payload":{"version":2}}`)) {
		t.Fatal("different JSON payloads were treated as equal")
	}
}

func TestScheduledOperationJobRequestHashIsStableAndContentBound(t *testing.T) {
	schedule := OperationJobSchedule{
		ID: "schedule-a", JobType: "cleanup", PartitionKey: "p0",
		NextRunAt:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		PayloadJSON: json.RawMessage(`{"schema_version":1}`),
	}
	first, err := scheduledOperationJobRequestHash(schedule)
	if err != nil {
		t.Fatal(err)
	}
	second, err := scheduledOperationJobRequestHash(schedule)
	if err != nil || first != second || len(first) != 64 {
		t.Fatalf("hashes first=%q second=%q err=%v", first, second, err)
	}
	schedule.PayloadJSON = json.RawMessage(`{"schema_version":2}`)
	changed, err := scheduledOperationJobRequestHash(schedule)
	if err != nil || changed == first {
		t.Fatalf("content mutation hash=%q err=%v", changed, err)
	}
}

type fakeOperationJobScheduleDispatchRepository struct {
	key      string
	now      time.Time
	scan     int
	dispatch int
	budgets  map[string]int
	result   OperationJobScheduleDispatchResult
	err      error
}

func (f *fakeOperationJobScheduleDispatchRepository) DispatchDueOperationJobSchedules(_ context.Context, key string, now time.Time, scan, dispatch int, budgets map[string]int) (OperationJobScheduleDispatchResult, error) {
	f.key, f.now, f.scan, f.dispatch, f.budgets = key, now, scan, dispatch, budgets
	return f.result, f.err
}

func TestOperationJobScheduleDispatcherUsesRegistryBudgetsAndBounds(t *testing.T) {
	registry := NewOperationJobHandlerRegistry()
	if err := registry.Register(OperationJobRegistration{JobType: "alpha", Handler: noopJobHandler, Concurrency: 3}); err != nil {
		t.Fatal(err)
	}
	repo := &fakeOperationJobScheduleDispatchRepository{result: OperationJobScheduleDispatchResult{Scanned: 2, Enqueued: 1}}
	dispatcher := OperationJobScheduleDispatcher{Repository: repo, Registry: registry}
	now := time.Now()
	result, err := dispatcher.RunOnce(context.Background(), now)
	if err != nil || result.Enqueued != 1 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if repo.key != operationJobSchedulerKey || repo.scan != 100 || repo.dispatch != 50 || repo.budgets["alpha"] != 3 || !repo.now.Equal(now.UTC()) {
		t.Fatalf("dispatch call = %#v", repo)
	}
	repo.err = errors.New("database unavailable")
	if _, err := dispatcher.RunOnce(context.Background(), now); err == nil {
		t.Fatal("repository error was hidden")
	}
}

func TestMySQLOperationJobScheduleCRUDDispatchAndWatermark(t *testing.T) {
	db, tenantID := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	// The forward migration is intentionally replay-safe for interrupted
	// deployments that may have created only part of the additive contract.
	runEmbeddedMigrationAgain(t, db, "044")

	first, err := store.CreateOperationJobSchedule(ctx, OperationJobSchedule{
		TenantID: tenantID, Name: "First", JobType: "scheduled_test", PartitionKey: "first",
		CronExpression: "@hourly", Timezone: "UTC", PayloadJSON: json.RawMessage(`{"schema_version":1,"payload":{"name":"first"}}`),
		Enabled: true, MaxInflight: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.CreateOperationJobSchedule(ctx, OperationJobSchedule{
		TenantID: tenantID, Name: "Second", JobType: "scheduled_test", PartitionKey: "second",
		CronExpression: "@hourly", Timezone: "UTC", PayloadJSON: json.RawMessage(`{"schema_version":1,"payload":{"name":"second"}}`),
		Enabled: true, MaxInflight: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateOperationJobSchedule(ctx, OperationJobSchedule{
		TenantID: tenantID, Name: "Duplicate", JobType: first.JobType, PartitionKey: first.PartitionKey,
		CronExpression: "@daily", Timezone: "UTC", PayloadJSON: json.RawMessage(`{}`), Enabled: true,
	}); !errors.Is(err, ErrOperationJobScheduleExists) {
		t.Fatalf("duplicate error = %v", err)
	}
	items, total, err := store.ListOperationJobSchedules(ctx, tenantID, OperationJobScheduleFilter{Search: "Sec", JobType: "scheduled_test", Limit: 10})
	if err != nil || total != 1 || len(items) != 1 || items[0].ID != second.ID {
		t.Fatalf("list items=%#v total=%d err=%v", items, total, err)
	}
	first.Name = "First updated"
	updated, err := store.UpdateOperationJobSchedule(ctx, first, first.RowVersion)
	if err != nil || updated.RowVersion != first.RowVersion+1 {
		t.Fatalf("updated=%#v err=%v", updated, err)
	}
	if _, err := store.UpdateOperationJobSchedule(ctx, first, first.RowVersion); !errors.Is(err, ErrOperationJobScheduleConflict) {
		t.Fatalf("stale update error = %v", err)
	}

	if _, err := db.ExecContext(ctx, `UPDATE operation_job_schedules SET next_run_at = ? WHERE id IN (?, ?)`, now.Add(-2*time.Minute), first.ID, second.ID); err != nil {
		t.Fatal(err)
	}
	registry := NewOperationJobHandlerRegistry()
	if err := registry.Register(OperationJobRegistration{JobType: "scheduled_test", Handler: noopJobHandler, Concurrency: 1}); err != nil {
		t.Fatal(err)
	}
	dispatcher := OperationJobScheduleDispatcher{Repository: store, Registry: registry, ScanLimit: 10, DispatchLimit: 10}
	dispatched, err := dispatcher.RunOnce(ctx, now)
	if err != nil || dispatched.Scanned != 2 || dispatched.Enqueued != 1 || dispatched.Backpressured != 1 {
		t.Fatalf("first dispatch=%#v err=%v", dispatched, err)
	}
	var job OperationJob
	for _, scheduleID := range []ID{first.ID, second.ID} {
		candidate, scanErr := scanOperationJob(db.QueryRowContext(ctx, `SELECT `+operationJobColumns+` FROM operation_jobs WHERE schedule_id = ?`, scheduleID))
		if scanErr == nil {
			job = candidate
			break
		}
	}
	if job.ID == "" || job.ScheduleID == "" || job.JobType != "scheduled_test" {
		t.Fatalf("scheduled job = %#v", job)
	}
	leased, err := store.LeaseNextOperationJob(ctx, job.JobType, "schedule-test-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteOperationJobSucceeded(ctx, leased.ID, leased.LeaseToken, "done"); err != nil {
		t.Fatal(err)
	}
	secondDispatch, err := dispatcher.RunOnce(ctx, now.Add(time.Second))
	if err != nil || secondDispatch.Enqueued != 1 {
		t.Fatalf("second dispatch=%#v err=%v", secondDispatch, err)
	}

	if err := store.AdvanceSystemOperationJobWatermark(ctx, "reconcile", "partition-1", 20); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceSystemOperationJobWatermark(ctx, "reconcile", "partition-1", 10); err != nil {
		t.Fatal(err)
	}
	watermark, err := store.GetSystemOperationJobWatermark(ctx, "reconcile", "partition-1")
	if err != nil || watermark != 20 {
		t.Fatalf("watermark=%d err=%v", watermark, err)
	}
	if value, known, err := store.LookupSystemOperationJobWatermark(ctx, "reconcile", "never-written"); err != nil || known || value != 0 {
		t.Fatalf("missing watermark value=%d known=%t err=%v", value, known, err)
	}
	if value, known, err := store.LookupSystemOperationJobWatermark(ctx, "reconcile", "partition-1"); err != nil || !known || value != 20 {
		t.Fatalf("stored watermark value=%d known=%t err=%v", value, known, err)
	}

	systemPartition := "system-" + string(tenantID)
	desiredSystem := OperationJobSchedule{Name: "System reconcile", JobType: "system_reconcile_test", PartitionKey: systemPartition,
		CronExpression: "@hourly", Timezone: "UTC", PayloadJSON: json.RawMessage(`{"schema_version":1,"payload":{"version":1}}`), Enabled: true, MaxInflight: 1}
	systemFirst, err := store.EnsureSystemOperationJobSchedule(ctx, desiredSystem)
	if err != nil || systemFirst.ScopeType != OperationJobScopeSystem || systemFirst.TenantID != "" {
		t.Fatalf("system schedule=%#v err=%v", systemFirst, err)
	}
	systemSame, err := store.EnsureSystemOperationJobSchedule(ctx, desiredSystem)
	if err != nil || systemSame.ID != systemFirst.ID || systemSame.RowVersion != systemFirst.RowVersion || !systemSame.NextRunAt.Equal(systemFirst.NextRunAt) {
		t.Fatalf("unchanged system schedule first=%#v same=%#v err=%v", systemFirst, systemSame, err)
	}
	desiredSystem.PayloadJSON = json.RawMessage(`{"schema_version":1,"payload":{"version":2}}`)
	systemUpdated, err := store.EnsureSystemOperationJobSchedule(ctx, desiredSystem)
	if err != nil || systemUpdated.ID != systemFirst.ID || systemUpdated.RowVersion != systemFirst.RowVersion+1 {
		t.Fatalf("updated system schedule=%#v err=%v", systemUpdated, err)
	}
	stale, err := store.CreateOperationJobSchedule(ctx, OperationJobSchedule{ScopeType: OperationJobScopeSystem,
		Name: "Stale system reconcile", JobType: desiredSystem.JobType, PartitionKey: systemPartition + "-old",
		CronExpression: "@hourly", Timezone: "UTC", PayloadJSON: json.RawMessage(`{"schema_version":1}`), Enabled: true, MaxInflight: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DisableSystemOperationJobSchedulesExcept(ctx, desiredSystem.JobType, systemUpdated.ID); err != nil {
		t.Fatal(err)
	}
	staleStored, err := getOperationJobScheduleByScope(ctx, db, OperationJobScopeSystem, "", stale.ID)
	if err != nil || staleStored.Enabled {
		t.Fatalf("stale schedule=%#v err=%v", staleStored, err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM operation_job_schedules WHERE id IN (?, ?)`, systemFirst.ID, stale.ID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM operation_job_system_watermarks WHERE job_type = 'reconcile' AND partition_key IN ('partition-1','never-written')`)
	})

	latestFirst, err := store.GetOperationJobSchedule(ctx, tenantID, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteOperationJobSchedule(ctx, tenantID, first.ID, latestFirst.RowVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetOperationJobSchedule(ctx, tenantID, first.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("deleted schedule error = %v", err)
	}
	var retainedJobs int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM operation_jobs WHERE job_type = ?`, "scheduled_test").Scan(&retainedJobs); err != nil || retainedJobs != 2 {
		t.Fatalf("retained jobs=%d err=%v", retainedJobs, err)
	}
}
