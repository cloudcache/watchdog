package watchdog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeMaintenanceRepo struct {
	idempotency atomic.Int64
	enrollment  atomic.Int64
	jobs        atomic.Int64
	quiet       atomic.Int64
	exports     atomic.Int64
}

func (f *fakeMaintenanceRepo) PurgeExpiredQuietHours(context.Context, time.Time, int) (int64, error) {
	f.quiet.Add(1)
	return 0, nil
}

func (f *fakeMaintenanceRepo) PurgeExpiredExports(context.Context, time.Time, int) (int64, error) {
	f.exports.Add(1)
	return 0, nil
}

func (f *fakeMaintenanceRepo) PurgeExpiredIdempotencyRecords(context.Context, time.Time, int) (int64, error) {
	f.idempotency.Add(1)
	return 0, nil
}
func (f *fakeMaintenanceRepo) PurgeExpiredEnrollmentSecrets(context.Context, time.Time, int) (int64, error) {
	f.enrollment.Add(1)
	return 0, nil
}
func (f *fakeMaintenanceRepo) PurgeTerminalOperationJobs(context.Context, time.Time, int) (int64, error) {
	f.jobs.Add(1)
	return 0, nil
}

func TestNewStoreMaintenanceRegistersExpectedTasks(t *testing.T) {
	m := NewStoreMaintenance(&fakeMaintenanceRepo{}, nil)
	names := map[string]bool{}
	for _, task := range m.tasks {
		names[task.Name] = true
		if task.Interval <= 0 || task.Run == nil {
			t.Fatalf("task %q misconfigured: interval=%v run-nil=%t", task.Name, task.Interval, task.Run == nil)
		}
	}
	for _, want := range []string{"idempotency_records", "enrollment_secrets", "operation_jobs", "quiet_hours", "expired_exports"} {
		if !names[want] {
			t.Fatalf("missing maintenance task %q; got %v", want, names)
		}
	}
}

func TestPeriodicMaintenanceRunsRegisteredTasks(t *testing.T) {
	var runs atomic.Int64
	done := make(chan struct{})
	m := &PeriodicMaintenance{}
	m.Register(MaintenanceTask{
		Name:     "test",
		Interval: 10 * time.Millisecond,
		Run: func(context.Context) (int64, error) {
			if runs.Add(1) == 1 {
				close(done)
			}
			return 3, nil
		},
	})
	// Registration guards: empty name / nil run are dropped.
	m.Register(MaintenanceTask{Name: "", Run: func(context.Context) (int64, error) { return 0, nil }})
	m.Register(MaintenanceTask{Name: "x", Run: nil})
	if len(m.tasks) != 1 {
		t.Fatalf("registered tasks = %d, want 1", len(m.tasks))
	}

	// Shorten the startup delay path by using the ticker: run once and stop.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.runTask(ctx, m.tasks[0])
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("maintenance task did not run")
	}
}

func TestDrainPurgeStopsBelowBatch(t *testing.T) {
	var calls atomic.Int64
	drain := drainPurge(func(context.Context) (int64, error) {
		n := calls.Add(1)
		if n < 3 {
			return maintenancePurgeBatch, nil // full batch => keep going
		}
		return 5, nil // partial batch => stop
	})
	total, err := drain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || total != maintenancePurgeBatch*2+5 {
		t.Fatalf("calls=%d total=%d", calls.Load(), total)
	}

	// An error surfaces and stops the drain.
	boom := errors.New("boom")
	errCalls := 0
	drainErr := drainPurge(func(context.Context) (int64, error) {
		errCalls++
		return maintenancePurgeBatch, boom
	})
	if _, err := drainErr(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}
	if errCalls != 1 {
		t.Fatalf("error must stop drain immediately, calls=%d", errCalls)
	}
}

// TestMySQLMaintenancePurges seeds expired and live rows and confirms each
// purge removes only the expired ones, and drainPurge clears a backlog larger
// than one batch.
func TestMySQLMaintenancePurges(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	now := time.Now().UTC()

	// idempotency_records: one expired, one live.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO idempotency_records (tenant_id, idempotency_key, request_hash, response_status, response_body, expires_at)
		VALUES (?, 'expired', ?, 201, '', ?), (?, 'live', ?, 201, '', ?)
	`, tenant, strings.Repeat("a", 64), now.Add(-time.Hour), tenant, strings.Repeat("b", 64), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	removed, err := store.PurgeExpiredIdempotencyRecords(ctx, now, 1000)
	if err != nil || removed != 1 {
		t.Fatalf("idempotency purge removed=%d err=%v", removed, err)
	}
	var idemLeft int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM idempotency_records WHERE tenant_id = ?", tenant).Scan(&idemLeft); err != nil || idemLeft != 1 {
		t.Fatalf("idempotency left=%d err=%v", idemLeft, err)
	}

	// enrollment secrets: expired-unused purged, expired-but-used kept,
	// live-unused kept.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_enrollment_secrets (id, tenant_id, secret_hash, module_key, agent_type, mode, collector_name, expires_at, created_by)
		VALUES ('sec_exp_unused', ?, 'h', 'flow', 'flow_collect', 'listen', 'a', ?, 'u'),
		       ('sec_live_unused', ?, 'h', 'flow', 'flow_collect', 'listen', 'b', ?, 'u')
	`, tenant, now.Add(-time.Hour), tenant, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_enrollment_secrets (id, tenant_id, secret_hash, module_key, agent_type, mode, collector_name, expires_at, used_at, used_by_collector_id, created_by)
		VALUES ('sec_exp_used', ?, 'h', 'flow', 'flow_collect', 'listen', 'c', ?, ?, 'collector_x', 'u')
	`, tenant, now.Add(-time.Hour), now.Add(-30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	removed, err = store.PurgeExpiredEnrollmentSecrets(ctx, now, 1000)
	if err != nil || removed != 1 {
		t.Fatalf("enrollment purge removed=%d err=%v", removed, err)
	}
	var usedLeft, liveLeft int
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM collector_enrollment_secrets WHERE id = 'sec_exp_used'").Scan(&usedLeft)
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM collector_enrollment_secrets WHERE id = 'sec_live_unused'").Scan(&liveLeft)
	if usedLeft != 1 || liveLeft != 1 {
		t.Fatalf("used secret kept=%d live secret kept=%d", usedLeft, liveLeft)
	}

	// operation_jobs: a terminal old job purged, a terminal recent job and a
	// running job kept.
	mustJob := func(id, status string, finished *time.Time) {
		var f any
		if finished != nil {
			f = *finished
		}
		if status == "running" {
			if _, err := db.ExecContext(ctx, `
				INSERT INTO operation_jobs (id, tenant_id, job_type, status, idempotency_key, request_hash, checkpoint_json, lease_owner, lease_token, lease_expires_at)
				VALUES (?, ?, 'maint', 'running', ?, ?, '{}', 'o', 't', ?)
			`, id, tenant, id, strings.Repeat("c", 64), now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			return
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO operation_jobs (id, tenant_id, job_type, status, idempotency_key, request_hash, checkpoint_json, finished_at)
			VALUES (?, ?, 'maint', ?, ?, ?, '{}', ?)
		`, id, tenant, status, id, strings.Repeat("c", 64), f); err != nil {
			t.Fatal(err)
		}
	}
	old := now.Add(-operationJobRetention - time.Hour)
	recent := now.Add(-time.Hour)
	mustJob("job_old", "succeeded", &old)
	mustJob("job_recent", "succeeded", &recent)
	mustJob("job_running", "running", nil)
	removed, err = store.PurgeTerminalOperationJobs(ctx, now.Add(-operationJobRetention), 1000)
	if err != nil || removed != 1 {
		t.Fatalf("operation job purge removed=%d err=%v", removed, err)
	}
	var jobsLeft int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM operation_jobs WHERE tenant_id = ? AND job_type = 'maint'", tenant).Scan(&jobsLeft); err != nil || jobsLeft != 2 {
		t.Fatalf("operation jobs left=%d err=%v", jobsLeft, err)
	}

	// quiet_hours: an expired one-time window is purged; a future one-time and a
	// daily window (recurs, never expires) are kept.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status)
		VALUES ('user_maint_qh', ?, 'qh-maint@test.local', 'QH', 'active')
	`, tenant); err != nil {
		t.Fatal(err)
	}
	seedQuietHour := func(id, windowType string, end time.Time) {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO quiet_hours (id, tenant_id, user_id, system_id, window_type, start_at, end_at)
			VALUES (?, ?, 'user_maint_qh', '', ?, ?, ?)
		`, id, tenant, windowType, now.Add(-2*time.Hour), end); err != nil {
			t.Fatal(err)
		}
	}
	seedQuietHour("qh_expired", "one-time", now.Add(-time.Hour))
	seedQuietHour("qh_future", "one-time", now.Add(time.Hour))
	seedQuietHour("qh_daily", "daily", now.Add(-time.Hour))
	removed, err = store.PurgeExpiredQuietHours(ctx, now, 1000)
	if err != nil || removed != 1 {
		t.Fatalf("quiet hours purge removed=%d err=%v", removed, err)
	}
	var quietLeft int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM quiet_hours WHERE tenant_id = ?", tenant).Scan(&quietLeft); err != nil || quietLeft != 2 {
		t.Fatalf("quiet hours left=%d err=%v", quietLeft, err)
	}

	// export_tasks: a completed export past its TTL is purged; a completed
	// export with a future TTL and a pending export (NULL expires_at) are kept.
	seedExport := func(id, status string, expires any) {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO export_tasks (id, tenant_id, created_by, period_type, range_start, range_end, aggregation, value_mode, format, status, expires_at)
			VALUES (?, ?, 'user_maint_qh', 'fixed', ?, ?, 'p95_5m', 'raw', 'csv', ?, ?)
		`, id, tenant, now.Add(-time.Hour), now, status, expires); err != nil {
			t.Fatal(err)
		}
	}
	seedExport("exp_expired", "complete", now.Add(-time.Hour))
	seedExport("exp_future", "complete", now.Add(time.Hour))
	seedExport("exp_pending", "pending", nil)
	removed, err = store.PurgeExpiredExports(ctx, now, 1000)
	if err != nil || removed != 1 {
		t.Fatalf("export purge removed=%d err=%v", removed, err)
	}
	var exportsLeft int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM export_tasks WHERE tenant_id = ?", tenant).Scan(&exportsLeft); err != nil || exportsLeft != 2 {
		t.Fatalf("exports left=%d err=%v", exportsLeft, err)
	}

	// drainPurge clears a backlog larger than one batch (seed batch+2 expired
	// idempotency rows, purge in batches of `maintenancePurgeBatch`).
	for i := 0; i < maintenancePurgeBatch+2; i++ {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO idempotency_records (tenant_id, idempotency_key, request_hash, response_status, response_body, expires_at)
			VALUES (?, ?, ?, 200, '', ?)
		`, tenant, fmt.Sprintf("bulk-%d", i), strings.Repeat("d", 64), now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	drain := drainPurge(func(ctx context.Context) (int64, error) {
		return store.PurgeExpiredIdempotencyRecords(ctx, now, maintenancePurgeBatch)
	})
	total, err := drain(ctx)
	if err != nil || total != maintenancePurgeBatch+2 {
		t.Fatalf("drain total=%d err=%v", total, err)
	}
}
