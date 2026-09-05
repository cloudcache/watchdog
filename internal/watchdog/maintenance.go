package watchdog

import (
	"context"
	"time"
)

// PLAT-04B periodic maintenance. Expiry-driven bulk deletes (idempotency
// records past their TTL, unused enrollment secrets past expiry, terminal
// operation jobs past a retention window) are simple idempotent deletes that
// do not need the operation_jobs lease/retry/checkpoint machinery, so they run
// as a direct periodic component rather than as tenant-scoped async jobs.

// MaintenanceRepository is the set of expiry purges the reaper drives. Each
// method deletes in bounded batches and returns the rows removed so a run can
// keep going until the table is drained.
type MaintenanceRepository interface {
	PurgeExpiredIdempotencyRecords(ctx context.Context, now time.Time, limit int) (int64, error)
	PurgeExpiredEnrollmentSecrets(ctx context.Context, now time.Time, limit int) (int64, error)
	PurgeTerminalOperationJobs(ctx context.Context, before time.Time, limit int) (int64, error)
	PurgeExpiredQuietHours(ctx context.Context, now time.Time, limit int) (int64, error)
}

const (
	maintenancePurgeBatch     = 1000
	operationJobRetention     = 14 * 24 * time.Hour
	maintenanceDefaultRunFreq = time.Hour
)

func (s *MySQLStore) PurgeExpiredIdempotencyRecords(ctx context.Context, now time.Time, limit int) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM idempotency_records WHERE expires_at <= ? LIMIT ?
	`, now.UTC(), limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *MySQLStore) PurgeExpiredEnrollmentSecrets(ctx context.Context, now time.Time, limit int) (int64, error) {
	// Consumed secrets keep their used_by trail (auditable enrollment
	// evidence); only unused-and-expired secrets are reaped.
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM collector_enrollment_secrets
		WHERE used_at IS NULL AND expires_at <= ? LIMIT ?
	`, now.UTC(), limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *MySQLStore) PurgeTerminalOperationJobs(ctx context.Context, before time.Time, limit int) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM operation_jobs
		WHERE status IN ('succeeded', 'failed', 'canceled')
			AND finished_at IS NOT NULL AND finished_at <= ? LIMIT ?
	`, before.UTC(), limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// PurgeExpiredQuietHours reaps one-time quiet-hour windows whose end has passed.
// Daily windows recur and never expire; future one-time windows are kept. This
// is the MySQL successor to the PocketBase deleteOldQuietHours cron.
func (s *MySQLStore) PurgeExpiredQuietHours(ctx context.Context, now time.Time, limit int) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM quiet_hours WHERE window_type = 'one-time' AND end_at <= ? LIMIT ?
	`, now.UTC(), limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// MaintenanceTask is one registered periodic purge.
type MaintenanceTask struct {
	Name     string
	Interval time.Duration
	// Run performs one pass and returns how many rows it removed. It should
	// drain in batches internally; the reaper calls it once per interval.
	Run func(ctx context.Context) (int64, error)
}

// PeriodicMaintenance runs registered tasks on their own tickers until the
// context ends. It is intentionally not built on operation_jobs: these are
// global, tenant-agnostic infrastructure purges.
type PeriodicMaintenance struct {
	tasks []MaintenanceTask
	Logf  func(string, ...any)
}

func (m *PeriodicMaintenance) Register(task MaintenanceTask) {
	if task.Name == "" || task.Run == nil {
		return
	}
	if task.Interval <= 0 {
		task.Interval = maintenanceDefaultRunFreq
	}
	m.tasks = append(m.tasks, task)
}

func (m *PeriodicMaintenance) logf(format string, args ...any) {
	if m.Logf != nil {
		m.Logf(format, args...)
	}
}

// Start launches one goroutine per task. Each runs once shortly after start
// (so a fresh boot drains any backlog) and then on its interval.
func (m *PeriodicMaintenance) Start(ctx context.Context) {
	for _, task := range m.tasks {
		go m.runTask(ctx, task)
	}
}

func (m *PeriodicMaintenance) runTask(ctx context.Context, task MaintenanceTask) {
	ticker := time.NewTicker(task.Interval)
	defer ticker.Stop()
	// Initial pass after a short delay so startup is not stampeded.
	initial := time.NewTimer(time.Minute)
	defer initial.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-initial.C:
			m.execute(ctx, task)
		case <-ticker.C:
			m.execute(ctx, task)
		}
	}
}

func (m *PeriodicMaintenance) execute(ctx context.Context, task MaintenanceTask) {
	removed, err := task.Run(ctx)
	if err != nil {
		m.logf("maintenance %s: %v", task.Name, err)
		return
	}
	if removed > 0 {
		m.logf("maintenance %s: removed %d rows", task.Name, removed)
	}
}

// NewStoreMaintenance builds the standard purge set for a MySQL-backed store.
func NewStoreMaintenance(repo MaintenanceRepository, logf func(string, ...any)) *PeriodicMaintenance {
	m := &PeriodicMaintenance{Logf: logf}
	m.Register(MaintenanceTask{
		Name:     "idempotency_records",
		Interval: time.Hour,
		Run:      drainPurge(func(ctx context.Context) (int64, error) { return repo.PurgeExpiredIdempotencyRecords(ctx, time.Now().UTC(), maintenancePurgeBatch) }),
	})
	m.Register(MaintenanceTask{
		Name:     "enrollment_secrets",
		Interval: time.Hour,
		Run:      drainPurge(func(ctx context.Context) (int64, error) { return repo.PurgeExpiredEnrollmentSecrets(ctx, time.Now().UTC(), maintenancePurgeBatch) }),
	})
	m.Register(MaintenanceTask{
		Name:     "operation_jobs",
		Interval: 6 * time.Hour,
		Run: drainPurge(func(ctx context.Context) (int64, error) {
			return repo.PurgeTerminalOperationJobs(ctx, time.Now().UTC().Add(-operationJobRetention), maintenancePurgeBatch)
		}),
	})
	m.Register(MaintenanceTask{
		Name:     "quiet_hours",
		Interval: time.Hour,
		Run: drainPurge(func(ctx context.Context) (int64, error) {
			return repo.PurgeExpiredQuietHours(ctx, time.Now().UTC(), maintenancePurgeBatch)
		}),
	})
	return m
}

// drainPurge repeats a batched purge until a pass removes fewer than the batch
// size, so one scheduled run fully drains the backlog without a single
// unbounded DELETE.
func drainPurge(purge func(ctx context.Context) (int64, error)) func(ctx context.Context) (int64, error) {
	return func(ctx context.Context) (int64, error) {
		var total int64
		for {
			removed, err := purge(ctx)
			total += removed
			if err != nil {
				return total, err
			}
			if removed < maintenancePurgeBatch {
				return total, nil
			}
			if ctx.Err() != nil {
				return total, ctx.Err()
			}
		}
	}
}
