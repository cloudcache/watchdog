package watchdog

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func noopJobHandler(context.Context, OperationJob) (string, error) { return "", nil }

func TestOperationJobRegistryRejectsDuplicatesAndEmpty(t *testing.T) {
	registry := NewOperationJobHandlerRegistry()
	if err := registry.Register(OperationJobRegistration{JobType: "", Handler: noopJobHandler}); err == nil {
		t.Fatal("empty job type must be rejected")
	}
	if err := registry.Register(OperationJobRegistration{JobType: "a", Handler: nil}); err == nil {
		t.Fatal("nil handler must be rejected")
	}
	if err := registry.Register(OperationJobRegistration{JobType: "a", Handler: noopJobHandler}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(OperationJobRegistration{JobType: "a", Handler: noopJobHandler}); err == nil {
		t.Fatal("duplicate job type must be rejected")
	}
	if regs := registry.registrations(); len(regs) != 1 || regs[0].Concurrency != 1 {
		t.Fatalf("registrations = %+v", regs)
	}
}

// TestMySQLOperationJobSchedulerMultiType registers two job types with a
// concurrency budget and confirms the scheduler drains both through the real
// lease/complete path.
func TestMySQLOperationJobSchedulerMultiType(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var alpha, beta atomic.Uint32
	registry := NewOperationJobHandlerRegistry()
	if err := registry.Register(OperationJobRegistration{
		JobType: "sched_alpha", Concurrency: 2, LeaseFor: 3 * time.Second, RetryBase: time.Nanosecond,
		Handler: func(context.Context, OperationJob) (string, error) { alpha.Add(1); return "a", nil },
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(OperationJobRegistration{
		JobType: "sched_beta", Concurrency: 1, LeaseFor: 3 * time.Second, RetryBase: time.Nanosecond,
		Handler: func(context.Context, OperationJob) (string, error) { beta.Add(1); return "b", nil },
	}); err != nil {
		t.Fatal(err)
	}
	if started := StartOperationJobScheduler(ctx, store, registry, "sched-host", nil); started != 3 {
		t.Fatalf("scheduler started %d workers, want 3", started)
	}

	var alphaJobs []ID
	for i := 0; i < 4; i++ {
		job, err := store.EnqueueOperationJob(ctx, OperationJob{
			TenantID: tenant, JobType: "sched_alpha",
			IdempotencyKey: "alpha-" + string(rune('a'+i)), RequestHash: strings.Repeat("1", 64),
		})
		if err != nil {
			t.Fatal(err)
		}
		alphaJobs = append(alphaJobs, job.ID)
	}
	betaJob, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenant, JobType: "sched_beta", IdempotencyKey: "beta-1", RequestHash: strings.Repeat("2", 64),
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, id := range alphaJobs {
		waitForOperationJob(t, store, tenant, id, OperationJobStatusSucceeded)
	}
	waitForOperationJob(t, store, tenant, betaJob.ID, OperationJobStatusSucceeded)
	if alpha.Load() != 4 || beta.Load() != 1 {
		t.Fatalf("handler runs alpha=%d beta=%d, want 4/1", alpha.Load(), beta.Load())
	}
}
