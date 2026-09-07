package watchdog

import (
	"context"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

// TestListFlowRollupBucketStatesReducesLedger drives the real MySQL query: it
// seeds flow_rollup jobs across buckets, generations, and statuses, then asserts
// the per-bucket reduction the reaper depends on. Gated on the operation-job
// test DSN like the other operation-job integration tests.
func TestListFlowRollupBucketStatesReducesLedger(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	base := int64(1_800_000_000) // 60-aligned
	bucketAt := func(i int64) time.Time { return time.Unix(base+60*i, 0).UTC() }

	// Seed a queued job for (bucket, generation), then move it to a terminal
	// status when asked, so the key format and status filter are exercised end to
	// end. Buckets: 1 succeeded; 2 all-failed (gen1,gen2); 3 failed then succeeded;
	// 4 still queued (in flight).
	seed := func(bucketIndex int64, generation uint64, status string) {
		job, err := NewFlowRollupOperationJob(tenant, flowch.RollupOneMinute, bucketAt(bucketIndex), generation)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.EnqueueOperationJob(ctx, job); err != nil {
			t.Fatal(err)
		}
		if status == OperationJobStatusQueued {
			return
		}
		if _, err := db.ExecContext(ctx,
			"UPDATE operation_jobs SET status = ?, finished_at = NOW(3) WHERE tenant_id = ? AND idempotency_key = ?",
			status, tenant, flowRollupIdempotencyKey(flowch.RollupOneMinute, bucketAt(bucketIndex), generation)); err != nil {
			t.Fatal(err)
		}
	}
	seed(1, 1, OperationJobStatusSucceeded)
	seed(2, 1, OperationJobStatusFailed)
	seed(2, 2, OperationJobStatusFailed)
	seed(3, 1, OperationJobStatusFailed)
	seed(3, 2, OperationJobStatusSucceeded)
	seed(4, 1, OperationJobStatusQueued)

	states, err := store.ListFlowRollupBucketStates(ctx, tenant, flowch.RollupOneMinute, base+60, base+60*5)
	if err != nil {
		t.Fatal(err)
	}
	byBucket := make(map[int64]FlowRollupBucketState, len(states))
	for _, state := range states {
		byBucket[state.BucketUnix] = state
	}
	want := map[int64]FlowRollupBucketState{
		base + 60*1: {BucketUnix: base + 60*1, MaxGeneration: 1, Succeeded: true},
		base + 60*2: {BucketUnix: base + 60*2, MaxGeneration: 2},
		base + 60*3: {BucketUnix: base + 60*3, MaxGeneration: 2, Succeeded: true},
		base + 60*4: {BucketUnix: base + 60*4, MaxGeneration: 1, Pending: true},
	}
	if len(states) != len(want) {
		t.Fatalf("got %d bucket states, want %d: %+v", len(states), len(want), states)
	}
	for bucketUnix, expected := range want {
		got, ok := byBucket[bucketUnix]
		if !ok || got != expected {
			t.Fatalf("bucket %d state = %+v (present=%v), want %+v", bucketUnix, got, ok, expected)
		}
	}

	// The range is half-open: the exclusive upper bound must drop bucket 5.
	seed(5, 1, OperationJobStatusSucceeded)
	states, err = store.ListFlowRollupBucketStates(ctx, tenant, flowch.RollupOneMinute, base+60, base+60*5)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range states {
		if state.BucketUnix == base+60*5 {
			t.Fatalf("exclusive upper bound leaked bucket 5 into the result")
		}
	}

	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM operation_jobs WHERE tenant_id = ?", tenant)
	})
}
