package watchdog

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

// reaperBase is 60-aligned so 1m buckets land on it; rb(i) is the i-th bucket.
const reaperBase = int64(1_800_000_000)

func rb(i int64) int64 { return reaperBase + 60*i }

type fakeReaperStore struct {
	watermarks map[string]uint64
	states     []FlowRollupBucketState
	advanced   map[string]uint64
	enqueued   []OperationJob
	lastFrom   int64
	lastTo     int64
}

func (f *fakeReaperStore) GetOperationJobWatermark(_ context.Context, _ ID, _, partitionKey string) (uint64, error) {
	if v, ok := f.watermarks[partitionKey]; ok {
		return v, nil
	}
	return 0, sql.ErrNoRows
}

func (f *fakeReaperStore) AdvanceOperationJobWatermark(_ context.Context, _ ID, _, partitionKey string, value uint64) error {
	if f.advanced == nil {
		f.advanced = map[string]uint64{}
	}
	f.advanced[partitionKey] = value
	return nil
}

func (f *fakeReaperStore) ListFlowRollupBucketStates(_ context.Context, _ ID, _ flowch.RollupResolution, from, to int64) ([]FlowRollupBucketState, error) {
	f.lastFrom, f.lastTo = from, to
	var out []FlowRollupBucketState
	for _, s := range f.states {
		if s.BucketUnix >= from && s.BucketUnix < to {
			out = append(out, s)
		}
	}
	return out, nil
}

func (f *fakeReaperStore) EnqueueOperationJob(_ context.Context, job OperationJob) (OperationJob, error) {
	f.enqueued = append(f.enqueued, job)
	return job, nil
}

type fakeReaperRunner struct {
	needsRepair   map[int64]bool
	repairs       map[string]int
	permanentGaps int
}

func (f *fakeReaperRunner) BucketNeedsRepair(_ context.Context, _ string, _ flowch.RollupResolution, bucket time.Time) (bool, error) {
	return f.needsRepair[bucket.Unix()], nil
}

func (f *fakeReaperRunner) RecordReaperRepair(reason string) {
	if f.repairs == nil {
		f.repairs = map[string]int{}
	}
	f.repairs[reason]++
}

func (f *fakeReaperRunner) RecordPermanentGap() { f.permanentGaps++ }

func minutePartitions() (scheduled, completed string) {
	return flowRollupWatermarkPartition(flowch.RollupOneMinute), flowRollupCompletedPartition(flowch.RollupOneMinute)
}

func decodeEnqueued(t *testing.T, job OperationJob) (int64, uint64) {
	t.Helper()
	_, bucket, generation, err := parseFlowRollupIdempotencyKey(job.IdempotencyKey)
	if err != nil {
		t.Fatalf("undecodable idempotency key %q: %v", job.IdempotencyKey, err)
	}
	return bucket.Unix(), generation
}

func runReapSeries(t *testing.T, store *fakeReaperStore, runner *fakeReaperRunner, cfg FlowRollupReaperConfig, now time.Time) {
	t.Helper()
	if cfg.MaxBucketsPerScan == 0 {
		cfg.MaxBucketsPerScan = 100
	}
	if cfg.RetryCap == 0 {
		cfg.RetryCap = 5
	}
	reaper := &FlowRollupReaper{Store: store, Runner: runner, Config: cfg}
	if err := reaper.reapSeries(context.Background(), "tenant-a", flowch.RollupOneMinute, now.UTC()); err != nil {
		t.Fatalf("reapSeries: %v", err)
	}
}

func TestReaperAdvancesCompletionThroughContiguousSuccess(t *testing.T) {
	scheduled, completed := minutePartitions()
	store := &fakeReaperStore{
		watermarks: map[string]uint64{scheduled: uint64(rb(5)), completed: uint64(rb(0))},
		states: []FlowRollupBucketState{
			{BucketUnix: rb(1), MaxGeneration: 1, Succeeded: true},
			{BucketUnix: rb(2), MaxGeneration: 1, Succeeded: true},
			{BucketUnix: rb(3), MaxGeneration: 1, Succeeded: true},
			{BucketUnix: rb(4), MaxGeneration: 1, Succeeded: true},
			{BucketUnix: rb(5), MaxGeneration: 1, Succeeded: true},
		},
	}
	runner := &fakeReaperRunner{}
	runReapSeries(t, store, runner, FlowRollupReaperConfig{}, time.Unix(rb(6), 0))
	if store.advanced[completed] != uint64(rb(5)) {
		t.Fatalf("completion watermark = %d, want %d", store.advanced[completed], rb(5))
	}
	if len(store.enqueued) != 0 || len(runner.repairs) != 0 || runner.permanentGaps != 0 {
		t.Fatalf("healthy series should not repair: enqueued=%d repairs=%v gaps=%d", len(store.enqueued), runner.repairs, runner.permanentGaps)
	}
}

func TestReaperRedrivesFailedBucketAndFreezesFrontier(t *testing.T) {
	scheduled, completed := minutePartitions()
	store := &fakeReaperStore{
		watermarks: map[string]uint64{scheduled: uint64(rb(5)), completed: uint64(rb(0))},
		states: []FlowRollupBucketState{
			{BucketUnix: rb(1), MaxGeneration: 1, Succeeded: true},
			{BucketUnix: rb(2), MaxGeneration: 1, Succeeded: true},
			{BucketUnix: rb(3), MaxGeneration: 1}, // all attempts terminal, under cap
			{BucketUnix: rb(4), MaxGeneration: 1, Succeeded: true},
			{BucketUnix: rb(5), MaxGeneration: 1, Succeeded: true},
		},
	}
	runner := &fakeReaperRunner{}
	runReapSeries(t, store, runner, FlowRollupReaperConfig{}, time.Unix(rb(6), 0))
	// Frontier freezes at the last contiguous success before the hole.
	if store.advanced[completed] != uint64(rb(2)) {
		t.Fatalf("completion watermark = %d, want %d (frozen before hole)", store.advanced[completed], rb(2))
	}
	if len(store.enqueued) != 1 {
		t.Fatalf("want one repair enqueue, got %d", len(store.enqueued))
	}
	bucket, generation := decodeEnqueued(t, store.enqueued[0])
	if bucket != rb(3) || generation != 2 {
		t.Fatalf("repair enqueued bucket=%d gen=%d, want %d/2", bucket, generation, rb(3))
	}
	if runner.repairs[flowch.RollupRepairFailed] != 1 {
		t.Fatalf("failed-repair count = %v", runner.repairs)
	}
}

func TestReaperFillsGapAtGenerationOne(t *testing.T) {
	scheduled, completed := minutePartitions()
	store := &fakeReaperStore{
		watermarks: map[string]uint64{scheduled: uint64(rb(3)), completed: uint64(rb(0))},
		states: []FlowRollupBucketState{
			{BucketUnix: rb(1), MaxGeneration: 1, Succeeded: true},
			// rb(2) missing entirely — no job ever landed.
			{BucketUnix: rb(3), MaxGeneration: 1, Succeeded: true},
		},
	}
	runner := &fakeReaperRunner{}
	runReapSeries(t, store, runner, FlowRollupReaperConfig{}, time.Unix(rb(4), 0))
	if store.advanced[completed] != uint64(rb(1)) {
		t.Fatalf("completion watermark = %d, want %d", store.advanced[completed], rb(1))
	}
	if len(store.enqueued) != 1 {
		t.Fatalf("want one gap enqueue, got %d", len(store.enqueued))
	}
	bucket, generation := decodeEnqueued(t, store.enqueued[0])
	if bucket != rb(2) || generation != 1 {
		t.Fatalf("gap enqueued bucket=%d gen=%d, want %d/1", bucket, generation, rb(2))
	}
	if runner.repairs[flowch.RollupRepairGap] != 1 {
		t.Fatalf("gap-repair count = %v", runner.repairs)
	}
}

func TestReaperWaitsOnPendingWithoutReenqueue(t *testing.T) {
	scheduled, completed := minutePartitions()
	store := &fakeReaperStore{
		watermarks: map[string]uint64{scheduled: uint64(rb(2)), completed: uint64(rb(0))},
		states: []FlowRollupBucketState{
			{BucketUnix: rb(1), MaxGeneration: 1, Pending: true},
			{BucketUnix: rb(2), MaxGeneration: 1, Succeeded: true},
		},
	}
	runner := &fakeReaperRunner{}
	runReapSeries(t, store, runner, FlowRollupReaperConfig{}, time.Unix(rb(3), 0))
	if _, advanced := store.advanced[completed]; advanced {
		t.Fatalf("completion watermark advanced past a pending bucket: %v", store.advanced)
	}
	if len(store.enqueued) != 0 {
		t.Fatalf("pending bucket must not be re-enqueued, got %d", len(store.enqueued))
	}
}

func TestReaperAbandonsBucketAtRetryCapAndStepsPast(t *testing.T) {
	scheduled, completed := minutePartitions()
	store := &fakeReaperStore{
		watermarks: map[string]uint64{scheduled: uint64(rb(3)), completed: uint64(rb(0))},
		states: []FlowRollupBucketState{
			{BucketUnix: rb(1), MaxGeneration: 5}, // at RetryCap, never succeeded
			{BucketUnix: rb(2), MaxGeneration: 1, Succeeded: true},
			{BucketUnix: rb(3), MaxGeneration: 1, Succeeded: true},
		},
	}
	runner := &fakeReaperRunner{}
	runReapSeries(t, store, runner, FlowRollupReaperConfig{RetryCap: 5}, time.Unix(rb(4), 0))
	if store.advanced[completed] != uint64(rb(3)) {
		t.Fatalf("completion watermark = %d, want %d (stepped past abandoned gap)", store.advanced[completed], rb(3))
	}
	if runner.permanentGaps != 1 {
		t.Fatalf("permanent gaps = %d, want 1", runner.permanentGaps)
	}
	if len(store.enqueued) != 0 {
		t.Fatalf("an abandoned bucket must not be re-enqueued, got %d", len(store.enqueued))
	}
}

func TestReaperReconcilesLateDataOnSucceededBucket(t *testing.T) {
	scheduled, completed := minutePartitions()
	store := &fakeReaperStore{
		watermarks: map[string]uint64{scheduled: uint64(rb(2)), completed: uint64(rb(0))},
		states: []FlowRollupBucketState{
			{BucketUnix: rb(1), MaxGeneration: 1, Succeeded: true},
			{BucketUnix: rb(2), MaxGeneration: 1, Succeeded: true},
		},
	}
	runner := &fakeReaperRunner{needsRepair: map[int64]bool{rb(1): true}}
	runReapSeries(t, store, runner, FlowRollupReaperConfig{ReconcileWindow: 24 * time.Hour}, time.Unix(rb(2)+300, 0))
	// A late-data repair does not stop the frontier: the bucket still succeeded.
	if store.advanced[completed] != uint64(rb(2)) {
		t.Fatalf("completion watermark = %d, want %d", store.advanced[completed], rb(2))
	}
	if len(store.enqueued) != 1 {
		t.Fatalf("want one late repair, got %d", len(store.enqueued))
	}
	bucket, generation := decodeEnqueued(t, store.enqueued[0])
	if bucket != rb(1) || generation != 2 {
		t.Fatalf("late repair bucket=%d gen=%d, want %d/2", bucket, generation, rb(1))
	}
	if runner.repairs[flowch.RollupRepairLate] != 1 {
		t.Fatalf("late-repair count = %v", runner.repairs)
	}
}

func TestReaperColdStartBoundsWindowToRecentBuckets(t *testing.T) {
	scheduled, completed := minutePartitions()
	store := &fakeReaperStore{
		watermarks: map[string]uint64{scheduled: uint64(rb(1000))}, // no completion watermark yet
		states: []FlowRollupBucketState{
			{BucketUnix: rb(998), MaxGeneration: 1, Succeeded: true},
			{BucketUnix: rb(999), MaxGeneration: 1, Succeeded: true},
			{BucketUnix: rb(1000), MaxGeneration: 1, Succeeded: true},
		},
	}
	runner := &fakeReaperRunner{}
	runReapSeries(t, store, runner, FlowRollupReaperConfig{MaxBucketsPerScan: 3}, time.Unix(rb(1001), 0))
	// Only the bounded recent window is scanned — never all history back to bucket 0.
	if store.lastFrom != rb(998) || store.lastTo != rb(1000)+60 {
		t.Fatalf("scan window = [%d,%d), want [%d,%d)", store.lastFrom, store.lastTo, rb(998), rb(1000)+60)
	}
	if store.advanced[completed] != uint64(rb(1000)) {
		t.Fatalf("completion watermark = %d, want %d", store.advanced[completed], rb(1000))
	}
}
