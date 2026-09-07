package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

type memoryFlowStorageStore struct {
	*memoryFlowRollupStore
	policies []FlowStoragePolicy
	states   map[string]FlowStoragePartitionState
	beginErr error
}

func newMemoryFlowStorageStore(policies ...FlowStoragePolicy) *memoryFlowStorageStore {
	return &memoryFlowStorageStore{memoryFlowRollupStore: newMemoryFlowRollupStore(), policies: policies, states: map[string]FlowStoragePartitionState{}}
}

func (store *memoryFlowStorageStore) ListPublishedFlowStoragePolicies(_ context.Context, after ID, limit int) ([]FlowStoragePolicy, ID, error) {
	items := make([]FlowStoragePolicy, 0, limit)
	for _, policy := range store.policies {
		if policy.Status == FlowStoragePolicyPublished && policy.TenantID > after && len(items) < limit {
			items = append(items, policy)
		}
	}
	var next ID
	if len(items) == limit {
		next = items[len(items)-1].TenantID
	}
	return items, next, nil
}

func (store *memoryFlowStorageStore) GetFlowStoragePolicy(_ context.Context, tenantID, policyID ID) (FlowStoragePolicy, error) {
	for _, policy := range store.policies {
		if policy.TenantID == tenantID && policy.ID == policyID {
			return policy, nil
		}
	}
	return FlowStoragePolicy{}, sql.ErrNoRows
}

func (store *memoryFlowStorageStore) BeginFlowStoragePartition(_ context.Context, policy FlowStoragePolicy, day time.Time, generation uint64, jobID ID) (FlowStoragePartitionState, error) {
	if store.beginErr != nil {
		return FlowStoragePartitionState{}, store.beginErr
	}
	key := string(policy.TenantID) + ":" + day.Format("2006-01-02")
	if state, ok := store.states[key]; ok && state.State == FlowStoragePartitionReconciled && state.Generation == generation {
		return state, nil
	}
	state := FlowStoragePartitionState{TenantID: policy.TenantID, SourceDate: day, PolicyID: policy.ID,
		PolicyVersion: policy.PolicyVersion, State: FlowStoragePartitionSealed, Generation: generation, DownsampleJobID: jobID}
	store.states[key] = state
	return state, nil
}

func (store *memoryFlowStorageStore) ListFlowStorageRepairCandidates(_ context.Context, policy FlowStoragePolicy, limit int) ([]FlowStoragePartitionState, error) {
	items := make([]FlowStoragePartitionState, 0)
	for _, state := range store.states {
		if state.PolicyID == policy.ID && state.PolicyVersion == policy.PolicyVersion && state.State == FlowStoragePartitionFailed {
			items = append(items, state)
			if len(items) == limit {
				break
			}
		}
	}
	return items, nil
}

func (store *memoryFlowStorageStore) MarkFlowStoragePartitionDownsampleWritten(_ context.Context, policy FlowStoragePolicy, day time.Time, generation uint64, jobID ID, now time.Time) (FlowStoragePartitionState, error) {
	key := string(policy.TenantID) + ":" + day.Format("2006-01-02")
	state := store.states[key]
	if state.Generation != generation || state.DownsampleJobID != jobID {
		return FlowStoragePartitionState{}, ErrFlowStorageTransition
	}
	state.State, state.DownsampledAt = FlowStoragePartitionDownsampleWritten, now
	store.states[key] = state
	return state, nil
}

func (store *memoryFlowStorageStore) CompleteFlowStoragePartition(_ context.Context, policy FlowStoragePolicy, day time.Time, generation uint64, jobID ID, source, archive FlowStoragePartitionCounters, now time.Time) (FlowStoragePartitionState, error) {
	key := string(policy.TenantID) + ":" + day.Format("2006-01-02")
	state := store.states[key]
	state.Source, state.Archive, state.DownsampledAt = &source, &archive, now
	if source != archive {
		state.State, state.LastErrorCode = FlowStoragePartitionFailed, "COUNTER_MISMATCH"
		store.states[key] = state
		return state, ErrFlowStorageTransition
	}
	state.State, state.ReconciledAt, state.DeleteEligibleAt = FlowStoragePartitionReconciled, now, now.Add(time.Duration(policy.DeleteGraceSeconds)*time.Second)
	store.states[key] = state
	return state, nil
}

type recordingFlowStorageDayRunner struct {
	requests []flowch.RollupRequest
	raw      flowch.StorageCounters
	archive  flowch.StorageCounters
	err      error
}

func (runner *recordingFlowStorageDayRunner) Run(_ context.Context, request flowch.RollupRequest) error {
	runner.requests = append(runner.requests, request)
	return runner.err
}

func (runner *recordingFlowStorageDayRunner) DayStorageCounters(context.Context, string, time.Time) (flowch.StorageCounters, flowch.StorageCounters, error) {
	return runner.raw, runner.archive, runner.err
}

func flowStoragePolicyFixture() FlowStoragePolicy {
	return FlowStoragePolicy{ID: "policy-a", TenantID: "tenant-a", PolicyVersion: 3, Status: FlowStoragePolicyPublished,
		BootstrapFrom: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), RawRetentionSeconds: 7 * 86400,
		DownsampleResolutionSeconds: 3600, ArchiveRetentionSeconds: 365 * 86400,
		LateArrivalSeconds: 600, DeleteGraceSeconds: 86400, MaxPartitionsPerRun: 2}
}

func TestFlowStorageDownsampleJobIsStableAndTenantScoped(t *testing.T) {
	policy := flowStoragePolicyFixture()
	day := policy.BootstrapFrom
	first, err := NewFlowStorageDownsampleOperationJob(policy, day, 2)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := NewFlowStorageDownsampleOperationJob(policy, day, 2)
	if err != nil {
		t.Fatal(err)
	}
	repair, err := NewFlowStorageDownsampleOperationJob(policy, day, 3)
	if err != nil {
		t.Fatal(err)
	}
	if first.TenantID != policy.TenantID || first.IdempotencyKey != replay.IdempotencyKey || first.RequestHash != replay.RequestHash ||
		first.IdempotencyKey == repair.IdempotencyKey || first.RequestHash == repair.RequestHash {
		t.Fatalf("job identities first=%+v replay=%+v repair=%+v", first, replay, repair)
	}
}

func TestFlowStorageDownsampleHandlerBuildsAndReconcilesWholeUTCDay(t *testing.T) {
	policy := flowStoragePolicyFixture()
	store := newMemoryFlowStorageStore(policy)
	counters := flowch.StorageCounters{RecordCount: 9, RawBytes: 100, RawPackets: 10, EstimatedBytes: 1000, EstimatedPackets: 100, EstimatedValidRecords: 8}
	runner := &recordingFlowStorageDayRunner{raw: counters, archive: counters}
	job, err := NewFlowStorageDownsampleOperationJob(policy, policy.BootstrapFrom, 2)
	if err != nil {
		t.Fatal(err)
	}
	job.ID = "job-a"
	job.CreatedAt = policy.BootstrapFrom.Add(48 * time.Hour)
	result, err := NewFlowStorageDownsampleJobHandler(store, runner)(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	wantGeneration, _ := flowStorageGeneration(policy.PolicyVersion, 2)
	if result != "clickhouse:tenant-a:1h:2026-09-01:p3:g"+fmt.Sprint(wantGeneration) || len(runner.requests) != 24 {
		t.Fatalf("result=%q requests=%d", result, len(runner.requests))
	}
	for hour, request := range runner.requests {
		want := policy.BootstrapFrom.Add(time.Duration(hour) * time.Hour)
		if request.Resolution != flowch.RollupOneHour || request.Generation != wantGeneration || !request.Bucket.Equal(want) {
			t.Fatalf("hour %d request=%+v", hour, request)
		}
	}
	state := store.states["tenant-a:2026-09-01"]
	if state.State != FlowStoragePartitionReconciled || state.Source == nil || *state.Source != flowStorageCounters(counters) {
		t.Fatalf("state=%+v", state)
	}
	// A lease retry after completion is a no-op and must not rewrite 24 hours.
	if _, err := NewFlowStorageDownsampleJobHandler(store, runner)(context.Background(), job); err != nil || len(runner.requests) != 24 {
		t.Fatalf("replay requests=%d err=%v", len(runner.requests), err)
	}
}

func TestFlowStorageDownsampleCounterMismatchIsTerminalAndRecorded(t *testing.T) {
	policy := flowStoragePolicyFixture()
	store := newMemoryFlowStorageStore(policy)
	runner := &recordingFlowStorageDayRunner{raw: flowch.StorageCounters{RecordCount: 2}, archive: flowch.StorageCounters{RecordCount: 1}}
	job, _ := NewFlowStorageDownsampleOperationJob(policy, policy.BootstrapFrom, 1)
	job.ID, job.CreatedAt = "job-a", policy.BootstrapFrom.Add(48*time.Hour)
	if _, err := NewFlowStorageDownsampleJobHandler(store, runner)(context.Background(), job); err == nil || !IsTerminalJobError(err) {
		t.Fatalf("mismatch error=%v", err)
	}
	if state := store.states["tenant-a:2026-09-01"]; state.State != FlowStoragePartitionFailed || state.LastErrorCode != "COUNTER_MISMATCH" {
		t.Fatalf("state=%+v", state)
	}
}

func TestFlowStorageSchedulerUsesRetentionAndLateWindowWithDurableWatermark(t *testing.T) {
	policy := flowStoragePolicyFixture()
	policy.RawRetentionSeconds = 86400
	policy.LateArrivalSeconds = 86400 + 600
	store := newMemoryFlowStorageStore(policy)
	service := FlowStorageLifecycleService{Store: store, MaxPoliciesPerScan: 10, MaxPartitionsPerScan: 10}
	now := time.Date(2026, 9, 4, 0, 5, 0, 0, time.UTC)
	count, err := service.ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	// Sep 2 is still inside the longer late-arrival window even though its
	// one-day raw retention has elapsed.
	if count != 1 || len(store.jobs) != 1 {
		t.Fatalf("first count=%d jobs=%d", count, len(store.jobs))
	}
	count, err = service.ScanOnce(context.Background(), now.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(store.jobs) != 2 {
		t.Fatalf("second count=%d jobs=%d", count, len(store.jobs))
	}
	for _, job := range store.jobs {
		if job.JobType != FlowStorageDownsampleJobType {
			t.Fatalf("unexpected job=%+v", job)
		}
	}
}

func TestFlowStorageSchedulerDoesNotDownsampleBeforeRawRetention(t *testing.T) {
	policy := flowStoragePolicyFixture()
	store := newMemoryFlowStorageStore(policy)
	service := FlowStorageLifecycleService{Store: store, MaxPoliciesPerScan: 10, MaxPartitionsPerScan: 10}
	now := policy.BootstrapFrom.Add(7*24*time.Hour + 23*time.Hour)
	count, err := service.ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 || len(store.jobs) != 0 {
		t.Fatalf("scheduled before the newest event in the UTC day reached retention: count=%d jobs=%d", count, len(store.jobs))
	}
	count, err = service.ScanOnce(context.Background(), policy.BootstrapFrom.Add(8*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(store.jobs) != 1 {
		t.Fatalf("first UTC day was not scheduled at its retention boundary: count=%d jobs=%d", count, len(store.jobs))
	}
}

func TestFlowStorageSchedulerDoesNotAdvancePastFailedEnqueue(t *testing.T) {
	policy := flowStoragePolicyFixture()
	store := newMemoryFlowStorageStore(policy)
	store.enqueueErr = errors.New("mysql unavailable")
	_, err := scheduleFlowStoragePolicy(context.Background(), store, policy, time.Date(2026, 9, 11, 1, 0, 0, 0, time.UTC), 2)
	if !errors.Is(err, store.enqueueErr) || len(store.watermarks) != 0 {
		t.Fatalf("error=%v watermarks=%v", err, store.watermarks)
	}
}

func TestFlowStorageSchedulerPersistsPartitionBeforeWatermark(t *testing.T) {
	policy := flowStoragePolicyFixture()
	store := newMemoryFlowStorageStore(policy)
	store.beginErr = errors.New("partition state unavailable")
	now := time.Date(2026, 9, 11, 1, 0, 0, 0, time.UTC)
	_, err := scheduleFlowStoragePolicy(context.Background(), store, policy, now, 1)
	if !errors.Is(err, store.beginErr) || len(store.jobs) != 1 || len(store.watermarks) != 0 || len(store.states) != 0 {
		t.Fatalf("first attempt error=%v jobs=%d watermarks=%v states=%v", err, len(store.jobs), store.watermarks, store.states)
	}
	store.beginErr = nil
	count, err := scheduleFlowStoragePolicy(context.Background(), store, policy, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(store.jobs) != 1 || len(store.watermarks) != 1 || len(store.states) != 1 {
		t.Fatalf("recovery count=%d jobs=%d watermarks=%v states=%v", count, len(store.jobs), store.watermarks, store.states)
	}
}

func TestFlowStorageSchedulerRepairsFailedPartitionWithNextAttemptBeforeAdvancing(t *testing.T) {
	policy := flowStoragePolicyFixture()
	policy.MaxPartitionsPerRun = 1
	store := newMemoryFlowStorageStore(policy)
	failedGeneration, _ := flowStorageGeneration(policy.PolicyVersion, 2)
	store.states["tenant-a:2026-09-01"] = FlowStoragePartitionState{
		TenantID: policy.TenantID, SourceDate: policy.BootstrapFrom, PolicyID: policy.ID,
		PolicyVersion: policy.PolicyVersion, State: FlowStoragePartitionFailed, Generation: failedGeneration,
	}
	count, err := scheduleFlowStoragePolicy(context.Background(), store, policy, time.Date(2026, 9, 4, 1, 0, 0, 0, time.UTC), 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(store.jobs) != 1 || len(store.watermarks) != 0 {
		t.Fatalf("count=%d jobs=%d watermarks=%v", count, len(store.jobs), store.watermarks)
	}
	var queued OperationJob
	for _, job := range store.jobs {
		queued = job
	}
	var payload flowStorageDownsamplePayload
	if err := DecodeJobPayload(queued.CheckpointJSON, FlowStorageDownsamplePayloadVersion, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.RepairAttempt != 3 || payload.Generation != failedGeneration+1 {
		t.Fatalf("repair payload=%+v", payload)
	}
}
