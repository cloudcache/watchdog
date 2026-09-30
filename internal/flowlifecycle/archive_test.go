package flowlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/opjob"
)

func archivePolicyFixture() Policy {
	return Policy{
		ID: "01JARCHIVEPOLICY000000000", Version: 3, Status: PolicyPublished,
		BootstrapFrom:       time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		RawRetentionSeconds: 86400, ArchiveResolutionSeconds: 3600,
		LateArrivalSeconds: 600, DeleteGraceSeconds: 3600, MaxPartitionsPerRun: 3,
	}
}

type archiveStoreStub struct {
	policy Policy
	state  PartitionState
}

func (store *archiveStoreStub) GetPolicy(context.Context, string) (Policy, error) {
	return store.policy, nil
}

func (store *archiveStoreStub) BeginArchive(_ context.Context, policy Policy, day time.Time, generation uint64, jobID string) (PartitionState, error) {
	if store.state.State == PartitionReconciled && store.state.Generation == generation {
		return store.state, nil
	}
	_, attempt, _ := SplitGeneration(generation)
	store.state = PartitionState{
		SourceDate: UTCDate(day), PolicyID: policy.ID, PolicyVersion: policy.Version,
		State: PartitionSealed, Generation: generation, RepairAttempt: attempt, ArchiveJobID: jobID,
	}
	return store.state, nil
}

func (store *archiveStoreStub) MarkArchiveWritten(_ context.Context, _ Policy, _ time.Time, generation uint64, jobID string, at time.Time) error {
	if store.state.Generation != generation || store.state.ArchiveJobID != jobID || store.state.State != PartitionSealed {
		return ErrTransition
	}
	store.state.State, store.state.ArchivedAt = PartitionArchiveWritten, at
	return nil
}

func (store *archiveStoreStub) CompleteArchive(_ context.Context, _ Policy, _ time.Time, generation uint64, jobID string, source, archive Counters, at time.Time) error {
	if store.state.Generation != generation || store.state.ArchiveJobID != jobID || store.state.State != PartitionArchiveWritten {
		return ErrTransition
	}
	store.state.Source, store.state.Archive = source, archive
	if source != archive {
		store.state.State, store.state.LastErrorCode = PartitionFailed, "COUNTER_MISMATCH"
		return ErrTransition
	}
	store.state.State, store.state.ReconciledAt = PartitionReconciled, at
	return nil
}

func (store *archiveStoreStub) HoldArchive(_ context.Context, _ Policy, _ time.Time, generation uint64, jobID string, source, archive Counters, _ time.Time) error {
	if store.state.Generation != generation || store.state.ArchiveJobID != jobID || store.state.State != PartitionSealed {
		return ErrTransition
	}
	store.state.Source, store.state.Archive = source, archive
	store.state.State, store.state.LastErrorCode = PartitionFailed, ArchiveHoldRawIncomplete
	return nil
}

type archiveRunnerStub struct {
	requests []flowch.RollupRequest
	raw      flowch.StorageCounters
	archive  flowch.StorageCounters
	runErr   error
	countErr error
}

func (runner *archiveRunnerStub) Run(_ context.Context, request flowch.RollupRequest) error {
	runner.requests = append(runner.requests, request)
	return runner.runErr
}

func (runner *archiveRunnerStub) DayStorageCounters(context.Context, time.Time) (flowch.StorageCounters, flowch.StorageCounters, error) {
	return runner.raw, runner.archive, runner.countErr
}

func archiveJobFixture(t *testing.T, policy Policy, attempt uint32) opjob.Job {
	t.Helper()
	job, err := NewArchiveOperationJob(policy, policy.BootstrapFrom, attempt)
	if err != nil {
		t.Fatal(err)
	}
	job.ID = "01JARCHIVEJOB0000000000000"
	job.CreatedAt = policy.BootstrapFrom.Add(72 * time.Hour)
	return job
}

func TestArchiveOperationJobIdentityIsStableAndRepairSpecific(t *testing.T) {
	policy := archivePolicyFixture()
	first, err := NewArchiveOperationJob(policy, policy.BootstrapFrom, 1)
	if err != nil {
		t.Fatal(err)
	}
	replay, _ := NewArchiveOperationJob(policy, policy.BootstrapFrom, 1)
	repair, _ := NewArchiveOperationJob(policy, policy.BootstrapFrom, 2)
	if first.IdempotencyKey != replay.IdempotencyKey || first.RequestHash != replay.RequestHash {
		t.Fatalf("replay identity changed: first=%+v replay=%+v", first, replay)
	}
	if first.IdempotencyKey == repair.IdempotencyKey || first.RequestHash == repair.RequestHash {
		t.Fatalf("repair reused prior identity: first=%+v repair=%+v", first, repair)
	}
}

func TestArchiveHandlerBuildsAndReconcilesWholeUTCDay(t *testing.T) {
	policy := archivePolicyFixture()
	store := &archiveStoreStub{policy: policy}
	counters := flowch.StorageCounters{RecordCount: 11, RawBytes: 12, RawPackets: 13, EstimatedBytes: 14, EstimatedPackets: 15, EstimatedValidRecords: 10}
	runner := &archiveRunnerStub{raw: counters, archive: counters}
	job := archiveJobFixture(t, policy, 2)
	result, err := NewArchiveHandler(store, runner)(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	wantGeneration, _ := Generation(policy.Version, 2)
	if result != fmt.Sprintf("clickhouse:flow:1h:2026-09-01:p3:g%d", wantGeneration) || len(runner.requests) != 25 {
		t.Fatalf("result=%q requests=%d", result, len(runner.requests))
	}
	for hour, request := range runner.requests[:24] {
		if request.Resolution != flowch.RollupOneHour || request.Generation != wantGeneration ||
			!request.Bucket.Equal(policy.BootstrapFrom.Add(time.Duration(hour)*time.Hour)) {
			t.Fatalf("hour %d request=%+v", hour, request)
		}
	}
	if daily := runner.requests[24]; daily.Resolution != flowch.RollupOneDay || daily.Generation != wantGeneration || !daily.Bucket.Equal(policy.BootstrapFrom) {
		t.Fatalf("daily request=%+v", daily)
	}
	if store.state.State != PartitionReconciled || store.state.Source != lifecycleCounters(counters) {
		t.Fatalf("state=%+v", store.state)
	}
	if _, err := NewArchiveHandler(store, runner)(context.Background(), job); err != nil || len(runner.requests) != 25 {
		t.Fatalf("completed replay rewrote archive: requests=%d err=%v", len(runner.requests), err)
	}
}

func TestArchiveHandlerForwardsResourceLimits(t *testing.T) {
	policy := archivePolicyFixture()
	store := &archiveStoreStub{policy: policy}
	counters := flowch.StorageCounters{RecordCount: 11, RawBytes: 12, RawPackets: 13, EstimatedBytes: 14, EstimatedPackets: 15, EstimatedValidRecords: 10}
	runner := &archiveRunnerStub{raw: counters, archive: counters}
	limits := ArchiveLimits{MaxThreads: 4, Priority: 10, MaxMemoryBytes: 6 << 30}
	if _, err := NewArchiveHandlerWithLimits(store, runner, limits)(context.Background(), archiveJobFixture(t, policy, 1)); err != nil {
		t.Fatal(err)
	}
	if len(runner.requests) != 25 {
		t.Fatalf("requests=%d", len(runner.requests))
	}
	for _, request := range runner.requests {
		if request.MaxThreads != 4 || request.Priority != 10 || request.MaxMemoryBytes != 6<<30 {
			t.Fatalf("archive rollup ignored its limits: %+v", request)
		}
	}
}

func TestArchiveHandlerResumesFromCheckpointAndClassifiesFailures(t *testing.T) {
	policy := archivePolicyFixture()
	job := archiveJobFixture(t, policy, 1)
	var payload archivePayload
	if err := opjob.DecodePayload(job.CheckpointJSON, archivePayloadSchema, &payload); err != nil {
		t.Fatal(err)
	}
	payload.NextHour = 17
	job.CheckpointJSON, _ = opjob.EncodePayload(archivePayloadSchema, payload)
	store := &archiveStoreStub{policy: policy}
	runner := &archiveRunnerStub{}
	if _, err := NewArchiveHandler(store, runner)(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if len(runner.requests) != 8 || !runner.requests[0].Bucket.Equal(policy.BootstrapFrom.Add(17*time.Hour)) || runner.requests[7].Resolution != flowch.RollupOneDay {
		t.Fatalf("resume requests=%d first=%v", len(runner.requests), runner.requests[0].Bucket)
	}

	retryable := errors.New("ClickHouse temporarily unavailable")
	runner = &archiveRunnerStub{runErr: retryable}
	store = &archiveStoreStub{policy: policy}
	job = archiveJobFixture(t, policy, 1)
	if _, err := NewArchiveHandler(store, runner)(context.Background(), job); !errors.Is(err, retryable) || opjob.IsTerminalError(err) {
		t.Fatalf("retryable failure classification=%v", err)
	}

	permanent := flowch.Permanent(errors.New("invalid archive query"))
	runner = &archiveRunnerStub{runErr: permanent}
	store = &archiveStoreStub{policy: policy}
	if _, err := NewArchiveHandler(store, runner)(context.Background(), job); !opjob.IsTerminalError(err) {
		t.Fatalf("permanent failure was not terminal: %v", err)
	}
}

// TestArchiveHandlerHoldsDayWhoseRawWasRemoved: a day whose raw facts hold
// fewer records than its published aggregate (raw dropped outside the
// lifecycle) is held, never rebuilt into a smaller archive.
func TestArchiveHandlerHoldsDayWhoseRawWasRemoved(t *testing.T) {
	policy := archivePolicyFixture()
	store := &archiveStoreStub{policy: policy}
	runner := &archiveRunnerStub{
		raw:     flowch.StorageCounters{RecordCount: 5},
		archive: flowch.StorageCounters{RecordCount: 11},
	}
	_, err := NewArchiveHandler(store, runner)(context.Background(), archiveJobFixture(t, policy, 1))
	if !opjob.IsTerminalError(err) || len(runner.requests) != 0 {
		t.Fatalf("partial raw was rebuilt: err=%v requests=%d", err, len(runner.requests))
	}
	if store.state.State != PartitionFailed || store.state.LastErrorCode != ArchiveHoldRawIncomplete ||
		store.state.Source.RecordCount != 5 || store.state.Archive.RecordCount != 11 {
		t.Fatalf("held state=%+v", store.state)
	}
}

func TestArchiveHandlerCounterMismatchIsTerminalAndRecorded(t *testing.T) {
	policy := archivePolicyFixture()
	store := &archiveStoreStub{policy: policy}
	runner := &archiveRunnerStub{
		raw:     flowch.StorageCounters{RecordCount: 2, RawBytes: 100},
		archive: flowch.StorageCounters{RecordCount: 1, RawBytes: 100},
	}
	if _, err := NewArchiveHandler(store, runner)(context.Background(), archiveJobFixture(t, policy, 1)); !opjob.IsTerminalError(err) {
		t.Fatalf("counter mismatch was not terminal: %v", err)
	}
	if store.state.State != PartitionFailed || store.state.LastErrorCode != "COUNTER_MISMATCH" {
		t.Fatalf("state=%+v", store.state)
	}
}

type archiveSchedulerStoreStub struct {
	policy         Policy
	partitions     map[time.Time]PartitionState
	repairs        []PartitionState
	superseded     []PartitionState
	late           []PartitionState
	archiveThrough time.Time
	lateBefore     time.Time
}

func (store *archiveSchedulerStoreStub) GetPublishedPolicy(context.Context) (Policy, error) {
	if store.policy.ID == "" {
		return Policy{}, sql.ErrNoRows
	}
	return store.policy, nil
}

func (store *archiveSchedulerStoreStub) GetPolicy(context.Context, string) (Policy, error) {
	return store.policy, nil
}

func (store *archiveSchedulerStoreStub) GetPartition(_ context.Context, day time.Time) (PartitionState, error) {
	state, ok := store.partitions[UTCDate(day)]
	if !ok {
		return PartitionState{}, sql.ErrNoRows
	}
	return state, nil
}

func (store *archiveSchedulerStoreStub) BeginArchive(_ context.Context, policy Policy, day time.Time, generation uint64, jobID string) (PartitionState, error) {
	_, attempt, _ := SplitGeneration(generation)
	state := PartitionState{SourceDate: UTCDate(day), PolicyID: policy.ID, PolicyVersion: policy.Version,
		State: PartitionSealed, Generation: generation, RepairAttempt: attempt, ArchiveJobID: jobID}
	store.partitions[state.SourceDate] = state
	return state, nil
}

func (store *archiveSchedulerStoreStub) ListArchiveRepairCandidates(context.Context, int) ([]PartitionState, error) {
	return append([]PartitionState(nil), store.repairs...), nil
}

func (store *archiveSchedulerStoreStub) ListSupersededPartitions(context.Context, Policy, int) ([]PartitionState, error) {
	return append([]PartitionState(nil), store.superseded...), nil
}

func (store *archiveSchedulerStoreStub) ListLateCheckCandidates(_ context.Context, before time.Time, _ int) ([]PartitionState, error) {
	store.lateBefore = before
	return append([]PartitionState(nil), store.late...), nil
}

func (store *archiveSchedulerStoreStub) RecordLateCheck(_ context.Context, state PartitionState, source, archive Counters, at time.Time) error {
	state.LateCheckedAt, state.Source, state.Archive = at, source, archive
	if source != archive {
		state.State, state.LastErrorCode = PartitionFailed, "LATE_ARRIVAL"
	}
	store.partitions[state.SourceDate] = state
	return nil
}

func (store *archiveSchedulerStoreStub) ArchiveThrough(context.Context, time.Time, time.Time) (time.Time, error) {
	return store.archiveThrough, nil
}

type archiveEnqueuerStub struct {
	jobs []opjob.Job
}

func (jobs *archiveEnqueuerStub) Enqueue(_ context.Context, job opjob.Job) (opjob.Job, error) {
	job.ID = fmt.Sprintf("01JARCHIVEJOB%013d", len(jobs.jobs)+1)
	jobs.jobs = append(jobs.jobs, job)
	return job, nil
}

func TestArchiveSchedulerUsesRetentionLateWindowAndBudget(t *testing.T) {
	policy := archivePolicyFixture()
	policy.RawRetentionSeconds = 86400
	policy.LateArrivalSeconds = 90000
	store := &archiveSchedulerStoreStub{policy: policy, partitions: map[time.Time]PartitionState{}, archiveThrough: policy.BootstrapFrom}
	jobs := &archiveEnqueuerStub{}
	scheduler := ArchiveScheduler{Store: store, Jobs: jobs, Runner: &archiveRunnerStub{}, LateCheckEvery: 6 * time.Hour, MaxPartitions: 10, MaxLateChecks: 5}
	now := time.Date(2026, 9, 3, 0, 30, 0, 0, time.UTC)
	count, err := scheduler.ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 || len(jobs.jobs) != 0 {
		t.Fatalf("archive scheduled inside late window: count=%d jobs=%d", count, len(jobs.jobs))
	}
	count, err = scheduler.ScanOnce(context.Background(), now.Add(31*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(jobs.jobs) != 1 {
		t.Fatalf("eligible day not scheduled: count=%d jobs=%d", count, len(jobs.jobs))
	}
	if !store.lateBefore.Equal(now.Add(31*time.Minute - 6*time.Hour)) {
		t.Fatalf("late-check rotation cutoff=%v", store.lateBefore)
	}
}

func TestArchiveSchedulerDoesNotStallBehindHeldDay(t *testing.T) {
	policy := archivePolicyFixture()
	held, settled, next := policy.BootstrapFrom, policy.BootstrapFrom.Add(24*time.Hour), policy.BootstrapFrom.Add(48*time.Hour)
	now := next.Add(49 * time.Hour)
	for _, test := range []struct {
		code      string
		scheduled int
	}{
		{code: ArchiveHoldRawIncomplete, scheduled: 1},
		// An ordinary failure is repaired first; newer days wait for it.
		{code: "", scheduled: 0},
	} {
		store := &archiveSchedulerStoreStub{policy: policy, archiveThrough: held, partitions: map[time.Time]PartitionState{
			held:    {SourceDate: held, State: PartitionFailed, LastErrorCode: test.code},
			settled: {SourceDate: settled, State: PartitionReconciled},
		}}
		jobs := &archiveEnqueuerStub{}
		scheduler := ArchiveScheduler{Store: store, Jobs: jobs, Runner: &archiveRunnerStub{}, LateCheckEvery: 6 * time.Hour, MaxPartitions: 10, MaxLateChecks: 5}
		count, err := scheduler.ScanOnce(context.Background(), now)
		if err != nil || count != test.scheduled || len(jobs.jobs) != test.scheduled {
			t.Fatalf("code=%q count=%d jobs=%d err=%v", test.code, count, len(jobs.jobs), err)
		}
		if test.scheduled == 1 && store.partitions[next].State != PartitionSealed {
			t.Fatalf("day after the held one was not scheduled: %+v", store.partitions[next])
		}
	}
}

func TestArchiveSchedulerRepairsLateArrivalWithNextGeneration(t *testing.T) {
	policy := archivePolicyFixture()
	day := policy.BootstrapFrom
	generation, _ := Generation(policy.Version, 1)
	state := PartitionState{SourceDate: day, PolicyID: policy.ID, PolicyVersion: policy.Version,
		State: PartitionReconciled, Generation: generation, RepairAttempt: 1}
	store := &archiveSchedulerStoreStub{
		policy: policy, partitions: map[time.Time]PartitionState{day: state}, late: []PartitionState{state},
		archiveThrough: day,
	}
	jobs := &archiveEnqueuerStub{}
	runner := &archiveRunnerStub{raw: flowch.StorageCounters{RecordCount: 2}, archive: flowch.StorageCounters{RecordCount: 1}}
	scheduler := ArchiveScheduler{Store: store, Jobs: jobs, Runner: runner, LateCheckEvery: time.Hour, MaxPartitions: 1, MaxLateChecks: 1}
	count, err := scheduler.ScanOnce(context.Background(), day.Add(72*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(jobs.jobs) != 1 {
		t.Fatalf("late repair count=%d jobs=%d", count, len(jobs.jobs))
	}
	var payload archivePayload
	if err := opjob.DecodePayload(jobs.jobs[0].CheckpointJSON, archivePayloadSchema, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.RepairAttempt != 2 || payload.Generation != generation+1 {
		t.Fatalf("repair payload=%+v", payload)
	}
	if got := store.partitions[day]; got.State != PartitionSealed || got.RepairAttempt != 2 {
		t.Fatalf("repair state=%+v", got)
	}
}
