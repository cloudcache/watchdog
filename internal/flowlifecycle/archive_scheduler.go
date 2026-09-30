package flowlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/opjob"
)

type archiveSchedulerStore interface {
	GetPublishedPolicy(context.Context) (Policy, error)
	GetPolicy(context.Context, string) (Policy, error)
	GetPartition(context.Context, time.Time) (PartitionState, error)
	BeginArchive(context.Context, Policy, time.Time, uint64, string) (PartitionState, error)
	ListArchiveRepairCandidates(context.Context, int) ([]PartitionState, error)
	ListSupersededPartitions(context.Context, Policy, int) ([]PartitionState, error)
	ListLateCheckCandidates(context.Context, time.Time, int) ([]PartitionState, error)
	RecordLateCheck(context.Context, PartitionState, Counters, Counters, time.Time) error
	ArchiveThrough(context.Context, time.Time, time.Time) (time.Time, error)
}

type archiveJobEnqueuer interface {
	Enqueue(context.Context, opjob.Job) (opjob.Job, error)
}

// ArchiveScheduler is the single-domain bounded producer. It rotates late-data
// checks, repairs failed days first, then schedules new eligible UTC days in
// order. It never deletes ClickHouse partitions.
type ArchiveScheduler struct {
	Store          archiveSchedulerStore
	Jobs           archiveJobEnqueuer
	Runner         archiveRunner
	Interval       time.Duration
	LateCheckEvery time.Duration
	MaxPartitions  int
	MaxLateChecks  int
	Logf           func(string, ...any)
}

func (scheduler *ArchiveScheduler) Run(ctx context.Context) {
	if scheduler == nil || scheduler.Store == nil || scheduler.Jobs == nil || scheduler.Runner == nil ||
		scheduler.Interval <= 0 || scheduler.LateCheckEvery <= 0 || scheduler.MaxPartitions < 1 || scheduler.MaxLateChecks < 1 {
		return
	}
	run := func(now time.Time) {
		count, err := scheduler.ScanOnce(ctx, now)
		if scheduler.Logf != nil && (err != nil || count > 0) {
			scheduler.Logf("Flow archive lifecycle scheduled=%d err=%v", count, err)
		}
	}
	run(time.Now())
	ticker := time.NewTicker(scheduler.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			run(now)
		}
	}
}

func (scheduler *ArchiveScheduler) ScanOnce(ctx context.Context, now time.Time) (int, error) {
	if scheduler == nil || scheduler.Store == nil || scheduler.Jobs == nil || scheduler.Runner == nil || now.IsZero() ||
		scheduler.LateCheckEvery <= 0 || scheduler.MaxPartitions < 1 || scheduler.MaxPartitions > 366 || scheduler.MaxLateChecks < 1 || scheduler.MaxLateChecks > 366 {
		return 0, ErrInvalidPolicy
	}
	scheduled := 0
	late, err := scheduler.Store.ListLateCheckCandidates(ctx, now.UTC().Add(-scheduler.LateCheckEvery), scheduler.MaxLateChecks)
	if err != nil {
		return 0, err
	}
	for _, state := range late {
		raw, archive, err := scheduler.Runner.DayStorageCounters(ctx, state.SourceDate)
		if err != nil {
			return scheduled, err
		}
		sourceCounters, archiveCounters := lifecycleCounters(raw), lifecycleCounters(archive)
		if err := scheduler.Store.RecordLateCheck(ctx, state, sourceCounters, archiveCounters, now); err != nil {
			return scheduled, err
		}
		if sourceCounters != archiveCounters && scheduled < scheduler.MaxPartitions {
			if err := scheduler.scheduleRepair(ctx, state); err != nil {
				return scheduled, err
			}
			scheduled++
		}
	}

	if scheduled < scheduler.MaxPartitions {
		repairs, err := scheduler.Store.ListArchiveRepairCandidates(ctx, scheduler.MaxPartitions-scheduled)
		if err != nil {
			return scheduled, err
		}
		for _, state := range repairs {
			if err := scheduler.scheduleRepair(ctx, state); err != nil {
				return scheduled, err
			}
			scheduled++
		}
	}
	if scheduled >= scheduler.MaxPartitions {
		return scheduled, nil
	}

	policy, err := scheduler.Store.GetPublishedPolicy(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return scheduled, nil
	}
	if err != nil {
		return scheduled, err
	}
	policy, err = NormalizePolicy(policy)
	if err != nil || policy.Status != PolicyPublished {
		return scheduled, ErrInvalidPolicy
	}
	remaining := scheduler.MaxPartitions - scheduled
	if policyBudget := int(policy.MaxPartitionsPerRun); remaining > policyBudget {
		remaining = policyBudget
	}
	if remaining <= 0 || !now.UTC().After(policy.BootstrapFrom) {
		return scheduled, nil
	}
	// A day archived under a retired revision is re-archived under the published
	// one: the deletion gate accepts only the published version, so leaving it
	// would strand its raw facts forever (every configuration change publishes a
	// new revision).
	superseded, err := scheduler.Store.ListSupersededPartitions(ctx, policy, remaining)
	if err != nil {
		return scheduled, err
	}
	for _, state := range superseded {
		if err := scheduler.scheduleArchive(ctx, policy, state.SourceDate, 1); err != nil {
			return scheduled, err
		}
		scheduled++
		remaining--
	}
	if remaining <= 0 {
		return scheduled, nil
	}
	boundaryEnd := UTCDate(now).Add(24 * time.Hour)
	next, err := scheduler.Store.ArchiveThrough(ctx, policy.BootstrapFrom, boundaryEnd)
	if err != nil {
		return scheduled, err
	}
	next = UTCDate(next)
	for added := 0; added < remaining; added++ {
		eligibleAt, err := ArchiveEligibleAt(next, policy)
		if err != nil || eligibleAt.After(now.UTC()) {
			if err != nil {
				return scheduled, err
			}
			break
		}
		if state, err := scheduler.Store.GetPartition(ctx, next); err == nil {
			switch state.State {
			case PartitionReconciled, PartitionDeleteEligible, PartitionRawDeleted:
				// Settled days follow a held one; walk past them.
				next = next.Add(24 * time.Hour)
				added--
				continue
			case PartitionFailed:
				if state.LastErrorCode == ArchiveHoldRawIncomplete {
					// A held day is never repaired, so it must not stop newer
					// days; its raw stays until an operator resolves it.
					next = next.Add(24 * time.Hour)
					added--
					continue
				}
			}
			// The continuous boundary stopped on an in-flight/failed day. Its
			// existing or repair job must finish before newer days are exposed.
			break
		} else if !errors.Is(err, sql.ErrNoRows) {
			return scheduled, err
		}
		job, err := NewArchiveOperationJob(policy, next, 1)
		if err != nil {
			return scheduled, err
		}
		queued, err := scheduler.Jobs.Enqueue(ctx, job)
		if err != nil {
			return scheduled, err
		}
		generation, _ := Generation(policy.Version, 1)
		if _, err := scheduler.Store.BeginArchive(ctx, policy, next, generation, queued.ID); err != nil {
			return scheduled, err
		}
		scheduled++
		next = next.Add(24 * time.Hour)
	}
	return scheduled, nil
}

func (scheduler *ArchiveScheduler) scheduleRepair(ctx context.Context, state PartitionState) error {
	if state.RepairAttempt == math.MaxUint32 {
		return ErrTransition
	}
	policy, err := scheduler.Store.GetPolicy(ctx, state.PolicyID)
	if err != nil {
		return err
	}
	attempt := state.RepairAttempt + 1
	if policy.Status != PolicyPublished {
		// Repair a day of a retired revision under the published one (see the
		// superseded scan in ScanOnce) rather than on a version it can never be
		// deleted under.
		published, err := scheduler.Store.GetPublishedPolicy(ctx)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			policy, attempt = published, 1
		}
	}
	return scheduler.scheduleArchive(ctx, policy, state.SourceDate, attempt)
}

func (scheduler *ArchiveScheduler) scheduleArchive(ctx context.Context, policy Policy, day time.Time, attempt uint32) error {
	job, err := NewArchiveOperationJob(policy, day, attempt)
	if err != nil {
		return err
	}
	queued, err := scheduler.Jobs.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	generation, _ := Generation(policy.Version, attempt)
	_, err = scheduler.Store.BeginArchive(ctx, policy, day, generation, queued.ID)
	return err
}

var _ archiveJobEnqueuer = (*opjob.Store)(nil)
var _ archiveRunner = (*flowch.RollupRunner)(nil)
