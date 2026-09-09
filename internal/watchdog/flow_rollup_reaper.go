package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

// flowRollupReaperStore is the durable surface the reaper needs: the job ledger
// state per bucket, both watermarks, and the enqueue path. MySQLStore satisfies
// it; the narrow interface keeps the reaper unit-testable without a database.
type flowRollupReaperStore interface {
	ListFlowRollupBucketStates(ctx context.Context, tenantID ID, resolution flowch.RollupResolution, fromBucketUnix, toBucketUnixExclusive int64) ([]FlowRollupBucketState, error)
	GetOperationJobWatermark(ctx context.Context, tenantID ID, jobType, partitionKey string) (uint64, error)
	AdvanceOperationJobWatermark(ctx context.Context, tenantID ID, jobType, partitionKey string, value uint64) error
	EnqueueOperationJob(ctx context.Context, job OperationJob) (OperationJob, error)
}

// flowRollupReaperRunner is the ClickHouse-backed surface: late-data detection
// plus the reaper's own metric counters. *flowch.RollupRunner satisfies it.
type flowRollupReaperRunner interface {
	BucketNeedsRepair(ctx context.Context, resolution flowch.RollupResolution, bucket time.Time) (bool, error)
	RecordReaperRepair(reason string)
	RecordPermanentGap()
}

type FlowRollupReaperConfig struct {
	Interval          time.Duration
	MaxTenantsPerScan int
	// MaxBucketsPerScan bounds the buckets examined per tenant/resolution per
	// cycle, so a stuck frontier or a cold-start backlog is chipped away a bounded
	// window at a time instead of scanning all history.
	MaxBucketsPerScan int
	// RetryCap is the greatest generation the reaper will drive a bucket to before
	// abandoning it as a counted permanent gap. Generation 1 is the scheduler's
	// initial roll, so the reaper adds RetryCap-1 retries.
	RetryCap uint64
	// ReconcileWindow limits late-data reconciliation to buckets closed within it,
	// bounding ClickHouse load. Zero disables reconciliation (F2 off).
	ReconcileWindow time.Duration
}

// FlowRollupReaper closes the two holes the enqueue-time watermark leaves: a
// bucket whose jobs all failed becomes a permanent silent gap (F1), and late
// base data after a bucket's roll is never recomputed (F2). It tracks a durable
// completion watermark (the greatest bucket up to which every bucket is either
// succeeded or a counted permanent gap) and, over a bounded window above it,
// re-enqueues the next generation for failed/missing buckets and for succeeded
// buckets whose base row count no longer matches their aggregate.
type FlowRollupReaper struct {
	Store   flowRollupReaperStore
	Tenants FlowRollupTenantSource
	Runner  flowRollupReaperRunner
	Config  FlowRollupReaperConfig
	Logf    func(string, ...any)
	now     func() time.Time

	tenantCursor ID
}

func (r *FlowRollupReaper) Run(ctx context.Context) {
	if !r.ready() {
		return
	}
	r.reap(ctx, r.clock())
	ticker := time.NewTicker(r.Config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.reap(ctx, now)
		}
	}
}

func (r *FlowRollupReaper) ready() bool {
	return r != nil && r.Store != nil && r.Tenants != nil && r.Runner != nil &&
		r.Config.Interval > 0 && r.Config.MaxTenantsPerScan > 0 && r.Config.MaxBucketsPerScan > 0 && r.Config.RetryCap > 0
}

func (r *FlowRollupReaper) clock() time.Time {
	if r.now != nil {
		return r.now().UTC()
	}
	return time.Now().UTC()
}

func (r *FlowRollupReaper) logf(format string, args ...any) {
	if r.Logf != nil {
		r.Logf(format, args...)
	}
}

// reap pages one bounded set of active tenants and advances the fairness cursor.
// Progress is durable in the watermarks, so losing the cursor on restart can
// repeat a scan but cannot lose or duplicate a logical repair (enqueue is
// idempotent on the generation key).
func (r *FlowRollupReaper) reap(ctx context.Context, now time.Time) {
	tenants, next, err := r.Tenants.ListFlowRollupTenantIDs(ctx, r.tenantCursor, r.Config.MaxTenantsPerScan)
	if err != nil {
		r.logf("flow rollup reaper tenant scan failed: %v", err)
		return
	}
	if len(tenants) == 0 {
		r.tenantCursor = ""
		return
	}
	for _, tenantID := range canonicalTenantIDs(tenants) {
		for _, resolution := range []flowch.RollupResolution{flowch.RollupOneMinute, flowch.RollupOneHour} {
			if err := r.reapSeries(ctx, tenantID, resolution, now); err != nil {
				r.logf("flow rollup reaper %s tenant %s failed: %v", resolution, tenantID, err)
			}
		}
	}
	r.tenantCursor = ID(next)
}

// reapSeries advances one tenant/resolution completion watermark over a bounded
// window above it, re-driving failed/missing buckets and reconciling recently
// closed successes for late data. The frontier advances through a contiguous run
// of succeeded (or abandoned) buckets; the first pending/failed/missing bucket
// freezes it, so a hole is never silently skipped — only a bucket that has
// exhausted RetryCap at the frontier is abandoned, counted, and stepped past.
func (r *FlowRollupReaper) reapSeries(ctx context.Context, tenantID ID, resolution flowch.RollupResolution, now time.Time) error {
	duration := time.Minute
	if resolution == flowch.RollupOneHour {
		duration = time.Hour
	}
	durSec := int64(duration / time.Second)

	scheduledWM, err := r.Store.GetOperationJobWatermark(ctx, tenantID, FlowRollupJobType, flowRollupWatermarkPartition(resolution))
	if errors.Is(err, sql.ErrNoRows) {
		return nil // nothing scheduled yet
	}
	if err != nil {
		return err
	}
	scheduled := int64(scheduledWM)

	completedWM, err := r.Store.GetOperationJobWatermark(ctx, tenantID, FlowRollupJobType, flowRollupCompletedPartition(resolution))
	var completed int64
	switch {
	case err == nil:
		completed = int64(completedWM)
	case errors.Is(err, sql.ErrNoRows):
		completed = 0
	default:
		return err
	}

	span := int64(r.Config.MaxBucketsPerScan-1) * durSec
	var lo int64
	if completed == 0 {
		// Cold start: bound the backlog to a recent window instead of all history.
		if lo = scheduled - span; lo < 0 {
			lo = 0
		}
	} else {
		lo = completed + durSec
	}
	if lo > scheduled {
		return nil // frontier has caught up to the scheduler
	}
	hi := lo + span
	if hi > scheduled {
		hi = scheduled
	}

	states, err := r.Store.ListFlowRollupBucketStates(ctx, tenantID, resolution, lo, hi+durSec)
	if err != nil {
		return err
	}
	stateByBucket := make(map[int64]FlowRollupBucketState, len(states))
	for _, state := range states {
		stateByBucket[state.BucketUnix] = state
	}

	// The frontier is "every bucket up to here is resolved". In steady state it
	// starts at the completion watermark. On a cold start the scan begins at a
	// bounded window above bucket 0, so baseline the frontier at the window bottom
	// rather than claiming the unscanned buckets below it as done.
	frontier := completed
	if completed == 0 && lo > 0 {
		frontier = lo - durSec
	}
	advancing := true
	reconcileFloor := int64(-1)
	if r.Config.ReconcileWindow > 0 {
		reconcileFloor = now.Add(-r.Config.ReconcileWindow).Unix()
	}

	for bucketUnix := lo; bucketUnix <= hi; bucketUnix += durSec {
		state, known := stateByBucket[bucketUnix]
		bucket := time.Unix(bucketUnix, 0).UTC()
		switch {
		case known && state.Succeeded:
			if reconcileFloor >= 0 && bucketUnix >= reconcileFloor && state.MaxGeneration < r.Config.RetryCap {
				r.reconcile(ctx, tenantID, resolution, bucket, state.MaxGeneration)
			}
			if advancing {
				frontier = bucketUnix
			}
		case known && state.Pending:
			advancing = false // in flight; wait for it, freeze the frontier
		case known && state.MaxGeneration >= r.Config.RetryCap:
			// All attempts terminal and the retry cap is exhausted: a permanent gap.
			if advancing {
				r.Runner.RecordPermanentGap()
				frontier = bucketUnix // count the hole and step past it to stay bounded
			} else {
				advancing = false
			}
		default:
			// A missing bucket (gap) or one whose attempts all failed under the cap.
			nextGeneration := uint64(1)
			reason := flowch.RollupRepairGap
			if known {
				nextGeneration = state.MaxGeneration + 1
				reason = flowch.RollupRepairFailed
			}
			if err := r.enqueue(ctx, tenantID, resolution, bucket, nextGeneration); err != nil {
				return err
			}
			r.Runner.RecordReaperRepair(reason)
			advancing = false // hole here; freeze the frontier
		}
	}

	if frontier > completed {
		return r.Store.AdvanceOperationJobWatermark(ctx, tenantID, FlowRollupJobType, flowRollupCompletedPartition(resolution), uint64(frontier))
	}
	return nil
}

// reconcile re-enqueues the next generation when a succeeded bucket's base row
// count no longer matches its aggregate (late arrivals, F2). Reconciliation
// errors are logged, not fatal: they must not stall the frontier for the series.
func (r *FlowRollupReaper) reconcile(ctx context.Context, tenantID ID, resolution flowch.RollupResolution, bucket time.Time, currentGeneration uint64) {
	needs, err := r.Runner.BucketNeedsRepair(ctx, resolution, bucket)
	if err != nil {
		r.logf("flow rollup reaper reconcile %s tenant %s bucket %d failed: %v", resolution, tenantID, bucket.Unix(), err)
		return
	}
	if !needs {
		return
	}
	if err := r.enqueue(ctx, tenantID, resolution, bucket, currentGeneration+1); err != nil {
		r.logf("flow rollup reaper repair enqueue %s tenant %s bucket %d failed: %v", resolution, tenantID, bucket.Unix(), err)
		return
	}
	r.Runner.RecordReaperRepair(flowch.RollupRepairLate)
}

func (r *FlowRollupReaper) enqueue(ctx context.Context, tenantID ID, resolution flowch.RollupResolution, bucket time.Time, generation uint64) error {
	job, err := NewFlowRollupOperationJob(tenantID, resolution, bucket, generation)
	if err != nil {
		return err
	}
	_, err = r.Store.EnqueueOperationJob(ctx, job)
	return err
}
