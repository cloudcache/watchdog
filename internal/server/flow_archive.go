package server

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowlifecycle"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/opjob"
)

const (
	flowArchiveScanInterval = 5 * time.Minute
	flowLateCheckInterval   = 6 * time.Hour
	flowArchiveScanBudget   = 7
)

// startFlowArchive runs the installation-wide cold lifecycle on the shared
// operation_jobs engine. Retention values come only from the published MySQL
// policy; these constants bound scheduler work and never define retention.
func (s *Server) startFlowArchive() error {
	if s.clickHouseBatch == nil {
		return nil
	}
	if s.db == nil || s.jobs == nil {
		return errors.New("Flow archive management dependencies are not initialized")
	}
	runner, err := flowch.NewRollupRunner(s.clickHouseBatch)
	if err != nil {
		return err
	}
	// Report requests read coverage markers through s.flowRollup on the
	// interactive pool, so a long rollup or archive job holding the batch
	// connection cannot stall them. Background work below keeps the batch runner.
	reader, err := flowch.NewRollupRunner(s.clickHouse)
	if err != nil {
		return err
	}
	s.flowRollup = reader
	store := s.flowLifecycle
	if store == nil {
		store = flowlifecycle.NewStore(s.db)
	}
	s.flowLifecycle = store
	s.flowDeleteEvidence = runner
	s.flowArchiveDeleteEvidence = runner
	workerContext, cancel := context.WithCancel(context.Background())
	s.flowArchiveCancel = cancel
	worker := &opjob.Worker{
		Repo: s.jobs, JobType: flowlifecycle.ArchiveJobType, Owner: "watchdog-server/flow-archive",
		Handler: flowlifecycle.NewArchiveHandler(store, runner), Logf: log.Printf,
	}
	deleteWorker := &opjob.Worker{
		Repo: s.jobs, JobType: flowlifecycle.RawDeleteJobType, Owner: "watchdog-server/flow-raw-delete",
		Handler: flowlifecycle.NewRawDeleteHandler(store, runner), Logf: log.Printf,
		OnTerminalFailure: func(job opjob.Job, code, detail string) {
			failureContext, cancelFailure := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelFailure()
			if err := store.MarkRawDeleteTerminalFailure(failureContext, job.ID, code, detail, time.Now()); err != nil {
				log.Printf("watchdog Flow raw deletion %s terminal receipt: %v", job.ID, err)
			}
		},
	}
	archiveDeleteWorker := &opjob.Worker{
		Repo: s.jobs, JobType: flowlifecycle.ArchiveDeleteJobType, Owner: "watchdog-server/flow-archive-delete",
		Handler: flowlifecycle.NewArchiveDeleteHandler(store, runner), Logf: log.Printf,
		OnTerminalFailure: func(job opjob.Job, code, detail string) {
			failureContext, cancelFailure := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelFailure()
			if err := store.MarkArchiveDeleteTerminalFailure(failureContext, job.ID, code, detail, time.Now()); err != nil {
				log.Printf("watchdog Flow archive deletion %s terminal receipt: %v", job.ID, err)
			}
		},
	}
	scheduler := &flowlifecycle.ArchiveScheduler{
		Store: store, Jobs: s.jobs, Runner: runner,
		Interval: flowArchiveScanInterval, LateCheckEvery: flowLateCheckInterval,
		MaxPartitions: flowArchiveScanBudget, MaxLateChecks: flowArchiveScanBudget, Logf: log.Printf,
	}
	go worker.Run(workerContext)
	go deleteWorker.Run(workerContext)
	go archiveDeleteWorker.Run(workerContext)
	go scheduler.Run(workerContext)
	if s.cfg.Flow.Lifecycle.Enabled {
		go s.runFlowLifecycleConfig(workerContext, store, runner)
	}
	if s.cfg.Flow.HotRollup.Enabled {
		hot := &flowHotRollupScheduler{config: s.cfg.Flow.HotRollup, runner: runner, logf: log.Printf}
		go hot.Run(workerContext)
	}
	return nil
}

// flowExactRawTail is how much of a query's recent tail may be read from raw
// facts to fill marker gaps exactly. The hot scheduler publishes a tier only
// after its hour seals, so a healthy query always has a raw tail shorter than
// this; an older gap is reported as missing coverage instead.
const flowExactRawTail = 3 * time.Hour

// flowCoverage summarizes the readable generation markers of one tier over a
// query range.
type flowCoverage struct {
	firstGap, lastCovered time.Time
	readable, expected    int
}

func summarizeFlowCoverage(markers []flowch.RollupMarker, from, to time.Time, step time.Duration, minimumGeneration uint64) flowCoverage {
	readable := make(map[int64]struct{}, len(markers))
	for _, marker := range markers {
		if marker.Generation != 0 && (marker.Generation >= lifecycleGenerationFloor || marker.Generation >= minimumGeneration) {
			readable[marker.Bucket.UTC().Unix()] = struct{}{}
		}
	}
	from, to = from.UTC(), to.UTC()
	coverage := flowCoverage{firstGap: to, lastCovered: from}
	for bucket := from; bucket.Before(to); bucket = bucket.Add(step) {
		coverage.expected++
		if _, ok := readable[bucket.Unix()]; ok {
			coverage.readable++
			coverage.lastCovered = bucket.Add(step)
		} else if coverage.firstGap.Equal(to) {
			coverage.firstGap = bucket
		}
	}
	return coverage
}

// boundary returns where a hybrid query switches from generation-marked
// aggregates to raw facts. Before it, the archive side reads every bucket with
// a readable marker and returns nothing for a gap, which the query's
// completeness ratio reports; after it, raw facts are authoritative. Coverage
// need not be a contiguous prefix: a gap that ends before the last readable
// bucket (a permanent hole, or a range starting before the retained data) would
// otherwise push every later bucket, including healthy aggregates, onto raw
// facts and exhaust the read budget. A gap within flowExactRawTail of to is
// still read exactly from raw.
func (coverage flowCoverage) boundary(to time.Time) time.Time {
	if !coverage.lastCovered.After(coverage.firstGap) || to.UTC().Sub(coverage.firstGap) <= flowExactRawTail {
		return coverage.firstGap
	}
	return coverage.lastCovered
}

func (s *Server) flowCoverage(ctx context.Context, resolution flowch.RollupResolution, from, to time.Time) (flowCoverage, error) {
	markers, err := s.flowRollup.GenerationMarkers(ctx, resolution, from, to)
	if err != nil {
		return flowCoverage{}, err
	}
	step := time.Hour
	switch resolution {
	case flowch.RollupOneMinute:
		step = time.Minute
	case flowch.RollupOneDay:
		step = 24 * time.Hour
	}
	return summarizeFlowCoverage(markers, from, to, step, s.flowReadableGenerationFloor()), nil
}

// flowQueryResolution selects the rollup tier for a query bucket. Only the cold
// archive writes 1d, so a daily bucket is served from the hourly tier (with the
// same display interval) until 1d covers the whole range, instead of sending
// the range to raw facts.
func (s *Server) flowQueryResolution(ctx context.Context, bucket flowquery.Bucket, from, to time.Time) (flowquery.Bucket, flowch.RollupResolution, error) {
	switch bucket {
	case flowquery.BucketOneMinute:
		return bucket, flowch.RollupOneMinute, nil
	case flowquery.BucketOneDay:
		daily, err := s.flowCoverage(ctx, flowch.RollupOneDay, from, to)
		if err != nil {
			return bucket, "", err
		}
		if daily.readable == daily.expected {
			return bucket, flowch.RollupOneDay, nil
		}
		return flowquery.BucketOneHour, flowch.RollupOneHour, nil
	default:
		return bucket, flowch.RollupOneHour, nil
	}
}

// applyFlowStorageBoundary splits a query between the generation-marked
// aggregate of the selected physical tier and raw facts (see
// flowCoverage.boundary). A lifecycle state alone is not sufficient: older
// reconciled days may predate a newly introduced derived tier and therefore
// have no rows in that table.
func (s *Server) applyFlowStorageBoundary(ctx context.Context, request *flowquery.Request) error {
	if request == nil || (s.flowLifecycle == nil && s.flowRollup == nil) {
		return nil
	}
	request.StorageV2 = true
	request.ArchiveThrough = request.From.UTC()
	request.MinimumGeneration = s.flowReadableGenerationFloor()
	// The remote-port archive is deliberately top-N bounded. An explicit port
	// lookup must stay exact, so it uses raw facts instead of treating an absent
	// long-tail aggregate row as zero.
	if request.Dimension == flowquery.DimensionRemotePort && len(request.Filters.DimensionValues) > 0 {
		return nil
	}
	boundary := request.From.UTC()
	if s.flowRollup != nil {
		bucket, resolution, err := s.flowQueryResolution(ctx, request.Bucket, request.From, request.To)
		if err != nil {
			return err
		}
		request.Bucket = bucket
		coverage, err := s.flowCoverage(ctx, resolution, request.From, request.To)
		if err != nil {
			return err
		}
		boundary = coverage.boundary(request.To)
	} else if s.flowLifecycle != nil && request.Bucket == flowquery.BucketOneHour {
		var err error
		boundary, err = s.flowLifecycle.ArchiveThrough(ctx, request.From, request.To)
		if err != nil {
			return err
		}
	}
	request.ArchiveThrough = boundary
	return nil
}

func (s *Server) applyFlowOverseasStorageBoundary(ctx context.Context, request *flowquery.OverseasRequest) error {
	if request == nil || (s.flowLifecycle == nil && s.flowRollup == nil) {
		return nil
	}
	request.StorageV2 = true
	request.ArchiveThrough = request.From.UTC()
	request.MinimumGeneration = s.flowReadableGenerationFloor()
	boundary := request.From.UTC()
	if s.flowRollup != nil {
		bucket, resolution, err := s.flowQueryResolution(ctx, request.Bucket, request.From, request.To)
		if err != nil {
			return err
		}
		request.Bucket = bucket
		coverage, err := s.flowCoverage(ctx, resolution, request.From, request.To)
		if err != nil {
			return err
		}
		boundary = coverage.boundary(request.To)
	} else if s.flowLifecycle != nil && request.Bucket == flowquery.BucketOneHour {
		var err error
		boundary, err = s.flowLifecycle.ArchiveThrough(ctx, request.From, request.To)
		if err != nil {
			return err
		}
	}
	request.ArchiveThrough = boundary
	return nil
}

func (s *Server) flowReadableGenerationFloor() uint64 {
	if s == nil || !s.cfg.Flow.HotRollup.Enabled {
		return 0
	}
	return s.cfg.Flow.HotRollup.MinimumGeneration
}
