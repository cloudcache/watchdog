package watchdog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

const (
	FlowRollupJobType        = "flow_rollup"
	FlowRollupPayloadVersion = 1
)

type flowRollupJobPayload struct {
	Resolution flowch.RollupResolution `json:"resolution"`
	BucketUnix int64                   `json:"bucket_unix"`
	Generation uint64                  `json:"generation"`
}

// FlowBucketRollupRunner is the ClickHouse side effect owned by one leased
// operation-job attempt.
type FlowBucketRollupRunner interface {
	Run(context.Context, flowch.RollupRequest) error
}

// FlowRollupJobStore is deliberately narrower than OperationJobRepository:
// the enqueue scheduler writes jobs and reads only its durable job ledger.
type FlowRollupJobStore interface {
	EnqueueOperationJob(context.Context, OperationJob) (OperationJob, error)
	GetOperationJobWatermark(context.Context, ID, string, string) (uint64, error)
	AdvanceOperationJobWatermark(context.Context, ID, string, string, uint64) error
}

type FlowRollupGenerationReader interface {
	LatestGeneration(context.Context, flowch.RollupResolution, time.Time) (uint64, error)
}

type FlowRollupScheduleConfig struct {
	LateArrivalWindow       time.Duration
	BootstrapLookback       time.Duration
	MaxBucketsPerSeriesScan int
	MaxBucketsPerScan       int
}

type FlowRollupScanResult struct {
	Scheduled       int
	SeriesScanned   int
	BootstrapSeries int
	BudgetExhausted bool
	// StoppedEarly is set when the per-scan bucket budget stopped the scan before
	// every listed tenant was iterated, leaving later tenants unprocessed.
	// LastProcessedTenant is the greatest tenant id whose series were all
	// scheduled this scan; the fairness cursor resumes after it.
	StoppedEarly        bool
	LastProcessedTenant ID
}

type FlowRollupTenantSource interface {
	ListFlowRollupTenantIDs(context.Context, ID, int) ([]ID, string, error)
}

type FlowRollupService struct {
	Scheduler         *FlowRollupScheduler
	Tenants           FlowRollupTenantSource
	Interval          time.Duration
	MaxTenantsPerScan int
	Logf              func(string, ...any)
	// Reaper, when set, runs alongside the scheduler to close the silent-gap and
	// late-arrival holes (F1/F2). It shares this service's lifecycle: Run starts
	// it and it stops when ctx is done.
	Reaper *FlowRollupReaper

	tenantCursor ID
}

type FlowRollupScheduler struct {
	store  FlowRollupJobStore
	config FlowRollupScheduleConfig
}

// Keep the production implementations tied to the Flow job contracts. This
// prevents tests from silently growing methods that the hub runtime does not
// actually provide.
var (
	_ FlowRollupJobStore         = (*MySQLStore)(nil)
	_ FlowRollupTenantSource     = (*MySQLStore)(nil)
	_ FlowRollupGenerationReader = (*flowch.RollupRunner)(nil)
)

func NewFlowRollupScheduler(store FlowRollupJobStore, config FlowRollupScheduleConfig) (*FlowRollupScheduler, error) {
	if store == nil {
		return nil, errors.New("flow rollup job store is required")
	}
	if config.LateArrivalWindow < 0 || config.BootstrapLookback <= 0 ||
		config.MaxBucketsPerSeriesScan <= 0 || config.MaxBucketsPerScan <= 0 {
		return nil, errors.New("flow rollup lateness, lookback, and scan budgets are invalid")
	}
	return &FlowRollupScheduler{store: store, config: config}, nil
}

func (s *FlowRollupService) Run(ctx context.Context) {
	if s == nil || s.Scheduler == nil || s.Tenants == nil || s.Interval <= 0 || s.MaxTenantsPerScan <= 0 {
		return
	}
	if s.Reaper != nil {
		go s.Reaper.Run(ctx)
	}
	s.scan(ctx, time.Now())
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.scan(ctx, now)
		}
	}
}

func (s *FlowRollupService) scan(ctx context.Context, now time.Time) {
	result, err := s.ScanOnce(ctx, now)
	if err != nil {
		if s.Logf != nil {
			s.Logf("flow rollup bucket scan failed: %v", err)
		}
		return
	}
	if s.Logf != nil && (result.Scheduled > 0 || result.BudgetExhausted) {
		s.Logf("flow rollup bucket scan scheduled=%d series=%d bootstrap_series=%d budget_exhausted=%t", result.Scheduled, result.SeriesScanned, result.BootstrapSeries, result.BudgetExhausted)
	}
}

// ScanOnce pages a fair, bounded set of active Flow tenants and advances the
// in-memory tenant cursor. Bucket progress itself remains durable in
// operation_jobs, so losing this fairness cursor on restart can repeat scans
// but cannot lose or duplicate a logical execution.
func (s *FlowRollupService) ScanOnce(ctx context.Context, now time.Time) (FlowRollupScanResult, error) {
	if s == nil || s.Scheduler == nil || s.Tenants == nil || s.MaxTenantsPerScan <= 0 {
		return FlowRollupScanResult{}, errors.New("flow rollup service is not initialized")
	}
	tenants, next, err := s.Tenants.ListFlowRollupTenantIDs(ctx, s.tenantCursor, s.MaxTenantsPerScan)
	if err != nil {
		return FlowRollupScanResult{}, err
	}
	if len(tenants) == 0 && s.tenantCursor != "" {
		s.tenantCursor = ""
		tenants, next, err = s.Tenants.ListFlowRollupTenantIDs(ctx, "", s.MaxTenantsPerScan)
		if err != nil {
			return FlowRollupScanResult{}, err
		}
	}
	previousCursor := s.tenantCursor
	result, err := s.Scheduler.ScanClosedBuckets(ctx, tenants, now)
	if err != nil {
		return FlowRollupScanResult{}, err
	}
	// Advance the fairness cursor only over tenants actually processed. When the
	// per-scan bucket budget stops the scan mid-list, resume at the first
	// unprocessed tenant next time instead of skipping the rest until the cursor
	// cycles all the way around — which starved later tenants during catch-up.
	switch {
	case !result.StoppedEarly:
		s.tenantCursor = ID(next)
	case result.LastProcessedTenant != "":
		s.tenantCursor = result.LastProcessedTenant
	default:
		s.tenantCursor = previousCursor
	}
	return result, nil
}

// ScanClosedBuckets advances the durable scheduled watermark for each
// tenant/resolution. The watermark is the greatest v1 bucket represented in
// operation_jobs, regardless of execution status: queued/failed work remains
// visible and the operation-job worker owns its retry lifecycle.
func (s *FlowRollupScheduler) ScanClosedBuckets(ctx context.Context, tenantIDs []ID, now time.Time) (FlowRollupScanResult, error) {
	var result FlowRollupScanResult
	if s == nil || s.store == nil {
		return result, errors.New("flow rollup scheduler is not initialized")
	}
	if now.IsZero() {
		return result, errors.New("flow rollup scan time is required")
	}
	tenants := canonicalTenantIDs(tenantIDs)
	for _, tenantID := range tenants {
		for _, resolution := range []flowch.RollupResolution{flowch.RollupOneMinute, flowch.RollupOneHour} {
			if result.Scheduled >= s.config.MaxBucketsPerScan {
				result.BudgetExhausted = true
				result.StoppedEarly = true
				return result, nil
			}
			result.SeriesScanned++
			eligibleEnd, duration := flowRollupEligibleEnd(now, s.config.LateArrivalWindow, resolution)
			watermark, err := s.store.GetOperationJobWatermark(ctx, tenantID, FlowRollupJobType, flowRollupWatermarkPartition(resolution))
			var next time.Time
			switch {
			case err == nil:
				if watermark > math.MaxInt64 {
					return result, fmt.Errorf("invalid %s rollup watermark for tenant %s", resolution, tenantID)
				}
				next = time.Unix(int64(watermark), 0).UTC().Add(duration)
			case errors.Is(err, sql.ErrNoRows):
				result.BootstrapSeries++
				next = ceilBucket(eligibleEnd.Add(-s.config.BootstrapLookback), duration)
			default:
				return result, fmt.Errorf("read %s rollup watermark for tenant %s: %w", resolution, tenantID, err)
			}
			seriesCount := 0
			for next.Before(eligibleEnd) && seriesCount < s.config.MaxBucketsPerSeriesScan && result.Scheduled < s.config.MaxBucketsPerScan {
				job, err := NewFlowRollupOperationJob(tenantID, resolution, next, 1)
				if err != nil {
					return result, err
				}
				if _, err := s.store.EnqueueOperationJob(ctx, job); err != nil {
					return result, fmt.Errorf("enqueue %s rollup for tenant %s bucket %s: %w", resolution, tenantID, next.Format(time.RFC3339), err)
				}
				if err := s.store.AdvanceOperationJobWatermark(ctx, tenantID, FlowRollupJobType, flowRollupWatermarkPartition(resolution), uint64(next.Unix())); err != nil {
					return result, fmt.Errorf("advance %s rollup watermark for tenant %s bucket %s: %w", resolution, tenantID, next.Format(time.RFC3339), err)
				}
				result.Scheduled++
				seriesCount++
				next = next.Add(duration)
			}
			if next.Before(eligibleEnd) {
				result.BudgetExhausted = true
			}
		}
		result.LastProcessedTenant = tenantID
	}
	return result, nil
}

// EnqueueFlowRollupRepair creates the next generation for a bucket. Two
// concurrent callers observe or create the same generation key and therefore
// converge through operation_jobs' unique idempotency constraint.
func EnqueueFlowRollupRepair(ctx context.Context, store FlowRollupJobStore, generations FlowRollupGenerationReader, tenantID ID, resolution flowch.RollupResolution, bucket time.Time) (OperationJob, error) {
	if store == nil || generations == nil {
		return OperationJob{}, errors.New("flow rollup job store and generation reader are required")
	}
	current, err := generations.LatestGeneration(ctx, resolution, bucket)
	if err != nil {
		return OperationJob{}, err
	}
	if current == 0 {
		return OperationJob{}, errors.New("flow rollup repair requires an existing bucket generation")
	}
	if current == math.MaxUint64 {
		return OperationJob{}, errors.New("flow rollup generation is exhausted")
	}
	job, err := NewFlowRollupOperationJob(tenantID, resolution, bucket, current+1)
	if err != nil {
		return OperationJob{}, err
	}
	return store.EnqueueOperationJob(ctx, job)
}

// NewFlowRollupOperationJob freezes the v1 payload and its execution
// idempotency identity. tenant is already part of operation_jobs' unique key;
// generation is part of this key so a forward repair is a new job.
func NewFlowRollupOperationJob(tenantID ID, resolution flowch.RollupResolution, bucket time.Time, generation uint64) (OperationJob, error) {
	if tenantID == "" || len(tenantID) > 26 {
		return OperationJob{}, errors.New("flow rollup tenant must contain 1..26 characters")
	}
	if bucket.Unix() < 0 {
		return OperationJob{}, errors.New("flow rollup bucket must not predate the Unix epoch")
	}
	generatedAt := bucket.Add(time.Minute).UTC()
	if resolution == flowch.RollupOneHour {
		generatedAt = bucket.Add(time.Hour).UTC()
	}
	request := flowch.RollupRequest{
		Resolution: resolution, Bucket: bucket,
		Generation: generation, GeneratedAt: generatedAt,
	}
	if err := flowch.ValidateRollupRequest(request); err != nil {
		return OperationJob{}, err
	}
	payload, err := EncodeJobPayload(FlowRollupPayloadVersion, flowRollupJobPayload{
		Resolution: resolution, BucketUnix: bucket.Unix(), Generation: generation,
	})
	if err != nil {
		return OperationJob{}, err
	}
	idempotencyKey := flowRollupIdempotencyKey(resolution, bucket, generation)
	digest := sha256.Sum256(append([]byte("watchdog.flow_rollup.job.v1\x00"+string(tenantID)+"\x00"), payload...))
	return OperationJob{
		TenantID: tenantID, JobType: FlowRollupJobType, IdempotencyKey: idempotencyKey,
		RequestHash: hex.EncodeToString(digest[:]), CheckpointJSON: payload,
	}, nil
}

func NewFlowRollupJobHandler(runner FlowBucketRollupRunner) OperationJobHandler {
	return func(ctx context.Context, job OperationJob) (string, error) {
		if runner == nil {
			return "", TerminalJobError(errors.New("flow rollup runner is not initialized"))
		}
		if job.JobType != FlowRollupJobType || job.TenantID == "" || job.CreatedAt.IsZero() {
			return "", TerminalJobError(errors.New("flow rollup job type, tenant, and created_at are required"))
		}
		var payload flowRollupJobPayload
		if err := DecodeJobPayload(job.CheckpointJSON, FlowRollupPayloadVersion, &payload); err != nil {
			return "", err
		}
		request := flowch.RollupRequest{
			Resolution: payload.Resolution,
			Bucket:     time.Unix(payload.BucketUnix, 0).UTC(), Generation: payload.Generation,
			GeneratedAt: job.CreatedAt.UTC(),
		}
		if err := flowch.ValidateRollupRequest(request); err != nil {
			return "", TerminalJobError(err)
		}
		if err := runner.Run(ctx, request); err != nil {
			var permanent *flowch.PermanentError
			if errors.As(err, &permanent) {
				return "", TerminalJobError(err)
			}
			return "", err
		}
		return fmt.Sprintf("clickhouse:%s:%s:%d:g%d", job.TenantID, payload.Resolution, payload.BucketUnix, payload.Generation), nil
	}
}

func (s *MySQLStore) GetOperationJobWatermark(ctx context.Context, tenantID ID, jobType, partitionKey string) (uint64, error) {
	if tenantID == "" || len(tenantID) > 26 || jobType == "" || len(jobType) > 64 || partitionKey == "" || len(partitionKey) > 128 {
		return 0, errors.New("operation job watermark tenant, type, and partition are required")
	}
	var value uint64
	err := s.db.QueryRowContext(ctx, `
		SELECT watermark_value FROM operation_job_watermarks
		WHERE tenant_id = ? AND job_type = ? AND partition_key = ?
	`, tenantID, jobType, partitionKey).Scan(&value)
	return value, err
}

func (s *MySQLStore) AdvanceOperationJobWatermark(ctx context.Context, tenantID ID, jobType, partitionKey string, value uint64) error {
	if tenantID == "" || len(tenantID) > 26 || jobType == "" || len(jobType) > 64 || partitionKey == "" || len(partitionKey) > 128 {
		return errors.New("operation job watermark tenant, type, and partition are required")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO operation_job_watermarks (
			tenant_id, job_type, partition_key, watermark_value
		) VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			watermark_value = GREATEST(watermark_value, VALUES(watermark_value)),
			row_version = row_version + 1
	`, tenantID, jobType, partitionKey, value)
	return err
}

// FlowRollupBucketState summarizes one bucket's flow_rollup jobs across all of
// its generations: the greatest generation attempted, whether any generation
// succeeded, and whether any attempt is still in flight. The reaper uses it to
// decide whether a bucket is covered, still working, or needs re-driving.
type FlowRollupBucketState struct {
	BucketUnix    int64
	MaxGeneration uint64
	Succeeded     bool
	Pending       bool
}

// ListFlowRollupBucketStates reduces the flow_rollup job ledger to one state per
// bucket over [fromBucketUnix, toBucketUnixExclusive), for one tenant and
// resolution. The idempotency key encodes resolution+bucket+generation, so a
// range over it uses the (tenant_id, job_type, idempotency_key) unique index and
// the reaper scans only its bounded recent window. Results are ordered by bucket.
func (s *MySQLStore) ListFlowRollupBucketStates(ctx context.Context, tenantID ID, resolution flowch.RollupResolution, fromBucketUnix, toBucketUnixExclusive int64) ([]FlowRollupBucketState, error) {
	if tenantID == "" || len(tenantID) > 26 {
		return nil, errors.New("flow rollup bucket-state tenant is required")
	}
	if fromBucketUnix < 0 || toBucketUnixExclusive < fromBucketUnix {
		return nil, errors.New("flow rollup bucket-state range is invalid")
	}
	low, err := flowRollupBucketKeyPrefix(resolution, time.Unix(fromBucketUnix, 0).UTC())
	if err != nil {
		return nil, err
	}
	high, err := flowRollupBucketKeyPrefix(resolution, time.Unix(toBucketUnixExclusive, 0).UTC())
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT idempotency_key, status FROM operation_jobs
		WHERE tenant_id = ? AND job_type = ?
			AND idempotency_key >= ? AND idempotency_key < ?
		ORDER BY idempotency_key
	`, tenantID, FlowRollupJobType, low, high)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byBucket := make(map[int64]*FlowRollupBucketState)
	order := make([]int64, 0, 16)
	for rows.Next() {
		var key, status string
		if err := rows.Scan(&key, &status); err != nil {
			return nil, err
		}
		res, bucket, generation, err := parseFlowRollupIdempotencyKey(key)
		if err != nil || res != resolution {
			continue
		}
		bucketUnix := bucket.Unix()
		state, ok := byBucket[bucketUnix]
		if !ok {
			state = &FlowRollupBucketState{BucketUnix: bucketUnix}
			byBucket[bucketUnix] = state
			order = append(order, bucketUnix)
		}
		if generation > state.MaxGeneration {
			state.MaxGeneration = generation
		}
		// Classify by terminal-ness: succeeded covers the bucket; failed/canceled
		// are terminal non-successes; everything else (queued/running/paused/
		// validating/cancel_requested) is still in flight, so the reaper waits.
		switch status {
		case OperationJobStatusSucceeded:
			state.Succeeded = true
		case OperationJobStatusFailed, OperationJobStatusCanceled:
		default:
			state.Pending = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	states := make([]FlowRollupBucketState, 0, len(order))
	for _, bucketUnix := range order {
		states = append(states, *byBucket[bucketUnix])
	}
	return states, nil
}

func (s *MySQLStore) ListFlowRollupTenantIDs(ctx context.Context, after ID, limit int) ([]ID, string, error) {
	if limit <= 0 || limit > 10_000 {
		return nil, "", errors.New("flow rollup tenant scan limit must be 1..10000")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT a.tenant_id
		FROM collector_agents a
		INNER JOIN tenants t ON t.id = a.tenant_id
		WHERE a.module_key = 'flow' AND a.agent_type = 'flow_collect'
			AND a.deleted_at IS NULL AND t.status = 'active' AND a.tenant_id > ?
		ORDER BY a.tenant_id LIMIT ?
	`, after, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	tenants := make([]ID, 0, limit+1)
	for rows.Next() {
		var tenantID ID
		if err := rows.Scan(&tenantID); err != nil {
			return nil, "", err
		}
		tenants = append(tenants, tenantID)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(tenants) > limit {
		tenants = tenants[:limit]
		next = string(tenants[len(tenants)-1])
	}
	return tenants, next, nil
}

func flowRollupEligibleEnd(now time.Time, lateWindow time.Duration, resolution flowch.RollupResolution) (time.Time, time.Duration) {
	duration := time.Minute
	if resolution == flowch.RollupOneHour {
		duration = time.Hour
	}
	return now.UTC().Add(-lateWindow).Truncate(duration), duration
}

func ceilBucket(value time.Time, duration time.Duration) time.Time {
	value = value.UTC()
	floor := value.Truncate(duration)
	if floor.Equal(value) {
		return floor
	}
	return floor.Add(duration)
}

func canonicalTenantIDs(values []ID) []ID {
	seen := make(map[ID]struct{}, len(values))
	out := make([]ID, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func flowRollupKeyPrefix(resolution flowch.RollupResolution) (string, error) {
	if resolution != flowch.RollupOneMinute && resolution != flowch.RollupOneHour {
		return "", fmt.Errorf("unsupported rollup resolution %q", resolution)
	}
	return "flow_rollup:v1:" + string(resolution) + ":", nil
}

func flowRollupWatermarkPartition(resolution flowch.RollupResolution) string {
	return "v1:" + string(resolution)
}

// flowRollupCompletedPartition names the reaper's completion watermark: the
// greatest bucket up to which every bucket has a successful (or reaper-abandoned)
// generation. It is distinct from the scheduler's enqueue watermark, so the two
// advance independently.
func flowRollupCompletedPartition(resolution flowch.RollupResolution) string {
	return "v1:done:" + string(resolution)
}

func flowRollupBucketKeyPrefix(resolution flowch.RollupResolution, bucket time.Time) (string, error) {
	prefix, err := flowRollupKeyPrefix(resolution)
	if err != nil {
		return "", err
	}
	if bucket.Unix() < 0 {
		return "", errors.New("flow rollup bucket must not predate the Unix epoch")
	}
	return fmt.Sprintf("%s%020d:g", prefix, bucket.Unix()), nil
}

func flowRollupIdempotencyKey(resolution flowch.RollupResolution, bucket time.Time, generation uint64) string {
	prefix, _ := flowRollupBucketKeyPrefix(resolution, bucket)
	return fmt.Sprintf("%s%020d", prefix, generation)
}

func parseFlowRollupIdempotencyKey(key string) (flowch.RollupResolution, time.Time, uint64, error) {
	parts := strings.Split(key, ":")
	if len(parts) != 5 || parts[0] != "flow_rollup" || parts[1] != "v1" || len(parts[3]) != 20 || len(parts[4]) != 21 || !strings.HasPrefix(parts[4], "g") {
		return "", time.Time{}, 0, fmt.Errorf("invalid flow rollup idempotency key %q", key)
	}
	resolution := flowch.RollupResolution(parts[2])
	if _, err := flowRollupKeyPrefix(resolution); err != nil {
		return "", time.Time{}, 0, err
	}
	bucketUnix, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || bucketUnix < 0 {
		return "", time.Time{}, 0, fmt.Errorf("invalid flow rollup bucket in key %q", key)
	}
	generation, err := strconv.ParseUint(strings.TrimPrefix(parts[4], "g"), 10, 64)
	if err != nil || generation == 0 {
		return "", time.Time{}, 0, fmt.Errorf("invalid flow rollup generation in key %q", key)
	}
	return resolution, time.Unix(bucketUnix, 0).UTC(), generation, nil
}
