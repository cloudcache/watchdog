package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

type memoryFlowRollupStore struct {
	jobs            map[string]OperationJob
	watermarks      map[string]uint64
	enqueueErr      error
	advanceFailures int
}

func newMemoryFlowRollupStore() *memoryFlowRollupStore {
	return &memoryFlowRollupStore{jobs: map[string]OperationJob{}, watermarks: map[string]uint64{}}
}

func (s *memoryFlowRollupStore) EnqueueOperationJob(_ context.Context, job OperationJob) (OperationJob, error) {
	if s.enqueueErr != nil {
		return OperationJob{}, s.enqueueErr
	}
	identity := string(job.TenantID) + "\x00" + job.JobType + "\x00" + job.IdempotencyKey
	if existing, ok := s.jobs[identity]; ok {
		if existing.RequestHash != job.RequestHash {
			return OperationJob{}, ErrOperationJobHashMismatch
		}
		return existing, nil
	}
	job.ID = ID("job-" + job.IdempotencyKey)
	job.Status = OperationJobStatusQueued
	job.CreatedAt = time.Date(2026, 9, 5, 12, 30, 0, 0, time.UTC)
	s.jobs[identity] = job
	return job, nil
}

func (s *memoryFlowRollupStore) GetOperationJobWatermark(_ context.Context, tenantID ID, jobType, partitionKey string) (uint64, error) {
	value, ok := s.watermarks[string(tenantID)+"\x00"+jobType+"\x00"+partitionKey]
	if !ok {
		return 0, sql.ErrNoRows
	}
	return value, nil
}

func (s *memoryFlowRollupStore) AdvanceOperationJobWatermark(_ context.Context, tenantID ID, jobType, partitionKey string, value uint64) error {
	if s.advanceFailures > 0 {
		s.advanceFailures--
		return errors.New("watermark unavailable")
	}
	key := string(tenantID) + "\x00" + jobType + "\x00" + partitionKey
	if value > s.watermarks[key] {
		s.watermarks[key] = value
	}
	return nil
}

type recordingFlowRollupRunner struct {
	requests         []flowch.RollupRequest
	err              error
	latestGeneration uint64
}

type cancelAwareFlowRollupRunner struct{ canceled chan struct{} }

func (r *cancelAwareFlowRollupRunner) Run(ctx context.Context, _ flowch.RollupRequest) error {
	<-ctx.Done()
	close(r.canceled)
	return ctx.Err()
}

type recordingFlowRollupTenantSource struct {
	tenants []ID
	afters  []ID
}

func (s *recordingFlowRollupTenantSource) ListFlowRollupTenantIDs(_ context.Context, after ID, limit int) ([]ID, string, error) {
	s.afters = append(s.afters, after)
	start := 0
	for start < len(s.tenants) && s.tenants[start] <= after {
		start++
	}
	end := start + limit
	if end > len(s.tenants) {
		end = len(s.tenants)
	}
	items := append([]ID(nil), s.tenants[start:end]...)
	next := ""
	if end < len(s.tenants) && len(items) > 0 {
		next = string(items[len(items)-1])
	}
	return items, next, nil
}

func (r *recordingFlowRollupRunner) Run(_ context.Context, request flowch.RollupRequest) error {
	r.requests = append(r.requests, request)
	return r.err
}

func (r *recordingFlowRollupRunner) LatestGeneration(_ context.Context, _ string, _ flowch.RollupResolution, _ time.Time) (uint64, error) {
	return r.latestGeneration, r.err
}

func TestFlowRollupOperationJobIdentityIncludesGeneration(t *testing.T) {
	bucket := time.Date(2026, 9, 5, 12, 34, 0, 0, time.UTC)
	first, err := NewFlowRollupOperationJob("tenant-a", flowch.RollupOneMinute, bucket, 7)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := NewFlowRollupOperationJob("tenant-a", flowch.RollupOneMinute, bucket, 7)
	if err != nil {
		t.Fatal(err)
	}
	repair, err := NewFlowRollupOperationJob("tenant-a", flowch.RollupOneMinute, bucket, 8)
	if err != nil {
		t.Fatal(err)
	}
	if first.IdempotencyKey != replay.IdempotencyKey || first.RequestHash != replay.RequestHash || string(first.CheckpointJSON) != string(replay.CheckpointJSON) {
		t.Fatal("same logical execution did not produce a stable job identity")
	}
	if first.IdempotencyKey == repair.IdempotencyKey || first.RequestHash == repair.RequestHash {
		t.Fatal("repair generation reused the previous execution identity")
	}
	resolution, parsedBucket, generation, err := parseFlowRollupIdempotencyKey(first.IdempotencyKey)
	if err != nil || resolution != flowch.RollupOneMinute || !parsedBucket.Equal(bucket) || generation != 7 {
		t.Fatalf("parsed identity = %s %s g%d, err=%v", resolution, parsedBucket, generation, err)
	}
	var envelope struct {
		SchemaVersion int                    `json:"schema_version"`
		Payload       map[string]interface{} `json:"payload"`
	}
	if err := json.Unmarshal(first.CheckpointJSON, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.SchemaVersion != FlowRollupPayloadVersion {
		t.Fatalf("payload schema version=%d", envelope.SchemaVersion)
	}
	if _, exists := envelope.Payload["tenant_id"]; exists {
		t.Fatal("payload duplicated the authoritative operation-job tenant")
	}
	if _, exists := envelope.Payload["generated_at"]; exists {
		t.Fatal("payload contains retry-unstable generated_at")
	}
}

func TestFlowRollupOperationJobRejectsTimezoneAndAlignmentErrors(t *testing.T) {
	aligned := time.Date(2026, 9, 5, 12, 34, 0, 0, time.UTC)
	for _, test := range []struct {
		name       string
		resolution flowch.RollupResolution
		bucket     time.Time
		generation uint64
	}{
		{name: "unknown resolution", resolution: "5m", bucket: aligned, generation: 1},
		{name: "unaligned minute", resolution: flowch.RollupOneMinute, bucket: aligned.Add(time.Second), generation: 1},
		{name: "unaligned hour", resolution: flowch.RollupOneHour, bucket: aligned, generation: 1},
		{name: "non UTC", resolution: flowch.RollupOneMinute, bucket: aligned.In(time.FixedZone("UTC+8", 8*60*60)), generation: 1},
		{name: "zero generation", resolution: flowch.RollupOneMinute, bucket: aligned, generation: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewFlowRollupOperationJob("tenant-a", test.resolution, test.bucket, test.generation); err == nil {
				t.Fatal("invalid rollup job was accepted")
			}
		})
	}
}

func TestFlowRollupHandlerUsesJobTenantAndStableCreationTime(t *testing.T) {
	runner := &recordingFlowRollupRunner{}
	bucket := time.Date(2026, 9, 5, 12, 34, 0, 0, time.UTC)
	job, err := NewFlowRollupOperationJob("tenant-a", flowch.RollupOneMinute, bucket, 3)
	if err != nil {
		t.Fatal(err)
	}
	job.CreatedAt = time.Date(2026, 9, 5, 20, 40, 1, 123_000_000, time.FixedZone("database", 8*60*60))
	result, err := NewFlowRollupJobHandler(runner)(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if result != "clickhouse:tenant-a:1m:1788611640:g3" || len(runner.requests) != 1 {
		t.Fatalf("result=%q requests=%+v", result, runner.requests)
	}
	request := runner.requests[0]
	if request.TenantID != "tenant-a" || !request.Bucket.Equal(bucket) || request.Generation != 3 || request.GeneratedAt.Location() != time.UTC {
		t.Fatalf("request=%+v", request)
	}
}

func TestFlowRollupHandlerClassifiesPayloadAndClickHouseErrors(t *testing.T) {
	bucket := time.Date(2026, 9, 5, 12, 34, 0, 0, time.UTC)
	job, err := NewFlowRollupOperationJob("tenant-a", flowch.RollupOneMinute, bucket, 1)
	if err != nil {
		t.Fatal(err)
	}
	job.CreatedAt = bucket.Add(time.Minute)

	badPayload, err := EncodeJobPayload(2, flowRollupJobPayload{Resolution: flowch.RollupOneMinute, BucketUnix: bucket.Unix(), Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	badJob := job
	badJob.CheckpointJSON = badPayload
	if _, err := NewFlowRollupJobHandler(&recordingFlowRollupRunner{})(context.Background(), badJob); err == nil || !IsTerminalJobError(err) {
		t.Fatalf("old/future payload must fail terminally, got %v", err)
	}
	badJob.CheckpointJSON = json.RawMessage(`{"resolution":"1m","bucket_unix":1788611640,"generation":1}`)
	if _, err := NewFlowRollupJobHandler(&recordingFlowRollupRunner{})(context.Background(), badJob); err == nil || !IsTerminalJobError(err) {
		t.Fatalf("unversioned legacy payload must fail terminally, got %v", err)
	}

	permanent := &recordingFlowRollupRunner{err: flowch.Permanent(errors.New("schema mismatch"))}
	if _, err := NewFlowRollupJobHandler(permanent)(context.Background(), job); err == nil || !IsTerminalJobError(err) {
		t.Fatalf("permanent ClickHouse error must fail terminally, got %v", err)
	}
	retryable := errors.New("connection reset")
	if _, err := NewFlowRollupJobHandler(&recordingFlowRollupRunner{err: retryable})(context.Background(), job); !errors.Is(err, retryable) || IsTerminalJobError(err) {
		t.Fatalf("temporary ClickHouse error classification=%v", err)
	}
}

func TestFlowRollupHandlerPropagatesCancellation(t *testing.T) {
	bucket := time.Date(2026, 9, 5, 12, 34, 0, 0, time.UTC)
	job, err := NewFlowRollupOperationJob("tenant-a", flowch.RollupOneMinute, bucket, 1)
	if err != nil {
		t.Fatal(err)
	}
	job.CreatedAt = bucket.Add(time.Minute)
	runner := &cancelAwareFlowRollupRunner{canceled: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := NewFlowRollupJobHandler(runner)(ctx, job)
		done <- err
	}()
	cancel()
	select {
	case <-runner.canceled:
	case <-time.After(time.Second):
		t.Fatal("rollup handler did not propagate cancellation")
	}
	if err := <-done; !errors.Is(err, context.Canceled) || IsTerminalJobError(err) {
		t.Fatalf("cancellation classification=%v", err)
	}
}

func TestFlowRollupSchedulerUsesClosedBucketsDurableWatermarkAndBudgets(t *testing.T) {
	store := newMemoryFlowRollupStore()
	scheduler, err := NewFlowRollupScheduler(store, FlowRollupScheduleConfig{
		LateArrivalWindow: 2 * time.Minute, BootstrapLookback: 2 * time.Hour,
		MaxBucketsPerSeriesScan: 3, MaxBucketsPerScan: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 5, 20, 7, 45, 0, time.FixedZone("UTC+8", 8*60*60))
	first, err := scheduler.ScanClosedBuckets(context.Background(), []ID{"tenant-b", "tenant-a", "tenant-a", ""}, now)
	if err != nil {
		t.Fatal(err)
	}
	if first.Scheduled != 10 || first.SeriesScanned != 4 || first.BootstrapSeries != 4 || !first.BudgetExhausted {
		t.Fatalf("first scan=%+v", first)
	}
	keys := flowRollupKeys(store)
	for _, forbidden := range []string{
		flowRollupIdempotencyKey(flowch.RollupOneMinute, time.Date(2026, 9, 5, 12, 5, 0, 0, time.UTC), 1),
		flowRollupIdempotencyKey(flowch.RollupOneHour, time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC), 1),
	} {
		for _, key := range keys {
			if key == forbidden {
				t.Fatalf("open/late bucket was scheduled: %s", key)
			}
		}
	}

	second, err := scheduler.ScanClosedBuckets(context.Background(), []ID{"tenant-a"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if second.Scheduled != 3 || second.BootstrapSeries != 0 {
		t.Fatalf("watermark did not resume the next bounded range: %+v", second)
	}
	if len(store.jobs) != first.Scheduled+second.Scheduled {
		t.Fatalf("job count=%d", len(store.jobs))
	}
}

func TestFlowRollupSchedulerAdvancesOnlyAfterEnqueueAndRecoversCrashGap(t *testing.T) {
	store := newMemoryFlowRollupStore()
	store.advanceFailures = 1
	scheduler, err := NewFlowRollupScheduler(store, FlowRollupScheduleConfig{
		LateArrivalWindow: time.Minute, BootstrapLookback: time.Minute,
		MaxBucketsPerSeriesScan: 1, MaxBucketsPerScan: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 5, 12, 10, 0, 0, time.UTC)
	if result, err := scheduler.ScanClosedBuckets(context.Background(), []ID{"tenant-a"}, now); err == nil || result.Scheduled != 0 {
		t.Fatalf("failed watermark advance result=%+v err=%v", result, err)
	}
	if len(store.jobs) != 1 || len(store.watermarks) != 0 {
		t.Fatalf("job must be durable before watermark: jobs=%d watermarks=%v", len(store.jobs), store.watermarks)
	}
	result, err := scheduler.ScanClosedBuckets(context.Background(), []ID{"tenant-a"}, now)
	if err != nil || result.Scheduled != 1 || len(store.jobs) != 1 || len(store.watermarks) != 1 {
		t.Fatalf("crash-gap recovery result=%+v jobs=%d watermarks=%v err=%v", result, len(store.jobs), store.watermarks, err)
	}
}

func TestFlowRollupRepairAdvancesGenerationAndRequiresBaseGeneration(t *testing.T) {
	store := newMemoryFlowRollupStore()
	bucket := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	if _, err := EnqueueFlowRollupRepair(context.Background(), store, &recordingFlowRollupRunner{}, "tenant-a", flowch.RollupOneHour, bucket); err == nil {
		t.Fatal("repair without an original generation was accepted")
	}
	initial, err := NewFlowRollupOperationJob("tenant-a", flowch.RollupOneHour, bucket, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueOperationJob(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	repair, err := EnqueueFlowRollupRepair(context.Background(), store, &recordingFlowRollupRunner{latestGeneration: 1}, "tenant-a", flowch.RollupOneHour, bucket)
	if err != nil {
		t.Fatal(err)
	}
	_, _, generation, err := parseFlowRollupIdempotencyKey(repair.IdempotencyKey)
	if err != nil || generation != 2 {
		t.Fatalf("repair generation=%d err=%v", generation, err)
	}
}

func TestFlowRollupServicePagesTenantsAndWrapsFairly(t *testing.T) {
	store := newMemoryFlowRollupStore()
	scheduler, err := NewFlowRollupScheduler(store, FlowRollupScheduleConfig{
		LateArrivalWindow: time.Minute, BootstrapLookback: time.Minute,
		MaxBucketsPerSeriesScan: 2, MaxBucketsPerScan: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	tenants := &recordingFlowRollupTenantSource{tenants: []ID{"tenant-a", "tenant-b", "tenant-c"}}
	service := &FlowRollupService{Scheduler: scheduler, Tenants: tenants, MaxTenantsPerScan: 2}
	now := time.Date(2026, 9, 5, 12, 10, 0, 0, time.UTC)
	if _, err := service.ScanOnce(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ScanOnce(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ScanOnce(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if len(tenants.afters) != 3 || tenants.afters[0] != "" || tenants.afters[1] != "tenant-b" || tenants.afters[2] != "" {
		t.Fatalf("tenant cursors=%v", tenants.afters)
	}
}

func TestFlowRollupServiceResumesAtUnprocessedTenantUnderBudget(t *testing.T) {
	store := newMemoryFlowRollupStore()
	// A per-scan budget of two buckets is exactly one tenant's 1m+1h series, so
	// each scan finishes one tenant and stops at the next.
	scheduler, err := NewFlowRollupScheduler(store, FlowRollupScheduleConfig{
		LateArrivalWindow: time.Minute, BootstrapLookback: 3 * time.Hour,
		MaxBucketsPerSeriesScan: 1, MaxBucketsPerScan: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	tenants := &recordingFlowRollupTenantSource{tenants: []ID{"tenant-a", "tenant-b", "tenant-c"}}
	service := &FlowRollupService{Scheduler: scheduler, Tenants: tenants, MaxTenantsPerScan: 3}
	now := time.Date(2026, 9, 5, 12, 10, 0, 0, time.UTC)

	first, err := service.ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if !first.StoppedEarly || first.LastProcessedTenant != "tenant-a" {
		t.Fatalf("scan 1 result = %+v, want StoppedEarly at tenant-a", first)
	}
	if _, err := service.ScanOnce(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ScanOnce(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	// The resume cursors must walk a -> b, not restart at "" each scan. Before the
	// fix the full-page cursor advanced to "" every scan, so every scan restarted
	// at tenant-a and tenant-b/c starved.
	if len(tenants.afters) != 3 || tenants.afters[0] != "" || tenants.afters[1] != "tenant-a" || tenants.afters[2] != "tenant-b" {
		t.Fatalf("resume cursors=%v, want [\"\" tenant-a tenant-b]", tenants.afters)
	}
	// All three tenants were scheduled (2 series each), so none starved.
	if scheduled := len(flowRollupKeys(store)); scheduled != 6 {
		t.Fatalf("scheduled jobs=%d, want 6 (2 per tenant, none starved)", scheduled)
	}
}

func TestMySQLFlowRollupLedgerAndRepair(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	bucket := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	job, err := NewFlowRollupOperationJob(tenant, flowch.RollupOneHour, bucket, 1)
	if err != nil {
		t.Fatal(err)
	}
	job, err = store.EnqueueOperationJob(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceOperationJobWatermark(context.Background(), tenant, FlowRollupJobType, flowRollupWatermarkPartition(flowch.RollupOneHour), uint64(bucket.Unix())); err != nil {
		t.Fatal(err)
	}
	if latest, err := store.GetOperationJobWatermark(context.Background(), tenant, FlowRollupJobType, flowRollupWatermarkPartition(flowch.RollupOneHour)); err != nil || latest != uint64(bucket.Unix()) {
		t.Fatalf("latest bucket=%d err=%v", latest, err)
	}
	repair, err := EnqueueFlowRollupRepair(context.Background(), store, &recordingFlowRollupRunner{latestGeneration: 1}, tenant, flowch.RollupOneHour, bucket)
	if err != nil {
		t.Fatal(err)
	}
	worker := &OperationJobWorker{
		Repo: store, JobType: FlowRollupJobType, Owner: "flow-rollup-test",
		PollInterval: 10 * time.Millisecond, LeaseFor: time.Second,
		Handler: NewFlowRollupJobHandler(&recordingFlowRollupRunner{}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go worker.Run(ctx)
	waitForOperationJob(t, store, tenant, job.ID, OperationJobStatusSucceeded)
	waitForOperationJob(t, store, tenant, repair.ID, OperationJobStatusSucceeded)
}

func flowRollupKeys(store *memoryFlowRollupStore) []string {
	keys := make([]string, 0, len(store.jobs))
	for _, job := range store.jobs {
		keys = append(keys, job.IdempotencyKey)
	}
	sort.Strings(keys)
	return keys
}
