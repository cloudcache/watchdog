package watchdog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

const (
	FlowStorageDownsampleJobType        = "flow_storage_downsample"
	FlowStorageDownsamplePayloadVersion = 1
)

type flowStorageDownsamplePayload struct {
	SourceDate    string `json:"source_date"`
	PolicyID      ID     `json:"policy_id"`
	PolicyVersion uint64 `json:"policy_version"`
	RepairAttempt uint32 `json:"repair_attempt"`
	Generation    uint64 `json:"generation"`
}

type FlowStorageDayRunner interface {
	Run(context.Context, flowch.RollupRequest) error
	DayStorageCounters(context.Context, time.Time) (flowch.StorageCounters, flowch.StorageCounters, error)
}

type FlowStorageDownsampleStore interface {
	FlowStorageJobRepository
	GetFlowStoragePolicy(context.Context, ID, ID) (FlowStoragePolicy, error)
}

func NewFlowStorageDownsampleOperationJob(policy FlowStoragePolicy, sourceDate time.Time, repairAttempt uint32) (OperationJob, error) {
	day := utcDate(sourceDate)
	generation, generationErr := flowStorageGeneration(policy.PolicyVersion, repairAttempt)
	if policy.ID == "" || policy.TenantID == "" || policy.PolicyVersion == 0 ||
		(policy.Status != FlowStoragePolicyPublished && policy.Status != FlowStoragePolicyRetired) || day.IsZero() || generationErr != nil {
		return OperationJob{}, ErrFlowStorageInvalid
	}
	payload, err := EncodeJobPayload(FlowStorageDownsamplePayloadVersion, flowStorageDownsamplePayload{
		SourceDate: day.Format("2006-01-02"), PolicyID: policy.ID, PolicyVersion: policy.PolicyVersion,
		RepairAttempt: repairAttempt, Generation: generation,
	})
	if err != nil {
		return OperationJob{}, err
	}
	key := fmt.Sprintf("v1:%s:p%d:a%d", day.Format("20060102"), policy.PolicyVersion, repairAttempt)
	digest := sha256.Sum256(append([]byte("watchdog.flow_storage_downsample.v1\x00"+string(policy.TenantID)+"\x00"), payload...))
	return OperationJob{TenantID: policy.TenantID, JobType: FlowStorageDownsampleJobType, IdempotencyKey: key,
		RequestHash: hex.EncodeToString(digest[:]), CheckpointJSON: payload}, nil
}

// flowStorageGeneration makes lifecycle generations monotonic across policy
// revisions without reading ClickHouse during enqueue. The high 32 bits are
// the immutable policy revision and the low 32 bits are its repair attempt.
// Migration preflight rejects legacy aggregate generations in this namespace.
func flowStorageGeneration(policyVersion uint64, repairAttempt uint32) (uint64, error) {
	if policyVersion == 0 || policyVersion > uint64(^uint32(0)) || repairAttempt == 0 {
		return 0, ErrFlowStorageInvalid
	}
	return policyVersion<<32 | uint64(repairAttempt), nil
}

func NewFlowStorageDownsampleJobHandler(store FlowStorageDownsampleStore, runner FlowStorageDayRunner) OperationJobHandler {
	return func(ctx context.Context, job OperationJob) (string, error) {
		if store == nil || runner == nil {
			return "", TerminalJobError(errors.New("Flow storage downsample dependencies are not initialized"))
		}
		if job.JobType != FlowStorageDownsampleJobType || job.TenantID == "" || job.ID == "" || job.CreatedAt.IsZero() {
			return "", TerminalJobError(errors.New("Flow storage downsample job identity is incomplete"))
		}
		var payload flowStorageDownsamplePayload
		if err := DecodeJobPayload(job.CheckpointJSON, FlowStorageDownsamplePayloadVersion, &payload); err != nil {
			return "", err
		}
		day, err := time.Parse("2006-01-02", payload.SourceDate)
		expectedGeneration, generationErr := flowStorageGeneration(payload.PolicyVersion, payload.RepairAttempt)
		if err != nil || payload.PolicyID == "" || payload.PolicyVersion == 0 || generationErr != nil || payload.Generation != expectedGeneration {
			return "", TerminalJobError(ErrFlowStorageInvalid)
		}
		policy, err := store.GetFlowStoragePolicy(ctx, job.TenantID, payload.PolicyID)
		if err != nil {
			return "", err
		}
		if policy.PolicyVersion != payload.PolicyVersion ||
			(policy.Status != FlowStoragePolicyPublished && policy.Status != FlowStoragePolicyRetired) {
			return "", TerminalJobError(ErrFlowStorageTransition)
		}
		state, err := store.BeginFlowStoragePartition(ctx, policy, day, payload.Generation, job.ID)
		if err != nil {
			return "", err
		}
		if state.State == FlowStoragePartitionReconciled && state.Generation == payload.Generation {
			return flowStorageDownsampleResultRef(job.TenantID, day, payload.PolicyVersion, payload.Generation), nil
		}
		generatedAt := job.CreatedAt.UTC()
		if generatedAt.Before(day.Add(24 * time.Hour)) {
			return "", TerminalJobError(errors.New("Flow storage downsample job was created before the UTC day closed"))
		}
		for hour := 0; hour < 24; hour++ {
			bucket := day.Add(time.Duration(hour) * time.Hour)
			if err := runner.Run(ctx, flowch.RollupRequest{Resolution: flowch.RollupOneHour,
				Bucket: bucket, Generation: payload.Generation, GeneratedAt: generatedAt}); err != nil {
				var permanent *flowch.PermanentError
				if errors.As(err, &permanent) {
					return "", TerminalJobError(err)
				}
				return "", err
			}
		}
		if _, err := store.MarkFlowStoragePartitionDownsampleWritten(ctx, policy, day, payload.Generation, job.ID, time.Now()); err != nil {
			return "", err
		}
		raw, archive, err := runner.DayStorageCounters(ctx, day)
		if err != nil {
			return "", err
		}
		_, err = store.CompleteFlowStoragePartition(ctx, policy, day, payload.Generation, job.ID,
			flowStorageCounters(raw), flowStorageCounters(archive), time.Now())
		if errors.Is(err, ErrFlowStorageTransition) && raw != archive {
			return "", TerminalJobError(fmt.Errorf("Flow storage UTC-day conservation failed: raw=%+v archive=%+v", raw, archive))
		}
		if err != nil {
			return "", err
		}
		return flowStorageDownsampleResultRef(job.TenantID, day, payload.PolicyVersion, payload.Generation), nil
	}
}

func flowStorageCounters(value flowch.StorageCounters) FlowStoragePartitionCounters {
	return FlowStoragePartitionCounters{RecordCount: value.RecordCount, RawBytes: value.RawBytes, RawPackets: value.RawPackets,
		EstimatedBytes: value.EstimatedBytes, EstimatedPackets: value.EstimatedPackets, EstimatedValidRecords: value.EstimatedValidRecords}
}

func flowStorageDownsampleResultRef(tenantID ID, day time.Time, policyVersion, generation uint64) string {
	return fmt.Sprintf("clickhouse:%s:1h:%s:p%d:g%d", tenantID, day.Format("2006-01-02"), policyVersion, generation)
}

type FlowStorageSchedulerStore interface {
	FlowRollupJobStore
	ListPublishedFlowStoragePolicies(context.Context, ID, int) ([]FlowStoragePolicy, ID, error)
	ListFlowStorageRepairCandidates(context.Context, FlowStoragePolicy, int) ([]FlowStoragePartitionState, error)
	BeginFlowStoragePartition(context.Context, FlowStoragePolicy, time.Time, uint64, ID) (FlowStoragePartitionState, error)
}

type FlowStorageLifecycleService struct {
	Store                FlowStorageSchedulerStore
	Interval             time.Duration
	MaxPoliciesPerScan   int
	MaxPartitionsPerScan int
	Logf                 func(string, ...any)
	afterTenant          ID
}

func (service *FlowStorageLifecycleService) Run(ctx context.Context) {
	if service == nil || service.Store == nil || service.Interval <= 0 || service.MaxPoliciesPerScan < 1 || service.MaxPartitionsPerScan < 1 {
		return
	}
	service.scan(ctx, time.Now())
	ticker := time.NewTicker(service.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			service.scan(ctx, now)
		}
	}
}

func (service *FlowStorageLifecycleService) scan(ctx context.Context, now time.Time) {
	count, err := service.ScanOnce(ctx, now)
	if service.Logf != nil && (err != nil || count > 0) {
		service.Logf("flow storage lifecycle scheduled=%d err=%v", count, err)
	}
}

func (service *FlowStorageLifecycleService) ScanOnce(ctx context.Context, now time.Time) (int, error) {
	if service == nil || service.Store == nil || service.MaxPoliciesPerScan < 1 || service.MaxPartitionsPerScan < 1 || now.IsZero() {
		return 0, ErrFlowStorageInvalid
	}
	policies, next, err := service.Store.ListPublishedFlowStoragePolicies(ctx, service.afterTenant, service.MaxPoliciesPerScan)
	if err != nil {
		return 0, err
	}
	if len(policies) == 0 && service.afterTenant != "" {
		service.afterTenant = ""
		policies, next, err = service.Store.ListPublishedFlowStoragePolicies(ctx, "", service.MaxPoliciesPerScan)
		if err != nil {
			return 0, err
		}
	}
	scheduled := 0
	for _, policy := range policies {
		budget := int(policy.MaxPartitionsPerRun)
		if remaining := service.MaxPartitionsPerScan - scheduled; budget > remaining {
			budget = remaining
		}
		if budget <= 0 {
			break
		}
		count, err := scheduleFlowStoragePolicy(ctx, service.Store, policy, now, budget)
		if err != nil {
			return scheduled, err
		}
		scheduled += count
		service.afterTenant = policy.TenantID
	}
	if scheduled < service.MaxPartitionsPerScan {
		service.afterTenant = next
	}
	return scheduled, nil
}

func scheduleFlowStoragePolicy(ctx context.Context, store FlowStorageSchedulerStore, policy FlowStoragePolicy, now time.Time, budget int) (int, error) {
	if policy.Status != FlowStoragePolicyPublished || budget < 1 {
		return 0, ErrFlowStorageInvalid
	}
	repairs, err := store.ListFlowStorageRepairCandidates(ctx, policy, budget)
	if err != nil {
		return 0, err
	}
	scheduled := 0
	for _, state := range repairs {
		if state.Generation>>32 != policy.PolicyVersion || uint32(state.Generation) == ^uint32(0) {
			return scheduled, ErrFlowStorageTransition
		}
		repairAttempt := uint32(state.Generation) + 1
		generation, err := flowStorageGeneration(policy.PolicyVersion, repairAttempt)
		if err != nil {
			return scheduled, err
		}
		job, err := NewFlowStorageDownsampleOperationJob(policy, state.SourceDate, repairAttempt)
		if err != nil {
			return scheduled, err
		}
		queued, err := store.EnqueueOperationJob(ctx, job)
		if err != nil {
			return scheduled, err
		}
		if _, err := store.BeginFlowStoragePartition(ctx, policy, state.SourceDate, generation, queued.ID); err != nil {
			return scheduled, err
		}
		scheduled++
	}
	if scheduled == budget {
		return scheduled, nil
	}

	partitionKey := fmt.Sprintf("policy:%s:v%d", policy.ID, policy.PolicyVersion)
	watermark, err := store.GetOperationJobWatermark(ctx, policy.TenantID, FlowStorageDownsampleJobType, partitionKey)
	next := policy.BootstrapFrom
	switch {
	case err == nil:
		next = time.Unix(int64(watermark), 0).UTC().Add(24 * time.Hour)
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return 0, err
	}
	agingWindow := time.Duration(policy.RawRetentionSeconds) * time.Second
	if lateWindow := time.Duration(policy.LateArrivalSeconds) * time.Second; lateWindow > agingWindow {
		agingWindow = lateWindow
	}
	eligibleExclusive := utcDate(now.UTC().Add(-agingWindow))
	for next.Before(eligibleExclusive) && scheduled < budget {
		generation, err := flowStorageGeneration(policy.PolicyVersion, 1)
		if err != nil {
			return scheduled, err
		}
		job, err := NewFlowStorageDownsampleOperationJob(policy, next, 1)
		if err != nil {
			return scheduled, err
		}
		queued, err := store.EnqueueOperationJob(ctx, job)
		if err != nil {
			return scheduled, err
		}
		if _, err := store.BeginFlowStoragePartition(ctx, policy, next, generation, queued.ID); err != nil {
			return scheduled, err
		}
		if err := store.AdvanceOperationJobWatermark(ctx, policy.TenantID, FlowStorageDownsampleJobType, partitionKey, uint64(next.Unix())); err != nil {
			return scheduled, err
		}
		scheduled++
		next = next.Add(24 * time.Hour)
	}
	return scheduled, nil
}
