package watchdog

import (
	"context"
	"errors"
	"strings"
	"time"
)

const FlowStorageArchiveResolution = time.Hour

const (
	FlowStoragePolicyDraft     = "draft"
	FlowStoragePolicyPublished = "published"
	FlowStoragePolicyRetired   = "retired"
)

const (
	FlowStoragePartitionSealed            = "sealed"
	FlowStoragePartitionDownsampleWritten = "downsample_written"
	FlowStoragePartitionReconciled        = "reconciled"
	FlowStoragePartitionDeleteEligible    = "delete_eligible"
	FlowStoragePartitionRawDeleted        = "raw_deleted"
	FlowStoragePartitionFailed            = "failed"
)

var (
	ErrFlowStorageInvalid         = errors.New("flow storage lifecycle input is invalid")
	ErrFlowStorageVersionConflict = errors.New("flow storage lifecycle row version conflict")
	ErrFlowStorageTransition      = errors.New("flow storage lifecycle transition is invalid")
	ErrFlowStorageRawDeleteLocked = errors.New("raw Flow deletion is locked until durable Kafka coverage reconciliation is enabled")
)

// FlowStoragePolicy is an immutable-on-publish retention/downsample revision.
// Durations are represented as seconds at the API and SQL boundary so the
// stored contract is independent of Go duration encoding.
type FlowStoragePolicy struct {
	ID                          ID        `json:"id"`
	TenantID                    ID        `json:"tenant_id"`
	PolicyVersion               uint64    `json:"policy_version"`
	Status                      string    `json:"status"`
	BootstrapFrom               time.Time `json:"bootstrap_from"`
	RawRetentionSeconds         uint64    `json:"raw_retention_seconds"`
	DownsampleResolutionSeconds uint32    `json:"downsample_resolution_seconds"`
	ArchiveRetentionSeconds     uint64    `json:"archive_retention_seconds"`
	LateArrivalSeconds          uint32    `json:"late_arrival_seconds"`
	DeleteGraceSeconds          uint32    `json:"delete_grace_seconds"`
	MaxPartitionsPerRun         uint32    `json:"max_partitions_per_run"`
	RawDeleteEnabled            bool      `json:"raw_delete_enabled"`
	RowVersion                  uint64    `json:"row_version"`
	CreatedBy                   ID        `json:"created_by,omitempty"`
	PublishedBy                 ID        `json:"published_by,omitempty"`
	RetiredBy                   ID        `json:"retired_by,omitempty"`
	CreatedAt                   time.Time `json:"created_at"`
	PublishedAt                 time.Time `json:"published_at,omitzero"`
	RetiredAt                   time.Time `json:"retired_at,omitzero"`
}

type FlowStoragePartitionCounters struct {
	RecordCount           uint64 `json:"record_count"`
	RawBytes              uint64 `json:"raw_bytes"`
	RawPackets            uint64 `json:"raw_packets"`
	EstimatedBytes        uint64 `json:"estimated_bytes"`
	EstimatedPackets      uint64 `json:"estimated_packets"`
	EstimatedValidRecords uint64 `json:"estimated_valid_records"`
}

type FlowStoragePartitionState struct {
	TenantID         ID                            `json:"tenant_id"`
	SourceDate       time.Time                     `json:"source_date"`
	PolicyID         ID                            `json:"policy_id"`
	PolicyVersion    uint64                        `json:"policy_version"`
	State            string                        `json:"state"`
	Generation       uint64                        `json:"generation"`
	Source           *FlowStoragePartitionCounters `json:"source,omitempty"`
	Archive          *FlowStoragePartitionCounters `json:"archive,omitempty"`
	DownsampleJobID  ID                            `json:"downsample_job_id,omitempty"`
	DeleteJobID      ID                            `json:"delete_job_id,omitempty"`
	DownsampledAt    time.Time                     `json:"downsampled_at,omitzero"`
	ReconciledAt     time.Time                     `json:"reconciled_at,omitzero"`
	DeleteEligibleAt time.Time                     `json:"delete_eligible_at,omitzero"`
	RawDeletedAt     time.Time                     `json:"raw_deleted_at,omitzero"`
	LastErrorCode    string                        `json:"last_error_code,omitempty"`
	LastErrorDetail  string                        `json:"last_error_detail,omitempty"`
	RowVersion       uint64                        `json:"row_version"`
	CreatedAt        time.Time                     `json:"created_at"`
	UpdatedAt        time.Time                     `json:"updated_at"`
}

type FlowStoragePartitionFilter struct {
	State  string
	From   time.Time
	To     time.Time
	Limit  int
	Offset int
}

type FlowStorageLifecycleRepository interface {
	ListFlowStoragePolicies(context.Context, ID) ([]FlowStoragePolicy, error)
	GetFlowStoragePolicy(context.Context, ID, ID) (FlowStoragePolicy, error)
	CreateFlowStoragePolicyDraft(context.Context, FlowStoragePolicy) (FlowStoragePolicy, error)
	UpdateFlowStoragePolicyDraft(context.Context, FlowStoragePolicy, uint64) (FlowStoragePolicy, error)
	DeleteFlowStoragePolicyDraft(context.Context, ID, ID, uint64) error
	PublishFlowStoragePolicy(context.Context, ID, ID, ID, uint64, time.Time) (FlowStoragePolicy, error)
	ListFlowStoragePartitions(context.Context, ID, FlowStoragePartitionFilter) ([]FlowStoragePartitionState, int, error)
}

// FlowStorageArchiveBoundaryRepository exposes only days whose greatest
// attempted archive generation reconciled. Query code uses the returned
// boundary to make archive/raw ranges disjoint; it never guesses from age.
type FlowStorageArchiveBoundaryRepository interface {
	FlowStorageArchiveThrough(context.Context, ID, time.Time, time.Time) (time.Time, error)
}

// FlowStorageJobRepository is the narrow lifecycle surface used by the
// scheduler and leased operation-job handler. It does not expose raw deletion.
type FlowStorageJobRepository interface {
	ListPublishedFlowStoragePolicies(context.Context, ID, int) ([]FlowStoragePolicy, ID, error)
	ListFlowStorageRepairCandidates(context.Context, FlowStoragePolicy, int) ([]FlowStoragePartitionState, error)
	BeginFlowStoragePartition(context.Context, FlowStoragePolicy, time.Time, uint64, ID) (FlowStoragePartitionState, error)
	MarkFlowStoragePartitionDownsampleWritten(context.Context, FlowStoragePolicy, time.Time, uint64, ID, time.Time) (FlowStoragePartitionState, error)
	CompleteFlowStoragePartition(context.Context, FlowStoragePolicy, time.Time, uint64, ID, FlowStoragePartitionCounters, FlowStoragePartitionCounters, time.Time) (FlowStoragePartitionState, error)
}

func normalizeFlowStoragePolicy(policy FlowStoragePolicy) (FlowStoragePolicy, error) {
	policy.Status = strings.TrimSpace(policy.Status)
	if policy.Status == "" {
		policy.Status = FlowStoragePolicyDraft
	}
	if policy.DownsampleResolutionSeconds == 0 {
		policy.DownsampleResolutionSeconds = uint32(FlowStorageArchiveResolution / time.Second)
	}
	policy.BootstrapFrom = utcDate(policy.BootstrapFrom)
	if policy.TenantID == "" || len(policy.TenantID) > 26 || (policy.ID != "" && len(policy.ID) > 26) ||
		policy.Status != FlowStoragePolicyDraft || policy.BootstrapFrom.IsZero() ||
		policy.RawRetentionSeconds < 86400 || policy.RawRetentionSeconds > 315576000 ||
		policy.DownsampleResolutionSeconds != uint32(FlowStorageArchiveResolution/time.Second) ||
		(policy.ArchiveRetentionSeconds != 0 && policy.ArchiveRetentionSeconds <= policy.RawRetentionSeconds) ||
		policy.LateArrivalSeconds > 604800 || policy.DeleteGraceSeconds < 3600 || policy.DeleteGraceSeconds > 2592000 ||
		policy.MaxPartitionsPerRun < 1 || policy.MaxPartitionsPerRun > 366 {
		return FlowStoragePolicy{}, ErrFlowStorageInvalid
	}
	return policy, nil
}

func utcDate(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}
