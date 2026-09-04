package watchdog

import (
	"context"
	"errors"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

const FlowStateCleanupJobType = "flow_state_cleanup"

var (
	ErrFlowStateCleanupConflict            = errors.New("flow state-cleanup job version or lease conflict")
	ErrFlowStateCleanupIdempotencyConflict = errors.New("flow state-cleanup idempotency key was reused with a different request")
	ErrFlowStateCleanupRetryNotAllowed     = errors.New("flow state-cleanup job cannot be retried")
	ErrFlowStateCleanupCheckpointMissing   = errors.New("flow state-cleanup checkpoint was not observed")
	ErrFlowStateCleanupCheckpointInvalid   = errors.New("flow state-cleanup checkpoint is invalid")
)

type OperationJobStatus string

const (
	OperationJobQueued    OperationJobStatus = "queued"
	OperationJobRunning   OperationJobStatus = "running"
	OperationJobSucceeded OperationJobStatus = "succeeded"
	OperationJobFailed    OperationJobStatus = "failed"
	OperationJobCanceled  OperationJobStatus = "canceled"
)

type FlowStateCleanupJob struct {
	ID              ID
	TenantID        ID
	Status          OperationJobStatus
	IdempotencyKey  string
	RequestHash     string
	Snapshot        flowcollect.StateCleanupSnapshot
	LeaseOwner      string
	LeaseToken      string
	LeaseExpiresAt  time.Time
	NextAttemptAt   time.Time
	AttemptCount    uint32
	LastErrorCode   string
	LastErrorDetail string
	RowVersion      uint64
	CreatedBy       ID
	CreatedAt       time.Time
	StartedAt       time.Time
	HeartbeatAt     time.Time
	FinishedAt      time.Time
	UpdatedAt       time.Time
}

type FlowStateCleanupRepository interface {
	CreateFlowStateCleanupJob(context.Context, FlowStateCleanupJob) (FlowStateCleanupJob, error)
	GetFlowStateCleanupJob(context.Context, ID, ID) (FlowStateCleanupJob, error)
	ClaimFlowStateCleanupJob(context.Context, string, string, time.Duration) (FlowStateCleanupJob, bool, error)
	RenewFlowStateCleanupLease(context.Context, ID, string, time.Duration) error
	SaveFlowStateCleanupCheckpoint(context.Context, ID, string, uint64, flowcollect.StateCleanupSnapshot, time.Time) (FlowStateCleanupJob, error)
	RequeueFlowStateCleanupJob(context.Context, ID, string, uint64, string, string, time.Time) (FlowStateCleanupJob, error)
	FailFlowStateCleanupJob(context.Context, ID, string, uint64, string, string) (FlowStateCleanupJob, error)
}

type FlowStateCleanupAuthority struct {
	TransferID        ID
	TenantID          ID
	ExporterID        ID
	OldCollectorID    ID
	OldPlanRevision   uint64
	OldOwnershipEpoch uint64
	ApprovalID        ID
}

type FlowStateCleanupControlRepository interface {
	CreateFlowStateCleanupJob(context.Context, FlowStateCleanupJob) (FlowStateCleanupJob, error)
	GetFlowStateCleanupJob(context.Context, ID, ID) (FlowStateCleanupJob, error)
	GetFlowStateCleanupJobByIdempotency(context.Context, ID, string) (FlowStateCleanupJob, bool, error)
	GetFlowStateCleanupAuthority(context.Context, ID, ID) (FlowStateCleanupAuthority, error)
	RetryFlowStateCleanupJob(context.Context, ID, ID, uint64, ID) (FlowStateCleanupJob, error)
}
