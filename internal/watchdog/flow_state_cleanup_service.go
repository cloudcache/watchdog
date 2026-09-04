package watchdog

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

type FlowStateCleanupCreateRequest struct {
	TransferID     ID
	Kind           flowcollect.StateCheckpointKind
	IdentityKey    []byte
	IdempotencyKey string
}

type FlowStateCleanupJobController interface {
	Create(context.Context, ID, ID, FlowStateCleanupCreateRequest) (FlowStateCleanupJob, error)
	Get(context.Context, ID, ID) (FlowStateCleanupJob, error)
	Retry(context.Context, ID, ID, uint64, ID) (FlowStateCleanupJob, error)
}

type FlowStateCleanupJobService struct {
	repository FlowStateCleanupControlRepository
	scanner    flowStateCleanupKeyScanner
	now        func() time.Time
	newID      func() (ID, error)
	closeCtx   context.Context
	close      context.CancelFunc
	mu         sync.Mutex
	closing    bool
	wait       sync.WaitGroup
}

func NewFlowStateCleanupJobService(repository FlowStateCleanupControlRepository, scanner flowStateCleanupKeyScanner) (*FlowStateCleanupJobService, error) {
	if repository == nil || scanner == nil {
		return nil, errors.New("flow state-cleanup control repository and scanner are required")
	}
	closeCtx, closeService := context.WithCancel(context.Background())
	return &FlowStateCleanupJobService{
		repository: repository, scanner: scanner, now: time.Now, newID: newFlowStateCleanupJobID,
		closeCtx: closeCtx, close: closeService,
	}, nil
}

func (s *FlowStateCleanupJobService) Create(ctx context.Context, tenantID, actorID ID, req FlowStateCleanupCreateRequest) (FlowStateCleanupJob, error) {
	if s == nil || s.repository == nil || s.scanner == nil || ctx == nil || tenantID == "" || actorID == "" || req.TransferID == "" || len(req.IdentityKey) != sha256.Size || req.IdempotencyKey == "" || len(req.IdempotencyKey) > 128 {
		return FlowStateCleanupJob{}, errors.New("flow state-cleanup request is incomplete")
	}
	requestCtx, done, err := s.beginRequest(ctx)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	defer done()
	ctx = requestCtx
	if req.Kind != flowcollect.StateCheckpointDecoder && req.Kind != flowcollect.StateCheckpointQuality {
		return FlowStateCleanupJob{}, errors.New("flow state-cleanup checkpoint kind is invalid")
	}
	authority, err := s.repository.GetFlowStateCleanupAuthority(ctx, tenantID, req.TransferID)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	if !validFlowStateCleanupAuthority(authority, tenantID, req.TransferID) {
		return FlowStateCleanupJob{}, ErrFlowStateCleanupCheckpointInvalid
	}
	if existing, found, err := s.repository.GetFlowStateCleanupJobByIdempotency(ctx, tenantID, req.IdempotencyKey); err != nil {
		return FlowStateCleanupJob{}, err
	} else if found {
		if !matchesFlowStateCleanupRequest(existing, authority, actorID, req) {
			return FlowStateCleanupJob{}, ErrFlowStateCleanupIdempotencyConflict
		}
		return existing, nil
	}

	kafkaKey, err := flowcollect.StateCleanupKafkaKey(req.Kind, req.IdentityKey, authority.OldOwnershipEpoch)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	observed, err := s.scanner.Scan(ctx, kafkaKey)
	if err != nil {
		return FlowStateCleanupJob{}, fmt.Errorf("scan old flow state-cleanup checkpoint: %w", err)
	}
	if !observed.Present || observed.LastRecord == nil {
		return FlowStateCleanupJob{}, ErrFlowStateCleanupCheckpointMissing
	}
	highWatermark, ok := observed.HighWatermark(observed.LastRecord.Partition)
	if !ok || observed.LastRecord.Offset < 0 || highWatermark <= observed.LastRecord.Offset || observed.CapturedAt.IsZero() {
		return FlowStateCleanupJob{}, ErrFlowStateCleanupCheckpointInvalid
	}
	createdAt := s.now().UTC()
	machine, err := flowcollect.NewStateCleanupJobFromPayload("pending", string(authority.ApprovalID), string(actorID), req.Kind, observed.Value, createdAt)
	if err != nil {
		return FlowStateCleanupJob{}, fmt.Errorf("%w: %v", ErrFlowStateCleanupCheckpointInvalid, err)
	}
	old := machine.Snapshot().Old
	if old.Kind != req.Kind || !bytes.Equal(old.KafkaKey, kafkaKey) || !bytes.Equal(old.IdentityKey, req.IdentityKey) || old.TenantID != string(tenantID) || old.ExporterID != string(authority.ExporterID) || old.CollectorID != string(authority.OldCollectorID) || old.RegistryVersion != authority.OldPlanRevision || old.OwnershipEpoch != authority.OldOwnershipEpoch {
		return FlowStateCleanupJob{}, ErrFlowStateCleanupCheckpointInvalid
	}
	jobID, err := s.newID()
	if err != nil {
		return FlowStateCleanupJob{}, fmt.Errorf("create flow state-cleanup job ID: %w", err)
	}
	machine, err = flowcollect.NewStateCleanupJobFromPayload(string(jobID), string(authority.ApprovalID), string(actorID), req.Kind, observed.Value, createdAt)
	if err != nil {
		return FlowStateCleanupJob{}, fmt.Errorf("%w: %v", ErrFlowStateCleanupCheckpointInvalid, err)
	}
	snapshot := machine.Snapshot()
	requestHash, err := FlowStateCleanupRequestHash(snapshot)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	return s.repository.CreateFlowStateCleanupJob(ctx, FlowStateCleanupJob{
		ID: jobID, TenantID: tenantID, IdempotencyKey: req.IdempotencyKey,
		RequestHash: requestHash, Snapshot: snapshot, CreatedBy: actorID,
		NextAttemptAt: createdAt,
	})
}

func (s *FlowStateCleanupJobService) Get(ctx context.Context, tenantID, jobID ID) (FlowStateCleanupJob, error) {
	if s == nil || s.repository == nil || ctx == nil {
		return FlowStateCleanupJob{}, errors.New("flow state-cleanup service is unavailable")
	}
	requestCtx, done, err := s.beginRequest(ctx)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	defer done()
	return s.repository.GetFlowStateCleanupJob(requestCtx, tenantID, jobID)
}

func (s *FlowStateCleanupJobService) Retry(ctx context.Context, tenantID, jobID ID, expectedRowVersion uint64, actorID ID) (FlowStateCleanupJob, error) {
	if s == nil || s.repository == nil || ctx == nil {
		return FlowStateCleanupJob{}, errors.New("flow state-cleanup service is unavailable")
	}
	requestCtx, done, err := s.beginRequest(ctx)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	defer done()
	return s.repository.RetryFlowStateCleanupJob(requestCtx, tenantID, jobID, expectedRowVersion, actorID)
}

func (s *FlowStateCleanupJobService) beginRequest(ctx context.Context) (context.Context, func(), error) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil, nil, errors.New("flow state-cleanup service is closing")
	}
	s.wait.Add(1)
	s.mu.Unlock()
	requestCtx, cancel := context.WithCancel(ctx)
	stopClose := context.AfterFunc(s.closeCtx, cancel)
	return requestCtx, func() {
		stopClose()
		cancel()
		s.wait.Done()
	}, nil
}

func (s *FlowStateCleanupJobService) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if !s.closing {
		s.closing = true
		s.close()
	}
	s.mu.Unlock()
	s.wait.Wait()
	return nil
}

func validFlowStateCleanupAuthority(authority FlowStateCleanupAuthority, tenantID, transferID ID) bool {
	return authority.TransferID == transferID && authority.TenantID == tenantID && authority.ExporterID != "" && authority.OldCollectorID != "" && authority.OldPlanRevision > 0 && authority.OldOwnershipEpoch > 0 && authority.ApprovalID != ""
}

func matchesFlowStateCleanupRequest(job FlowStateCleanupJob, authority FlowStateCleanupAuthority, actorID ID, req FlowStateCleanupCreateRequest) bool {
	old := job.Snapshot.Old
	expectedKey, err := flowcollect.StateCleanupKafkaKey(req.Kind, req.IdentityKey, authority.OldOwnershipEpoch)
	return err == nil && job.CreatedBy == actorID && job.Snapshot.RequestedBy == string(actorID) && job.Snapshot.ApprovalID == string(authority.ApprovalID) && old.Kind == req.Kind && bytes.Equal(old.IdentityKey, req.IdentityKey) && bytes.Equal(old.KafkaKey, expectedKey) && old.TenantID == string(authority.TenantID) && old.ExporterID == string(authority.ExporterID) && old.CollectorID == string(authority.OldCollectorID) && old.RegistryVersion == authority.OldPlanRevision && old.OwnershipEpoch == authority.OldOwnershipEpoch
}

func newFlowStateCleanupJobID() (ID, error) {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return ID("flowclean_" + hex.EncodeToString(value[:])), nil
}
