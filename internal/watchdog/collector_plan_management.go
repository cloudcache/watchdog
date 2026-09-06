package watchdog

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"time"
)

var ErrCollectorPlanInvalidRequest = errors.New("collector plan request is invalid")

// CollectorPlanCreateRequest is the trusted service input for either a new
// spec or a clone. Identity, version, hashes, signatures and lifecycle status
// are deliberately absent: the repository derives them under its row lock.
type CollectorPlanCreateRequest struct {
	TenantID          ID
	CollectorID       ID
	ActorID           ID
	PlanSchemaVersion uint16
	SpecJSON          json.RawMessage
	FromConfigVersion uint64
	NotBefore         time.Time
	ExpiresAt         time.Time
}

type CollectorPlanPageFilter struct {
	Limit         int
	BeforeVersion uint64
}

type CollectorPlanManagementController interface {
	CreateCollectorPlanRevision(context.Context, CollectorPlanCreateRequest) (CollectorPlanRevision, error)
	ListCollectorPlanRevisions(context.Context, ID, ID, CollectorPlanPageFilter) ([]CollectorPlanRevision, string, error)
	ActivateCollectorPlanRevision(context.Context, CollectorPlanActivation) (CollectorPlanRevision, error)
}

type CollectorPlanManagementService struct {
	repository CollectorPlanRepository
	signer     CollectorPlanSigner
}

func NewCollectorPlanManagementService(repository CollectorPlanRepository, signer CollectorPlanSigner) (*CollectorPlanManagementService, error) {
	if repository == nil {
		return nil, errors.New("collector plan repository is required")
	}
	return &CollectorPlanManagementService{repository: repository, signer: signer}, nil
}

func (s *CollectorPlanManagementService) CreateCollectorPlanRevision(ctx context.Context, request CollectorPlanCreateRequest) (CollectorPlanRevision, error) {
	if s == nil || s.repository == nil || s.signer == nil {
		return CollectorPlanRevision{}, ErrCollectorPlanSigningKeyUnavailable
	}
	if err := validateCollectorPlanCreateRequest(request, time.Now().UTC()); err != nil {
		return CollectorPlanRevision{}, err
	}
	return s.repository.CreateNextCollectorPlanRevision(ctx, request, s.signer)
}

func (s *CollectorPlanManagementService) ListCollectorPlanRevisions(ctx context.Context, tenantID, collectorID ID, filter CollectorPlanPageFilter) ([]CollectorPlanRevision, string, error) {
	if s == nil || s.repository == nil {
		return nil, "", errors.New("collector plan repository is unavailable")
	}
	return s.repository.ListCollectorPlanRevisions(ctx, tenantID, collectorID, filter)
}

func (s *CollectorPlanManagementService) ActivateCollectorPlanRevision(ctx context.Context, activation CollectorPlanActivation) (CollectorPlanRevision, error) {
	if s == nil || s.repository == nil {
		return CollectorPlanRevision{}, errors.New("collector plan repository is unavailable")
	}
	return s.repository.ActivateCollectorPlanRevision(ctx, activation)
}

func validateCollectorPlanCreateRequest(request CollectorPlanCreateRequest, now time.Time) error {
	if request.TenantID == "" || len(request.TenantID) > 26 || request.CollectorID == "" || len(request.CollectorID) > 26 || request.ActorID == "" || len(request.ActorID) > 26 {
		return ErrCollectorPlanInvalidRequest
	}
	hasSpec := len(request.SpecJSON) != 0
	hasClone := request.FromConfigVersion != 0
	if hasSpec == hasClone || request.ExpiresAt.IsZero() || !now.Before(request.ExpiresAt) {
		return ErrCollectorPlanInvalidRequest
	}
	if hasSpec && request.PlanSchemaVersion == 0 {
		return ErrCollectorPlanInvalidRequest
	}
	if hasClone && request.PlanSchemaVersion != 0 {
		return ErrCollectorPlanInvalidRequest
	}
	if request.ExpiresAt.Nanosecond()%int(time.Millisecond) != 0 || (!request.NotBefore.IsZero() && request.NotBefore.Nanosecond()%int(time.Millisecond) != 0) || (!request.NotBefore.IsZero() && !request.NotBefore.Before(request.ExpiresAt)) {
		return ErrCollectorPlanInvalidRequest
	}
	if hasSpec {
		if _, _, err := CanonicalCollectorPlanJSON(request.SpecJSON); err != nil {
			return ErrCollectorPlanInvalidRequest
		}
	}
	return nil
}

func encodeCollectorPlanCursor(version uint64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatUint(version, 10)))
}

func decodeCollectorPlanCursor(cursor string) (uint64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, ErrCollectorPlanInvalidRequest
	}
	version, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil || version == 0 {
		return 0, ErrCollectorPlanInvalidRequest
	}
	return version, nil
}

var _ CollectorPlanManagementController = (*CollectorPlanManagementService)(nil)
