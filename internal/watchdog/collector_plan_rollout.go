package watchdog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
)

var (
	ErrCollectorPlanRolloutInvalid  = errors.New("collector plan rollout is invalid")
	ErrCollectorPlanRolloutConflict = errors.New("collector plan rollout version conflict")
	ErrCollectorPlanRolloutEmpty    = errors.New("collector plan rollout selector matched no collectors")
)

type CollectorPlanRolloutStatus string

const (
	CollectorPlanRolloutSchemaVersion uint16                     = 1
	CollectorPlanRolloutDraft         CollectorPlanRolloutStatus = "draft"
	CollectorPlanRolloutPreviewed     CollectorPlanRolloutStatus = "previewed"
	CollectorPlanRolloutSkippedWave   uint32                     = ^uint32(0)
)

type CollectorPlanRolloutSelector struct {
	ModuleKey    string   `json:"module_key"`
	AgentType    string   `json:"agent_type,omitempty"`
	Status       string   `json:"status"`
	CollectorIDs []string `json:"collector_ids,omitempty"`
}

type CollectorPlanRolloutStrategy struct {
	CanaryCount    uint32 `json:"canary_count"`
	WaveSize       uint32 `json:"wave_size"`
	MinSoakSeconds uint32 `json:"min_soak_seconds"`
	FailureBudget  uint32 `json:"failure_budget"`
}

type CollectorPlanRollout struct {
	ID                   ID
	TenantID             ID
	ModuleKey            string
	RolloutSchemaVersion uint16
	SelectorJSON         json.RawMessage
	SelectorHash         string
	SpecJSON             json.RawMessage
	SpecHash             string
	PlanSchemaVersion    uint16
	StrategyJSON         json.RawMessage
	StrategyHash         string
	Status               CollectorPlanRolloutStatus
	ExpiresAt            time.Time
	RowVersion           uint64
	CreatedBy            ID
	UpdatedBy            ID
	CreatedAt            time.Time
	UpdatedAt            time.Time
	PreviewedAt          time.Time
	CompletedAt          time.Time
}

type CollectorPlanRolloutCreateRequest struct {
	TenantID          ID
	ActorID           ID
	Selector          CollectorPlanRolloutSelector
	SpecJSON          json.RawMessage
	PlanSchemaVersion uint16
	Strategy          CollectorPlanRolloutStrategy
	ExpiresAt         time.Time
}

type CollectorPlanRolloutPreviewRequest struct {
	TenantID           ID
	RolloutID          ID
	ActorID            ID
	ExpectedRowVersion uint64
}

type CollectorPlanRolloutPreview struct {
	Rollout       CollectorPlanRollout
	MatchedCount  uint64
	EligibleCount uint64
	SkippedCount  uint64
	WaveCount     uint32
}

type CollectorPlanRolloutRepository interface {
	CreateCollectorPlanRollout(context.Context, CollectorPlanRolloutCreateRequest) (CollectorPlanRollout, error)
	PreviewCollectorPlanRollout(context.Context, CollectorPlanRolloutPreviewRequest) (CollectorPlanRolloutPreview, error)
}

type CollectorPlanRolloutController interface {
	CreateCollectorPlanRollout(context.Context, CollectorPlanRolloutCreateRequest) (CollectorPlanRollout, error)
	PreviewCollectorPlanRollout(context.Context, CollectorPlanRolloutPreviewRequest) (CollectorPlanRolloutPreview, error)
}

type CollectorPlanRolloutService struct {
	repository CollectorPlanRolloutRepository
}

func NewCollectorPlanRolloutService(repository CollectorPlanRolloutRepository) (*CollectorPlanRolloutService, error) {
	if repository == nil {
		return nil, errors.New("collector plan rollout repository is required")
	}
	return &CollectorPlanRolloutService{repository: repository}, nil
}

func (s *CollectorPlanRolloutService) CreateCollectorPlanRollout(ctx context.Context, request CollectorPlanRolloutCreateRequest) (CollectorPlanRollout, error) {
	if s == nil || s.repository == nil {
		return CollectorPlanRollout{}, errors.New("collector plan rollout repository is unavailable")
	}
	normalized, err := normalizeCollectorPlanRolloutCreateRequest(request, time.Now().UTC())
	if err != nil {
		return CollectorPlanRollout{}, err
	}
	return s.repository.CreateCollectorPlanRollout(ctx, normalized)
}

func (s *CollectorPlanRolloutService) PreviewCollectorPlanRollout(ctx context.Context, request CollectorPlanRolloutPreviewRequest) (CollectorPlanRolloutPreview, error) {
	if s == nil || s.repository == nil {
		return CollectorPlanRolloutPreview{}, errors.New("collector plan rollout repository is unavailable")
	}
	if !validCollectorEvidenceID(request.TenantID) || !validCollectorEvidenceID(request.RolloutID) || !validCollectorEvidenceID(request.ActorID) || request.ExpectedRowVersion == 0 {
		return CollectorPlanRolloutPreview{}, ErrCollectorPlanRolloutInvalid
	}
	return s.repository.PreviewCollectorPlanRollout(ctx, request)
}

func normalizeCollectorPlanRolloutCreateRequest(request CollectorPlanRolloutCreateRequest, now time.Time) (CollectorPlanRolloutCreateRequest, error) {
	if !validCollectorEvidenceID(request.TenantID) || !validCollectorEvidenceID(request.ActorID) || request.PlanSchemaVersion == 0 || request.ExpiresAt.IsZero() || !now.Before(request.ExpiresAt) || request.ExpiresAt.Nanosecond()%int(time.Millisecond) != 0 {
		return CollectorPlanRolloutCreateRequest{}, ErrCollectorPlanRolloutInvalid
	}
	selector, err := normalizeCollectorPlanRolloutSelector(request.Selector)
	if err != nil {
		return CollectorPlanRolloutCreateRequest{}, err
	}
	if err := validateCollectorPlanRolloutStrategy(request.Strategy); err != nil {
		return CollectorPlanRolloutCreateRequest{}, err
	}
	canonical, _, err := CanonicalCollectorPlanJSON(request.SpecJSON)
	if err != nil {
		return CollectorPlanRolloutCreateRequest{}, ErrCollectorPlanRolloutInvalid
	}
	request.Selector = selector
	request.SpecJSON = canonical
	request.ExpiresAt = request.ExpiresAt.UTC()
	return request, nil
}

func normalizeCollectorPlanRolloutSelector(selector CollectorPlanRolloutSelector) (CollectorPlanRolloutSelector, error) {
	selector.ModuleKey = strings.ToLower(strings.TrimSpace(selector.ModuleKey))
	selector.AgentType = strings.ToLower(strings.TrimSpace(selector.AgentType))
	selector.Status = strings.ToLower(strings.TrimSpace(selector.Status))
	if selector.Status == "" {
		selector.Status = "active"
	}
	if selector.ModuleKey == "" || len(selector.ModuleKey) > 64 || !isPrintableASCII(selector.ModuleKey) || len(selector.AgentType) > 64 || (selector.AgentType != "" && !isPrintableASCII(selector.AgentType)) || (selector.Status != "active" && selector.Status != "pending") || len(selector.CollectorIDs) > 5000 {
		return CollectorPlanRolloutSelector{}, ErrCollectorPlanRolloutInvalid
	}
	seen := make(map[string]struct{}, len(selector.CollectorIDs))
	collectorIDs := make([]string, len(selector.CollectorIDs))
	for index := range selector.CollectorIDs {
		value := strings.TrimSpace(selector.CollectorIDs[index])
		if !validCollectorEvidenceID(ID(value)) {
			return CollectorPlanRolloutSelector{}, ErrCollectorPlanRolloutInvalid
		}
		if _, duplicate := seen[value]; duplicate {
			return CollectorPlanRolloutSelector{}, ErrCollectorPlanRolloutInvalid
		}
		seen[value] = struct{}{}
		collectorIDs[index] = value
	}
	sort.Strings(collectorIDs)
	selector.CollectorIDs = collectorIDs
	return selector, nil
}

func validateCollectorPlanRolloutStrategy(strategy CollectorPlanRolloutStrategy) error {
	if strategy.CanaryCount == 0 || strategy.CanaryCount > 10000 ||
		strategy.WaveSize == 0 || strategy.WaveSize > 10000 ||
		strategy.MinSoakSeconds == 0 || strategy.MinSoakSeconds > uint32((7*24*time.Hour)/time.Second) ||
		strategy.FailureBudget >= strategy.CanaryCount || strategy.FailureBudget >= strategy.WaveSize {
		return ErrCollectorPlanRolloutInvalid
	}
	return nil
}

func canonicalCollectorPlanRolloutValue(value any) (json.RawMessage, string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, "", err
	}
	canonical, _, err := CanonicalCollectorPlanJSON(encoded)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(canonical)
	return canonical, hex.EncodeToString(digest[:]), nil
}

var _ CollectorPlanRolloutController = (*CollectorPlanRolloutService)(nil)
