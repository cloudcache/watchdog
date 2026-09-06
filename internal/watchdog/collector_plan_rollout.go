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
	CollectorPlanRolloutCanarying     CollectorPlanRolloutStatus = "canarying"
	CollectorPlanRolloutRolling       CollectorPlanRolloutStatus = "rolling"
	CollectorPlanRolloutPaused        CollectorPlanRolloutStatus = "paused"
	CollectorPlanRolloutCompleted     CollectorPlanRolloutStatus = "completed"
	CollectorPlanRolloutRolledBack    CollectorPlanRolloutStatus = "rolled_back"
	CollectorPlanRolloutKilled        CollectorPlanRolloutStatus = "killed"
	CollectorPlanRolloutSkippedWave   uint32                     = ^uint32(0)
)

type CollectorPlanRolloutTargetStatus string

const (
	CollectorPlanRolloutTargetPending         CollectorPlanRolloutTargetStatus = "pending"
	CollectorPlanRolloutTargetRevisionCreated CollectorPlanRolloutTargetStatus = "revision_created"
	CollectorPlanRolloutTargetActivated       CollectorPlanRolloutTargetStatus = "activated"
	CollectorPlanRolloutTargetACKed           CollectorPlanRolloutTargetStatus = "acked"
	CollectorPlanRolloutTargetFailed          CollectorPlanRolloutTargetStatus = "failed"
	CollectorPlanRolloutTargetReverted        CollectorPlanRolloutTargetStatus = "reverted"
	CollectorPlanRolloutTargetSkipped         CollectorPlanRolloutTargetStatus = "skipped"
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

type CollectorPlanRolloutListFilter struct {
	Query     string
	ModuleKey string
	Status    CollectorPlanRolloutStatus
	Sort      string
	Desc      bool
	Limit     int
	Offset    int
}

type CollectorPlanRolloutSummary struct {
	Matched         uint64 `json:"matched"`
	Eligible        uint64 `json:"eligible"`
	Pending         uint64 `json:"pending"`
	RevisionCreated uint64 `json:"revision_created"`
	Activated       uint64 `json:"activated"`
	ACKed           uint64 `json:"acked"`
	Failed          uint64 `json:"failed"`
	Reverted        uint64 `json:"reverted"`
	Skipped         uint64 `json:"skipped"`
}

type CollectorPlanRolloutTarget struct {
	TenantID                  ID
	RolloutID                 ID
	CollectorID               ID
	CollectorName             string
	AgentType                 string
	CollectorStatus           string
	ObservedHealth            string
	CollectorConfigVersion    uint64
	AcknowledgedConfigVersion uint64
	LastGoodConfigVersion     uint64
	LastSeenAt                time.Time
	Wave                      uint32
	ConfigVersion             uint64
	PriorConfigVersion        uint64
	Status                    CollectorPlanRolloutTargetStatus
	FailureReason             string
	ActivatedAt               time.Time
	ACKedAt                   time.Time
	RowVersion                uint64
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
}

type CollectorPlanRolloutTargetListFilter struct {
	Query  string
	Status CollectorPlanRolloutTargetStatus
	Health string
	Wave   *uint32
	Sort   string
	Desc   bool
	Limit  int
	Offset int
}

type CollectorPlanRolloutRepository interface {
	CreateCollectorPlanRollout(context.Context, CollectorPlanRolloutCreateRequest) (CollectorPlanRollout, error)
	PreviewCollectorPlanRollout(context.Context, CollectorPlanRolloutPreviewRequest) (CollectorPlanRolloutPreview, error)
	ListCollectorPlanRollouts(context.Context, ID, CollectorPlanRolloutListFilter) ([]CollectorPlanRollout, int64, error)
	GetCollectorPlanRollout(context.Context, ID, ID) (CollectorPlanRollout, CollectorPlanRolloutSummary, error)
	ListCollectorPlanRolloutTargets(context.Context, ID, ID, CollectorPlanRolloutTargetListFilter) ([]CollectorPlanRolloutTarget, int64, error)
}

type CollectorPlanRolloutController interface {
	CreateCollectorPlanRollout(context.Context, CollectorPlanRolloutCreateRequest) (CollectorPlanRollout, error)
	PreviewCollectorPlanRollout(context.Context, CollectorPlanRolloutPreviewRequest) (CollectorPlanRolloutPreview, error)
	ListCollectorPlanRollouts(context.Context, ID, CollectorPlanRolloutListFilter) ([]CollectorPlanRollout, int64, error)
	GetCollectorPlanRollout(context.Context, ID, ID) (CollectorPlanRollout, CollectorPlanRolloutSummary, error)
	ListCollectorPlanRolloutTargets(context.Context, ID, ID, CollectorPlanRolloutTargetListFilter) ([]CollectorPlanRolloutTarget, int64, error)
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

func (s *CollectorPlanRolloutService) ListCollectorPlanRollouts(ctx context.Context, tenantID ID, filter CollectorPlanRolloutListFilter) ([]CollectorPlanRollout, int64, error) {
	if s == nil || s.repository == nil {
		return nil, 0, errors.New("collector plan rollout repository is unavailable")
	}
	if !validCollectorEvidenceID(tenantID) {
		return nil, 0, ErrCollectorPlanRolloutInvalid
	}
	normalized, err := normalizeCollectorPlanRolloutListFilter(filter)
	if err != nil {
		return nil, 0, err
	}
	return s.repository.ListCollectorPlanRollouts(ctx, tenantID, normalized)
}

func (s *CollectorPlanRolloutService) GetCollectorPlanRollout(ctx context.Context, tenantID, rolloutID ID) (CollectorPlanRollout, CollectorPlanRolloutSummary, error) {
	if s == nil || s.repository == nil {
		return CollectorPlanRollout{}, CollectorPlanRolloutSummary{}, errors.New("collector plan rollout repository is unavailable")
	}
	if !validCollectorEvidenceID(tenantID) || !validCollectorEvidenceID(rolloutID) {
		return CollectorPlanRollout{}, CollectorPlanRolloutSummary{}, ErrCollectorPlanRolloutInvalid
	}
	return s.repository.GetCollectorPlanRollout(ctx, tenantID, rolloutID)
}

func (s *CollectorPlanRolloutService) ListCollectorPlanRolloutTargets(ctx context.Context, tenantID, rolloutID ID, filter CollectorPlanRolloutTargetListFilter) ([]CollectorPlanRolloutTarget, int64, error) {
	if s == nil || s.repository == nil {
		return nil, 0, errors.New("collector plan rollout repository is unavailable")
	}
	if !validCollectorEvidenceID(tenantID) || !validCollectorEvidenceID(rolloutID) {
		return nil, 0, ErrCollectorPlanRolloutInvalid
	}
	normalized, err := normalizeCollectorPlanRolloutTargetListFilter(filter)
	if err != nil {
		return nil, 0, err
	}
	return s.repository.ListCollectorPlanRolloutTargets(ctx, tenantID, rolloutID, normalized)
}

func normalizeCollectorPlanRolloutListFilter(filter CollectorPlanRolloutListFilter) (CollectorPlanRolloutListFilter, error) {
	filter.Query = strings.TrimSpace(filter.Query)
	filter.ModuleKey = strings.ToLower(strings.TrimSpace(filter.ModuleKey))
	filter.Status = CollectorPlanRolloutStatus(strings.ToLower(strings.TrimSpace(string(filter.Status))))
	filter.Sort = strings.ToLower(strings.TrimSpace(filter.Sort))
	if filter.Sort == "" {
		filter.Sort = "updated_at"
		filter.Desc = true
	}
	if len(filter.Query) > 190 || len(filter.ModuleKey) > 64 || (filter.ModuleKey != "" && !isPrintableASCII(filter.ModuleKey)) ||
		(filter.Status != "" && !validCollectorPlanRolloutStatus(filter.Status)) ||
		(filter.Sort != "module_key" && filter.Sort != "status" && filter.Sort != "expires_at" && filter.Sort != "created_at" && filter.Sort != "updated_at") ||
		filter.Limit <= 0 || filter.Limit > 200 || filter.Offset < 0 || filter.Offset > 1_000_000 {
		return CollectorPlanRolloutListFilter{}, ErrCollectorPlanRolloutInvalid
	}
	return filter, nil
}

func normalizeCollectorPlanRolloutTargetListFilter(filter CollectorPlanRolloutTargetListFilter) (CollectorPlanRolloutTargetListFilter, error) {
	filter.Query = strings.TrimSpace(filter.Query)
	filter.Status = CollectorPlanRolloutTargetStatus(strings.ToLower(strings.TrimSpace(string(filter.Status))))
	filter.Health = strings.ToLower(strings.TrimSpace(filter.Health))
	filter.Sort = strings.ToLower(strings.TrimSpace(filter.Sort))
	if filter.Sort == "" {
		filter.Sort = "wave"
	}
	if len(filter.Query) > 190 || (filter.Status != "" && !validCollectorPlanRolloutTargetStatus(filter.Status)) ||
		(filter.Health != "" && !validCollectorObservedHealth(filter.Health)) ||
		(filter.Sort != "collector" && filter.Sort != "agent_type" && filter.Sort != "wave" && filter.Sort != "status" && filter.Sort != "health" && filter.Sort != "updated_at") ||
		filter.Limit <= 0 || filter.Limit > 200 || filter.Offset < 0 || filter.Offset > 1_000_000 {
		return CollectorPlanRolloutTargetListFilter{}, ErrCollectorPlanRolloutInvalid
	}
	return filter, nil
}

func validCollectorPlanRolloutStatus(status CollectorPlanRolloutStatus) bool {
	switch status {
	case CollectorPlanRolloutDraft, CollectorPlanRolloutPreviewed, CollectorPlanRolloutCanarying,
		CollectorPlanRolloutRolling, CollectorPlanRolloutPaused, CollectorPlanRolloutCompleted,
		CollectorPlanRolloutRolledBack, CollectorPlanRolloutKilled:
		return true
	default:
		return false
	}
}

func validCollectorPlanRolloutTargetStatus(status CollectorPlanRolloutTargetStatus) bool {
	switch status {
	case CollectorPlanRolloutTargetPending, CollectorPlanRolloutTargetRevisionCreated,
		CollectorPlanRolloutTargetActivated, CollectorPlanRolloutTargetACKed,
		CollectorPlanRolloutTargetFailed, CollectorPlanRolloutTargetReverted,
		CollectorPlanRolloutTargetSkipped:
		return true
	default:
		return false
	}
}

func validCollectorObservedHealth(health string) bool {
	switch health {
	case "unknown", "warming", "healthy", "degraded", "stale", "unavailable":
		return true
	default:
		return false
	}
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
