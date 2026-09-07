package watchdog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowvpn"
)

const (
	VPNRuleKindPassive      = string(flowvpn.RuleKindPassive)
	VPNRuleKindIntelligence = string(flowvpn.RuleKindIntelligence)
	VPNRuleKindProbe        = string(flowvpn.RuleKindProbe)

	VPNRuleStatusDraft     = "draft"
	VPNRuleStatusActive    = "active"
	VPNRuleStatusSuspended = "suspended"
	VPNRuleStatusRetired   = "retired"
)

var (
	ErrVPNRuleInvalid         = errors.New("VPN rule is invalid")
	ErrVPNRuleNameConflict    = errors.New("VPN rule name already exists")
	ErrVPNRuleVersionConflict = errors.New("VPN rule changed since it was read")
)

// VPNRule is editable management state. A worker never consumes these rows
// directly; publication freezes canonical rules into an immutable rule set.
type VPNRule struct {
	ID                ID                 `json:"id"`
	TenantID          ID                 `json:"tenant_id"`
	Name              string             `json:"name"`
	Kind              string             `json:"kind"`
	RuleSchemaVersion uint16             `json:"rule_schema_version"`
	Match             flowvpn.Match      `json:"match"`
	Effect            flowvpn.RuleEffect `json:"effect"`
	Weight            uint16             `json:"weight"`
	Priority          uint16             `json:"priority"`
	Status            string             `json:"status"`
	RowVersion        uint64             `json:"row_version"`
	CreatedBy         ID                 `json:"created_by"`
	UpdatedBy         ID                 `json:"updated_by"`
	CreatedAt         time.Time          `json:"created_at"`
	UpdatedAt         time.Time          `json:"updated_at"`
}

type VPNRuleListFilter struct {
	Search     string
	Kind       string
	Effect     string
	Status     string
	SortBy     string
	Descending bool
	Limit      int
	Offset     int
}

type VPNRuleRepository interface {
	ListVPNRules(context.Context, ID, VPNRuleListFilter) ([]VPNRule, int64, error)
	GetVPNRule(context.Context, ID, ID) (VPNRule, error)
	CreateVPNRule(context.Context, VPNRule) (VPNRule, error)
	UpdateVPNRule(context.Context, VPNRule, uint64) (VPNRule, error)
	DeleteVPNRule(context.Context, ID, ID, ID, uint64) error
}

func normalizeVPNRule(item VPNRule) (VPNRule, error) {
	item.Name = strings.TrimSpace(item.Name)
	item.Kind = strings.ToLower(strings.TrimSpace(item.Kind))
	item.Status = strings.ToLower(strings.TrimSpace(item.Status))
	if item.ID == "" || item.TenantID == "" || item.CreatedBy == "" || item.UpdatedBy == "" {
		return VPNRule{}, fmt.Errorf("%w: identity, tenant and actors are required", ErrVPNRuleInvalid)
	}
	if item.Name == "" || len(item.Name) > 190 {
		return VPNRule{}, fmt.Errorf("%w: name must contain 1..190 characters", ErrVPNRuleInvalid)
	}
	if item.Kind == "" {
		item.Kind = VPNRuleKindPassive
	}
	if !validVPNRuleKind(item.Kind) {
		return VPNRule{}, fmt.Errorf("%w: unsupported kind", ErrVPNRuleInvalid)
	}
	if item.Status == "" {
		item.Status = VPNRuleStatusDraft
	}
	if !validVPNRuleStatus(item.Status) {
		return VPNRule{}, fmt.Errorf("%w: unsupported status", ErrVPNRuleInvalid)
	}
	canonical, err := flowvpn.NormalizeRule(flowvpn.Rule{
		ID: string(item.ID), Effect: item.Effect, Weight: item.Weight,
		Priority: item.Priority, Match: item.Match,
	})
	if err != nil {
		return VPNRule{}, fmt.Errorf("%w: %v", ErrVPNRuleInvalid, err)
	}
	item.RuleSchemaVersion = uint16(flowvpn.RuleSchemaV1)
	item.Match = canonical.Match
	return item, nil
}

func normalizeVPNRuleListFilter(filter VPNRuleListFilter) (VPNRuleListFilter, error) {
	filter.Search = strings.TrimSpace(filter.Search)
	filter.Kind = strings.ToLower(strings.TrimSpace(filter.Kind))
	filter.Effect = strings.ToLower(strings.TrimSpace(filter.Effect))
	filter.Status = strings.ToLower(strings.TrimSpace(filter.Status))
	filter.SortBy = strings.ToLower(strings.TrimSpace(filter.SortBy))
	if len(filter.Search) > 255 || (filter.Kind != "" && !validVPNRuleKind(filter.Kind)) ||
		(filter.Status != "" && !validVPNRuleStatus(filter.Status)) ||
		(filter.Effect != "" && filter.Effect != string(flowvpn.EffectScore) && filter.Effect != string(flowvpn.EffectAllow) && filter.Effect != string(flowvpn.EffectSuppress)) {
		return filter, ErrVPNRuleInvalid
	}
	if filter.SortBy == "" {
		filter.SortBy = "updated_at"
		filter.Descending = true
	}
	switch filter.SortBy {
	case "name", "kind", "effect", "weight", "priority", "status", "created_at", "updated_at":
	default:
		return filter, fmt.Errorf("%w: unsupported sort column", ErrVPNRuleInvalid)
	}
	if filter.Limit == 0 {
		filter.Limit = 25
	}
	if filter.Limit < 1 || filter.Limit > 200 || filter.Offset < 0 || filter.Offset > 100_000 {
		return filter, fmt.Errorf("%w: invalid pagination", ErrVPNRuleInvalid)
	}
	return filter, nil
}

func validVPNRuleKind(value string) bool {
	return value == VPNRuleKindPassive || value == VPNRuleKindIntelligence || value == VPNRuleKindProbe
}

func validVPNRuleStatus(value string) bool {
	return value == VPNRuleStatusDraft || value == VPNRuleStatusActive || value == VPNRuleStatusSuspended || value == VPNRuleStatusRetired
}
