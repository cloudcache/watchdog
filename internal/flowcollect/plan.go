package flowcollect

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"sort"
	"strings"
	"time"
)

type Protocol uint8

const (
	ProtocolSFlow5   Protocol = 1
	ProtocolNetFlow5 Protocol = 2
	ProtocolNetFlow9 Protocol = 3
	ProtocolIPFIX    Protocol = 4
)

type SamplingMode uint8

const (
	SamplingModeSampled   SamplingMode = 1
	SamplingModePreScaled SamplingMode = 2
)

type Observation struct {
	Direction uint32 `json:"direction"`
}

type SourceBinding struct {
	Protocol            Protocol `json:"protocol"`
	SourcePrefix        string   `json:"source_prefix"`
	ObservationDomainID *uint64  `json:"observation_domain_id,omitempty"`
	TenantID            string   `json:"tenant_id"`
	ExporterID          string   `json:"exporter_id"`
	TargetID            string   `json:"target_id"`
	DeviceID            string   `json:"device_id"`
	// OwnershipEpoch is increased by the control plane every time this
	// exporter binding moves to another collector. Schema-v1 plans implicitly
	// use epoch 1; schema-v2 plans must carry it explicitly.
	OwnershipEpoch uint64                 `json:"ownership_epoch,omitempty"`
	SamplingMode   SamplingMode           `json:"sampling_mode"`
	SamplingRules  []SamplingRule         `json:"sampling_rules,omitempty"`
	Observations   map[uint32]Observation `json:"observations,omitempty"`
	Enabled        bool                   `json:"enabled"`
}

type SamplingRule struct {
	ObservationDomainID *uint64      `json:"observation_domain_id,omitempty"`
	SubAgentID          *uint32      `json:"sub_agent_id,omitempty"`
	SourceIDType        *uint32      `json:"source_id_type,omitempty"`
	SourceIDValue       *uint32      `json:"source_id_value,omitempty"`
	IfIndex             *uint32      `json:"if_index,omitempty"`
	Mode                SamplingMode `json:"mode"`
	Rate                uint64       `json:"rate,omitempty"`
}

type Plan struct {
	SchemaVersion       uint32          `json:"schema_version"`
	Revision            uint64          `json:"revision"`
	CollectorID         string          `json:"collector_id"`
	NotBefore           time.Time       `json:"not_before"`
	ExpiresAt           time.Time       `json:"expires_at"`
	PartitionMapVersion uint32          `json:"partition_map_version"`
	PartitionMap        []uint32        `json:"partition_map"`
	Sources             []SourceBinding `json:"sources"`
}

type compiledBinding struct {
	SourceBinding
	prefix netip.Prefix
}

type Registry struct {
	plan     Plan
	bindings []compiledBinding
}

func CompilePlan(plan Plan, now time.Time) (*Registry, error) {
	return compilePlan(plan, now, true)
}

func compileHistoricalPlan(plan Plan) (*Registry, error) {
	return compilePlan(plan, time.Time{}, false)
}

func compilePlan(plan Plan, now time.Time, requireActive bool) (*Registry, error) {
	plan = clonePlan(plan)
	if plan.SchemaVersion != 1 && plan.SchemaVersion != 2 {
		return nil, fmt.Errorf("unsupported plan schema version %d", plan.SchemaVersion)
	}
	if plan.Revision == 0 || strings.TrimSpace(plan.CollectorID) == "" {
		return nil, errors.New("plan revision and collector_id are required")
	}
	if plan.ExpiresAt.IsZero() || (!plan.NotBefore.IsZero() && !plan.NotBefore.Before(plan.ExpiresAt)) {
		return nil, errors.New("plan validity interval is invalid")
	}
	if requireActive {
		if !plan.NotBefore.IsZero() && now.Before(plan.NotBefore) {
			return nil, errors.New("plan is not active yet")
		}
		if !now.Before(plan.ExpiresAt) {
			return nil, errors.New("plan is expired")
		}
	}
	if plan.PartitionMapVersion == 0 {
		return nil, errors.New("partition_map_version is required")
	}
	if len(plan.PartitionMap) != VirtualShardCount {
		return nil, fmt.Errorf("partition_map must have %d entries", VirtualShardCount)
	}
	for index, partition := range plan.PartitionMap {
		if partition > math.MaxInt32 {
			return nil, fmt.Errorf("partition_map[%d] exceeds Kafka partition range", index)
		}
	}
	compiled := make([]compiledBinding, 0, len(plan.Sources))
	seen := make(map[string]struct{}, len(plan.Sources))
	for i, source := range plan.Sources {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(source.SourcePrefix))
		if err != nil {
			return nil, fmt.Errorf("sources[%d].source_prefix: %w", i, err)
		}
		prefix = prefix.Masked()
		if source.Protocol < ProtocolSFlow5 || source.Protocol > ProtocolIPFIX {
			return nil, fmt.Errorf("sources[%d].protocol is invalid", i)
		}
		if strings.TrimSpace(source.TenantID) == "" || strings.TrimSpace(source.ExporterID) == "" || strings.TrimSpace(source.TargetID) == "" {
			return nil, fmt.Errorf("sources[%d] tenant_id, exporter_id, and target_id are required", i)
		}
		if plan.SchemaVersion >= 2 && source.OwnershipEpoch == 0 {
			return nil, fmt.Errorf("sources[%d].ownership_epoch is required", i)
		}
		if source.SamplingMode != SamplingModeSampled && source.SamplingMode != SamplingModePreScaled {
			return nil, fmt.Errorf("sources[%d].sampling_mode is invalid", i)
		}
		rules := make(map[string]struct{}, len(source.SamplingRules))
		for ruleIndex, rule := range source.SamplingRules {
			if rule.Mode != SamplingModeSampled && rule.Mode != SamplingModePreScaled {
				return nil, fmt.Errorf("sources[%d].sampling_rules[%d].mode is invalid", i, ruleIndex)
			}
			if source.Protocol == ProtocolSFlow5 && (rule.SourceIDType == nil || rule.SourceIDValue == nil) {
				return nil, fmt.Errorf("sources[%d].sampling_rules[%d] must select an exact sFlow source_id_type/source_id_value", i, ruleIndex)
			}
			if source.Protocol != ProtocolSFlow5 && rule.ObservationDomainID == nil && rule.IfIndex == nil {
				return nil, fmt.Errorf("sources[%d].sampling_rules[%d] must select an exact NetFlow/IPFIX domain or interface", i, ruleIndex)
			}
			key := samplingRuleKey(rule)
			if _, exists := rules[key]; exists {
				return nil, fmt.Errorf("sources[%d] has duplicate sampling rule %s", i, key)
			}
			rules[key] = struct{}{}
			for previous := 0; previous < ruleIndex; previous++ {
				if samplingRulesAmbiguous(source.SamplingRules[previous], rule) {
					return nil, fmt.Errorf("sources[%d].sampling_rules[%d] overlaps an equally specific rule", i, ruleIndex)
				}
			}
		}
		for ifIndex, observation := range source.Observations {
			if ifIndex == 0 || observation.Direction > 2 {
				return nil, fmt.Errorf("sources[%d].observations contains invalid interface or direction", i)
			}
		}
		domain := "*"
		if source.ObservationDomainID != nil {
			domain = fmt.Sprint(*source.ObservationDomainID)
		}
		key := fmt.Sprintf("%d|%s|%s", source.Protocol, prefix, domain)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate source binding %s", key)
		}
		seen[key] = struct{}{}
		compiled = append(compiled, compiledBinding{SourceBinding: source, prefix: prefix})
	}
	sort.SliceStable(compiled, func(i, j int) bool {
		if compiled[i].prefix.Bits() != compiled[j].prefix.Bits() {
			return compiled[i].prefix.Bits() > compiled[j].prefix.Bits()
		}
		return compiled[i].ObservationDomainID != nil && compiled[j].ObservationDomainID == nil
	})
	return &Registry{plan: plan, bindings: compiled}, nil
}

func (b SourceBinding) EffectiveOwnershipEpoch() uint64 {
	if b.OwnershipEpoch == 0 {
		return 1
	}
	return b.OwnershipEpoch
}

func samplingRuleKey(rule SamplingRule) string {
	return optionalUint64(rule.ObservationDomainID) + "/" + optionalUint32(rule.SubAgentID) + "/" + optionalUint32(rule.SourceIDType) + "/" + optionalUint32(rule.SourceIDValue) + "/" + optionalUint32(rule.IfIndex)
}

func optionalUint32(value *uint32) string {
	if value == nil {
		return "*"
	}
	return fmt.Sprint(*value)
}
func optionalUint64(value *uint64) string {
	if value == nil {
		return "*"
	}
	return fmt.Sprint(*value)
}

func samplingRulesAmbiguous(left, right SamplingRule) bool {
	leftSpecificity, rightSpecificity := samplingRuleSpecificity(left), samplingRuleSpecificity(right)
	if leftSpecificity != rightSpecificity {
		return false
	}
	if !optionalUint64Overlap(left.ObservationDomainID, right.ObservationDomainID) || !optionalUint32Overlap(left.SubAgentID, right.SubAgentID) || !optionalUint32Overlap(left.SourceIDType, right.SourceIDType) || !optionalUint32Overlap(left.SourceIDValue, right.SourceIDValue) || !optionalUint32Overlap(left.IfIndex, right.IfIndex) {
		return false
	}
	return left.Mode != right.Mode || left.Rate != right.Rate
}

func samplingRuleSpecificity(rule SamplingRule) int {
	count := 0
	if rule.ObservationDomainID != nil {
		count++
	}
	if rule.SubAgentID != nil {
		count++
	}
	if rule.SourceIDType != nil {
		count++
	}
	if rule.SourceIDValue != nil {
		count++
	}
	if rule.IfIndex != nil {
		count++
	}
	return count
}

func optionalUint32Overlap(left, right *uint32) bool {
	return left == nil || right == nil || *left == *right
}
func optionalUint64Overlap(left, right *uint64) bool {
	return left == nil || right == nil || *left == *right
}

func (r *Registry) Plan() Plan { return clonePlan(r.plan) }

func clonePlan(plan Plan) Plan {
	cloned := plan
	cloned.PartitionMap = append([]uint32(nil), plan.PartitionMap...)
	cloned.Sources = append([]SourceBinding(nil), plan.Sources...)
	for index := range cloned.Sources {
		source := &cloned.Sources[index]
		if source.ObservationDomainID != nil {
			value := *source.ObservationDomainID
			source.ObservationDomainID = &value
		}
		source.SamplingRules = append([]SamplingRule(nil), source.SamplingRules...)
		for ruleIndex := range source.SamplingRules {
			rule := &source.SamplingRules[ruleIndex]
			rule.ObservationDomainID = cloneUint64(rule.ObservationDomainID)
			rule.SubAgentID = cloneUint32(rule.SubAgentID)
			rule.SourceIDType = cloneUint32(rule.SourceIDType)
			rule.SourceIDValue = cloneUint32(rule.SourceIDValue)
			rule.IfIndex = cloneUint32(rule.IfIndex)
		}
		if source.Observations != nil {
			observations := make(map[uint32]Observation, len(source.Observations))
			for ifIndex, observation := range source.Observations {
				observations[ifIndex] = observation
			}
			source.Observations = observations
		}
	}
	return cloned
}

func cloneUint32(value *uint32) *uint32 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func (r *Registry) Admit(protocol Protocol, source netip.Addr, observationDomainID uint64) (SourceBinding, bool) {
	if source.Is4In6() {
		source = source.Unmap()
	}
	for _, binding := range r.bindings {
		if binding.Protocol != protocol || !binding.prefix.Contains(source) || !binding.Enabled {
			continue
		}
		if binding.ObservationDomainID != nil && *binding.ObservationDomainID != observationDomainID {
			continue
		}
		return binding.SourceBinding, true
	}
	return SourceBinding{}, false
}
