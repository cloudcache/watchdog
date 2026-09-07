// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

// Package flowvpn implements deterministic passive VPN candidate scoring. It
// never authorizes or executes active probes.
package flowvpn

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"sort"
	"strings"
	"time"
)

const (
	RuleSchemaV1       = uint32(1)
	maxRules           = 1_000
	maxValuesPerSignal = 256
)

type TransportHint string

const (
	HintQUIC TransportHint = "quic"
	HintTCP  TransportHint = "tcp"
	HintTLS  TransportHint = "tls"
)

type RuleEffect string

const (
	EffectScore    RuleEffect = "score"
	EffectAllow    RuleEffect = "allow"
	EffectSuppress RuleEffect = "suppress"
)

type RiskLevel string

const (
	RiskLow      RiskLevel = "low"
	RiskMedium   RiskLevel = "medium"
	RiskHigh     RiskLevel = "high"
	RiskCritical RiskLevel = "critical"
)

type Verdict string

const (
	VerdictObserve        Verdict = "observe"
	VerdictReview         Verdict = "review"
	VerdictProbeCandidate Verdict = "probe_candidate"
	VerdictAllowlisted    Verdict = "allowlisted"
	VerdictSuppressed     Verdict = "suppressed"
)

type Candidate struct {
	WindowStart           time.Time
	WindowEnd             time.Time
	ConversationKey       string
	LocalIP               netip.Addr
	RemoteIP              netip.Addr
	PrimaryProtocol       uint8
	PrimaryLocalPort      uint16
	PrimaryRemotePort     uint16
	LocalToRemoteBytes    uint64
	RemoteToLocalBytes    uint64
	FlowRecordCount       uint64
	ActiveBucketCount     uint32
	MaxDurationMS         uint64
	RemoteASN             uint32
	RemoteCountry         string
	RemotePrefixID        string
	TransportHints        []TransportHint
	CompleteRatio         float64
	DimensionSnapshotID   string
	GeoVersion            string
	ClassificationVersion uint32
}

type Match struct {
	RemotePorts       []uint16        `json:"remote_ports,omitempty"`
	Protocols         []uint8         `json:"protocols,omitempty"`
	RemoteASNs        []uint32        `json:"remote_asns,omitempty"`
	RemotePrefixIDs   []string        `json:"remote_prefix_ids,omitempty"`
	RemoteCountries   []string        `json:"remote_countries,omitempty"`
	TransportHints    []TransportHint `json:"transport_hints,omitempty"`
	MinDurationMS     *uint64         `json:"min_duration_ms,omitempty"`
	MinTotalBytes     *uint64         `json:"min_total_bytes,omitempty"`
	MinFlowRecords    *uint64         `json:"min_flow_records,omitempty"`
	MinActiveBuckets  *uint32         `json:"min_active_buckets,omitempty"`
	MinSymmetryRatio  *float64        `json:"min_symmetry_ratio,omitempty"`
	MinDominanceRatio *float64        `json:"min_dominance_ratio,omitempty"`
}

type Rule struct {
	ID       string     `json:"id"`
	Effect   RuleEffect `json:"effect"`
	Weight   uint16     `json:"weight"`
	Priority uint16     `json:"priority"`
	Match    Match      `json:"match"`
}

type RuleSet struct {
	SchemaVersion       uint32
	Version             string
	MediumThreshold     uint16
	HighThreshold       uint16
	CriticalThreshold   uint16
	ProbeThreshold      uint16
	MinimumCompleteness float64
	Rules               []Rule
}

type Evidence struct {
	RuleID       string     `json:"rule_id"`
	Effect       RuleEffect `json:"effect"`
	Contribution uint16     `json:"contribution"`
	Signals      []string   `json:"signals"`
}

type Result struct {
	RuleSetVersion        string     `json:"rule_set_version"`
	DimensionSnapshotID   string     `json:"dimension_snapshot_id"`
	GeoVersion            string     `json:"geo_version"`
	ClassificationVersion uint32     `json:"classification_version"`
	Score                 uint16     `json:"score"`
	ScoreCapped           bool       `json:"score_capped"`
	Level                 RiskLevel  `json:"level"`
	Verdict               Verdict    `json:"verdict"`
	SymmetryRatio         float64    `json:"symmetry_ratio"`
	DominanceRatio        float64    `json:"dominance_ratio"`
	Evidence              []Evidence `json:"evidence"`
	DecisionRuleID        string     `json:"decision_rule_id,omitempty"`
	ProbeRecommended      bool       `json:"probe_recommended"`
	ProbeBlockReason      string     `json:"probe_block_reason,omitempty"`
}

type compiledRule struct {
	rule Rule
}

type CompiledRuleSet struct {
	version             string
	mediumThreshold     uint16
	highThreshold       uint16
	criticalThreshold   uint16
	probeThreshold      uint16
	minimumCompleteness float64
	rules               []compiledRule
}

func CompileRuleSet(input RuleSet) (CompiledRuleSet, error) {
	if input.SchemaVersion != RuleSchemaV1 {
		return CompiledRuleSet{}, fmt.Errorf("unsupported VPN rule schema version %d", input.SchemaVersion)
	}
	if !validIdentifier(input.Version) {
		return CompiledRuleSet{}, errors.New("VPN rule-set version is invalid")
	}
	if input.MediumThreshold == 0 || input.MediumThreshold >= input.HighThreshold || input.HighThreshold >= input.CriticalThreshold || input.CriticalThreshold > 100 {
		return CompiledRuleSet{}, errors.New("VPN risk thresholds must satisfy 0 < medium < high < critical <= 100")
	}
	if input.ProbeThreshold == 0 || input.ProbeThreshold > 100 {
		return CompiledRuleSet{}, errors.New("VPN probe threshold must be 1..100")
	}
	if math.IsNaN(input.MinimumCompleteness) || math.IsInf(input.MinimumCompleteness, 0) || input.MinimumCompleteness < 0 || input.MinimumCompleteness > 1 {
		return CompiledRuleSet{}, errors.New("VPN minimum completeness must be between 0 and 1")
	}
	if len(input.Rules) == 0 || len(input.Rules) > maxRules {
		return CompiledRuleSet{}, fmt.Errorf("VPN rule count must be 1..%d", maxRules)
	}
	compiled := CompiledRuleSet{
		version: input.Version, mediumThreshold: input.MediumThreshold, highThreshold: input.HighThreshold,
		criticalThreshold: input.CriticalThreshold, probeThreshold: input.ProbeThreshold,
		minimumCompleteness: input.MinimumCompleteness, rules: make([]compiledRule, 0, len(input.Rules)),
	}
	seen := make(map[string]struct{}, len(input.Rules))
	for _, rule := range input.Rules {
		var err error
		rule, err = NormalizeRule(rule)
		if err != nil {
			return CompiledRuleSet{}, err
		}
		if _, exists := seen[rule.ID]; exists {
			return CompiledRuleSet{}, fmt.Errorf("VPN rule ID %q is duplicated", rule.ID)
		}
		seen[rule.ID] = struct{}{}
		compiled.rules = append(compiled.rules, compiledRule{rule: rule})
	}
	sort.Slice(compiled.rules, func(i, j int) bool { return compiled.rules[i].rule.ID < compiled.rules[j].rule.ID })
	return compiled, nil
}

// NormalizeRule validates one editable rule and returns the same canonical
// representation used by CompileRuleSet. Management APIs use this boundary so
// semantically equivalent drafts cannot produce different publication bytes.
func NormalizeRule(rule Rule) (Rule, error) {
	if !validIdentifier(rule.ID) {
		return Rule{}, errors.New("VPN rule ID is invalid")
	}
	if rule.Effect != EffectScore && rule.Effect != EffectAllow && rule.Effect != EffectSuppress {
		return Rule{}, fmt.Errorf("VPN rule %q has an unsupported effect", rule.ID)
	}
	if (rule.Effect == EffectScore && (rule.Weight == 0 || rule.Weight > 100)) || (rule.Effect != EffectScore && rule.Weight != 0) {
		return Rule{}, fmt.Errorf("VPN rule %q has an invalid weight for its effect", rule.ID)
	}
	canonical, err := canonicalMatch(rule.Match)
	if err != nil {
		return Rule{}, fmt.Errorf("VPN rule %q: %w", rule.ID, err)
	}
	rule.Match = canonical
	return rule, nil
}

func (r CompiledRuleSet) Evaluate(candidate Candidate) (Result, error) {
	if r.version == "" || len(r.rules) == 0 {
		return Result{}, errors.New("VPN rule set is not compiled")
	}
	normalized, err := normalizeCandidate(candidate)
	if err != nil {
		return Result{}, err
	}
	symmetry, dominance := trafficRatios(normalized.LocalToRemoteBytes, normalized.RemoteToLocalBytes)
	result := Result{
		RuleSetVersion: r.version, DimensionSnapshotID: normalized.DimensionSnapshotID,
		GeoVersion: normalized.GeoVersion, ClassificationVersion: normalized.ClassificationVersion,
		SymmetryRatio: symmetry, DominanceRatio: dominance,
	}
	var terminal *Rule
	var score uint32
	for _, current := range r.rules {
		matched, signals := matches(current.rule.Match, normalized, symmetry, dominance)
		if !matched {
			continue
		}
		evidence := Evidence{RuleID: current.rule.ID, Effect: current.rule.Effect, Signals: signals}
		if current.rule.Effect == EffectScore {
			evidence.Contribution = current.rule.Weight
			score += uint32(current.rule.Weight)
		} else if terminal == nil || terminalRuleBefore(current.rule, *terminal) {
			selected := current.rule
			terminal = &selected
		}
		result.Evidence = append(result.Evidence, evidence)
	}
	if score > 100 {
		result.Score, result.ScoreCapped = 100, true
	} else {
		result.Score = uint16(score)
	}
	result.Level = riskLevel(result.Score, r)
	result.Verdict = VerdictObserve
	if result.Score >= r.mediumThreshold {
		result.Verdict = VerdictReview
	}
	if terminal != nil {
		result.DecisionRuleID = terminal.ID
		if terminal.Effect == EffectSuppress {
			result.Verdict = VerdictSuppressed
		} else {
			result.Verdict = VerdictAllowlisted
		}
		result.ProbeBlockReason = "terminal_rule"
	} else if normalized.CompleteRatio < r.minimumCompleteness {
		result.ProbeBlockReason = "incomplete_window"
	} else if result.Score < r.probeThreshold {
		result.ProbeBlockReason = "below_threshold"
	} else {
		result.Verdict = VerdictProbeCandidate
		result.ProbeRecommended = true
	}
	return result, nil
}

func normalizeCandidate(input Candidate) (Candidate, error) {
	if input.WindowStart.IsZero() || input.WindowEnd.IsZero() || !input.WindowEnd.After(input.WindowStart) || input.WindowEnd.Sub(input.WindowStart) > 24*time.Hour {
		return Candidate{}, errors.New("VPN candidate window must be increasing and no longer than 24 hours")
	}
	recordID, err := hex.DecodeString(input.ConversationKey)
	if err != nil || len(recordID) != 32 || hex.EncodeToString(recordID) != input.ConversationKey {
		return Candidate{}, errors.New("VPN candidate conversation key must be canonical 64-character hexadecimal")
	}
	if !validIP(input.LocalIP) || !validIP(input.RemoteIP) {
		return Candidate{}, errors.New("VPN candidate local and remote IPs must be valid without zones")
	}
	if input.FlowRecordCount == 0 || input.ActiveBucketCount == 0 {
		return Candidate{}, errors.New("VPN candidate record and active bucket counts must be positive")
	}
	if uint64(input.ActiveBucketCount) > input.FlowRecordCount {
		return Candidate{}, errors.New("VPN candidate active bucket count cannot exceed flow record count")
	}
	if math.IsNaN(input.CompleteRatio) || math.IsInf(input.CompleteRatio, 0) || input.CompleteRatio < 0 || input.CompleteRatio > 1 {
		return Candidate{}, errors.New("VPN candidate completeness must be between 0 and 1")
	}
	if input.DimensionSnapshotID == "" || !validIdentifier(input.DimensionSnapshotID) {
		return Candidate{}, errors.New("VPN candidate dimension snapshot is invalid")
	}
	if input.GeoVersion == "" || !validIdentifier(input.GeoVersion) || input.ClassificationVersion == 0 {
		return Candidate{}, errors.New("VPN candidate Geo and classification versions are invalid")
	}
	if input.RemotePrefixID != "" && !validIdentifier(input.RemotePrefixID) {
		return Candidate{}, errors.New("VPN candidate remote prefix is invalid")
	}
	if input.RemoteCountry != "" && !validCountry(input.RemoteCountry) {
		return Candidate{}, errors.New("VPN candidate remote country must be an uppercase two-letter code")
	}
	hints, err := canonicalHints(input.TransportHints)
	if err != nil {
		return Candidate{}, err
	}
	input.WindowStart, input.WindowEnd = input.WindowStart.UTC(), input.WindowEnd.UTC()
	input.LocalIP, input.RemoteIP = input.LocalIP.Unmap(), input.RemoteIP.Unmap()
	input.TransportHints = hints
	return input, nil
}

func canonicalMatch(input Match) (Match, error) {
	if len(input.RemotePorts)+len(input.Protocols)+len(input.RemoteASNs)+len(input.RemotePrefixIDs)+len(input.RemoteCountries)+len(input.TransportHints) > maxValuesPerSignal*6 {
		return Match{}, errors.New("match contains too many values")
	}
	if len(input.RemotePorts) > maxValuesPerSignal || len(input.Protocols) > maxValuesPerSignal || len(input.RemoteASNs) > maxValuesPerSignal ||
		len(input.RemotePrefixIDs) > maxValuesPerSignal || len(input.RemoteCountries) > maxValuesPerSignal || len(input.TransportHints) > maxValuesPerSignal {
		return Match{}, errors.New("match signal contains too many values")
	}
	result := Match{
		RemotePorts: compactOrdered(input.RemotePorts), Protocols: compactOrdered(input.Protocols), RemoteASNs: compactOrdered(input.RemoteASNs),
		RemotePrefixIDs: compactStrings(input.RemotePrefixIDs), RemoteCountries: compactStrings(input.RemoteCountries),
	}
	var err error
	result.TransportHints, err = canonicalHints(input.TransportHints)
	if err != nil {
		return Match{}, err
	}
	for _, value := range result.RemotePorts {
		if value == 0 {
			return Match{}, errors.New("remote ports must be positive")
		}
	}
	for _, value := range result.Protocols {
		if value == 0 {
			return Match{}, errors.New("protocols must be positive")
		}
	}
	for _, value := range result.RemoteASNs {
		if value == 0 {
			return Match{}, errors.New("remote ASNs must be positive")
		}
	}
	for _, value := range result.RemotePrefixIDs {
		if !validIdentifier(value) {
			return Match{}, errors.New("remote prefix ID is invalid")
		}
	}
	for _, value := range result.RemoteCountries {
		if !validCountry(value) {
			return Match{}, errors.New("remote country must be an uppercase two-letter code")
		}
	}
	if input.MinDurationMS != nil {
		if *input.MinDurationMS == 0 {
			return Match{}, errors.New("minimum duration must be positive")
		}
		value := *input.MinDurationMS
		result.MinDurationMS = &value
	}
	if input.MinTotalBytes != nil {
		if *input.MinTotalBytes == 0 {
			return Match{}, errors.New("minimum total bytes must be positive")
		}
		value := *input.MinTotalBytes
		result.MinTotalBytes = &value
	}
	if input.MinFlowRecords != nil {
		if *input.MinFlowRecords == 0 {
			return Match{}, errors.New("minimum flow records must be positive")
		}
		value := *input.MinFlowRecords
		result.MinFlowRecords = &value
	}
	if input.MinActiveBuckets != nil {
		if *input.MinActiveBuckets == 0 {
			return Match{}, errors.New("minimum active buckets must be positive")
		}
		value := *input.MinActiveBuckets
		result.MinActiveBuckets = &value
	}
	if input.MinSymmetryRatio != nil {
		if err := validRatio(*input.MinSymmetryRatio, "minimum symmetry ratio"); err != nil {
			return Match{}, err
		}
		value := *input.MinSymmetryRatio
		result.MinSymmetryRatio = &value
	}
	if input.MinDominanceRatio != nil {
		if err := validRatio(*input.MinDominanceRatio, "minimum dominance ratio"); err != nil {
			return Match{}, err
		}
		value := *input.MinDominanceRatio
		result.MinDominanceRatio = &value
	}
	if len(result.RemotePorts) == 0 && len(result.Protocols) == 0 && len(result.RemoteASNs) == 0 && len(result.RemotePrefixIDs) == 0 &&
		len(result.RemoteCountries) == 0 && len(result.TransportHints) == 0 && result.MinDurationMS == nil && result.MinTotalBytes == nil &&
		result.MinFlowRecords == nil && result.MinActiveBuckets == nil && result.MinSymmetryRatio == nil && result.MinDominanceRatio == nil {
		return Match{}, errors.New("match must contain at least one signal")
	}
	return result, nil
}

func matches(match Match, candidate Candidate, symmetry, dominance float64) (bool, []string) {
	// signals is returned only when the rule fully matches; every early reject
	// returns (false, nil). Start nil so a rule that fails its first
	// discriminating check — the common case across many rules per candidate —
	// allocates nothing (previously an eager make([]string,0,12) per call).
	var signals []string
	if len(match.RemotePorts) > 0 {
		if !contains(match.RemotePorts, candidate.PrimaryRemotePort) {
			return false, nil
		}
		signals = append(signals, "remote_port")
	}
	if len(match.Protocols) > 0 {
		if !contains(match.Protocols, candidate.PrimaryProtocol) {
			return false, nil
		}
		signals = append(signals, "protocol")
	}
	if len(match.RemoteASNs) > 0 {
		if !contains(match.RemoteASNs, candidate.RemoteASN) {
			return false, nil
		}
		signals = append(signals, "remote_asn")
	}
	if len(match.RemotePrefixIDs) > 0 {
		if !contains(match.RemotePrefixIDs, candidate.RemotePrefixID) {
			return false, nil
		}
		signals = append(signals, "remote_prefix")
	}
	if len(match.RemoteCountries) > 0 {
		if !contains(match.RemoteCountries, candidate.RemoteCountry) {
			return false, nil
		}
		signals = append(signals, "remote_country")
	}
	if len(match.TransportHints) > 0 {
		if !intersects(match.TransportHints, candidate.TransportHints) {
			return false, nil
		}
		signals = append(signals, "transport_hint")
	}
	if match.MinDurationMS != nil {
		if candidate.MaxDurationMS < *match.MinDurationMS {
			return false, nil
		}
		signals = append(signals, "duration")
	}
	if match.MinTotalBytes != nil {
		if !sumAtLeast(candidate.LocalToRemoteBytes, candidate.RemoteToLocalBytes, *match.MinTotalBytes) {
			return false, nil
		}
		signals = append(signals, "total_bytes")
	}
	if match.MinFlowRecords != nil {
		if candidate.FlowRecordCount < *match.MinFlowRecords {
			return false, nil
		}
		signals = append(signals, "flow_records")
	}
	if match.MinActiveBuckets != nil {
		if candidate.ActiveBucketCount < *match.MinActiveBuckets {
			return false, nil
		}
		signals = append(signals, "active_buckets")
	}
	if match.MinSymmetryRatio != nil {
		if symmetry < *match.MinSymmetryRatio {
			return false, nil
		}
		signals = append(signals, "symmetry")
	}
	if match.MinDominanceRatio != nil {
		if dominance < *match.MinDominanceRatio {
			return false, nil
		}
		signals = append(signals, "dominance")
	}
	return true, signals
}

func trafficRatios(outbound, inbound uint64) (float64, float64) {
	maximum, minimum := outbound, inbound
	if inbound > outbound {
		maximum, minimum = inbound, outbound
	}
	if maximum == 0 {
		return 0, 0
	}
	symmetry := float64(minimum) / float64(maximum)
	return symmetry, 1 - symmetry
}

func sumAtLeast(left, right, threshold uint64) bool {
	return left >= threshold || right >= threshold-left
}

func riskLevel(score uint16, rules CompiledRuleSet) RiskLevel {
	switch {
	case score >= rules.criticalThreshold:
		return RiskCritical
	case score >= rules.highThreshold:
		return RiskHigh
	case score >= rules.mediumThreshold:
		return RiskMedium
	default:
		return RiskLow
	}
}

func terminalRuleBefore(left, right Rule) bool {
	if left.Priority != right.Priority {
		return left.Priority > right.Priority
	}
	if left.Effect != right.Effect {
		return left.Effect == EffectSuppress
	}
	return left.ID < right.ID
}

func canonicalHints(input []TransportHint) ([]TransportHint, error) {
	result := append([]TransportHint(nil), input...)
	for _, hint := range result {
		if hint != HintQUIC && hint != HintTCP && hint != HintTLS {
			return nil, fmt.Errorf("unsupported VPN transport hint %q", hint)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return compactOrdered(result), nil
}

func compactStrings(input []string) []string {
	result := append([]string(nil), input...)
	sort.Strings(result)
	return compactOrdered(result)
}

type ordered interface {
	~string | ~uint8 | ~uint16 | ~uint32
}

func compactOrdered[T ordered](input []T) []T {
	result := append([]T(nil), input...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	if len(result) < 2 {
		return result
	}
	write := 1
	for read := 1; read < len(result); read++ {
		if result[read] == result[write-1] {
			continue
		}
		result[write] = result[read]
		write++
	}
	return result[:write]
}

func contains[T ordered](values []T, wanted T) bool {
	index := sort.Search(len(values), func(index int) bool { return values[index] >= wanted })
	return index < len(values) && values[index] == wanted
}

func intersects[T ordered](left, right []T) bool {
	for _, value := range left {
		if contains(right, value) {
			return true
		}
	}
	return false
}

func validRatio(value float64, name string) error {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
		return fmt.Errorf("%s must be between 0 and 1", name)
	}
	return nil
}

func validIP(value netip.Addr) bool {
	return value.IsValid() && value.Zone() == ""
}

func validCountry(value string) bool {
	return len(value) == 2 && value[0] >= 'A' && value[0] <= 'Z' && value[1] >= 'A' && value[1] <= 'Z'
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("._:-", character) {
			continue
		}
		return false
	}
	return true
}
