// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowvpn

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

const (
	RuleSetBundleSchemaV1 = uint32(1)
	MaxRuleSetBundleBytes = 4 << 20
)

type RuleKind string

const (
	RuleKindPassive      RuleKind = "passive"
	RuleKindIntelligence RuleKind = "intelligence"
	RuleKindProbe        RuleKind = "probe"
)

// PublishedRule preserves the human explanation together with the executable
// rule. Findings can resolve historical labels from the exact producing
// bundle instead of consulting mutable drafts.
type PublishedRule struct {
	ID       string     `json:"id"`
	Name     string     `json:"name"`
	Kind     RuleKind   `json:"kind"`
	Effect   RuleEffect `json:"effect"`
	Weight   uint16     `json:"weight"`
	Priority uint16     `json:"priority"`
	Match    Match      `json:"match"`
}

// RuleSetBundle is the immutable wire object installed by workers. SnapshotID
// is the rule-set version written into findings; Version is the monotonic
// publication sequence.
type RuleSetBundle struct {
	SchemaVersion       uint32          `json:"schema_version"`
	SnapshotID          string          `json:"snapshot_id"`
	Version             uint64          `json:"version"`
	EffectiveFrom       time.Time       `json:"effective_from"`
	MediumThreshold     uint16          `json:"medium_threshold"`
	HighThreshold       uint16          `json:"high_threshold"`
	CriticalThreshold   uint16          `json:"critical_threshold"`
	ProbeThreshold      uint16          `json:"probe_threshold"`
	MinimumCompleteness float64         `json:"minimum_completeness"`
	Rules               []PublishedRule `json:"rules"`
}

type RuleSetBundleMetadata struct {
	SchemaVersion uint32
	SnapshotID    string
	Version       uint64
	EffectiveFrom time.Time
	Checksum      string
	RuleCount     int
}

// EncodeRuleSetBundle returns the sole canonical JSON representation accepted
// by DecodeAndCompileRuleSetBundle.
func EncodeRuleSetBundle(bundle RuleSetBundle) ([]byte, string, error) {
	canonical, _, err := canonicalRuleSetBundle(bundle)
	if err != nil {
		return nil, "", err
	}
	data, err := json.Marshal(canonical)
	if err != nil {
		return nil, "", err
	}
	if len(data) > MaxRuleSetBundleBytes {
		return nil, "", fmt.Errorf("VPN rule-set bundle exceeds %d bytes", MaxRuleSetBundleBytes)
	}
	digest := sha256.Sum256(data)
	return data, "sha256:" + hex.EncodeToString(digest[:]), nil
}

// DecodeAndCompileRuleSetBundle verifies storage integrity and the exact wire
// schema before returning an immutable scorer. It rejects semantically valid
// but non-canonical JSON so signatures and preview digests have one meaning.
func DecodeAndCompileRuleSetBundle(data []byte, expectedChecksum string) (CompiledRuleSet, RuleSetBundleMetadata, error) {
	if len(data) == 0 || len(data) > MaxRuleSetBundleBytes {
		return CompiledRuleSet{}, RuleSetBundleMetadata{}, fmt.Errorf("VPN rule-set bundle size must be 1..%d bytes", MaxRuleSetBundleBytes)
	}
	want, err := parseRuleSetChecksum(expectedChecksum)
	if err != nil {
		return CompiledRuleSet{}, RuleSetBundleMetadata{}, err
	}
	got := sha256.Sum256(data)
	if got != want {
		return CompiledRuleSet{}, RuleSetBundleMetadata{}, errors.New("VPN rule-set bundle checksum mismatch")
	}
	var bundle RuleSetBundle
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return CompiledRuleSet{}, RuleSetBundleMetadata{}, fmt.Errorf("decode VPN rule-set bundle: %w", err)
	}
	if err := ensureRuleSetJSONEOF(decoder); err != nil {
		return CompiledRuleSet{}, RuleSetBundleMetadata{}, fmt.Errorf("decode VPN rule-set bundle: %w", err)
	}
	canonical, compiled, err := canonicalRuleSetBundle(bundle)
	if err != nil {
		return CompiledRuleSet{}, RuleSetBundleMetadata{}, err
	}
	canonicalData, err := json.Marshal(canonical)
	if err != nil {
		return CompiledRuleSet{}, RuleSetBundleMetadata{}, err
	}
	if !bytes.Equal(data, canonicalData) {
		return CompiledRuleSet{}, RuleSetBundleMetadata{}, errors.New("VPN rule-set bundle is not canonical JSON")
	}
	return compiled, RuleSetBundleMetadata{
		SchemaVersion: canonical.SchemaVersion, SnapshotID: canonical.SnapshotID,
		Version:       canonical.Version,
		EffectiveFrom: canonical.EffectiveFrom, Checksum: expectedChecksum,
		RuleCount: len(canonical.Rules),
	}, nil
}

func canonicalRuleSetBundle(bundle RuleSetBundle) (RuleSetBundle, CompiledRuleSet, error) {
	if bundle.SchemaVersion != RuleSetBundleSchemaV1 {
		return RuleSetBundle{}, CompiledRuleSet{}, fmt.Errorf("unsupported VPN rule-set bundle schema version %d", bundle.SchemaVersion)
	}
	if !validIdentifier(bundle.SnapshotID) || bundle.Version == 0 {
		return RuleSetBundle{}, CompiledRuleSet{}, errors.New("VPN rule-set bundle identity and version are required")
	}
	effective := bundle.EffectiveFrom.UTC()
	_, offset := bundle.EffectiveFrom.Zone()
	if effective.IsZero() || offset != 0 || effective.Second() != 0 || effective.Nanosecond() != 0 {
		return RuleSetBundle{}, CompiledRuleSet{}, errors.New("VPN rule-set bundle effective_from must be a UTC minute boundary")
	}
	canonicalRules := make([]PublishedRule, len(bundle.Rules))
	for index, rule := range bundle.Rules {
		canonical, err := normalizePublishedRule(rule)
		if err != nil {
			return RuleSetBundle{}, CompiledRuleSet{}, err
		}
		canonicalRules[index] = canonical
	}
	sort.Slice(canonicalRules, func(i, j int) bool { return canonicalRules[i].ID < canonicalRules[j].ID })
	executableRules := make([]Rule, len(canonicalRules))
	for index, rule := range canonicalRules {
		executableRules[index] = Rule{
			ID: rule.ID, Effect: rule.Effect, Weight: rule.Weight,
			Priority: rule.Priority, Match: rule.Match,
		}
	}
	bundle.EffectiveFrom = effective
	bundle.Rules = canonicalRules
	compiled, err := CompileRuleSet(RuleSet{
		SchemaVersion: RuleSchemaV1, Version: bundle.SnapshotID,
		MediumThreshold: bundle.MediumThreshold, HighThreshold: bundle.HighThreshold,
		CriticalThreshold: bundle.CriticalThreshold, ProbeThreshold: bundle.ProbeThreshold,
		MinimumCompleteness: bundle.MinimumCompleteness, Rules: executableRules,
	})
	if err != nil {
		return RuleSetBundle{}, CompiledRuleSet{}, err
	}
	return bundle, compiled, nil
}

func normalizePublishedRule(rule PublishedRule) (PublishedRule, error) {
	rule.Name = strings.TrimSpace(rule.Name)
	if rule.Name == "" || len(rule.Name) > 190 {
		return PublishedRule{}, fmt.Errorf("VPN rule %q name must contain 1..190 characters", rule.ID)
	}
	if rule.Kind != RuleKindPassive && rule.Kind != RuleKindIntelligence && rule.Kind != RuleKindProbe {
		return PublishedRule{}, fmt.Errorf("VPN rule %q has an unsupported kind", rule.ID)
	}
	canonical, err := NormalizeRule(Rule{
		ID: rule.ID, Effect: rule.Effect, Weight: rule.Weight,
		Priority: rule.Priority, Match: rule.Match,
	})
	if err != nil {
		return PublishedRule{}, err
	}
	rule.ID, rule.Effect, rule.Weight = canonical.ID, canonical.Effect, canonical.Weight
	rule.Priority, rule.Match = canonical.Priority, canonical.Match
	return rule, nil
}

func parseRuleSetChecksum(value string) ([sha256.Size]byte, error) {
	var checksum [sha256.Size]byte
	if len(value) != len("sha256:")+hex.EncodedLen(len(checksum)) || !strings.HasPrefix(value, "sha256:") || value != strings.ToLower(value) {
		return checksum, errors.New("VPN rule-set checksum must be canonical sha256:<lowerhex>")
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	if err != nil || len(decoded) != len(checksum) {
		return checksum, errors.New("VPN rule-set checksum must be canonical sha256:<lowerhex>")
	}
	copy(checksum[:], decoded)
	return checksum, nil
}

func ensureRuleSetJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return errors.New("bundle must contain one JSON object")
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
