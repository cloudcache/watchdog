// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowvpn

import (
	"reflect"
	"testing"
)

func familyRuleSet(t *testing.T, rules []Rule) CompiledRuleSet {
	t.Helper()
	compiled, err := CompileRuleSet(RuleSet{
		SchemaVersion: RuleSchemaV1, Version: "vpn-family-1",
		MediumThreshold: 30, HighThreshold: 60, CriticalThreshold: 85, ProbeThreshold: 70, MinimumCompleteness: 0.8,
		Rules: rules,
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return compiled
}

// TestScorerCollectsFamilyHints: matched hint rules contribute their suspected
// family to the result, deduplicated and sorted; unmatched hint rules do not.
func TestScorerCollectsFamilyHints(t *testing.T) {
	// validCandidate: remote port 443, transport hints [tls, tcp].
	result, err := familyRuleSet(t, []Rule{
		{ID: "trojan-443", Effect: EffectScore, Weight: 20, FamilyHint: FamilyTrojan, Match: Match{RemotePorts: []uint16{443}}},
		{ID: "tls-hint", Effect: EffectScore, Weight: 10, FamilyHint: FamilyTLSUnknown, Match: Match{TransportHints: []TransportHint{HintTLS}}},
		{ID: "socks-1080", Effect: EffectScore, Weight: 15, FamilyHint: FamilySOCKS5, Match: Match{RemotePorts: []uint16{1080}}},
	}).Evaluate(validCandidate())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	// trojan-443 + tls-hint match (443, tls); socks-1080 does not. Sorted: "tls_unknown" < "trojan".
	if want := []ProtocolFamily{FamilyTLSUnknown, FamilyTrojan}; !reflect.DeepEqual(result.FamilyHints, want) {
		t.Fatalf("family hints = %v, want %v", result.FamilyHints, want)
	}
}

// TestFamilyHintDeduplicates: two matched rules hinting the same family yield one.
func TestFamilyHintDeduplicates(t *testing.T) {
	result, err := familyRuleSet(t, []Rule{
		{ID: "a", Effect: EffectScore, Weight: 20, FamilyHint: FamilyTrojan, Match: Match{RemotePorts: []uint16{443}}},
		{ID: "b", Effect: EffectScore, Weight: 10, FamilyHint: FamilyTrojan, Match: Match{TransportHints: []TransportHint{HintTLS}}},
	}).Evaluate(validCandidate())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if want := []ProtocolFamily{FamilyTrojan}; !reflect.DeepEqual(result.FamilyHints, want) {
		t.Fatalf("family hints = %v, want %v", result.FamilyHints, want)
	}
}

// TestFamilyHintValidation: an unsupported family hint is rejected at compile.
func TestFamilyHintValidation(t *testing.T) {
	if _, err := NormalizeRule(Rule{ID: "bad", Effect: EffectScore, Weight: 10, FamilyHint: ProtocolFamily("bogus"), Match: Match{RemotePorts: []uint16{443}}}); err == nil {
		t.Fatal("unsupported family hint must error")
	}
	if _, err := NormalizeRule(Rule{ID: "ok", Effect: EffectScore, Weight: 10, FamilyHint: FamilyShadowsocks, Match: Match{RemotePorts: []uint16{8388}}}); err != nil {
		t.Fatalf("valid family hint must normalize: %v", err)
	}
}
