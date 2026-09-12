// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowvpn

import "testing"

// TestCanonicalCIDRSegments checks the source/destination CIDR signals normalize
// to masked, deduplicated, sorted network form (so equal segment sets publish to
// identical bytes), reject invalid input, and count as a standalone signal.
func TestCanonicalCIDRSegments(t *testing.T) {
	rule, err := NormalizeRule(Rule{
		ID: "cidr", Effect: EffectScore, Weight: 10,
		Match: Match{RemoteCIDRs: []string{"10.5.0.0/8", "10.0.0.0/8"}, LocalCIDRs: []string{"192.0.2.10/24"}},
	})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if len(rule.Match.RemoteCIDRs) != 1 || rule.Match.RemoteCIDRs[0] != "10.0.0.0/8" {
		t.Fatalf("remote cidrs = %v, want masked+deduped [10.0.0.0/8]", rule.Match.RemoteCIDRs)
	}
	if len(rule.Match.LocalCIDRs) != 1 || rule.Match.LocalCIDRs[0] != "192.0.2.0/24" {
		t.Fatalf("local cidrs = %v, want masked [192.0.2.0/24]", rule.Match.LocalCIDRs)
	}

	if _, err := NormalizeRule(Rule{ID: "bad", Effect: EffectScore, Weight: 10, Match: Match{RemoteCIDRs: []string{"not-a-cidr"}}}); err == nil {
		t.Fatal("invalid CIDR must error")
	}
	// A CIDR-only match is a complete signal on its own.
	if _, err := NormalizeRule(Rule{ID: "only", Effect: EffectScore, Weight: 10, Match: Match{RemoteCIDRs: []string{"203.0.113.0/24"}}}); err != nil {
		t.Fatalf("cidr-only match must be valid: %v", err)
	}
}

// TestScorerMatchesCIDRSegments drives the segment signals through the public
// evaluate path: an in-range segment contributes its weight and records the
// signal; an out-of-range or wrong-family segment does not match.
func TestScorerMatchesCIDRSegments(t *testing.T) {
	build := func(match Match) CompiledRuleSet {
		t.Helper()
		compiled, err := CompileRuleSet(RuleSet{
			SchemaVersion: RuleSchemaV1, Version: "vpn-cidr-1",
			MediumThreshold: 30, HighThreshold: 60, CriticalThreshold: 85, ProbeThreshold: 70, MinimumCompleteness: 0.8,
			Rules: []Rule{{ID: "seg", Effect: EffectScore, Weight: 40, Match: match}},
		})
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return compiled
	}
	hasSignal := func(result Result, signal string) bool {
		for _, evidence := range result.Evidence {
			for _, s := range evidence.Signals {
				if s == signal {
					return true
				}
			}
		}
		return false
	}

	// validCandidate: LocalIP 192.0.2.10 (v4), RemoteIP 2001:db8::20 (v6).
	for _, tc := range []struct {
		name   string
		match  Match
		signal string
		want   bool
	}{
		{"remote in range (v6)", Match{RemoteCIDRs: []string{"2001:db8::/32"}}, "remote_cidr", true},
		{"remote out of range", Match{RemoteCIDRs: []string{"2001:dead::/32"}}, "remote_cidr", false},
		{"remote wrong family (v4 seg vs v6 ip)", Match{RemoteCIDRs: []string{"10.0.0.0/8"}}, "remote_cidr", false},
		{"local in range (v4)", Match{LocalCIDRs: []string{"192.0.2.0/24"}}, "local_cidr", true},
		{"local out of range", Match{LocalCIDRs: []string{"198.51.100.0/24"}}, "local_cidr", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := build(tc.match).Evaluate(validCandidate())
			if err != nil {
				t.Fatalf("evaluate: %v", err)
			}
			if got := hasSignal(result, tc.signal); got != tc.want {
				t.Fatalf("signal %q matched=%v, want %v (score=%d)", tc.signal, got, tc.want, result.Score)
			}
			if tc.want && result.Score != 40 {
				t.Fatalf("in-range segment must contribute weight 40, score=%d", result.Score)
			}
			if !tc.want && result.Score != 0 {
				t.Fatalf("out-of-range segment must contribute nothing, score=%d", result.Score)
			}
		})
	}
}
