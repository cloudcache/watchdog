// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowvpn

import "testing"

func localFeatureRuleSet(t *testing.T, match Match) CompiledRuleSet {
	t.Helper()
	compiled, err := CompileRuleSet(RuleSet{
		SchemaVersion: RuleSchemaV1, Version: "vpn-local-1",
		MediumThreshold: 30, HighThreshold: 60, CriticalThreshold: 85, ProbeThreshold: 70, MinimumCompleteness: 0.8,
		Rules: []Rule{{ID: "sig", Effect: EffectScore, Weight: 40, Match: match}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return compiled
}

func evidenceHasSignal(result Result, signal string) bool {
	for _, evidence := range result.Evidence {
		for _, s := range evidence.Signals {
			if s == signal {
				return true
			}
		}
	}
	return false
}

// TestScorerMatchesLocalTupleAndPacketSize drives the new Local 5-tuple and
// packet-size signals through the public evaluate path.
func TestScorerMatchesLocalTupleAndPacketSize(t *testing.T) {
	// validCandidate: PrimaryLocalPort 50000; enrich with local prefix + a small
	// median packet size to exercise the new inputs.
	cand := validCandidate()
	cand.LocalPrefixID = "office-lan"
	cand.PacketBytesP50 = 90

	for _, tc := range []struct {
		name   string
		match  Match
		signal string
		want   bool
	}{
		{"local port hit", Match{LocalPorts: []uint16{50_000}}, "local_port", true},
		{"local port miss", Match{LocalPorts: []uint16{1234}}, "local_port", false},
		{"local prefix hit", Match{LocalPrefixIDs: []string{"office-lan"}}, "local_prefix", true},
		{"local prefix miss", Match{LocalPrefixIDs: []string{"other-lan"}}, "local_prefix", false},
		{"packet floor hit", Match{MinPacketBytesP50: u64(50)}, "packet_bytes_p50", true},
		{"packet floor miss", Match{MinPacketBytesP50: u64(200)}, "packet_bytes_p50", false},
		{"packet ceiling hit (small packets)", Match{MaxPacketBytesP50: u64(100)}, "packet_bytes_p50", true},
		{"packet ceiling miss", Match{MaxPacketBytesP50: u64(50)}, "packet_bytes_p50", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := localFeatureRuleSet(t, tc.match).Evaluate(cand)
			if err != nil {
				t.Fatalf("evaluate: %v", err)
			}
			if got := evidenceHasSignal(result, tc.signal); got != tc.want {
				t.Fatalf("signal %q matched=%v want %v (score=%d)", tc.signal, got, tc.want, result.Score)
			}
		})
	}
}

// TestPacketSizeCeilingFailsClosedOnUnknown: when the materializer has not yet
// populated the packet size (0 = unknown), a "small packets" ceiling rule must
// not fire — we never confirm a feature we lack data for.
func TestPacketSizeCeilingFailsClosedOnUnknown(t *testing.T) {
	cand := validCandidate() // PacketBytesP50 defaults to 0 (unknown)
	result, err := localFeatureRuleSet(t, Match{MaxPacketBytesP50: u64(100)}).Evaluate(cand)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if evidenceHasSignal(result, "packet_bytes_p50") || result.Score != 0 {
		t.Fatalf("unknown packet size must not match a ceiling rule (score=%d)", result.Score)
	}
}

// TestLocalFeatureCanonicalization checks the new signals' validation and that a
// Local-only match is a complete signal.
func TestLocalFeatureCanonicalization(t *testing.T) {
	if _, err := NormalizeRule(Rule{ID: "p", Effect: EffectScore, Weight: 10, Match: Match{LocalPorts: []uint16{0}}}); err == nil {
		t.Fatal("zero local port must error")
	}
	if _, err := NormalizeRule(Rule{ID: "x", Effect: EffectScore, Weight: 10, Match: Match{LocalPrefixIDs: []string{"bad id!"}}}); err == nil {
		t.Fatal("invalid local prefix id must error")
	}
	if _, err := NormalizeRule(Rule{ID: "r", Effect: EffectScore, Weight: 10, Match: Match{MinPacketBytesP50: u64(100), MaxPacketBytesP50: u64(50)}}); err == nil {
		t.Fatal("min>max packet bytes must error")
	}
	if _, err := NormalizeRule(Rule{ID: "z", Effect: EffectScore, Weight: 10, Match: Match{MinPacketBytesP50: u64(0)}}); err == nil {
		t.Fatal("zero min packet bytes must error")
	}
	if _, err := NormalizeRule(Rule{ID: "ok", Effect: EffectScore, Weight: 10, Match: Match{LocalPorts: []uint16{1080}}}); err != nil {
		t.Fatalf("local-port-only match must be valid: %v", err)
	}
}

func u64(v uint64) *uint64 { return &v }
