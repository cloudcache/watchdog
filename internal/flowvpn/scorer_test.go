// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowvpn

import (
	"math"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestScorerCombinesExplainableEvidenceAndCapsScore(t *testing.T) {
	duration := uint64(60_000)
	totalBytes := uint64(1_500)
	flowRecords := uint64(10)
	activeBuckets := uint32(5)
	symmetry := 0.8
	rules := validRuleSet()
	rules.Rules = []Rule{
		{ID: "risk-prefix", Effect: EffectScore, Weight: 40, Match: Match{RemoteASNs: []uint32{64512}, RemotePrefixIDs: []string{"risk-prefix"}}},
		{ID: "port", Effect: EffectScore, Weight: 20, Match: Match{RemotePorts: []uint16{443}}},
		{ID: "long-symmetric", Effect: EffectScore, Weight: 30, Match: Match{
			MinDurationMS: &duration, MinTotalBytes: &totalBytes, MinFlowRecords: &flowRecords,
			MinActiveBuckets: &activeBuckets, MinSymmetryRatio: &symmetry,
		}},
		{ID: "tls", Effect: EffectScore, Weight: 25, Match: Match{TransportHints: []TransportHint{HintTLS}}},
	}
	compiled, err := CompileRuleSet(rules)
	if err != nil {
		t.Fatal(err)
	}
	candidate := validCandidate()
	candidate.LocalToRemoteBytes, candidate.RemoteToLocalBytes = 1_000, 900
	result, err := compiled.Evaluate(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if result.Score != 100 || !result.ScoreCapped || result.Level != RiskCritical || result.Verdict != VerdictProbeCandidate || !result.ProbeRecommended || result.ProbeBlockReason != "" {
		t.Fatalf("result=%+v", result)
	}
	if result.DimensionSnapshotID != candidate.DimensionSnapshotID || result.GeoVersion != candidate.GeoVersion || result.ClassificationVersion != candidate.ClassificationVersion {
		t.Fatalf("candidate provenance was not preserved: %+v", result)
	}
	if result.SymmetryRatio != 0.9 || math.Abs(result.DominanceRatio-0.1) > 1e-12 {
		t.Fatalf("ratios=%+v", result)
	}
	wantRuleIDs := []string{"long-symmetric", "port", "risk-prefix", "tls"}
	gotRuleIDs := make([]string, 0, len(result.Evidence))
	for _, evidence := range result.Evidence {
		gotRuleIDs = append(gotRuleIDs, evidence.RuleID)
	}
	if !reflect.DeepEqual(gotRuleIDs, wantRuleIDs) {
		t.Fatalf("evidence order=%v, want %v", gotRuleIDs, wantRuleIDs)
	}
}

func TestTransportHintIsEvidenceAndIsNotInferredFromPort(t *testing.T) {
	rules := validRuleSet()
	rules.ProbeThreshold = 10
	rules.Rules = []Rule{{ID: "tls", Effect: EffectScore, Weight: 20, Match: Match{TransportHints: []TransportHint{HintTLS}}}}
	compiled, err := CompileRuleSet(rules)
	if err != nil {
		t.Fatal(err)
	}
	candidate := validCandidate()
	candidate.PrimaryRemotePort = 443
	candidate.TransportHints = nil
	withoutHint, err := compiled.Evaluate(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if withoutHint.Score != 0 || len(withoutHint.Evidence) != 0 || withoutHint.ProbeRecommended {
		t.Fatalf("port 443 was incorrectly treated as a TLS hint: %+v", withoutHint)
	}
	candidate.TransportHints = []TransportHint{HintTLS}
	withHint, err := compiled.Evaluate(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if withHint.Score != 20 || len(withHint.Evidence) != 1 || !withHint.ProbeRecommended {
		t.Fatalf("explicit TLS hint was not scored: %+v", withHint)
	}
}

func TestScorerDistinguishesSymmetricAndOneWayBehavior(t *testing.T) {
	symmetricMinimum, dominanceMinimum := 0.8, 0.95
	rules := validRuleSet()
	rules.Rules = []Rule{
		{ID: "one-way", Effect: EffectScore, Weight: 35, Match: Match{MinDominanceRatio: &dominanceMinimum}},
		{ID: "symmetric", Effect: EffectScore, Weight: 25, Match: Match{MinSymmetryRatio: &symmetricMinimum}},
	}
	compiled, err := CompileRuleSet(rules)
	if err != nil {
		t.Fatal(err)
	}
	symmetricCandidate := validCandidate()
	symmetricCandidate.LocalToRemoteBytes, symmetricCandidate.RemoteToLocalBytes = 100, 90
	symmetric, err := compiled.Evaluate(symmetricCandidate)
	if err != nil {
		t.Fatal(err)
	}
	if symmetric.Score != 25 || len(symmetric.Evidence) != 1 || symmetric.Evidence[0].RuleID != "symmetric" {
		t.Fatalf("symmetric=%+v", symmetric)
	}
	oneWayCandidate := validCandidate()
	oneWayCandidate.LocalToRemoteBytes, oneWayCandidate.RemoteToLocalBytes = 100, 0
	oneWay, err := compiled.Evaluate(oneWayCandidate)
	if err != nil {
		t.Fatal(err)
	}
	if oneWay.Score != 35 || len(oneWay.Evidence) != 1 || oneWay.Evidence[0].RuleID != "one-way" || oneWay.DominanceRatio != 1 {
		t.Fatalf("oneWay=%+v", oneWay)
	}
}

func TestTerminalRulePrecedenceIsDeterministic(t *testing.T) {
	rules := validRuleSet()
	rules.Rules = []Rule{
		{ID: "allow-cn", Effect: EffectAllow, Priority: 10, Match: Match{RemoteCountries: []string{"CN"}}},
		{ID: "suppress-asn", Effect: EffectSuppress, Priority: 10, Match: Match{RemoteASNs: []uint32{64512}}},
		{ID: "score", Effect: EffectScore, Weight: 90, Match: Match{RemotePorts: []uint16{443}}},
	}
	compiled, err := CompileRuleSet(rules)
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Evaluate(validCandidate())
	if err != nil {
		t.Fatal(err)
	}
	if result.Verdict != VerdictSuppressed || result.DecisionRuleID != "suppress-asn" || result.ProbeRecommended || result.ProbeBlockReason != "terminal_rule" || result.Score != 90 {
		t.Fatalf("equal-priority terminal result=%+v", result)
	}
	rules.Rules[0].Priority = 11
	higherAllow, err := CompileRuleSet(rules)
	if err != nil {
		t.Fatal(err)
	}
	result, err = higherAllow.Evaluate(validCandidate())
	if err != nil {
		t.Fatal(err)
	}
	if result.Verdict != VerdictAllowlisted || result.DecisionRuleID != "allow-cn" {
		t.Fatalf("higher-priority allow result=%+v", result)
	}
}

func TestIncompleteCandidateCanBeScoredButCannotRecommendProbe(t *testing.T) {
	rules := validRuleSet()
	rules.Rules = []Rule{{ID: "score", Effect: EffectScore, Weight: 90, Match: Match{RemotePorts: []uint16{443}}}}
	compiled, err := CompileRuleSet(rules)
	if err != nil {
		t.Fatal(err)
	}
	candidate := validCandidate()
	candidate.CompleteRatio = 0.79
	result, err := compiled.Evaluate(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if result.Score != 90 || result.Verdict != VerdictReview || result.ProbeRecommended || result.ProbeBlockReason != "incomplete_window" {
		t.Fatalf("result=%+v", result)
	}
}

func TestRiskThresholdBoundaries(t *testing.T) {
	for _, test := range []struct {
		score uint16
		level RiskLevel
	}{
		{29, RiskLow}, {30, RiskMedium}, {60, RiskHigh}, {85, RiskCritical},
	} {
		t.Run(string(test.level), func(t *testing.T) {
			rules := validRuleSet()
			rules.ProbeThreshold = 100
			rules.Rules[0].Weight = test.score
			compiled, err := CompileRuleSet(rules)
			if err != nil {
				t.Fatal(err)
			}
			result, err := compiled.Evaluate(validCandidate())
			if err != nil {
				t.Fatal(err)
			}
			if result.Score != test.score || result.Level != test.level {
				t.Fatalf("score=%d result=%+v", test.score, result)
			}
		})
	}
}

func TestRuleCompilationIsDeterministicAndImmutable(t *testing.T) {
	rules := validRuleSet()
	rules.Rules = []Rule{
		{ID: "b", Effect: EffectScore, Weight: 20, Match: Match{RemotePorts: []uint16{8443, 443, 443}, TransportHints: []TransportHint{HintTLS, HintTCP}}},
		{ID: "a", Effect: EffectScore, Weight: 10, Match: Match{RemoteASNs: []uint32{64513, 64512}}},
	}
	first, err := CompileRuleSet(rules)
	if err != nil {
		t.Fatal(err)
	}
	rules.Rules[0], rules.Rules[1] = rules.Rules[1], rules.Rules[0]
	second, err := CompileRuleSet(rules)
	if err != nil {
		t.Fatal(err)
	}
	result1, err := first.Evaluate(validCandidate())
	if err != nil {
		t.Fatal(err)
	}
	result2, err := second.Evaluate(validCandidate())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result1, result2) {
		t.Fatalf("rule order changed result:\n%+v\n%+v", result1, result2)
	}
	rules.Rules[1].Match.RemotePorts[0] = 1
	afterMutation, err := first.Evaluate(validCandidate())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result1, afterMutation) {
		t.Fatal("compiled rules alias mutable input")
	}
}

func TestCompileRuleSetRejectsInvalidConfiguration(t *testing.T) {
	durationZero := uint64(0)
	bytesZero := uint64(0)
	recordsZero := uint64(0)
	bucketsZero := uint32(0)
	badRatio := math.NaN()
	tests := []struct {
		name  string
		apply func(*RuleSet)
	}{
		{"schema", func(value *RuleSet) { value.SchemaVersion = 2 }},
		{"version", func(value *RuleSet) { value.Version = "bad version" }},
		{"thresholds", func(value *RuleSet) { value.HighThreshold = value.MediumThreshold }},
		{"probe threshold", func(value *RuleSet) { value.ProbeThreshold = 0 }},
		{"completeness", func(value *RuleSet) { value.MinimumCompleteness = math.Inf(1) }},
		{"empty rules", func(value *RuleSet) { value.Rules = nil }},
		{"duplicate ID", func(value *RuleSet) { value.Rules = append(value.Rules, value.Rules[0]) }},
		{"effect", func(value *RuleSet) { value.Rules[0].Effect = "execute" }},
		{"score weight", func(value *RuleSet) { value.Rules[0].Weight = 0 }},
		{"terminal weight", func(value *RuleSet) { value.Rules[0].Effect, value.Rules[0].Weight = EffectAllow, 1 }},
		{"empty match", func(value *RuleSet) { value.Rules[0].Match = Match{} }},
		{"zero port", func(value *RuleSet) { value.Rules[0].Match = Match{RemotePorts: []uint16{0}} }},
		{"zero protocol", func(value *RuleSet) { value.Rules[0].Match = Match{Protocols: []uint8{0}} }},
		{"zero ASN", func(value *RuleSet) { value.Rules[0].Match = Match{RemoteASNs: []uint32{0}} }},
		{"prefix", func(value *RuleSet) { value.Rules[0].Match = Match{RemotePrefixIDs: []string{"bad prefix"}} }},
		{"country", func(value *RuleSet) { value.Rules[0].Match = Match{RemoteCountries: []string{"cn"}} }},
		{"hint", func(value *RuleSet) { value.Rules[0].Match = Match{TransportHints: []TransportHint{"ssh"}} }},
		{"duration", func(value *RuleSet) { value.Rules[0].Match = Match{MinDurationMS: &durationZero} }},
		{"total bytes", func(value *RuleSet) { value.Rules[0].Match = Match{MinTotalBytes: &bytesZero} }},
		{"flow records", func(value *RuleSet) { value.Rules[0].Match = Match{MinFlowRecords: &recordsZero} }},
		{"active buckets", func(value *RuleSet) { value.Rules[0].Match = Match{MinActiveBuckets: &bucketsZero} }},
		{"ratio", func(value *RuleSet) { value.Rules[0].Match = Match{MinSymmetryRatio: &badRatio} }},
		{"signal limit", func(value *RuleSet) { value.Rules[0].Match.RemotePorts = make([]uint16, maxValuesPerSignal+1) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rules := validRuleSet()
			test.apply(&rules)
			if _, err := CompileRuleSet(rules); err == nil {
				t.Fatal("invalid rule set was accepted")
			}
		})
	}
}

func TestEvaluateRejectsInvalidCandidates(t *testing.T) {
	compiled, err := CompileRuleSet(validRuleSet())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		apply func(*Candidate)
	}{
		{"window", func(value *Candidate) { value.WindowEnd = value.WindowStart }},
		{"key", func(value *Candidate) { value.ConversationKey = strings.ToUpper(strings.Repeat("ab", 32)) }},
		{"local IP", func(value *Candidate) { value.LocalIP = netip.Addr{} }},
		{"remote IP", func(value *Candidate) { value.RemoteIP = netip.MustParseAddr("fe80::1%en0") }},
		{"records", func(value *Candidate) { value.FlowRecordCount = 0 }},
		{"buckets", func(value *Candidate) { value.ActiveBucketCount = 0 }},
		{"bucket overflow", func(value *Candidate) { value.ActiveBucketCount = uint32(value.FlowRecordCount + 1) }},
		{"completeness", func(value *Candidate) { value.CompleteRatio = -1 }},
		{"snapshot", func(value *Candidate) { value.DimensionSnapshotID = "bad snapshot" }},
		{"geo version", func(value *Candidate) { value.GeoVersion = "bad version" }},
		{"classification version", func(value *Candidate) { value.ClassificationVersion = 0 }},
		{"prefix", func(value *Candidate) { value.RemotePrefixID = "bad prefix" }},
		{"country", func(value *Candidate) { value.RemoteCountry = "cn" }},
		{"hint", func(value *Candidate) { value.TransportHints = []TransportHint{"ssh"} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := validCandidate()
			test.apply(&candidate)
			if _, err := compiled.Evaluate(candidate); err == nil {
				t.Fatal("invalid candidate was accepted")
			}
		})
	}
	if _, err := (CompiledRuleSet{}).Evaluate(validCandidate()); err == nil {
		t.Fatal("uncompiled rule set was accepted")
	}
}

func validRuleSet() RuleSet {
	return RuleSet{
		SchemaVersion: RuleSchemaV1, Version: "vpn-rules-1",
		MediumThreshold: 30, HighThreshold: 60, CriticalThreshold: 85,
		ProbeThreshold: 70, MinimumCompleteness: 0.8,
		Rules: []Rule{{ID: "port", Effect: EffectScore, Weight: 20, Match: Match{RemotePorts: []uint16{443}}}},
	}
}

func validCandidate() Candidate {
	return Candidate{
		WindowStart:     time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC),
		WindowEnd:       time.Date(2026, 9, 5, 10, 5, 0, 0, time.UTC),
		ConversationKey: strings.Repeat("ab", 32),
		LocalIP:         netip.MustParseAddr("192.0.2.10"), RemoteIP: netip.MustParseAddr("2001:db8::20"),
		PrimaryProtocol: 6, PrimaryLocalPort: 50_000, PrimaryRemotePort: 443,
		LocalToRemoteBytes: 1_000, RemoteToLocalBytes: 900,
		FlowRecordCount: 10, ActiveBucketCount: 5, MaxDurationMS: 120_000,
		RemoteASN: 64512, RemoteCountry: "CN", RemotePrefixID: "risk-prefix",
		TransportHints: []TransportHint{HintTLS, HintTCP}, CompleteRatio: 1,
		DimensionSnapshotID: "snapshot-1", GeoVersion: "geo-1", ClassificationVersion: 1,
	}
}
