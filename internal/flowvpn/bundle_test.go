// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowvpn

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

func TestRuleSetBundleCanonicalRoundTrip(t *testing.T) {
	bundle := testRuleSetBundle()
	bundle.Rules = []PublishedRule{
		{ID: "rule-z", Name: "Allow different ASN", Kind: RuleKindIntelligence, Effect: EffectAllow, Priority: 2, Match: Match{RemoteASNs: []uint32{64513}}},
		{ID: "rule-a", Name: "  Risk HTTPS  ", Kind: RuleKindPassive, Effect: EffectScore, Weight: 25, Priority: 1, Match: Match{RemotePorts: []uint16{8443, 443, 443}}},
	}
	data, checksum, err := EncodeRuleSetBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Index(string(data), `"id":"rule-a"`) > strings.Index(string(data), `"id":"rule-z"`) ||
		!strings.Contains(string(data), `"name":"Risk HTTPS"`) || !strings.Contains(string(data), `"remote_ports":[443,8443]`) {
		t.Fatalf("bundle is not canonical: %s", data)
	}
	compiled, metadata, err := DecodeAndCompileRuleSetBundle(data, checksum)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.SnapshotID != bundle.SnapshotID || metadata.Version != 7 || metadata.RuleCount != 2 || metadata.Checksum != checksum {
		t.Fatalf("metadata = %+v", metadata)
	}
	result, err := compiled.Evaluate(validCandidate())
	if err != nil {
		t.Fatal(err)
	}
	if result.RuleSetVersion != bundle.SnapshotID || result.Score != 25 || result.Verdict != VerdictReview {
		t.Fatalf("result = %+v", result)
	}
}

func TestRuleSetBundlePreservesFamilyHint(t *testing.T) {
	bundle := testRuleSetBundle()
	bundle.Rules[0].FamilyHint = FamilyTrojan
	data, checksum, err := EncodeRuleSetBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	compiled, _, err := DecodeAndCompileRuleSetBundle(data, checksum)
	if err != nil {
		t.Fatal(err)
	}
	if compiled.rules[0].rule.FamilyHint != FamilyTrojan {
		t.Fatalf("family hint was not preserved: %q", compiled.rules[0].rule.FamilyHint)
	}
}

func TestRuleSetBundleRejectsInvalidWireAndConfiguration(t *testing.T) {
	valid, _, err := EncodeRuleSetBundle(testRuleSetBundle())
	if err != nil {
		t.Fatal(err)
	}
	badChecksum := "sha256:" + strings.Repeat("0", 64)
	if _, _, err := DecodeAndCompileRuleSetBundle(valid, badChecksum); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("checksum error = %v", err)
	}
	unknown := append(append([]byte(nil), valid[:len(valid)-1]...), []byte(`,"unknown":true}`)...)
	if _, _, err := DecodeAndCompileRuleSetBundle(unknown, checksumForRuleSetTest(unknown)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error = %v", err)
	}
	nonCanonical := append([]byte(" "), valid...)
	if _, _, err := DecodeAndCompileRuleSetBundle(nonCanonical, checksumForRuleSetTest(nonCanonical)); err == nil || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("canonical error = %v", err)
	}
	trailing := append(append([]byte(nil), valid...), []byte(` {}`)...)
	if _, _, err := DecodeAndCompileRuleSetBundle(trailing, checksumForRuleSetTest(trailing)); err == nil || !strings.Contains(err.Error(), "one JSON object") {
		t.Fatalf("trailing error = %v", err)
	}
	oversized := bytes.Repeat([]byte("x"), MaxRuleSetBundleBytes+1)
	if _, _, err := DecodeAndCompileRuleSetBundle(oversized, checksumForRuleSetTest(oversized)); err == nil || !strings.Contains(err.Error(), "bundle size") {
		t.Fatalf("size error = %v", err)
	}

	for name, mutate := range map[string]func(*RuleSetBundle){
		"schema":   func(bundle *RuleSetBundle) { bundle.SchemaVersion = 2 },
		"identity": func(bundle *RuleSetBundle) { bundle.SnapshotID = "" },
		"version":  func(bundle *RuleSetBundle) { bundle.Version = 0 },
		"timezone": func(bundle *RuleSetBundle) {
			bundle.EffectiveFrom = bundle.EffectiveFrom.In(time.FixedZone("west", -3600))
		},
		"minute":         func(bundle *RuleSetBundle) { bundle.EffectiveFrom = bundle.EffectiveFrom.Add(time.Second) },
		"thresholds":     func(bundle *RuleSetBundle) { bundle.HighThreshold = bundle.MediumThreshold },
		"empty rules":    func(bundle *RuleSetBundle) { bundle.Rules = nil },
		"empty name":     func(bundle *RuleSetBundle) { bundle.Rules[0].Name = " " },
		"invalid kind":   func(bundle *RuleSetBundle) { bundle.Rules[0].Kind = "future" },
		"duplicate rule": func(bundle *RuleSetBundle) { bundle.Rules = append(bundle.Rules, bundle.Rules[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			bundle := testRuleSetBundle()
			mutate(&bundle)
			if _, _, err := EncodeRuleSetBundle(bundle); err == nil {
				t.Fatal("expected bundle rejection")
			}
		})
	}
}

func testRuleSetBundle() RuleSetBundle {
	return RuleSetBundle{
		SchemaVersion: RuleSetBundleSchemaV1, SnapshotID: "snapshot-vpn-7",
		Version: 7, EffectiveFrom: time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC),
		MediumThreshold: 20, HighThreshold: 50, CriticalThreshold: 80,
		ProbeThreshold: 60, MinimumCompleteness: 0.9,
		Rules: []PublishedRule{{
			ID: "rule-score", Name: "TLS risk", Kind: RuleKindPassive,
			Effect: EffectScore, Weight: 25, Match: Match{RemotePorts: []uint16{443}},
		}},
	}
}

func checksumForRuleSetTest(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}
