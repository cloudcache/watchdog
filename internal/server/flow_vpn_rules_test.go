// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"testing"

	"github.com/cloudcache/watchdog/internal/flowvpn"
)

func validVPNRule() vpnRule {
	return vpnRule{
		ID: newID(), Name: "Risk HTTPS", Effect: flowvpn.EffectScore, Weight: 25,
		Match: flowvpn.Match{RemotePorts: []uint16{443}}, CreatedBy: "u1", UpdatedBy: "u1",
	}
}

// TestNormalizeVPNRule covers identity/name/kind/status defaults and the flowvpn
// scorer invariants (effect enum, score-weight 1..100, non-score weight 0).
func TestNormalizeVPNRule(t *testing.T) {
	t.Run("valid defaults kind and status", func(t *testing.T) {
		item, err := normalizeVPNRule(validVPNRule())
		if err != nil {
			t.Fatalf("normalize: %v", err)
		}
		if item.Kind != vpnRuleKindPassive || item.Status != vpnRuleStatusDraft || item.RuleSchemaVersion != uint16(flowvpn.RuleSchemaV1) {
			t.Fatalf("normalized = %+v", item)
		}
	})
	for _, tc := range []struct {
		name string
		mut  func(*vpnRule)
	}{
		{"missing actors", func(r *vpnRule) { r.CreatedBy = ""; r.UpdatedBy = "" }},
		{"empty name", func(r *vpnRule) { r.Name = "  " }},
		{"bad kind", func(r *vpnRule) { r.Kind = "bogus" }},
		{"bad status", func(r *vpnRule) { r.Status = "bogus" }},
		{"bad effect", func(r *vpnRule) { r.Effect = "bogus" }},
		{"score weight zero", func(r *vpnRule) { r.Weight = 0 }},
		{"allow with weight", func(r *vpnRule) { r.Effect = flowvpn.EffectAllow; r.Weight = 5 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := validVPNRule()
			tc.mut(&item)
			if _, err := normalizeVPNRule(item); err == nil {
				t.Fatalf("%s: expected error", tc.name)
			}
		})
	}
}

func TestValidVPNRuleKindStatus(t *testing.T) {
	if !validVPNRuleKind(vpnRuleKindIntelligence) || !validVPNRuleKind(vpnRuleKindProbe) || validVPNRuleKind("x") {
		t.Fatal("kind validation")
	}
	if !validVPNRuleStatus(vpnRuleStatusActive) || !validVPNRuleStatus(vpnRuleStatusRetired) || validVPNRuleStatus("x") {
		t.Fatal("status validation")
	}
}
