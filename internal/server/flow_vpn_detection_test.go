// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"testing"

	"github.com/cloudcache/watchdog/internal/flowvpn"
)

// TestVPNDetectionSettingsDefaults: an enabled-but-otherwise-empty config yields
// sensible window/interval/threshold defaults so an operator only sets enabled.
func TestVPNDetectionSettingsDefaults(t *testing.T) {
	s := vpnDetectionSettingsFrom(FlowVPNConfig{Enabled: true})
	if s.window.Seconds() != 300 || s.interval.Seconds() != 300 || s.lag.Seconds() != 120 {
		t.Fatalf("durations = %v/%v/%v", s.window, s.interval, s.lag)
	}
	if s.medium != 30 || s.high != 60 || s.critical != 85 || s.probe != 70 || s.minCompleteness != 0.8 {
		t.Fatalf("thresholds = %+v", s)
	}
	if s.maxCandidates != flowvpn.MaxScoredCandidates {
		t.Fatalf("max candidates = %d", s.maxCandidates)
	}
	// Explicit values are preserved.
	custom := vpnDetectionSettingsFrom(FlowVPNConfig{Enabled: true, WindowSeconds: 60, MediumThreshold: 10, HighThreshold: 20, CriticalThreshold: 30, ProbeThreshold: 25})
	if custom.window.Seconds() != 60 || custom.medium != 10 || custom.critical != 30 {
		t.Fatalf("custom settings = %+v", custom)
	}
}

// TestVPNRuleSetVersionDeterministic: the version is a stable identifier of the
// active rules and changes when the rules change.
func TestVPNRuleSetVersionDeterministic(t *testing.T) {
	rules := []flowvpn.Rule{{ID: "a", Effect: flowvpn.EffectScore, Weight: 20, Match: flowvpn.Match{RemotePorts: []uint16{443}}}}
	v1 := vpnRuleSetVersion(rules)
	if len(v1) == 0 || v1[:3] != "rs-" || v1 != vpnRuleSetVersion(rules) {
		t.Fatalf("version not stable: %q", v1)
	}
	changed := []flowvpn.Rule{{ID: "a", Effect: flowvpn.EffectScore, Weight: 25, Match: flowvpn.Match{RemotePorts: []uint16{443}}}}
	if vpnRuleSetVersion(changed) == v1 {
		t.Fatal("version must change when a rule changes")
	}
}
