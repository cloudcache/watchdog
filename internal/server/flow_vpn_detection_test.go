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
	if s.maxCandidates != flowvpn.MaxScoredCandidates {
		t.Fatalf("max candidates = %d", s.maxCandidates)
	}
	// Explicit values are preserved.
	custom := vpnDetectionSettingsFrom(FlowVPNConfig{Enabled: true, WindowSeconds: 60, MaxCandidates: 42})
	if custom.window.Seconds() != 60 || custom.maxCandidates != 42 {
		t.Fatalf("custom settings = %+v", custom)
	}
}
