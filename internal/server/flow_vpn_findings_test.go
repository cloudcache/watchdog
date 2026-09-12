// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowvpn"
)

func scoredFixture() flowvpn.ScoredCandidate {
	return flowvpn.ScoredCandidate{
		Candidate: flowvpn.Candidate{
			WindowStart:     time.Date(2026, 9, 9, 10, 0, 0, 0, time.UTC),
			WindowEnd:       time.Date(2026, 9, 9, 10, 5, 0, 0, time.UTC),
			ConversationKey: strings.Repeat("ab", 32), LocalIP: netip.MustParseAddr("192.0.2.10"),
			RemoteIP: netip.MustParseAddr("203.0.113.5"), DimensionSnapshotID: "snap-1",
			GeoVersion: "geo-1", ClassificationVersion: 1,
		},
		Score:      flowvpn.Result{Score: 80, Level: flowvpn.RiskHigh, Verdict: flowvpn.VerdictProbeCandidate},
		Generation: 2, GeneratedAt: time.Date(2026, 9, 9, 10, 6, 0, 0, time.UTC),
	}
}

// TestFindingKeyDeterministic: the natural key is stable for identical inputs and
// changes when any key component (window, conversation, version) changes.
func TestFindingKeyDeterministic(t *testing.T) {
	base := scoredFixture()
	key := findingKey(base)
	if len(key) != 64 || key != findingKey(base) {
		t.Fatalf("key is not a stable 64-char hex: %q", key)
	}
	for _, mut := range []func(*flowvpn.ScoredCandidate){
		func(s *flowvpn.ScoredCandidate) { s.Candidate.WindowEnd = s.Candidate.WindowEnd.Add(time.Minute) },
		func(s *flowvpn.ScoredCandidate) { s.Candidate.ConversationKey = strings.Repeat("cd", 32) },
		func(s *flowvpn.ScoredCandidate) { s.Candidate.GeoVersion = "geo-2" },
		func(s *flowvpn.ScoredCandidate) { s.Candidate.ClassificationVersion = 2 },
	} {
		changed := scoredFixture()
		mut(&changed)
		if findingKey(changed) == key {
			t.Fatal("key must change when a natural-key component changes")
		}
	}
}

func TestValidVPNDisposition(t *testing.T) {
	for _, ok := range []string{vpnDispositionUnreviewed, vpnDispositionConfirmed, vpnDispositionFalsePositive, vpnDispositionAllowed, vpnDispositionSuppressed} {
		if !validVPNDisposition(ok) {
			t.Fatalf("%q must be valid", ok)
		}
	}
	if validVPNDisposition("bogus") || validVPNDisposition("") {
		t.Fatal("invalid disposition accepted")
	}
}
