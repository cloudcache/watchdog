// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import "testing"

func TestAgentSharedTokenMatch(t *testing.T) {
	cases := []struct {
		shared, presented string
		want              bool
	}{
		{"", "", false},
		{"", "x", false},
		{"secret", "", false},
		{"secret", "secret", true},
		{"secret", "  secret  ", true},
		{"secret", "nope", false},
		{"secret", "secre", false},
	}
	for _, tc := range cases {
		if got := agentSharedTokenMatch(tc.shared, tc.presented); got != tc.want {
			t.Fatalf("agentSharedTokenMatch(%q,%q)=%v want %v", tc.shared, tc.presented, got, tc.want)
		}
	}
}

func TestNormalizeAgentRunSummary(t *testing.T) {
	if v, err := normalizeAgentRunSummary(nil); err != nil || v != nil {
		t.Fatalf("empty summary: v=%v err=%v", v, err)
	}
	if v, err := normalizeAgentRunSummary([]byte("  null ")); err != nil || v != nil {
		t.Fatalf("null summary: v=%v err=%v", v, err)
	}
	if v, err := normalizeAgentRunSummary([]byte(`{"records_in":200000,"drops":3,"kafka_lag":12}`)); err != nil || v == nil {
		t.Fatalf("valid object: v=%v err=%v", v, err)
	}
	if _, err := normalizeAgentRunSummary([]byte(`[1,2,3]`)); err == nil {
		t.Fatal("array summary should be rejected")
	}
	if _, err := normalizeAgentRunSummary([]byte(`not json`)); err == nil {
		t.Fatal("non-json summary should be rejected")
	}
	big := make([]byte, 8193)
	for i := range big {
		big[i] = 'a'
	}
	if _, err := normalizeAgentRunSummary(big); err == nil {
		t.Fatal("oversize summary should be rejected")
	}
}
