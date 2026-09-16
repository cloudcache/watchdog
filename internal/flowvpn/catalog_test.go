// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowvpn

import (
	"testing"
	"time"
)

func TestRuleSetCatalogKeepsLastKnownGood(t *testing.T) {
	first, firstMetadata := testPublishedRuleSet(t, "vpn-rules-1", 1, 20)
	catalog := NewRuleSetCatalog()
	if err := catalog.Install(first, firstMetadata); err != nil {
		t.Fatal(err)
	}

	// The same immutable snapshot is idempotent, but a changed checksum for the
	// same identity is corruption and must not replace the installed scorer.
	conflict := firstMetadata
	conflict.Checksum = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if err := catalog.Install(first, conflict); err == nil {
		t.Fatal("expected checksum conflict")
	}
	current, ok := catalog.Current()
	if !ok || current.Metadata.SnapshotID != firstMetadata.SnapshotID || current.Metadata.Checksum != firstMetadata.Checksum {
		t.Fatalf("last-known-good publication changed: %+v", current.Metadata)
	}

	second, secondMetadata := testPublishedRuleSet(t, "vpn-rules-2", 2, 30)
	if err := catalog.Install(second, secondMetadata); err != nil {
		t.Fatal(err)
	}
	current, ok = catalog.Current()
	if !ok || current.Metadata.SnapshotID != secondMetadata.SnapshotID {
		t.Fatalf("new verified publication was not installed: %+v", current.Metadata)
	}
	catalog.Restore(InstalledRuleSet{Rules: first, Metadata: firstMetadata}, true)
	current, ok = catalog.Current()
	if !ok || current.Metadata.SnapshotID != firstMetadata.SnapshotID {
		t.Fatalf("last-known-good publication was not restored: %+v", current.Metadata)
	}
	catalog.Restore(InstalledRuleSet{}, false)
	if _, ok := catalog.Current(); ok {
		t.Fatal("empty catalog was not restored")
	}
}

func testPublishedRuleSet(t *testing.T, snapshotID string, version uint64, weight uint16) (CompiledRuleSet, RuleSetBundleMetadata) {
	t.Helper()
	data, checksum, err := EncodeRuleSetBundle(RuleSetBundle{
		SchemaVersion: RuleSetBundleSchemaV1, SnapshotID: snapshotID, Version: version,
		EffectiveFrom: time.Unix(60, 0).UTC(), MediumThreshold: 30, HighThreshold: 60,
		CriticalThreshold: 85, ProbeThreshold: 70, MinimumCompleteness: 0.8,
		Rules: []PublishedRule{{
			ID: "tls-tunnel", Name: "TLS tunnel", Kind: RuleKindPassive,
			Effect: EffectScore, Weight: weight, Match: Match{RemotePorts: []uint16{443}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	rules, metadata, err := DecodeAndCompileRuleSetBundle(data, checksum)
	if err != nil {
		t.Fatal(err)
	}
	return rules, metadata
}
