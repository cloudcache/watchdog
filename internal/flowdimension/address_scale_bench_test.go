// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowdimension

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// Scale certification for the address library compile path (PLAT-04C3): the
// prefix trie build, selector-based set membership at ~50k sets, and the
// worst-case per-address overlap. These measure the real CompileBundle path
// without changing it. Run:
//
//	go test ./internal/flowdimension -run '^$' -bench 'CompileBundle|AddressSet' -benchmem
//	go test ./internal/flowdimension -run TestAddressLibraryScaleRetainedMemory -v
//
// A finding worth recording: CompileBundle caps per-address set membership at
// MaxAddressSetsPerRecord (default 32), so worst-case overlap is bounded by
// design — a single address can never fan out to an unbounded number of sets,
// and a deep include-chain that would exceed 32 is rejected at compile, not at
// query time. BenchmarkCompileAddressSetMaxOverlap measures compile at that
// design ceiling; BenchmarkCompileAddressSetScale measures many mostly-disjoint
// sets (the realistic large-deployment shape).

// buildScalePrefixes returns n deterministically labeled /24 prefixes. Each gets
// a unique "region" label so a selector set can address it disjointly, plus
// lower-cardinality country/city/asn labels.
func buildScalePrefixes(n int) []PrefixDefinition {
	countries := []string{"CN", "US", "JP", "DE", "GB"}
	prefixes := make([]PrefixDefinition, n)
	for i := range prefixes {
		prefixes[i] = PrefixDefinition{
			ID:   fmt.Sprintf("p%d", i),
			CIDR: fmt.Sprintf("%d.%d.%d.0/24", 10+(i>>16)%200, (i>>8)&0xff, i&0xff),
			Labels: map[string]string{
				"country": countries[i%len(countries)],
				"city":    fmt.Sprintf("city-%d", i%100),
				"asn":     fmt.Sprintf("%d", 1000+i%500),
				"region":  fmt.Sprintf("r%d", i),
			},
		}
	}
	return prefixes
}

// buildScaleDisjointSets returns m sets, each selecting one unique region, so
// every prefix belongs to at most one set (expansion 1) — the realistic shape
// of many mostly-disjoint address groups.
func buildScaleDisjointSets(m int) []AddressSetDefinition {
	sets := make([]AddressSetDefinition, m)
	for j := range sets {
		sets[j] = AddressSetDefinition{
			ID:             fmt.Sprintf("s%d", j),
			Selector:       LabelSelector{Labels: map[string][]string{"region": {fmt.Sprintf("r%d", j)}}},
			MatchDirection: "both",
			Enabled:        true,
		}
	}
	return sets
}

func scaleBundle(prefixes []PrefixDefinition, sets []AddressSetDefinition) SnapshotBundle {
	return SnapshotBundle{
		SchemaVersion: 1,
		SnapshotID:    "scale",
		Version:       1,
		EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Prefixes:      prefixes,
		AddressSets:   sets,
	}
}

func BenchmarkCompileBundlePrefixScale(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		bundle := scaleBundle(buildScalePrefixes(n), nil)
		b.Run(fmt.Sprintf("prefixes=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := CompileBundle(bundle, CompileLimits{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCompileAddressSetScale holds the prefix count fixed and varies only
// the set count so it isolates set-compile scaling. The default
// MaxAddressSets ceiling is 10000; the raised-limit rows characterize behavior
// beyond the default so the ceiling is a deliberate choice, not an unmeasured
// cliff.
func BenchmarkCompileAddressSetScale(b *testing.B) {
	const prefixCount = 20_000
	prefixes := buildScalePrefixes(prefixCount)
	for _, tc := range []struct {
		sets  int
		limit int
	}{
		{1_000, 0}, {5_000, 0}, {10_000, 0}, {50_000, 100_000},
	} {
		bundle := scaleBundle(prefixes, buildScaleDisjointSets(tc.sets))
		limits := CompileLimits{MaxAddressSets: tc.limit}
		b.Run(fmt.Sprintf("sets=%d", tc.sets), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := CompileBundle(bundle, limits); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCompileAddressSetMaxOverlap compiles at the per-address membership
// ceiling: every prefix carries 32 shared labels and 32 sets each select one of
// them, so every address fans out to all 32 sets (expansion 32, the default
// MaxAddressSetsPerRecord). This is the worst-case per-record resolution the
// compiler permits.
func BenchmarkCompileAddressSetMaxOverlap(b *testing.B) {
	const overlap = 32
	prefixes := buildScalePrefixes(20_000)
	for i := range prefixes {
		for k := 0; k < overlap; k++ {
			prefixes[i].Labels[fmt.Sprintf("k%d", k)] = "v"
		}
	}
	sets := make([]AddressSetDefinition, overlap)
	for k := range sets {
		sets[k] = AddressSetDefinition{
			ID:             fmt.Sprintf("overlap%d", k),
			Selector:       LabelSelector{Labels: map[string][]string{fmt.Sprintf("k%d", k): {"v"}}},
			MatchDirection: "both",
			Enabled:        true,
		}
	}
	bundle := scaleBundle(prefixes, sets)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := CompileBundle(bundle, CompileLimits{}); err != nil {
			b.Fatal(err)
		}
	}
}

// TestAddressLibraryScaleRetainedMemory records the per-replica heap held by a
// compiled snapshot at scale and trips if it regresses far past the baseline.
func TestAddressLibraryScaleRetainedMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("scale memory baseline is skipped in -short")
	}
	const prefixes, sets = 100_000, 10_000
	bundle := scaleBundle(buildScalePrefixes(prefixes), buildScaleDisjointSets(sets))

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	snapshot, err := CompileBundle(bundle, CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	runtime.KeepAlive(snapshot)

	perPrefix := float64(retained) / float64(prefixes)
	t.Logf("retained heap for %d prefixes + %d sets: %.1f MiB (%.0f B/prefix)",
		prefixes, sets, float64(retained)/(1<<20), perPrefix)
	// Loose tripwire: a compiled /24 entry with a handful of labels and one set
	// membership is well under 4 KiB retained; a large regression (leaked
	// intermediates, per-prefix map blowup) trips this without flapping on
	// allocator noise.
	if retained > 0 && perPrefix > 4096 {
		t.Fatalf("retained heap per prefix %.0f B exceeds 4096 B baseline tripwire", perPrefix)
	}
}
