package flowplan

import (
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"
)

func TestCatalogResolvesExactImmutableRevision(t *testing.T) {
	now := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	first := compileCatalogPlan(t, now, 1, "exporter-v1")
	second := compileCatalogPlan(t, now, 2, "exporter-v2")
	catalog, err := NewCatalog(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Install(second); err != nil {
		t.Fatal(err)
	}
	for revision, exporter := range map[uint64]string{1: "exporter-v1", 2: "exporter-v2"} {
		binding, err := catalog.Resolve("collector-a", revision, ProtocolNetFlow9, netip.MustParseAddr("192.0.2.10"), 42)
		if err != nil || binding.ExporterID != exporter {
			t.Fatalf("revision %d binding=%+v error=%v", revision, binding, err)
		}
	}
	if _, err := catalog.Resolve("collector-a", 3, ProtocolNetFlow9, netip.MustParseAddr("192.0.2.10"), 42); !errors.Is(err, ErrRegistryVersionUnavailable) {
		t.Fatalf("missing revision error=%v", err)
	}
	if err := catalog.Install(first); err != nil {
		t.Fatalf("idempotent install: %v", err)
	}
	conflict := compileCatalogPlan(t, now, 1, "different")
	if err := catalog.Install(conflict); !errors.Is(err, ErrRegistryVersionConflict) {
		t.Fatalf("conflict error=%v", err)
	}
}

func TestCatalogConcurrentResolveAndInstall(t *testing.T) {
	now := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	catalog, err := NewCatalog(compileCatalogPlan(t, now, 1, "exporter-v1"))
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 100 {
				if _, err := catalog.Resolve("collector-a", 1, ProtocolNetFlow9, netip.MustParseAddr("192.0.2.10"), 42); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	if err := catalog.Install(compileCatalogPlan(t, now, 2, "exporter-v2")); err != nil {
		t.Fatal(err)
	}
	wait.Wait()
}

func compileCatalogPlan(t testing.TB, now time.Time, revision uint64, exporterID string) *Registry {
	t.Helper()
	domain := uint64(42)
	registry, err := CompilePlan(Plan{
		SchemaVersion: 2, Revision: revision, CollectorID: "collector-a", NotBefore: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
		Sources: []SourceBinding{{
			Protocol: ProtocolNetFlow9, SourcePrefix: "192.0.2.0/24", ObservationDomainID: &domain,
			TenantID: "tenant-a", ExporterID: exporterID, TargetID: "target-a", OwnershipEpoch: 1,
			SamplingMode: SamplingModeSampled, Enabled: true,
		}},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}
