package flowworker

import (
	"errors"
	"sync"
	"testing"
)

func TestEnrichmentVersionCatalogSelectsAtomicPairsByEventTime(t *testing.T) {
	dimensionV1 := compileDimension(t, "dimension-1", 1, testMinute(12, 0), nil)
	dimensionV2 := compileDimension(t, "dimension-2", 2, testMinute(14, 0), nil)
	classificationV1 := compileClassification(t, 1, testMinute(12, 0), "dimension-1", "count", "count")
	classificationV2 := compileClassification(t, 2, testMinute(13, 0), "dimension-1", "count", "count")
	classificationV3 := compileClassification(t, 3, testMinute(14, 0), "dimension-2", "count", "drop")
	catalog, err := NewEnrichmentVersionCatalog(
		EnrichmentVersion{Dimension: dimensionV2, Classification: classificationV3},
		EnrichmentVersion{Dimension: dimensionV1, Classification: classificationV1},
		EnrichmentVersion{Dimension: dimensionV1, Classification: classificationV2},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		hour, minute          int
		dimensionVersion      uint64
		classificationVersion uint32
	}{
		{12, 59, 1, 1},
		{13, 0, 1, 2},
		{14, 0, 2, 3},
	} {
		version, err := catalog.Select(testMinute(test.hour, test.minute))
		if err != nil {
			t.Fatal(err)
		}
		metadata := version.Metadata()
		if metadata.DimensionVersion != test.dimensionVersion || metadata.ClassificationVersion != test.classificationVersion {
			t.Fatalf("selected = %+v", metadata)
		}
	}
	if _, err := catalog.Select(testMinute(11, 59)); !errors.Is(err, ErrNoEnrichmentVersion) {
		t.Fatalf("pre-history error = %v", err)
	}
}

func TestEnrichmentVersionCatalogRejectsInvalidOrNonMonotonicPairs(t *testing.T) {
	dimensionV1 := compileDimension(t, "dimension-1", 1, testMinute(12, 0), nil)
	dimensionV2 := compileDimension(t, "dimension-2", 2, testMinute(13, 0), nil)
	classificationV1 := compileClassification(t, 1, testMinute(12, 0), "dimension-1", "count", "count")
	catalog, err := NewEnrichmentVersionCatalog(EnrichmentVersion{Dimension: dimensionV1, Classification: classificationV1})
	if err != nil {
		t.Fatal(err)
	}
	mismatch := compileClassification(t, 2, testMinute(13, 0), "dimension-other", "count", "count")
	if err := catalog.Install(EnrichmentVersion{Dimension: dimensionV2, Classification: mismatch}); err == nil {
		t.Fatal("mismatched dimension reference was accepted")
	}
	classificationV2 := compileClassification(t, 2, testMinute(13, 0), "dimension-2", "count", "count")
	if err := catalog.Install(EnrichmentVersion{Dimension: dimensionV2, Classification: classificationV2}); err != nil {
		t.Fatal(err)
	}
	lateOldDimension := compileClassification(t, 3, testMinute(14, 0), "dimension-1", "count", "count")
	if err := catalog.Install(EnrichmentVersion{Dimension: dimensionV1, Classification: lateOldDimension}); err == nil {
		t.Fatal("dimension version rollback was accepted")
	}
	// A new dimension may take effect at a classification boundary later than the
	// object's baked build time: the object's EffectiveFrom is a build artifact
	// and does not gate the pair (decoupled from publish timing). Version
	// monotonicity still applies.
	dimensionV3 := compileDimension(t, "dimension-3", 3, testMinute(14, 0), nil)
	delayedClassification := compileClassification(t, 3, testMinute(15, 0), "dimension-3", "count", "count")
	if err := catalog.Install(EnrichmentVersion{Dimension: dimensionV3, Classification: delayedClassification}); err != nil {
		t.Fatalf("new dimension at a later classification boundary must be accepted: %v", err)
	}
	if selected, err := catalog.Select(testMinute(15, 0)); err != nil || selected.Metadata().DimensionVersion != 3 {
		t.Fatalf("v3 pair not selectable after decoupled install: err=%v meta=%+v", err, selected.Metadata())
	}
}

func TestEnrichmentVersionCatalogAllowsOlderDimensionAtRetainedHistoryHorizon(t *testing.T) {
	dimension := compileDimension(t, "dimension-1", 1, testMinute(12, 0), nil)
	classification := compileClassification(t, 1, testMinute(13, 0), "dimension-1", "count", "count")
	catalog, err := NewEnrichmentVersionCatalog(EnrichmentVersion{Dimension: dimension, Classification: classification})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Select(testMinute(12, 59)); !errors.Is(err, ErrNoEnrichmentVersion) {
		t.Fatalf("pre-horizon selection error=%v", err)
	}
	selected, err := catalog.Select(testMinute(13, 0))
	if err != nil {
		t.Fatal(err)
	}
	if metadata := selected.Metadata(); metadata.DimensionVersion != 1 || metadata.ClassificationVersion != 1 {
		t.Fatalf("retained horizon pair=%+v", metadata)
	}
}

func TestEnrichmentVersionCatalogReadersNeverObserveMixedPair(t *testing.T) {
	dimensionV1 := compileDimension(t, "dimension-1", 1, testMinute(12, 0), nil)
	classificationV1 := compileClassification(t, 1, testMinute(12, 0), "dimension-1", "count", "count")
	catalog, err := NewEnrichmentVersionCatalog(EnrichmentVersion{Dimension: dimensionV1, Classification: classificationV1})
	if err != nil {
		t.Fatal(err)
	}
	dimensionV2 := compileDimension(t, "dimension-2", 2, testMinute(13, 0), nil)
	classificationV2 := compileClassification(t, 2, testMinute(13, 0), "dimension-2", "count", "count")
	var readers sync.WaitGroup
	start := make(chan struct{})
	for index := 0; index < 8; index++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for iteration := 0; iteration < 1_000; iteration++ {
				version, selectErr := catalog.Select(testMinute(13, 30))
				if selectErr != nil {
					t.Errorf("select: %v", selectErr)
					return
				}
				if version.Dimension.Metadata().SnapshotID != version.Classification.Metadata().DimensionSnapshotID {
					t.Errorf("mixed pair = %+v", version.Metadata())
					return
				}
			}
		}()
	}
	close(start)
	if err := catalog.Install(EnrichmentVersion{Dimension: dimensionV2, Classification: classificationV2}); err != nil {
		t.Fatal(err)
	}
	readers.Wait()
}
