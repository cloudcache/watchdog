package flowdimension

import (
	"strings"
	"testing"
	"time"
)

func TestClassificationCatalogSelectsImmutableEventTimeProfiles(t *testing.T) {
	definition := testClassificationDefinition(1, testMinute(12, 0), "snapshot-1")
	first, err := CompileClassification(definition)
	if err != nil {
		t.Fatal(err)
	}
	definition.HomeISPIDs[0] = 999
	secondDefinition := testClassificationDefinition(2, testMinute(13, 0), "snapshot-2")
	secondDefinition.HomeCity = "330200"
	second, err := CompileClassification(secondDefinition)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewClassificationCatalog(second, first)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := catalog.Select("tenant-a", testMinute(12, 59))
	if err != nil || selected.Metadata().Version != 1 || selected.Metadata().DimensionSnapshotID != "snapshot-1" {
		t.Fatalf("selected=%+v err=%v", selected.Metadata(), err)
	}
	if category := selected.Classify(DirectionOut, GeoInfo{Country: "CN", AdminCode: "330100", ISPID: 3}); category != CategoryOnNetLocalCity {
		t.Fatalf("immutable profile category = %q", category)
	}
	selected, err = catalog.Select("tenant-a", testMinute(13, 0))
	if err != nil || selected.Metadata().Version != 2 {
		t.Fatalf("selected=%+v err=%v", selected.Metadata(), err)
	}
	if _, err := catalog.Select("tenant-a", testMinute(11, 59)); err != ErrNoClassificationSnapshot {
		t.Fatalf("pre-history error = %v", err)
	}
	if err := catalog.Install(first); err != nil {
		t.Fatalf("same snapshot must be idempotent: %v", err)
	}
}

func TestClassificationPoliciesProduceExplicitDisposition(t *testing.T) {
	definition := testClassificationDefinition(1, testMinute(12, 0), "snapshot-1")
	definition.InternalPolicy = RecordPolicyDrop
	snapshot, err := CompileClassification(definition)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Disposition(DirectionInternal) != DispositionDrop || snapshot.Disposition(DirectionTransit) != DispositionCount || snapshot.Disposition(DirectionOut) != DispositionCount {
		t.Fatalf("unexpected policy disposition: %+v", snapshot.Metadata())
	}
}

func TestCompileClassificationRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ClassificationDefinition)
		want   string
	}{
		{name: "identity", mutate: func(value *ClassificationDefinition) { value.DimensionSnapshotID = "" }, want: "required"},
		{name: "effective-time", mutate: func(value *ClassificationDefinition) { value.EffectiveFrom = value.EffectiveFrom.Add(time.Second) }, want: "minute"},
		{name: "province", mutate: func(value *ClassificationDefinition) { value.HomeProvince = "330100" }, want: "province"},
		{name: "city", mutate: func(value *ClassificationDefinition) { value.HomeCity = "320100" }, want: "inside"},
		{name: "duplicate-isp", mutate: func(value *ClassificationDefinition) { value.HomeISPIDs = []uint16{3, 3} }, want: "unique"},
		{name: "zero-isp", mutate: func(value *ClassificationDefinition) { value.HomeISPIDs = []uint16{0} }, want: "non-zero"},
		{name: "policy", mutate: func(value *ClassificationDefinition) { value.TransitPolicy = "archive" }, want: "count or drop"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := testClassificationDefinition(1, testMinute(12, 0), "snapshot-1")
			test.mutate(&definition)
			if _, err := CompileClassification(definition); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestClassificationCatalogRejectsConflictingTimeline(t *testing.T) {
	first, _ := CompileClassification(testClassificationDefinition(1, testMinute(12, 0), "snapshot-1"))
	catalog, err := NewClassificationCatalog(first)
	if err != nil {
		t.Fatal(err)
	}
	sameVersion, _ := CompileClassification(testClassificationDefinition(1, testMinute(13, 0), "snapshot-2"))
	if err := catalog.Install(sameVersion); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("same version error = %v", err)
	}
	sameTime, _ := CompileClassification(testClassificationDefinition(2, testMinute(12, 0), "snapshot-2"))
	if err := catalog.Install(sameTime); err == nil || !strings.Contains(err.Error(), "effective_from") {
		t.Fatalf("same time error = %v", err)
	}
	nonMonotonic, _ := CompileClassification(testClassificationDefinition(3, testMinute(11, 0), "snapshot-3"))
	if err := catalog.Install(nonMonotonic); err == nil || !strings.Contains(err.Error(), "not monotonic") {
		t.Fatalf("non-monotonic error = %v", err)
	}
}

func testClassificationDefinition(version uint32, effectiveFrom time.Time, dimensionSnapshotID string) ClassificationDefinition {
	return ClassificationDefinition{
		TenantID: "tenant-a", Version: version, EffectiveFrom: effectiveFrom, DimensionSnapshotID: dimensionSnapshotID,
		HomeProvince: "330000", HomeCity: "330100", HomeISPIDs: []uint16{3}, OverseasIncludesHMT: true,
		InternalPolicy: RecordPolicyCount, TransitPolicy: RecordPolicyCount,
	}
}
