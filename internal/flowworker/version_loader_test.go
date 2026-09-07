package flowworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

type memoryVersionObjectSource struct {
	objects map[string][]byte
	calls   []string
}

func (s *memoryVersionObjectSource) Fetch(_ context.Context, objectRef string, _ int) ([]byte, error) {
	s.calls = append(s.calls, objectRef)
	data, exists := s.objects[objectRef]
	if !exists {
		return nil, errors.New("not found")
	}
	return data, nil
}

type recordingVersionAcknowledger struct {
	failures int
	acks     []EnrichmentVersionAcknowledgement
}

func (a *recordingVersionAcknowledger) Acknowledge(_ context.Context, acknowledgement EnrichmentVersionAcknowledgement) error {
	a.acks = append(a.acks, acknowledgement)
	if a.failures > 0 {
		a.failures--
		return errors.New("temporary failure")
	}
	return nil
}

func TestVersionLoaderCompilesPublishesAndAcknowledgesExactPair(t *testing.T) {
	publication, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-1")
	catalog, _ := NewEnrichmentVersionCatalog()
	acks := &recordingVersionAcknowledger{}
	loader, err := NewVersionLoader(source, acks, catalog, VersionWorkerIdentity{
		WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3",
	}, VersionLoaderLimits{})
	if err != nil {
		t.Fatal(err)
	}
	installedAt := testMinute(12, 5)
	loader.now = func() time.Time { return installedAt }
	if err := loader.Install(context.Background(), publication); err != nil {
		t.Fatal(err)
	}
	version, err := catalog.Select("tenant-a", testMinute(12, 30))
	if err != nil {
		t.Fatal(err)
	}
	metadata := version.Metadata()
	if metadata.DimensionSnapshotID != publication.DimensionSnapshotID || metadata.DimensionChecksum != publication.Dimension.Checksum || metadata.ClassificationChecksum != publication.Classification.Checksum {
		t.Fatalf("installed metadata = %+v", metadata)
	}
	if len(acks.acks) != 1 {
		t.Fatalf("acks = %d", len(acks.acks))
	}
	ack := acks.acks[0]
	if ack.PublicationID != publication.PublicationID || ack.WorkerID != "worker-a" || ack.BootID != "boot-a" || !ack.InstalledAt.Equal(installedAt) || !ack.DimensionEffectiveFrom.Equal(publication.DimensionEffectiveFrom) || ack.DimensionChecksum != publication.Dimension.Checksum || ack.ClassificationChecksum != publication.Classification.Checksum {
		t.Fatalf("ack = %+v", ack)
	}
}

func TestVersionLoaderKeepsDimensionBundleSchemaV1Readable(t *testing.T) {
	publication, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-legacy")
	var bundle flowdimension.SnapshotBundle
	if err := json.Unmarshal(source.objects[publication.Dimension.ObjectRef], &bundle); err != nil {
		t.Fatal(err)
	}
	bundle.SchemaVersion = 1
	bundle.Operators = nil
	legacyData, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	source.objects[publication.Dimension.ObjectRef] = legacyData
	publication.Dimension.Checksum = versionObjectChecksum(legacyData)
	catalog, _ := NewEnrichmentVersionCatalog()
	acks := &recordingVersionAcknowledger{}
	loader, err := NewVersionLoader(source, acks, catalog, VersionWorkerIdentity{
		WorkerID: "worker-legacy", BootID: "boot-legacy", SoftwareVersion: "1.2.3",
	}, VersionLoaderLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := loader.Install(context.Background(), publication); err != nil {
		t.Fatal(err)
	}
	installed, exists := catalog.DimensionVersion("tenant-a", 1)
	if !exists || installed.Metadata().SchemaVersion != 1 || len(acks.acks) != 1 {
		t.Fatalf("legacy bundle was not installed and acknowledged: exists=%t metadata=%+v acks=%d", exists, installed.Metadata(), len(acks.acks))
	}
}

func TestVersionLoaderInstallsWADSAndEnrichesWithoutGeoCatalog(t *testing.T) {
	publication, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-wads")
	definition, err := flowdimension.CompileBundle(flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion, SnapshotID: publication.DimensionSnapshotID,
		TenantID: publication.TenantID, Version: publication.DimensionVersion, EffectiveFrom: publication.DimensionEffectiveFrom,
		GeoNodes: []flowdimension.GeoNodeDefinition{
			{ID: "city-hangzhou", Kind: "city", Code: "330100", Name: "Hangzhou", ParentID: "province-zhejiang", Enabled: true},
			{ID: "continent-asia", Kind: "continent", Code: "AS", Name: "Asia", Enabled: true},
			{ID: "country-cn", Kind: "country", Code: "CN", Name: "China", ParentID: "continent-asia", Enabled: true},
			{ID: "province-zhejiang", Kind: "province", Code: "330000", Name: "Zhejiang", ParentID: "country-cn", Enabled: true},
		},
		Operators: []flowdimension.OperatorDefinition{{ID: "customer-carrier", FlowISPID: 3, Code: "CUSTOMER", Name: "Customer Carrier", Category: "carrier", ASNs: []uint32{64500}, Enabled: true}},
		Prefixes: []flowdimension.PrefixDefinition{
			{ID: "local", CIDR: "10.0.0.0/8", Labels: map[string]string{"flow": "local", "business": "customer"}},
			{ID: "remote", CIDR: "203.0.113.0/24", Labels: map[string]string{
				"geo.continent": "AS", "geo.continent_id": "continent-asia", "geo.country": "CN", "geo.country_id": "country-cn",
				"geo.province": "330000", "geo.province_id": "province-zhejiang", "geo.city": "330100", "geo.city_id": "city-hangzhou",
			}},
		},
		AddressSets: []flowdimension.AddressSetDefinition{{ID: "remote-set", Name: "Remote", Members: []string{"203.0.113.0/24"}, MatchDirection: "both", Enabled: true}},
	}, flowdimension.CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	built, err := flowdimension.BuildAddressSnapshot(flowdimension.AddressSnapshotBuildInput{
		Definition: definition, BuilderVersion: "watchdog-test-1",
		SupplierOperators: []flowdimension.AddressSnapshotBuildOperator{{ID: 9, StableID: "supplier-carrier", Code: "SUPPLIER", Name: "Supplier Carrier", Category: "carrier", ASNs: []uint32{64500}, Enabled: true}},
		Sources: []flowdimension.AddressSnapshotBuildSource{{
			Slot: flowdimension.AddressSnapshotSourceCombined, ImportID: "import-1", ChecksumSHA256: "sha256:" + strings.Repeat("a", 64), SlotRowVersion: 1, RowCountV4: 1,
			Ranges: []flowdimension.AddressSnapshotBuildRange{{Start: netip.MustParseAddr("203.0.113.0"), End: netip.MustParseAddr("203.0.113.255"), Geo: flowdimension.AddressSnapshotBuildGeo{CountryCode: "CN", AdminCode: "330100", Subdivision: "Zhejiang", City: "Hangzhou"}, ISPID: 9, ASN: 64500}},
		}},
	}, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	delete(source.objects, publication.Dimension.ObjectRef)
	publication.Dimension = VersionObjectReference{
		ObjectRef: "objects/dimension-wads/address-snapshot.wads", Checksum: built.ChecksumSHA256,
		ObjectFormat: VersionObjectFormatWADS, ObjectFormatVersion: flowdimension.AddressSnapshotFormatVersion,
	}
	source.objects[publication.Dimension.ObjectRef] = built.Data

	catalog, _ := NewEnrichmentVersionCatalog()
	acks := &recordingVersionAcknowledger{}
	loader, err := NewVersionLoader(source, acks, catalog, VersionWorkerIdentity{WorkerID: "worker-wads", BootID: "boot-wads", SoftwareVersion: "1.2.3"}, VersionLoaderLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := loader.Install(context.Background(), publication); err != nil {
		t.Fatal(err)
	}
	installed, exists := catalog.DimensionVersion("tenant-a", 1)
	if _, ok := installed.(*flowdimension.AddressSnapshotIndex); !exists || !ok || len(acks.acks) != 1 {
		t.Fatalf("WADS install: exists=%t type=%T acks=%d", exists, installed, len(acks.acks))
	}
	enricher, err := NewEnricherWithVersionCatalog(catalog, nil, EnrichmentLimits{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := enricher.EnrichBatch(testBatch(testMinute(12, 30)))
	if err != nil {
		t.Fatal(err)
	}
	record := result.Records[0]
	if record.Category != flowdimension.CategoryOnNetLocalCity || record.SupplierCategory != flowdimension.CategoryOffNetInProvince ||
		record.RemoteGeo.ISPID != 3 || record.SupplierRemoteGeo.ISPID != 9 || record.RemoteASN != 64500 || record.RemoteASNSource != ASNSourceGeoV2 ||
		record.RemoteGeo.Version != "dimension-wads" || record.Dimensions.Remote.AddressSets.Count() != 1 {
		t.Fatalf("WADS enrichment = %+v", record)
	}
}

func TestVersionLoaderRejectsUnsupportedObjectFormatBeforeFetch(t *testing.T) {
	for _, mutate := range []func(*EnrichmentVersionPublication){
		func(publication *EnrichmentVersionPublication) {
			publication.Dimension.ObjectFormat = VersionObjectFormatWADS
		},
		func(publication *EnrichmentVersionPublication) {
			publication.Dimension.ObjectFormat = VersionObjectFormatWADS
			publication.Dimension.ObjectFormatVersion = flowdimension.AddressSnapshotFormatVersion + 1
		},
		func(publication *EnrichmentVersionPublication) {
			publication.Classification.ObjectFormat = VersionObjectFormatWADS
		},
	} {
		publication, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-format")
		mutate(&publication)
		catalog, _ := NewEnrichmentVersionCatalog()
		loader, _ := NewVersionLoader(source, &recordingVersionAcknowledger{}, catalog, VersionWorkerIdentity{WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3"}, VersionLoaderLimits{})
		if err := loader.Install(context.Background(), publication); !errors.Is(err, ErrInvalidVersionPublication) {
			t.Fatalf("format error = %v", err)
		}
		if len(source.calls) != 0 {
			t.Fatalf("invalid format fetched objects: %v", source.calls)
		}
	}
}

func TestLegacyJSONVersionWithoutGeoCatalogFailsClosedAtEventTime(t *testing.T) {
	publication, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-json")
	catalog, _ := NewEnrichmentVersionCatalog()
	loader, _ := NewVersionLoader(source, &recordingVersionAcknowledger{}, catalog, VersionWorkerIdentity{WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3"}, VersionLoaderLimits{})
	if err := loader.Install(context.Background(), publication); err != nil {
		t.Fatal(err)
	}
	enricher, err := NewEnricherWithVersionCatalog(catalog, nil, EnrichmentLimits{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = enricher.EnrichBatch(testBatch(testMinute(12, 30)))
	var blocked *VersionBlockedError
	if !errors.As(err, &blocked) || blocked.Dependency != "geo" || !errors.Is(blocked.Cause, flowdimension.ErrNoGeoIndex) {
		t.Fatalf("legacy missing Geo error = %v", err)
	}
}

func TestVersionLoaderDoesNotExposePartiallyValidatedPublication(t *testing.T) {
	publication, source := testVersionPublication(t, 2, testMinute(13, 0), "dimension-2")
	publication.ClassificationVersion++
	catalog, _ := NewEnrichmentVersionCatalog()
	acks := &recordingVersionAcknowledger{}
	loader, _ := NewVersionLoader(source, acks, catalog, VersionWorkerIdentity{
		WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3",
	}, VersionLoaderLimits{})
	if err := loader.Install(context.Background(), publication); !errors.Is(err, ErrInvalidVersionPublication) {
		t.Fatalf("install error = %v", err)
	}
	if _, err := catalog.Select("tenant-a", testMinute(13, 30)); !errors.Is(err, ErrNoEnrichmentVersion) {
		t.Fatalf("partially installed publication = %v", err)
	}
	if len(acks.acks) != 0 {
		t.Fatalf("invalid publication was acknowledged: %+v", acks.acks)
	}
}

func TestVersionLoaderRetriesAcknowledgementIdempotentlyAfterLocalInstall(t *testing.T) {
	publication, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-1")
	catalog, _ := NewEnrichmentVersionCatalog()
	acks := &recordingVersionAcknowledger{failures: 1}
	loader, _ := NewVersionLoader(source, acks, catalog, VersionWorkerIdentity{
		WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3",
	}, VersionLoaderLimits{})
	if err := loader.Install(context.Background(), publication); !errors.Is(err, ErrVersionAcknowledgement) {
		t.Fatalf("first install error = %v", err)
	}
	if _, err := catalog.Select("tenant-a", testMinute(12, 30)); err != nil {
		t.Fatalf("locally installed pair disappeared after ack failure: %v", err)
	}
	if err := loader.Install(context.Background(), publication); err != nil {
		t.Fatalf("retry install: %v", err)
	}
	if len(acks.acks) != 2 {
		t.Fatalf("ack attempts = %d", len(acks.acks))
	}
	if len(source.calls) != 2 {
		t.Fatalf("ACK retry fetched immutable objects again: %v", source.calls)
	}
}

func TestVersionLoaderReusesInstalledDimensionForHomeOnlyPublication(t *testing.T) {
	first, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-1")
	catalog, _ := NewEnrichmentVersionCatalog()
	loader, _ := NewVersionLoader(source, &recordingVersionAcknowledger{}, catalog, VersionWorkerIdentity{
		WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3",
	}, VersionLoaderLimits{})
	if err := loader.Install(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	classificationData, err := json.Marshal(flowdimension.ClassificationBundle{
		SchemaVersion: flowdimension.ClassificationSchemaVersion, TenantID: "tenant-a", Version: 2,
		EffectiveFrom: testMinute(13, 0), DimensionSnapshotID: "dimension-1",
		HomeProvince: "330000", HomeCity: "330100", HomeISPIDs: []uint16{4}, OverseasIncludesHMT: true,
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.PublicationID = "publication-2"
	second.ClassificationVersion = 2
	second.ClassificationEffectiveFrom = testMinute(13, 0)
	second.Classification = VersionObjectReference{ObjectRef: "objects/dimension-1/classification-2.json", Checksum: versionObjectChecksum(classificationData)}
	source.objects[second.Classification.ObjectRef] = classificationData
	if err := loader.Install(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	version1, _ := catalog.ClassificationVersion("tenant-a", 1)
	version2, _ := catalog.ClassificationVersion("tenant-a", 2)
	if version1.Dimension != version2.Dimension {
		t.Fatal("Home-only publication retained a duplicate dimension index")
	}
	if len(source.calls) != 3 || source.calls[2] != second.Classification.ObjectRef {
		t.Fatalf("object fetches = %v", source.calls)
	}
}

func TestVersionLoaderRejectsFaultySourceThatIgnoresByteLimit(t *testing.T) {
	publication, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-1")
	catalog, _ := NewEnrichmentVersionCatalog()
	acks := &recordingVersionAcknowledger{}
	loader, _ := NewVersionLoader(source, acks, catalog, VersionWorkerIdentity{
		WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3",
	}, VersionLoaderLimits{MaxDimensionObjectBytes: len(source.objects[publication.Dimension.ObjectRef]) - 1})
	if err := loader.Install(context.Background(), publication); !errors.Is(err, ErrVersionObjectUnavailable) {
		t.Fatalf("size error = %v", err)
	}
	if len(source.calls) != 1 || len(acks.acks) != 0 {
		t.Fatalf("calls=%v acks=%v", source.calls, acks.acks)
	}
}

func TestEnricherUsesAtomicVersionCatalog(t *testing.T) {
	publication, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-1")
	catalog, _ := NewEnrichmentVersionCatalog()
	loader, _ := NewVersionLoader(source, &recordingVersionAcknowledger{}, catalog, VersionWorkerIdentity{
		WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3",
	}, VersionLoaderLimits{})
	if err := loader.Install(context.Background(), publication); err != nil {
		t.Fatal(err)
	}
	geo := flowdimension.NewGeoCatalog()
	loadGeo(t, geo, "geo-1", testMinute(12, 0), "203.0.113.0,203.0.113.255,CN,330100,Zhejiang,Hangzhou,3,64500")
	enricher, err := NewEnricherWithVersionCatalog(catalog, geo, EnrichmentLimits{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := enricher.EnrichBatch(testBatch(testMinute(12, 30)))
	if err != nil || len(result.Records) != 1 || result.Records[0].ClassificationVersion != 1 || result.Records[0].Dimensions.SnapshotID != "dimension-1" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	batch := testBatch(testMinute(12, 30))
	reused := &EnrichedBatch{Records: make([]EnrichedRecord, 0, len(batch.Records))}
	allocations := testing.AllocsPerRun(1_000, func() {
		if err := enricher.EnrichBatchInto(batch, reused); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("atomic catalog hot-path allocations = %f", allocations)
	}
}

func testVersionPublication(t testing.TB, version uint32, effectiveFrom time.Time, snapshotID string) (EnrichmentVersionPublication, *memoryVersionObjectSource) {
	t.Helper()
	dimensionBundle := flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion, SnapshotID: snapshotID,
		TenantID: "tenant-a", Version: uint64(version), EffectiveFrom: effectiveFrom,
		Prefixes: []flowdimension.PrefixDefinition{
			{ID: "local", CIDR: "10.0.0.0/8", Labels: map[string]string{"flow": "local", "business": "customer"}},
			{ID: "remote", CIDR: "203.0.113.0/24", Labels: map[string]string{"provider": "carrier-a"}},
		},
	}
	classificationBundle := flowdimension.ClassificationBundle{
		SchemaVersion: flowdimension.ClassificationSchemaVersion, TenantID: "tenant-a", Version: version,
		EffectiveFrom: effectiveFrom, DimensionSnapshotID: snapshotID,
		HomeProvince: "330000", HomeCity: "330100", HomeISPIDs: []uint16{3}, OverseasIncludesHMT: true,
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	}
	dimensionData, err := json.Marshal(dimensionBundle)
	if err != nil {
		t.Fatal(err)
	}
	classificationData, err := json.Marshal(classificationBundle)
	if err != nil {
		t.Fatal(err)
	}
	dimensionRef := fmt.Sprintf("objects/%s/dimension.json", snapshotID)
	classificationRef := fmt.Sprintf("objects/%s/classification.json", snapshotID)
	publication := EnrichmentVersionPublication{
		PublicationID: fmt.Sprintf("publication-%d", version), TenantID: "tenant-a",
		DimensionSnapshotID: snapshotID, DimensionVersion: uint64(version), DimensionEffectiveFrom: effectiveFrom,
		Dimension:             VersionObjectReference{ObjectRef: dimensionRef, Checksum: versionObjectChecksum(dimensionData)},
		ClassificationVersion: version, ClassificationEffectiveFrom: effectiveFrom,
		Classification: VersionObjectReference{ObjectRef: classificationRef, Checksum: versionObjectChecksum(classificationData)},
	}
	return publication, &memoryVersionObjectSource{objects: map[string][]byte{
		dimensionRef: dimensionData, classificationRef: classificationData,
	}}
}

func versionObjectChecksum(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}
