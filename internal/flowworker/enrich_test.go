package flowworker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

func TestEnrichBatchSelectsEveryVersionByRecordEventTime(t *testing.T) {
	dimensionV1 := compileDimension(t, "dimension-1", 1, testMinute(12, 0), nil)
	dimensionV2 := compileDimension(t, "dimension-2", 2, testMinute(13, 0), nil)
	dimensions, err := flowdimension.NewSnapshotCatalog(dimensionV2, dimensionV1)
	if err != nil {
		t.Fatal(err)
	}
	classificationV1 := compileClassification(t, 1, testMinute(12, 0), "dimension-1", flowdimension.RecordPolicyCount, flowdimension.RecordPolicyCount)
	classificationV2 := compileClassification(t, 2, testMinute(13, 0), "dimension-2", flowdimension.RecordPolicyCount, flowdimension.RecordPolicyCount)
	classifications, err := flowdimension.NewClassificationCatalog(classificationV2, classificationV1)
	if err != nil {
		t.Fatal(err)
	}
	geo := flowdimension.NewGeoCatalog()
	loadGeo(t, geo, "geo-1", testMinute(12, 0), "203.0.113.0,203.0.113.255,CN,330100,Zhejiang,Hangzhou,3,64500")
	loadGeo(t, geo, "geo-2", testMinute(13, 0), "203.0.113.0,203.0.113.255,US,,California,San Francisco,4,64501")
	enricher := newTestEnricher(t, dimensions, geo, classifications)

	batch := testBatch(testMinute(12, 30))
	second := proto.Clone(batch.Records[0]).(*flowpb.NormalizedRecord)
	second.RecordIndex = 2
	second.EventTimeUnixMs = testMinute(13, 30).UnixMilli()
	batch.Records = append(batch.Records, second)
	enriched, err := enricher.EnrichBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if enriched.SchemaVersion != EnrichedBatchSchemaVersion || len(enriched.Records) != 2 {
		t.Fatalf("records = %d", len(enriched.Records))
	}
	first, secondResult := enriched.Records[0], enriched.Records[1]
	if first.Dimensions.SnapshotID != "dimension-1" || first.RemoteGeo.Version != "geo-1" || first.ClassificationVersion != 1 || first.Category != flowdimension.CategoryOnNetLocalCity {
		t.Fatalf("unexpected v1 enrichment: %+v", first)
	}
	if secondResult.Dimensions.SnapshotID != "dimension-2" || secondResult.RemoteGeo.Version != "geo-2" || secondResult.ClassificationVersion != 2 || secondResult.Category != flowdimension.CategoryOverseas {
		t.Fatalf("unexpected v2 enrichment: %+v", secondResult)
	}
	if first.RemoteASN != 64500 || first.RemoteASNSource != ASNSourceGeo || secondResult.RemoteASN != 64501 || secondResult.RemoteASNSource != ASNSourceGeo {
		t.Fatalf("unexpected remote ASN provenance: first=%d/%s second=%d/%s", first.RemoteASN, first.RemoteASNSource, secondResult.RemoteASN, secondResult.RemoteASNSource)
	}
	if first.SourcePort != 12345 || first.DestinationPort != 443 || first.LocalPort != 12345 || first.RemotePort != 443 || first.SourceASN != 65001 || first.DestinationASN != 65002 {
		t.Fatalf("normalized fields changed during enrichment: %+v", first)
	}
	if first.EstimatedBytes != batch.Records[0].EstimatedBytes || secondResult.EstimatedBytes != second.EstimatedBytes || first.DimensionFingerprint == secondResult.DimensionFingerprint {
		t.Fatalf("counter conservation or version fingerprint failed: first=%+v second=%+v", first, secondResult)
	}
	replayed := proto.Clone(batch).(*flowpb.NormalizedRecordBatch)
	replayed.ReplayGeneration = 7
	replayedResult, err := enricher.EnrichBatch(replayed)
	if err != nil {
		t.Fatal(err)
	}
	if replayedResult.Records[0].NormalizedRecordID != first.NormalizedRecordID || replayedResult.Records[0].DimensionFingerprint != first.DimensionFingerprint {
		t.Fatal("stable record identity or dimension fingerprint changed on replay")
	}
}

func TestEnrichBatchAppliesGeoOverrideAndPreservesExplicitUnknownASN(t *testing.T) {
	override := map[string]string{
		"flow.geo.country": "CN", "flow.geo.admin_code": "330100", "flow.geo.isp_id": "3", "flow.geo.asn": "0", "flow.geo.reason": "verified",
	}
	dimension := compileDimension(t, "dimension-1", 1, testMinute(12, 0), override)
	dimensions, _ := flowdimension.NewSnapshotCatalog(dimension)
	classification := compileClassification(t, 1, testMinute(12, 0), "dimension-1", flowdimension.RecordPolicyCount, flowdimension.RecordPolicyCount)
	classifications, _ := flowdimension.NewClassificationCatalog(classification)
	geo := flowdimension.NewGeoCatalog()
	loadGeo(t, geo, "geo-1", testMinute(12, 0), "203.0.113.0,203.0.113.255,US,,California,San Francisco,0,0")

	result, err := newTestEnricher(t, dimensions, geo, classifications).EnrichBatch(testBatch(testMinute(12, 30)))
	if err != nil {
		t.Fatal(err)
	}
	record := result.Records[0]
	if record.RemoteGeo.Country != "CN" || record.RemoteGeo.AdminCode != "330100" || record.RemoteGeo.ISPID != 3 || record.RemoteGeo.Version != "geo-1" || record.RemoteGeo.Source != "flow_geo_override" {
		t.Fatalf("unexpected Geo override: %+v", record.RemoteGeo)
	}
	if record.RemoteASN != 0 || record.RemoteASNSource != ASNSourceOverride {
		t.Fatalf("explicit override ASN=0 must suppress exporter fallback: %d/%s", record.RemoteASN, record.RemoteASNSource)
	}
	if record.Category != flowdimension.CategoryOnNetLocalCity {
		t.Fatalf("category = %q", record.Category)
	}
}

func TestEnrichBatchFallsBackToExporterASNWhenGeoHasNone(t *testing.T) {
	dimension := compileDimension(t, "dimension-1", 1, testMinute(12, 0), nil)
	dimensions, _ := flowdimension.NewSnapshotCatalog(dimension)
	classification := compileClassification(t, 1, testMinute(12, 0), "dimension-1", flowdimension.RecordPolicyCount, flowdimension.RecordPolicyCount)
	classifications, _ := flowdimension.NewClassificationCatalog(classification)
	geo := flowdimension.NewGeoCatalog()
	loadGeo(t, geo, "geo-1", testMinute(12, 0), "203.0.113.0,203.0.113.255,ZZ,,,,0,0")
	result, err := newTestEnricher(t, dimensions, geo, classifications).EnrichBatch(testBatch(testMinute(12, 30)))
	if err != nil {
		t.Fatal(err)
	}
	if result.Records[0].RemoteASN != 65002 || result.Records[0].RemoteASNSource != ASNSourceExporter {
		t.Fatalf("ASN fallback = %d/%s", result.Records[0].RemoteASN, result.Records[0].RemoteASNSource)
	}
}

func TestEnrichBatchReturnsPauseSignalForEveryMissingOrSkewedVersion(t *testing.T) {
	dimension := compileDimension(t, "dimension-1", 1, testMinute(12, 0), nil)
	validDimensions, _ := flowdimension.NewSnapshotCatalog(dimension)
	emptyDimensions, _ := flowdimension.NewSnapshotCatalog()
	classification := compileClassification(t, 1, testMinute(12, 0), "dimension-1", flowdimension.RecordPolicyCount, flowdimension.RecordPolicyCount)
	validClassifications, _ := flowdimension.NewClassificationCatalog(classification)
	emptyClassifications, _ := flowdimension.NewClassificationCatalog()
	skewed := compileClassification(t, 1, testMinute(12, 0), "dimension-other", flowdimension.RecordPolicyCount, flowdimension.RecordPolicyCount)
	skewedClassifications, _ := flowdimension.NewClassificationCatalog(skewed)
	validGeo := flowdimension.NewGeoCatalog()
	loadGeo(t, validGeo, "geo-1", testMinute(12, 0), "203.0.113.0,203.0.113.255,ZZ,,,,0,0")
	emptyGeo := flowdimension.NewGeoCatalog()
	tests := []struct {
		name           string
		dimensions     *flowdimension.SnapshotCatalog
		geo            *flowdimension.GeoCatalog
		classification *flowdimension.ClassificationCatalog
		dependency     string
		cause          error
	}{
		{name: "dimension", dimensions: emptyDimensions, geo: validGeo, classification: validClassifications, dependency: "dimension", cause: flowdimension.ErrNoDimensionSnapshot},
		{name: "classification", dimensions: validDimensions, geo: validGeo, classification: emptyClassifications, dependency: "classification", cause: flowdimension.ErrNoClassificationSnapshot},
		{name: "skew", dimensions: validDimensions, geo: validGeo, classification: skewedClassifications, dependency: "classification_dimension_pair", cause: ErrVersionSkew},
		{name: "geo", dimensions: validDimensions, geo: emptyGeo, classification: validClassifications, dependency: "geo", cause: flowdimension.ErrNoGeoIndex},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newTestEnricher(t, test.dimensions, test.geo, test.classification).EnrichBatch(testBatch(testMinute(12, 30)))
			var blocked *VersionBlockedError
			if !errors.Is(err, ErrVersionUnavailable) || !errors.Is(err, test.cause) || !errors.As(err, &blocked) || blocked.Dependency != test.dependency {
				t.Fatalf("blocked error = %#v", err)
			}
		})
	}
}

func TestEnrichBatchCarriesDropDispositionWithoutLosingCounters(t *testing.T) {
	dimension := compileDimension(t, "dimension-1", 1, testMinute(12, 0), nil)
	dimensions, _ := flowdimension.NewSnapshotCatalog(dimension)
	classification := compileClassification(t, 1, testMinute(12, 0), "dimension-1", flowdimension.RecordPolicyDrop, flowdimension.RecordPolicyCount)
	classifications, _ := flowdimension.NewClassificationCatalog(classification)
	geo := flowdimension.NewGeoCatalog()
	loadGeo(t, geo, "geo-1", testMinute(12, 0), "10.0.0.0,10.255.255.255,CN,330100,Zhejiang,Hangzhou,3,64500")
	batch := testBatch(testMinute(12, 30))
	destination := address16("10.2.3.4")
	batch.Records[0].DstIp = destination
	batch.VirtualShard = flowcollect.VirtualShard(batch.TenantId, mustAddress(batch.Records[0].SrcIp), mustAddress(destination))
	result, err := newTestEnricher(t, dimensions, geo, classifications).EnrichBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	record := result.Records[0]
	if record.Dimensions.Direction != flowdimension.DirectionInternal || record.Category != flowdimension.CategoryInternal || record.Disposition != flowdimension.DispositionDrop || record.EstimatedBytes != batch.Records[0].EstimatedBytes {
		t.Fatalf("unexpected internal disposition: %+v", record)
	}
}

func TestEnrichBatchIntoClearsPartialOutputOnBlockedRecord(t *testing.T) {
	dimension := compileDimension(t, "dimension-1", 1, testMinute(12, 0), nil)
	dimensions, _ := flowdimension.NewSnapshotCatalog(dimension)
	classification := compileClassification(t, 1, testMinute(12, 0), "dimension-1", flowdimension.RecordPolicyCount, flowdimension.RecordPolicyCount)
	classifications, _ := flowdimension.NewClassificationCatalog(classification)
	geo := flowdimension.NewGeoCatalog()
	loadGeo(t, geo, "geo-1", testMinute(12, 0), "203.0.113.0,203.0.113.255,ZZ,,,,0,0")
	enricher := newTestEnricher(t, dimensions, geo, classifications)
	batch := testBatch(testMinute(12, 30))
	second := proto.Clone(batch.Records[0]).(*flowpb.NormalizedRecord)
	second.RecordIndex = 2
	second.EventTimeUnixMs = testMinute(11, 30).UnixMilli()
	batch.Records = append(batch.Records, second)
	destination := &EnrichedBatch{Records: make([]EnrichedRecord, 1, 4)}
	if err := enricher.EnrichBatchInto(batch, destination); !errors.Is(err, ErrVersionUnavailable) {
		t.Fatalf("error = %v", err)
	}
	if len(destination.Records) != 0 {
		t.Fatalf("partial output escaped: %d records", len(destination.Records))
	}
}

func TestEnrichBatchIntoDoesNotAllocateWithReusableBuffer(t *testing.T) {
	dimension := compileDimension(t, "dimension-1", 1, testMinute(12, 0), nil)
	dimensions, _ := flowdimension.NewSnapshotCatalog(dimension)
	classification := compileClassification(t, 1, testMinute(12, 0), "dimension-1", flowdimension.RecordPolicyCount, flowdimension.RecordPolicyCount)
	classifications, _ := flowdimension.NewClassificationCatalog(classification)
	geo := flowdimension.NewGeoCatalog()
	loadGeo(t, geo, "geo-1", testMinute(12, 0), "203.0.113.0,203.0.113.255,CN,330100,Zhejiang,Hangzhou,3,64500")
	enricher := newTestEnricher(t, dimensions, geo, classifications)
	batch := testBatch(testMinute(12, 30))
	result := &EnrichedBatch{Records: make([]EnrichedRecord, 0, len(batch.Records))}
	allocations := testing.AllocsPerRun(1000, func() {
		if err := enricher.EnrichBatchInto(batch, result); err != nil || len(result.Records) != 1 {
			panic("unexpected enrichment result")
		}
	})
	if allocations != 0 {
		t.Fatalf("allocations/run = %f, want 0", allocations)
	}
}

func TestEnrichBatchRejectsCorruptNormalizedMessagesBeforeLookup(t *testing.T) {
	dimension := compileDimension(t, "dimension-1", 1, testMinute(12, 0), nil)
	dimensions, _ := flowdimension.NewSnapshotCatalog(dimension)
	classification := compileClassification(t, 1, testMinute(12, 0), "dimension-1", flowdimension.RecordPolicyCount, flowdimension.RecordPolicyCount)
	classifications, _ := flowdimension.NewClassificationCatalog(classification)
	geo := flowdimension.NewGeoCatalog()
	loadGeo(t, geo, "geo-1", testMinute(12, 0), "203.0.113.0,203.0.113.255,ZZ,,,,0,0")
	enricher := newTestEnricher(t, dimensions, geo, classifications)
	tests := []struct {
		name   string
		mutate func(*flowpb.NormalizedRecordBatch)
		want   string
	}{
		{name: "schema", mutate: func(batch *flowpb.NormalizedRecordBatch) { batch.BatchSchemaVersion = 2 }, want: "schema"},
		{name: "id", mutate: func(batch *flowpb.NormalizedRecordBatch) { batch.DatagramId = batch.DatagramId[:31] }, want: "32 bytes"},
		{name: "partition", mutate: func(batch *flowpb.NormalizedRecordBatch) { batch.VirtualShard++ }, want: "virtual shard"},
		{name: "address", mutate: func(batch *flowpb.NormalizedRecordBatch) { batch.Records[0].SrcIp = []byte{1, 2, 3, 4} }, want: "16-byte"},
		{name: "port", mutate: func(batch *flowpb.NormalizedRecordBatch) { batch.Records[0].DstPort = 65536 }, want: "port"},
		{name: "sampling-rate", mutate: func(batch *flowpb.NormalizedRecordBatch) { batch.Records[0].SamplingRate = 0 }, want: "sampling rate"},
		{name: "counter", mutate: func(batch *flowpb.NormalizedRecordBatch) { batch.Records[0].EstimatedBytes++ }, want: "counters"},
		{name: "duplicate", mutate: func(batch *flowpb.NormalizedRecordBatch) {
			batch.Records = append(batch.Records, proto.Clone(batch.Records[0]).(*flowpb.NormalizedRecord))
		}, want: "strictly increasing"},
		{name: "future-time", mutate: func(batch *flowpb.NormalizedRecordBatch) {
			batch.Records[0].EventTimeUnixMs = time.UnixMilli(batch.ReceivedAtUnixMs).Add(6 * time.Minute).UnixMilli()
		}, want: "future skew"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch := proto.Clone(testBatch(testMinute(12, 30))).(*flowpb.NormalizedRecordBatch)
			test.mutate(batch)
			if _, err := enricher.EnrichBatch(batch); !errors.Is(err, ErrInvalidNormalizedBatch) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func BenchmarkEnrichBatch1024RecordsReused(b *testing.B) {
	dimension := compileDimension(b, "dimension-1", 1, testMinute(12, 0), nil)
	dimensions, _ := flowdimension.NewSnapshotCatalog(dimension)
	classification := compileClassification(b, 1, testMinute(12, 0), "dimension-1", flowdimension.RecordPolicyCount, flowdimension.RecordPolicyCount)
	classifications, _ := flowdimension.NewClassificationCatalog(classification)
	geo := flowdimension.NewGeoCatalog()
	loadGeo(b, geo, "geo-1", testMinute(12, 0), "203.0.113.0,203.0.113.255,CN,330100,Zhejiang,Hangzhou,3,64500")
	enricher := newTestEnricher(b, dimensions, geo, classifications)
	batch := testBatch(testMinute(12, 30))
	batch.Records = make([]*flowpb.NormalizedRecord, 1024)
	for index := range batch.Records {
		record := proto.Clone(testBatch(testMinute(12, 30)).Records[0]).(*flowpb.NormalizedRecord)
		record.RecordIndex = uint32(index)
		batch.Records[index] = record
	}
	b.ReportAllocs()
	result := &EnrichedBatch{Records: make([]EnrichedRecord, 0, len(batch.Records))}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		err := enricher.EnrichBatchInto(batch, result)
		if err != nil || len(result.Records) != len(batch.Records) {
			b.Fatalf("records=%d err=%v", len(result.Records), err)
		}
	}
	b.ReportMetric(float64(b.N*len(batch.Records))/b.Elapsed().Seconds(), "records/s")
}

func compileDimension(t testing.TB, id string, version uint64, effectiveFrom time.Time, override map[string]string) *flowdimension.CompiledSnapshot {
	t.Helper()
	remoteLabels := map[string]string{"provider": "carrier-a"}
	for key, value := range override {
		remoteLabels[key] = value
	}
	snapshot, err := flowdimension.CompileBundle(flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion, SnapshotID: id, TenantID: "tenant-a", Version: version, EffectiveFrom: effectiveFrom,
		Prefixes: []flowdimension.PrefixDefinition{
			{ID: "local", CIDR: "10.0.0.0/8", Labels: map[string]string{"flow": "local", "business": "customer"}},
			{ID: "remote", CIDR: "203.0.113.0/24", Labels: remoteLabels},
		},
	}, flowdimension.CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func compileClassification(t testing.TB, version uint32, effectiveFrom time.Time, dimensionID string, internal, transit flowdimension.RecordPolicy) *flowdimension.ClassificationSnapshot {
	t.Helper()
	snapshot, err := flowdimension.CompileClassification(flowdimension.ClassificationDefinition{
		TenantID: "tenant-a", Version: version, EffectiveFrom: effectiveFrom, DimensionSnapshotID: dimensionID,
		HomeProvince: "330000", HomeCity: "330100", HomeISPIDs: []uint16{3}, OverseasIncludesHMT: true,
		InternalPolicy: internal, TransitPolicy: transit,
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func newTestEnricher(t testing.TB, dimensions *flowdimension.SnapshotCatalog, geo *flowdimension.GeoCatalog, classifications *flowdimension.ClassificationCatalog) *Enricher {
	t.Helper()
	enricher, err := NewEnricher(dimensions, geo, classifications, EnrichmentLimits{})
	if err != nil {
		t.Fatal(err)
	}
	return enricher
}

func testBatch(eventTime time.Time) *flowpb.NormalizedRecordBatch {
	source := mustAddress(address16("10.1.2.3"))
	destination := mustAddress(address16("203.0.113.20"))
	return &flowpb.NormalizedRecordBatch{
		BatchSchemaVersion: 1, NormalizedBatchId: bytes.Repeat([]byte{0x11}, 32), DatagramId: bytes.Repeat([]byte{0x22}, 32),
		VirtualShard: flowcollect.VirtualShard("tenant-a", source, destination), PartitionMapVersion: 1, PhysicalPartition: 7,
		TenantId: "tenant-a", CollectorId: "collector-a", ExporterId: "exporter-a", RegistryVersion: 9,
		ReceivedAtUnixMs: testMinute(14, 0).UnixMilli(), Protocol: 1, SourceIp: address16("192.0.2.10"),
		Records: []*flowpb.NormalizedRecord{{
			RecordIndex: 1, EventTimeUnixMs: eventTime.UnixMilli(), TargetId: "target-a", DeviceId: "device-a",
			ObservationIfIndex: 9, ObservationDirection: 1, InIf: 9, OutIf: 10,
			SrcIp: address16("10.1.2.3"), DstIp: address16("203.0.113.20"), SrcPort: 12345, DstPort: 443, IpProto: 6, TcpFlags: 0x12,
			SrcAs: 65001, DstAs: 65002, RawBytes: 100, RawPackets: 2, SamplingMode: 1, SamplingRate: 1000,
			EstimatedBytes: 100_000, EstimatedPackets: 2_000, FlowDurationMs: 250, QualityFlags: 4,
		}},
	}
}

func loadGeo(t testing.TB, catalog *flowdimension.GeoCatalog, version string, effectiveFrom time.Time, ipv4Row string) {
	t.Helper()
	directory := filepath.Join(t.TempDir(), version)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	operators := []flowdimension.GeoOperator{
		{ID: 3, Name: "Carrier 3", ShortName: "C3", Category: "carrier", Enabled: true},
		{ID: 4, Name: "Carrier 4", ShortName: "C4", Category: "carrier", Enabled: true},
	}
	dictionary := []flowdimension.GeoDictionaryEntry{{Kind: "city", Code: "330100", Name: "Hangzhou", Enabled: true}}
	files := map[string][]byte{
		"ipv4.csv.zst":   zstdBytes(t, []byte("ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn\n"+ipv4Row+"\n")),
		"ipv6.csv.zst":   zstdBytes(t, []byte("ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn\n")),
		"operators.json": jsonBytes(t, operators),
		"geo_dict.json":  jsonBytes(t, dictionary),
	}
	manifest := flowdimension.GeoManifest{
		Schema: flowdimension.GeoSchema, Version: version, GeneratedAt: effectiveFrom, EffectiveFrom: effectiveFrom,
		AdminCodeSystem: flowdimension.GeoAdminCodeSystem, UnknownCountry: flowdimension.GeoUnknownCountry,
		Files: make(map[string]flowdimension.GeoFileSpec, len(files)),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(directory, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		rows := uint64(0)
		switch name {
		case "ipv4.csv.zst":
			rows = 1
		case "operators.json":
			rows = uint64(len(operators))
		case "geo_dict.json":
			rows = uint64(len(dictionary))
		}
		manifest.Files[name] = flowdimension.GeoFileSpec{SHA256: hex.EncodeToString(digest[:]), Rows: rows}
	}
	if err := os.WriteFile(filepath.Join(directory, "manifest.json"), jsonBytes(t, manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	if changed, err := catalog.Reload(directory, flowdimension.GeoLoadLimits{}); err != nil || !changed {
		t.Fatalf("load Geo %s changed=%v err=%v", version, changed, err)
	}
}

func zstdBytes(t testing.TB, data []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	encoder, err := zstd.NewWriter(&buffer, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func jsonBytes(t testing.TB, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func address16(value string) []byte {
	address, err := netip.ParseAddr(value)
	if err != nil {
		panic(err)
	}
	bytes := address.As16()
	return append([]byte(nil), bytes[:]...)
}

func mustAddress(value []byte) netip.Addr {
	address, ok := parseAddress16(value)
	if !ok {
		panic("invalid address")
	}
	return address
}

func testMinute(hour, minute int) time.Time {
	return time.Date(2026, 9, 5, hour, minute, 0, 0, time.UTC)
}
