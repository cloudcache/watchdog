// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

func TestAddressSnapshotMatchesLegacyReaderOnSharedCorpus(t *testing.T) {
	effective := testMinute(12, 0)
	dimension, err := flowdimension.CompileBundle(flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion, SnapshotID: "dimension-parity", Version: 1, EffectiveFrom: effective,
		Operators: []flowdimension.OperatorDefinition{
			{ID: "customer-primary", FlowISPID: 3, Code: "PRIMARY", Name: "Primary", Category: "carrier", ASNs: []uint32{64500}, Enabled: true},
			{ID: "customer-secondary", FlowISPID: 4, Code: "SECONDARY", Name: "Secondary", Category: "carrier", ASNs: []uint32{64501}, Enabled: true},
		},
		Prefixes: []flowdimension.PrefixDefinition{
			{ID: "local-v4", CIDR: "10.0.0.0/8", Labels: map[string]string{"business": "customer", "flow": "local"}},
			{ID: "local-v6", CIDR: "2001:db8:1::/48", Labels: map[string]string{"business": "customer", "flow": "local"}},
			{ID: "remote-override", CIDR: "203.0.113.128/25", Labels: map[string]string{
				"flow.geo.asn": "64501", "flow.geo.isp_id": "4", "flow.geo.reason": "parity",
			}},
		},
		AddressSets: []flowdimension.AddressSetDefinition{{
			ID: "all-remote", Name: "All remote", Members: []string{"198.51.100.0/24", "203.0.113.0/24", "2001:db8:2::/48"}, MatchDirection: "both", Enabled: true,
		}},
	}, flowdimension.CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	classification := compileClassification(t, 1, effective, "dimension-parity", flowdimension.RecordPolicyCount, flowdimension.RecordPolicyCount)
	legacyDimensions, _ := flowdimension.NewSnapshotCatalog(dimension)
	legacyClassifications, _ := flowdimension.NewClassificationCatalog(classification)
	legacyGeo := loadAddressSnapshotParityGeo(t, effective)
	legacy := newTestEnricher(t, legacyDimensions, legacyGeo, legacyClassifications)

	built, err := flowdimension.BuildAddressSnapshot(flowdimension.AddressSnapshotBuildInput{
		Definition: dimension, BuilderVersion: "watchdog-parity-1",
		SupplierGeoNodes: []flowdimension.AddressSnapshotBuildGeoNode{
			{ID: "330000", Kind: "province", Code: "330000", Name: "Zhejiang", ParentID: "CN", Enabled: true},
			{ID: "330100", Kind: "city", Code: "330100", Name: "Hangzhou", ParentID: "330000", Enabled: true},
			{ID: "Asia", Kind: "continent", Code: "Asia", Name: "Asia", Enabled: true},
			{ID: "CN", Kind: "country", Code: "CN", Name: "China", ParentID: "Asia", Enabled: true},
			{ID: "NorthAmerica", Kind: "continent", Code: "NorthAmerica", Name: "North America", Enabled: true},
			{ID: "US", Kind: "country", Code: "US", Name: "United States", ParentID: "NorthAmerica", Enabled: true},
		},
		SupplierOperators: []flowdimension.AddressSnapshotBuildOperator{
			{ID: 3, StableID: "supplier-primary", Code: "PRIMARY", Name: "Primary", Category: "carrier", ASNs: []uint32{64500}, Enabled: true},
		},
		Sources: []flowdimension.AddressSnapshotBuildSource{{
			Slot: flowdimension.AddressSnapshotSourceCombined, ImportID: "parity-source", ChecksumSHA256: "sha256:" + strings.Repeat("a", 64), SlotRowVersion: 1, RowCountV4: 2, RowCountV6: 1,
			Ranges: []flowdimension.AddressSnapshotBuildRange{
				{Start: netip.MustParseAddr("198.51.100.0"), End: netip.MustParseAddr("198.51.100.255"), Geo: flowdimension.AddressSnapshotBuildGeo{CountryCode: "US", ContinentID: "NorthAmerica", CountryID: "US"}},
				{Start: netip.MustParseAddr("203.0.113.0"), End: netip.MustParseAddr("203.0.113.255"), Geo: flowdimension.AddressSnapshotBuildGeo{CountryCode: "CN", AdminCode: "330100", Subdivision: "Zhejiang", City: "Hangzhou", ContinentID: "Asia", CountryID: "CN", ProvinceID: "330000", CityID: "330100"}, ISPID: 3, ASN: 64500},
				{Start: netip.MustParseAddr("2001:db8:2::"), End: netip.MustParseAddr("2001:db8:2::ffff:ffff:ffff:ffff"), Geo: flowdimension.AddressSnapshotBuildGeo{CountryCode: "CN", AdminCode: "330100", Subdivision: "Zhejiang", City: "Hangzhou", ContinentID: "Asia", CountryID: "CN", ProvinceID: "330000", CityID: "330100"}, ISPID: 3, ASN: 64500},
			},
		}},
	}, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	classificationData, err := json.Marshal(flowdimension.ClassificationBundle{
		SchemaVersion: flowdimension.ClassificationSchemaVersion, Version: 1, EffectiveFrom: effective, DimensionSnapshotID: "dimension-parity",
		HomeProvince: "330000", HomeCity: "330100", HomeISPIDs: []uint16{3}, OverseasIncludesHMT: true,
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	publication := EnrichmentVersionPublication{
		PublicationID: "publication-parity", DimensionSnapshotID: "dimension-parity", DimensionVersion: 1, DimensionEffectiveFrom: effective,
		Dimension:             VersionObjectReference{ObjectRef: "dimension.wads", Checksum: built.ChecksumSHA256, ObjectFormat: VersionObjectFormatWADS, ObjectFormatVersion: flowdimension.AddressSnapshotFormatVersion},
		ClassificationVersion: 1, ClassificationEffectiveFrom: effective,
		Classification: VersionObjectReference{ObjectRef: "classification.json", Checksum: versionObjectChecksum(classificationData)},
	}
	versions, _ := NewEnrichmentVersionCatalog()
	loader, _ := NewVersionLoader(&memoryVersionObjectSource{objects: map[string][]byte{"dimension.wads": built.Data, "classification.json": classificationData}}, &recordingVersionAcknowledger{}, versions, VersionWorkerIdentity{WorkerID: "parity-worker", BootID: "parity-boot", SoftwareVersion: "test"}, VersionLoaderLimits{})
	if err := loader.Install(context.Background(), publication); err != nil {
		t.Fatal(err)
	}
	wads, err := NewEnricherWithVersionCatalog(versions, nil, EnrichmentLimits{})
	if err != nil {
		t.Fatal(err)
	}

	batch := addressSnapshotParityBatch(effective)
	legacyResult, err := legacy.EnrichBatch(cloneBatch(batch))
	if err != nil {
		t.Fatal(err)
	}
	wadsResult, err := wads.EnrichBatch(cloneBatch(batch))
	if err != nil {
		t.Fatal(err)
	}
	if len(legacyResult.Records) != len(wadsResult.Records) {
		t.Fatalf("record counts differ: legacy=%d WADS=%d", len(legacyResult.Records), len(wadsResult.Records))
	}
	for index := range legacyResult.Records {
		assertAddressSnapshotParity(t, legacyResult.Records[index], wadsResult.Records[index])
	}
}

func addressSnapshotParityBatch(effective time.Time) *RecordBatch {
	batch := testBatch(effective.Add(30 * time.Minute))
	addresses := [][2]string{
		{"10.1.2.3", "203.0.113.20"},
		{"10.1.2.3", "203.0.113.200"},
		{"2001:db8:1::10", "2001:db8:2::20"},
		{"10.1.2.3", "198.51.100.20"},
	}
	prototype := batch.Records[0]
	batch.Records = make([]*Record, len(addresses))
	for index, pair := range addresses {
		record := cloneRecord(prototype)
		record.RecordIndex = uint32(index)
		record.SourceIP, record.DestinationIP = address16(pair[0]), address16(pair[1])
		batch.Records[index] = record
	}
	return batch
}

func assertAddressSnapshotParity(t *testing.T, legacy, wads EnrichedRecord) {
	t.Helper()
	if legacy.Dimensions.Direction != wads.Dimensions.Direction || legacy.Dimensions.Business != wads.Dimensions.Business ||
		legacy.Dimensions.Local.PrefixID != wads.Dimensions.Local.PrefixID || legacy.Dimensions.Remote.PrefixID != wads.Dimensions.Remote.PrefixID ||
		!reflect.DeepEqual(legacy.Dimensions.Local.AddressSets.IDs(), wads.Dimensions.Local.AddressSets.IDs()) ||
		!reflect.DeepEqual(legacy.Dimensions.Remote.AddressSets.IDs(), wads.Dimensions.Remote.AddressSets.IDs()) {
		t.Fatalf("dimension parity differs\nlegacy=%+v\nWADS=%+v", legacy.Dimensions, wads.Dimensions)
	}
	legacyRemote, wadsRemote := legacy.RemoteGeo, wads.RemoteGeo
	legacySupplier, wadsSupplier := legacy.SupplierRemoteGeo, wads.SupplierRemoteGeo
	legacyRemote.Version, wadsRemote.Version = "", ""
	legacySupplier.Version, wadsSupplier.Version = "", ""
	if legacyRemote != wadsRemote || legacySupplier != wadsSupplier || legacy.RemoteASN != wads.RemoteASN || legacy.RemoteASNSource != wads.RemoteASNSource ||
		legacy.SupplierRemoteASN != wads.SupplierRemoteASN || legacy.SupplierRemoteASNSource != wads.SupplierRemoteASNSource ||
		legacy.Category != wads.Category || legacy.SupplierCategory != wads.SupplierCategory || legacy.CustomerGeoOverrideFields != wads.CustomerGeoOverrideFields ||
		legacy.Disposition != wads.Disposition || legacy.ClassificationVersion != wads.ClassificationVersion {
		t.Fatalf("attribution parity differs\nlegacy=%+v\nWADS=%+v", legacy, wads)
	}
}

func loadAddressSnapshotParityGeo(t *testing.T, effective time.Time) *flowdimension.GeoCatalog {
	t.Helper()
	directory := t.TempDir()
	header := "ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn,geo_leaf_code\n"
	files := map[string][]byte{
		"ipv4.csv.zst": zstdBytes(t, []byte(header+
			"198.51.100.0,198.51.100.255,US,,,,0,0,US\n"+
			"203.0.113.0,203.0.113.255,CN,330100,Zhejiang,Hangzhou,3,64500,330100\n")),
		"ipv6.csv.zst":   zstdBytes(t, []byte(header+"2001:db8:2::,2001:db8:2::ffff:ffff:ffff:ffff,CN,330100,Zhejiang,Hangzhou,3,64500,330100\n")),
		"operators.json": jsonBytes(t, []flowdimension.GeoOperator{{ID: 3, Name: "Primary", ShortName: "P", Category: "carrier", Enabled: true}}),
		"geo_dict.json": jsonBytes(t, []flowdimension.GeoDictionaryEntry{
			{Kind: "continent", Code: "Asia", Name: "Asia", Enabled: true},
			{Kind: "continent", Code: "NorthAmerica", Name: "North America", Enabled: true},
			{Kind: "country", Code: "CN", Name: "China", ParentCode: "Asia", Enabled: true},
			{Kind: "country", Code: "US", Name: "United States", ParentCode: "NorthAmerica", Enabled: true},
			{Kind: "province", Code: "330000", Name: "Zhejiang", ParentCode: "CN", Enabled: true},
			{Kind: "city", Code: "330100", Name: "Hangzhou", ParentCode: "330000", Enabled: true},
		}),
	}
	manifest := flowdimension.GeoManifest{
		Schema: flowdimension.GeoSchemaV2, Version: "geo-parity", GeneratedAt: effective, EffectiveFrom: effective,
		AdminCodeSystem: flowdimension.GeoAdminCodeSystem, UnknownCountry: flowdimension.GeoUnknownCountry, Files: make(map[string]flowdimension.GeoFileSpec, len(files)),
	}
	rows := map[string]uint64{"ipv4.csv.zst": 2, "ipv6.csv.zst": 1, "operators.json": 1, "geo_dict.json": 6}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(directory, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		manifest.Files[name] = flowdimension.GeoFileSpec{SHA256: hex.EncodeToString(digest[:]), Rows: rows[name]}
	}
	if err := os.WriteFile(filepath.Join(directory, "manifest.json"), jsonBytes(t, manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	catalog := flowdimension.NewGeoCatalog()
	if changed, err := catalog.Reload(directory, flowdimension.GeoLoadLimits{}); err != nil || !changed {
		t.Fatalf("load parity Geo changed=%t err=%v", changed, err)
	}
	return catalog
}
