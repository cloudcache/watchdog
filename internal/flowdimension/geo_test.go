package flowdimension

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
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestLoadGeoIndexValidatesAndLooksUpBothFamilies(t *testing.T) {
	directory := writeGeoFixture(t, t.TempDir(), "geo-v1",
		[]string{
			"1.0.1.0,1.0.3.255,CN,350100,福建省,福州市,3,4134",
			"8.8.8.0,8.8.8.255,US,,California,Mountain View,0,15169",
		},
		[]string{"2001:db8::,2001:db8::ffff,CN,350100,福建省,福州市,3,4134"},
		[]GeoOperator{{ID: 3, Name: "中国电信", ShortName: "电信", Category: "carrier", Enabled: true}},
		[]GeoDictionaryEntry{
			{Kind: "province", Code: "350000", Name: "福建省", Enabled: true},
			{Kind: "city", Code: "350100", Name: "福州市", ParentCode: "350000", Enabled: true},
		},
	)
	index, err := LoadGeoIndex(directory, GeoLoadLimits{})
	if err != nil {
		t.Fatal(err)
	}
	metadata := index.Metadata()
	if metadata.Version != "geo-v1" || metadata.IPv4Rows != 2 || metadata.IPv6Rows != 1 ||
		metadata.DistinctInfoRows != 2 || metadata.OperatorRows != 1 || metadata.DictionaryRows != 2 || metadata.EffectiveFrom.Minute() != 0 ||
		!strings.HasPrefix(metadata.ManifestChecksum, "sha256:") {
		t.Fatalf("unexpected metadata: %#v", metadata)
	}

	for _, test := range []struct {
		address string
		country string
		asn     uint32
	}{
		{address: "1.0.1.0", country: "CN", asn: 4134},
		{address: "1.0.3.255", country: "CN", asn: 4134},
		{address: "::ffff:1.0.1.1", country: "CN", asn: 4134},
		{address: "8.8.8.8", country: "US", asn: 15169},
		{address: "2001:db8::100", country: "CN", asn: 4134},
	} {
		actual, ok := index.Lookup(netip.MustParseAddr(test.address))
		if !ok || actual.Country != test.country || actual.ASN != test.asn || actual.Version != "geo-v1" || actual.Source != GeoSchema {
			t.Fatalf("Lookup(%s) = %#v, %v", test.address, actual, ok)
		}
	}
	for _, address := range []string{"1.0.0.255", "1.0.4.0", "2001:db9::1"} {
		if actual, ok := index.Lookup(netip.MustParseAddr(address)); ok {
			t.Fatalf("Lookup(%s) unexpectedly returned %#v", address, actual)
		}
	}
	operator, ok := index.Operator(3)
	if !ok || operator.ShortName != "电信" {
		t.Fatalf("Operator(3) = %#v, %v", operator, ok)
	}
	entry, ok := index.Dictionary("city", "350100")
	if !ok || entry.ParentCode != "350000" {
		t.Fatalf("Dictionary(city, 350100) = %#v, %v", entry, ok)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		_, _ = index.Lookup(netip.MustParseAddr("2001:db8::100"))
	}); allocations != 0 {
		t.Fatalf("geo lookup allocates %.2f objects per call", allocations)
	}
}

func TestLoadGeoIndexAcceptsExplicitVersionedEmptySnapshot(t *testing.T) {
	directory := writeGeoFixture(t, t.TempDir(), "geo-empty", nil, nil, nil, nil)
	index, err := LoadGeoIndex(directory, GeoLoadLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if metadata := index.Metadata(); metadata.Version != "geo-empty" || metadata.IPv4Rows != 0 || metadata.IPv6Rows != 0 {
		t.Fatalf("unexpected empty snapshot metadata: %#v", metadata)
	}
	if _, ok := index.Lookup(netip.MustParseAddr("192.0.2.1")); ok {
		t.Fatal("empty snapshot must not match an address")
	}
}

func TestLoadGeoIndexRejectsInvalidManifestsAndFiles(t *testing.T) {
	t.Run("unknown manifest field", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", nil, nil, nil, nil)
		manifestPath := filepath.Join(directory, "manifest.json")
		data := readTestFile(t, manifestPath)
		data = bytes.Replace(data, []byte(`"schema":"flow-geo-v1"`), []byte(`"schema":"flow-geo-v1","unexpected":true`), 1)
		writeTestFile(t, manifestPath, data)
		if _, err := LoadGeoIndex(directory, GeoLoadLimits{}); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("expected strict manifest failure, got %v", err)
		}
	})

	t.Run("effective time must be a UTC minute", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", nil, nil, nil, nil)
		updateManifest(t, directory, func(manifest *GeoManifest) { manifest.EffectiveFrom = manifest.EffectiveFrom.Add(time.Second) })
		if _, err := LoadGeoIndex(directory, GeoLoadLimits{}); err == nil || !strings.Contains(err.Error(), "UTC minute") {
			t.Fatalf("expected effective time failure, got %v", err)
		}
	})

	t.Run("dictionary null is not an explicit array", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", nil, nil, nil, nil)
		path := filepath.Join(directory, "operators.json")
		writeTestFile(t, path, []byte("null"))
		updateManifestFileChecksum(t, directory, "operators.json")
		if _, err := LoadGeoIndex(directory, GeoLoadLimits{}); err == nil || !strings.Contains(err.Error(), "JSON array") {
			t.Fatalf("expected explicit array failure, got %v", err)
		}
	})

	t.Run("file checksum", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", nil, nil, nil, nil)
		path := filepath.Join(directory, "operators.json")
		writeTestFile(t, path, append(readTestFile(t, path), ' '))
		if _, err := LoadGeoIndex(directory, GeoLoadLimits{}); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
			t.Fatalf("expected checksum failure, got %v", err)
		}
	})

	t.Run("row count", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", []string{"192.0.2.0,192.0.2.255,ZZ,,,,0,0"}, nil, nil, nil)
		updateManifest(t, directory, func(manifest *GeoManifest) {
			manifest.Files["ipv4.csv.zst"] = GeoFileSpec{SHA256: manifest.Files["ipv4.csv.zst"].SHA256, Rows: 0}
		})
		if _, err := LoadGeoIndex(directory, GeoLoadLimits{}); err == nil || !strings.Contains(err.Error(), "more rows") {
			t.Fatalf("expected row count failure, got %v", err)
		}
	})

	t.Run("overlapping ranges", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", []string{
			"192.0.2.0,192.0.2.200,ZZ,,,,0,0",
			"192.0.2.128,192.0.2.255,ZZ,,,,0,0",
		}, nil, nil, nil)
		if _, err := LoadGeoIndex(directory, GeoLoadLimits{}); err == nil || !strings.Contains(err.Error(), "overlaps") {
			t.Fatalf("expected overlap failure, got %v", err)
		}
	})

	t.Run("operator foreign key", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", []string{"192.0.2.0,192.0.2.255,ZZ,,,,7,64500"}, nil, nil, nil)
		if _, err := LoadGeoIndex(directory, GeoLoadLimits{}); err == nil || !strings.Contains(err.Error(), "operators.json") {
			t.Fatalf("expected operator foreign key failure, got %v", err)
		}
	})

	t.Run("admin foreign key", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", []string{"192.0.2.0,192.0.2.255,CN,110100,北京,北京,0,0"}, nil, nil, nil)
		if _, err := LoadGeoIndex(directory, GeoLoadLimits{}); err == nil || !strings.Contains(err.Error(), "geo_dict.json") {
			t.Fatalf("expected admin foreign key failure, got %v", err)
		}
	})

	t.Run("HMT normalization", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", []string{"192.0.2.0,192.0.2.255,HK,,香港,香港,0,0"}, nil, nil, nil)
		if _, err := LoadGeoIndex(directory, GeoLoadLimits{}); err == nil || !strings.Contains(err.Error(), "normalized") {
			t.Fatalf("expected HMT normalization failure, got %v", err)
		}
	})

	t.Run("dictionary parent", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", nil, nil, nil, []GeoDictionaryEntry{
			{Kind: "city", Code: "110100", Name: "北京市", ParentCode: "110000", Enabled: true},
		})
		if _, err := LoadGeoIndex(directory, GeoLoadLimits{}); err == nil || !strings.Contains(err.Error(), "parent_code") {
			t.Fatalf("expected dictionary parent failure, got %v", err)
		}
	})

	t.Run("dictionary parent cycle", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", nil, nil, nil, []GeoDictionaryEntry{
			{Kind: "province", Code: "110000", Name: "北京市", ParentCode: "110100", Enabled: true},
			{Kind: "city", Code: "110100", Name: "北京市", ParentCode: "110000", Enabled: true},
		})
		if _, err := LoadGeoIndex(directory, GeoLoadLimits{}); err == nil || !strings.Contains(err.Error(), "parent cycle") {
			t.Fatalf("expected dictionary cycle failure, got %v", err)
		}
	})

	t.Run("configured row limit", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", []string{"192.0.2.0,192.0.2.255,ZZ,,,,0,0"}, nil, nil, nil)
		limits := normalizeGeoLoadLimits(GeoLoadLimits{})
		updateManifest(t, directory, func(manifest *GeoManifest) {
			manifest.Files["ipv4.csv.zst"] = GeoFileSpec{SHA256: manifest.Files["ipv4.csv.zst"].SHA256, Rows: 2}
		})
		limits.MaxIPv4Rows = 1
		if _, err := LoadGeoIndex(directory, limits); err == nil || !strings.Contains(err.Error(), "row count exceeds") {
			t.Fatalf("expected configured row limit failure, got %v", err)
		}
	})

	t.Run("bounded CSV row", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", []string{"192.0.2.0,192.0.2.255,ZZ,," + strings.Repeat("x", 2048) + ",city,0,0"}, nil, nil, nil)
		if _, err := LoadGeoIndex(directory, GeoLoadLimits{MaxCSVRowBytes: 512}); err == nil || !strings.Contains(err.Error(), "token too long") {
			t.Fatalf("expected bounded row failure, got %v", err)
		}
	})

	t.Run("bounded uncompressed stream", func(t *testing.T) {
		directory := writeGeoFixture(t, t.TempDir(), "geo-v1", []string{"192.0.2.0,192.0.2.255,ZZ,,,,0,0"}, nil, nil, nil)
		if _, err := LoadGeoIndex(directory, GeoLoadLimits{MaxUncompressedBytes: 64}); err == nil {
			t.Fatal("expected uncompressed stream limit failure")
		}
	})
}

func TestGeoCatalogReloadIsAtomicAndRetainsHistoricalVersions(t *testing.T) {
	root := t.TempDir()
	v1 := writeGeoFixture(t, root, "geo-v1", []string{"192.0.2.0,192.0.2.255,ZZ,,,,0,64500"}, nil, nil, nil)
	v2 := writeGeoFixture(t, root, "geo-v2", []string{"198.51.100.0,198.51.100.255,ZZ,,,,0,64501"}, nil, nil, nil)
	updateManifest(t, v2, func(manifest *GeoManifest) { manifest.EffectiveFrom = manifest.EffectiveFrom.Add(time.Minute) })
	current := filepath.Join(root, "current")
	replaceTestSymlink(t, current, v1)
	catalog := NewGeoCatalog()

	changed, err := catalog.Reload(current, GeoLoadLimits{})
	if err != nil || !changed {
		t.Fatalf("initial Reload() = %v, %v", changed, err)
	}
	first, ok := catalog.Active()
	if !ok || first.Metadata().Version != "geo-v1" {
		t.Fatalf("unexpected initial active index: %#v, %v", first, ok)
	}
	changed, err = catalog.Reload(current, GeoLoadLimits{})
	if err != nil || changed {
		t.Fatalf("unchanged Reload() = %v, %v", changed, err)
	}

	replaceTestSymlink(t, current, v2)
	updateManifest(t, v2, func(manifest *GeoManifest) {
		spec := manifest.Files["ipv4.csv.zst"]
		spec.SHA256 = strings.Repeat("0", 64)
		manifest.Files["ipv4.csv.zst"] = spec
	})
	if changed, err = catalog.Reload(current, GeoLoadLimits{}); err == nil || changed {
		t.Fatalf("invalid Reload() = %v, %v", changed, err)
	}
	active, _ := catalog.Active()
	if active != first {
		t.Fatal("failed reload replaced the active index")
	}

	v2 = writeGeoFixture(t, root, "geo-v2-fixed", []string{"198.51.100.0,198.51.100.255,ZZ,,,,0,64501"}, nil, nil, nil)
	updateManifest(t, v2, func(manifest *GeoManifest) { manifest.EffectiveFrom = manifest.EffectiveFrom.Add(time.Minute) })
	replaceTestSymlink(t, current, v2)
	if changed, err = catalog.Reload(current, GeoLoadLimits{}); err != nil || !changed {
		t.Fatalf("second version Reload() = %v, %v", changed, err)
	}
	active, _ = catalog.Active()
	if active.Metadata().Version != "geo-v2-fixed" {
		t.Fatalf("active version = %q", active.Metadata().Version)
	}
	if historical, ok := catalog.Get("geo-v1"); !ok || historical != first {
		t.Fatal("first version was not retained for historical records")
	}

	if selected, err := catalog.Select(time.Date(2026, 9, 5, 12, 0, 30, 0, time.UTC)); err != nil || selected != first {
		t.Fatalf("historical Select() = %#v, %v", selected, err)
	}
	if selected, err := catalog.Select(time.Date(2026, 9, 5, 12, 1, 0, 0, time.UTC)); err != nil || selected != active {
		t.Fatalf("new Select() = %#v, %v", selected, err)
	}
	if _, err := catalog.Select(time.Date(2026, 9, 5, 12, 0, 59, 0, time.UTC).Add(-time.Minute)); !errors.Is(err, ErrNoGeoIndex) {
		t.Fatalf("Select() before first version error = %v", err)
	}

	replaceTestSymlink(t, current, v1)
	if changed, err = catalog.Reload(current, GeoLoadLimits{}); err == nil || changed || !strings.Contains(err.Error(), "rollback requires a new version") {
		t.Fatalf("old-version rollback Reload() = %v, %v", changed, err)
	}
	active, _ = catalog.Active()
	if active.Metadata().Version != "geo-v2-fixed" {
		t.Fatal("old-version rollback replaced the active index")
	}
	updateManifest(t, v1, func(manifest *GeoManifest) { manifest.GeneratedAt = manifest.GeneratedAt.Add(time.Second) })
	if changed, err = catalog.Reload(current, GeoLoadLimits{}); err == nil || changed || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("mutated version Reload() = %v, %v", changed, err)
	}
	active, _ = catalog.Active()
	if active.Metadata().Version != "geo-v2-fixed" {
		t.Fatal("immutable-version rejection replaced the active index")
	}
}

func TestGeoCatalogLoadsHistoricalVersionsWithoutChangingActiveAndPrunesSafely(t *testing.T) {
	root := t.TempDir()
	v1 := writeGeoFixture(t, root, "geo-v1", []string{"192.0.2.0,192.0.2.255,ZZ,,,,0,64500"}, nil, nil, nil)
	v2 := writeGeoFixture(t, root, "geo-v2", []string{"198.51.100.0,198.51.100.255,ZZ,,,,0,64501"}, nil, nil, nil)
	updateManifest(t, v2, func(manifest *GeoManifest) { manifest.EffectiveFrom = manifest.EffectiveFrom.Add(time.Minute) })
	catalog := NewGeoCatalog()
	if _, err := catalog.Reload(v2, GeoLoadLimits{}); err != nil {
		t.Fatal(err)
	}
	active, _ := catalog.Active()
	if installed, err := catalog.LoadHistorical(v1, GeoLoadLimits{}); err != nil || !installed {
		t.Fatalf("LoadHistorical() = %v, %v", installed, err)
	}
	if current, _ := catalog.Active(); current != active {
		t.Fatal("historical load changed active version")
	}
	if selected, err := catalog.Select(time.Date(2026, 9, 5, 12, 0, 30, 0, time.UTC)); err != nil || selected.Metadata().Version != "geo-v1" {
		t.Fatalf("historical Select() = %#v, %v", selected, err)
	}
	if removed := catalog.RetainVersions([]string{"geo-missing"}); removed != 0 {
		t.Fatalf("unknown reference pruned %d versions", removed)
	}
	if _, ok := catalog.Get("geo-v1"); !ok {
		t.Fatal("unknown reference must conservatively retain history")
	}
	if removed := catalog.RetainVersions(nil); removed != 1 {
		t.Fatalf("RetainVersions() removed %d versions", removed)
	}
	if _, ok := catalog.Get("geo-v1"); ok {
		t.Fatal("unreferenced historical version was retained")
	}
	if _, err := catalog.Select(time.Date(2026, 9, 5, 12, 0, 30, 0, time.UTC)); !errors.Is(err, ErrNoGeoIndex) {
		t.Fatalf("pruned interval Select() error = %v", err)
	}
	if current, ok := catalog.Active(); !ok || current != active {
		t.Fatal("active version was pruned")
	}
}

func TestGeoCatalogReadersSeeOnlyCompleteIndexesDuringReload(t *testing.T) {
	root := t.TempDir()
	v1 := writeGeoFixture(t, root, "geo-v1", []string{"192.0.2.0,192.0.2.255,ZZ,,,,0,64500"}, nil, nil, nil)
	v2 := writeGeoFixture(t, root, "geo-v2", []string{"198.51.100.0,198.51.100.255,ZZ,,,,0,64501"}, nil, nil, nil)
	updateManifest(t, v2, func(manifest *GeoManifest) { manifest.EffectiveFrom = manifest.EffectiveFrom.Add(time.Minute) })
	catalog := NewGeoCatalog()
	if _, err := catalog.Reload(v1, GeoLoadLimits{}); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	start := make(chan struct{})
	errorsSeen := make(chan string, 8)
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for range 1000 {
				index, ok := catalog.Active()
				if !ok {
					errorsSeen <- "missing active index"
					return
				}
				version := index.Metadata().Version
				switch version {
				case "geo-v1":
					if _, ok := index.Lookup(netip.MustParseAddr("192.0.2.1")); !ok {
						errorsSeen <- "incomplete geo-v1"
						return
					}
				case "geo-v2":
					if _, ok := index.Lookup(netip.MustParseAddr("198.51.100.1")); !ok {
						errorsSeen <- "incomplete geo-v2"
						return
					}
				default:
					errorsSeen <- "unexpected active version"
					return
				}
			}
		}()
	}
	close(start)
	if _, err := catalog.Reload(v2, GeoLoadLimits{}); err != nil {
		t.Fatal(err)
	}
	wait.Wait()
	close(errorsSeen)
	for message := range errorsSeen {
		t.Error(message)
	}
}

func BenchmarkGeoIndexLookup(b *testing.B) {
	ranges := make([]geoRange4, 1<<16)
	infos := make([]GeoInfo, len(ranges))
	for position := range ranges {
		start := uint32(position) << 16
		ranges[position] = geoRange4{start: start, end: start + 255, info: uint32(position)}
		infos[position] = GeoInfo{Country: "ZZ", ASN: uint32(position), Version: "geo-bench"}
	}
	index := &GeoIndex{ipv4: ranges, infos: infos}
	addresses := []netip.Addr{netip.MustParseAddr("0.0.0.10"), netip.MustParseAddr("127.255.0.10"), netip.MustParseAddr("255.255.0.10")}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_, _ = index.Lookup(addresses[iteration%len(addresses)])
	}
}

type testTB interface {
	Helper()
	Fatalf(string, ...any)
}

func writeGeoFixture(t testTB, root, version string, ipv4, ipv6 []string, operators []GeoOperator, dictionary []GeoDictionaryEntry) string {
	t.Helper()
	directory := filepath.Join(root, version)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if operators == nil {
		operators = []GeoOperator{}
	}
	if dictionary == nil {
		dictionary = []GeoDictionaryEntry{}
	}
	files := map[string][]byte{
		"ipv4.csv.zst":   encodeTestZstd(t, geoCSV(ipv4)),
		"ipv6.csv.zst":   encodeTestZstd(t, geoCSV(ipv6)),
		"operators.json": marshalTestJSON(t, operators),
		"geo_dict.json":  marshalTestJSON(t, dictionary),
	}
	manifest := GeoManifest{
		Schema: GeoSchema, Version: version, GeneratedAt: time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC),
		EffectiveFrom:   time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC),
		AdminCodeSystem: GeoAdminCodeSystem, UnknownCountry: GeoUnknownCountry, Files: make(map[string]GeoFileSpec, len(files)),
	}
	for name, data := range files {
		writeTestFile(t, filepath.Join(directory, name), data)
		rows := len(operators)
		switch name {
		case "ipv4.csv.zst":
			rows = len(ipv4)
		case "ipv6.csv.zst":
			rows = len(ipv6)
		case "geo_dict.json":
			rows = len(dictionary)
		}
		digest := sha256.Sum256(data)
		manifest.Files[name] = GeoFileSpec{SHA256: hex.EncodeToString(digest[:]), Rows: uint64(rows)}
	}
	writeTestFile(t, filepath.Join(directory, "manifest.json"), marshalTestJSON(t, manifest))
	return directory
}

func geoCSV(rows []string) []byte {
	all := append([]string{"ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn"}, rows...)
	return []byte(strings.Join(all, "\n") + "\n")
}

func encodeTestZstd(t testTB, data []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	encoder, err := zstd.NewWriter(&buffer, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := encoder.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buffer.Bytes()
}

func marshalTestJSON(t testTB, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return data
}

func updateManifest(t *testing.T, directory string, update func(*GeoManifest)) {
	t.Helper()
	path := filepath.Join(directory, "manifest.json")
	var manifest GeoManifest
	if err := json.Unmarshal(readTestFile(t, path), &manifest); err != nil {
		t.Fatal(err)
	}
	update(&manifest)
	writeTestFile(t, path, marshalTestJSON(t, manifest))
}

func updateManifestFileChecksum(t *testing.T, directory, name string) {
	t.Helper()
	data := readTestFile(t, filepath.Join(directory, name))
	digest := sha256.Sum256(data)
	updateManifest(t, directory, func(manifest *GeoManifest) {
		spec := manifest.Files[name]
		spec.SHA256 = hex.EncodeToString(digest[:])
		manifest.Files[name] = spec
	})
}

func readTestFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeTestFile(t testTB, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func replaceTestSymlink(t *testing.T, link, target string) {
	t.Helper()
	temporary := link + ".next"
	_ = os.Remove(temporary)
	if err := os.Symlink(target, temporary); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, link); err != nil {
		t.Fatal(err)
	}
}
