package watchdog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func writeGeoFixtureFile(t *testing.T, dir, name string, data []byte, manifest *FlowGeoManifest, rows int) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	manifest.Files[name] = FlowGeoManifestRef{SHA256: hex.EncodeToString(digest[:]), Rows: rows}
}

func compressCSV(t *testing.T, csv string) []byte {
	t.Helper()
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	return encoder.EncodeAll([]byte(csv), nil)
}

func writeGeoFixtureBundle(t *testing.T, mutate func(manifest *FlowGeoManifest, dir string)) string {
	t.Helper()
	dir := t.TempDir()
	manifest := &FlowGeoManifest{
		Schema:          "flow-geo-v1",
		Version:         "test-1",
		GeneratedAt:     "2026-09-05T00:00:00Z",
		AdminCodeSystem: "GB/T2260-6",
		UnknownCountry:  "ZZ",
		Files:           map[string]FlowGeoManifestRef{},
	}
	v4 := "ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn\n" +
		"1.96.0.0,1.96.255.255,CN,810000,,,0,4760\n" +
		"8.8.8.0,8.8.8.255,US,,CA,,0,15169\n" +
		"124.160.0.0,124.160.135.255,CN,330100,,,1,4134\n" +
		"124.160.136.0,124.160.136.255,CN,440000,,,3,9808\n"
	writeGeoFixtureFile(t, dir, "ipv4.csv.zst", compressCSV(t, v4), manifest, 4)
	v6 := "ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn\n" +
		"2400::,2400:ffff:ffff:ffff:ffff:ffff:ffff:ffff,CN,440100,,,1,4134\n"
	writeGeoFixtureFile(t, dir, "ipv6.csv.zst", compressCSV(t, v6), manifest, 1)
	operators, _ := json.Marshal([]FlowGeoOperator{
		{ID: 1, Name: "中国电信", ShortName: "电信", Category: "carrier", Enabled: true},
		{ID: 3, Name: "中国移动", ShortName: "移动", Category: "carrier", Enabled: true},
	})
	writeGeoFixtureFile(t, dir, "operators.json", operators, manifest, 2)
	dictionary, _ := json.Marshal([]FlowGeoDictEntry{
		{Kind: "province", Code: "330000", Name: "浙江", Enabled: true},
		{Kind: "province", Code: "440000", Name: "广东", Enabled: true},
		{Kind: "province", Code: "810000", Name: "香港", Enabled: true},
	})
	writeGeoFixtureFile(t, dir, "geo_dict.json", dictionary, manifest, 3)
	if mutate != nil {
		mutate(manifest, dir)
	}
	manifestRaw, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), manifestRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestFlowGeoBundleLoadAndLookup(t *testing.T) {
	dir := writeGeoFixtureBundle(t, nil)
	index, err := LoadFlowGeoBundle(dir)
	if err != nil {
		t.Fatal(err)
	}
	if index.RowsV4 != 4 || index.RowsV6 != 1 {
		t.Fatalf("rows v4=%d v6=%d", index.RowsV4, index.RowsV6)
	}

	info, found := index.Lookup(netip.MustParseAddr("124.160.136.66"))
	if !found || info.Country != "CN" || info.AdminCode != "440000" || info.ISPID != 3 || info.ISPName != "中国移动" || info.ASN != 9808 {
		t.Fatalf("corrected range lookup = %+v found=%v", info, found)
	}
	if info.Subdivision != "广东" {
		t.Fatalf("province name from dictionary = %q", info.Subdivision)
	}

	info, found = index.Lookup(netip.MustParseAddr("124.160.1.1"))
	if !found || info.AdminCode != "330100" || info.ISPName != "中国电信" {
		t.Fatalf("base range lookup = %+v", info)
	}

	info, found = index.Lookup(netip.MustParseAddr("1.96.8.8"))
	if !found || info.Country != "CN" || info.AdminCode != "810000" {
		t.Fatalf("hmt lookup = %+v", info)
	}

	info, found = index.Lookup(netip.MustParseAddr("8.8.8.8"))
	if !found || info.Country != "US" || info.Subdivision != "CA" {
		t.Fatalf("overseas lookup = %+v", info)
	}

	if _, found = index.Lookup(netip.MustParseAddr("9.9.9.9")); found {
		t.Fatal("uncovered address must not resolve")
	}

	info, found = index.Lookup(netip.MustParseAddr("2400::1"))
	if !found || info.AdminCode != "440100" {
		t.Fatalf("v6 lookup = %+v", info)
	}
	// IPv4-mapped input resolves through the v4 table.
	info, found = index.Lookup(netip.MustParseAddr("::ffff:8.8.8.8"))
	if !found || info.Country != "US" {
		t.Fatalf("v4-mapped lookup = %+v", info)
	}
}

func TestFlowGeoBundleRejectsTampering(t *testing.T) {
	dir := writeGeoFixtureBundle(t, nil)
	path := filepath.Join(dir, "operators.json")
	data, _ := os.ReadFile(path)
	if err := os.WriteFile(path, append(data, ' '), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFlowGeoBundle(dir); err == nil {
		t.Fatal("checksum mismatch must fail the load")
	}
}

func TestFlowGeoBundleRejectsUnsortedRows(t *testing.T) {
	dir := writeGeoFixtureBundle(t, func(manifest *FlowGeoManifest, dir string) {
		csv := "ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn\n" +
			"124.160.0.0,124.160.255.255,CN,330100,,,1,4134\n" +
			"8.8.8.0,8.8.8.255,US,,CA,,0,15169\n"
		writeGeoFixtureFile(t, dir, "ipv4.csv.zst", compressCSV(t, csv), manifest, 2)
	})
	if _, err := LoadFlowGeoBundle(dir); err == nil {
		t.Fatal("unsorted rows must fail the load")
	}
}

func TestFlowGeoBundleRejectsRowCountMismatch(t *testing.T) {
	dir := writeGeoFixtureBundle(t, func(manifest *FlowGeoManifest, _ string) {
		ref := manifest.Files["ipv4.csv.zst"]
		ref.Rows++
		manifest.Files["ipv4.csv.zst"] = ref
	})
	if _, err := LoadFlowGeoBundle(dir); err == nil {
		t.Fatal("row count mismatch must fail the load")
	}
}

func TestFlowGeoServiceKeepsServingAfterFailedReload(t *testing.T) {
	dir := writeGeoFixtureBundle(t, nil)
	service := NewFlowGeoService(dir)
	if err := service.Reload(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if err := service.Reload(); err == nil {
		t.Fatal("reload without manifest must fail")
	}
	if service.Index() == nil {
		t.Fatal("failed reload must keep the previous index serving")
	}
	status := service.Status()
	if !status.Loaded || status.LastError == "" {
		t.Fatalf("status = %+v", status)
	}
}

// TestFlowGeoLoadsExportedBundle validates a real exporter product end to end
// when WATCHDOG_TEST_FLOW_GEO_DIR points at a bundle directory.
func TestFlowGeoLoadsExportedBundle(t *testing.T) {
	dir := os.Getenv("WATCHDOG_TEST_FLOW_GEO_DIR")
	if dir == "" {
		t.Skip("WATCHDOG_TEST_FLOW_GEO_DIR is not set")
	}
	index, err := LoadFlowGeoBundle(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("bundle version=%s v4=%d v6=%d operators=%d", index.Version, index.RowsV4, index.RowsV6, len(index.operators))
	if index.RowsV4 == 0 {
		t.Fatal("bundle has no IPv4 rows")
	}
}
