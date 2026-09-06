package watchdog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/klauspost/compress/zstd"
)

// PLAT-04D: the hub's FlowGeoService is a thin adapter over the Flow
// data-plane's flowdimension.GeoCatalog. These tests exercise the adapter
// (reload/lookup/status, keep-serving-after-failed-reload) against a bundle
// built with flowdimension's exported flow-geo-v1 format; the loader's own
// validation is tested in the flowdimension package.

func writeFlowGeoBundle(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	compress := func(csv string) []byte {
		encoder, err := zstd.NewWriter(nil)
		if err != nil {
			t.Fatal(err)
		}
		return encoder.EncodeAll([]byte(csv), nil)
	}
	header := "ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn"
	ipv4 := header + "\n" +
		"1.96.0.0,1.96.255.255,CN,810000,,,1,4760\n" +
		"8.8.8.0,8.8.8.255,US,,CA,,0,15169\n"
	ipv6 := header + "\n" +
		"2400::,2400:ffff:ffff:ffff:ffff:ffff:ffff:ffff,CN,440000,,,1,4134\n"
	operators, _ := json.Marshal([]flowdimension.GeoOperator{
		{ID: 1, Name: "中国电信", ShortName: "电信", Category: "carrier", Enabled: true},
	})
	dictionary, _ := json.Marshal([]flowdimension.GeoDictionaryEntry{
		{Kind: "province", Code: "810000", Name: "香港", Enabled: true},
		{Kind: "province", Code: "440000", Name: "广东", Enabled: true},
	})

	files := map[string]struct {
		data []byte
		rows uint64
	}{
		"ipv4.csv.zst":   {compress(ipv4), 2},
		"ipv6.csv.zst":   {compress(ipv6), 1},
		"operators.json": {operators, 1},
		"geo_dict.json":  {dictionary, 2},
	}
	manifest := map[string]any{
		"schema":            flowdimension.GeoSchemaV1,
		"version":           "test-1",
		"generated_at":      time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC),
		"effective_from":    time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC),
		"admin_code_system": flowdimension.GeoAdminCodeSystem,
		"unknown_country":   flowdimension.GeoUnknownCountry,
	}
	specs := map[string]flowdimension.GeoFileSpec{}
	for name, f := range files {
		if err := os.WriteFile(filepath.Join(dir, name), f.data, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(f.data)
		specs[name] = flowdimension.GeoFileSpec{SHA256: hex.EncodeToString(digest[:]), Rows: f.rows}
	}
	manifest["files"] = specs
	raw, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestFlowGeoServiceReloadLookupStatus(t *testing.T) {
	dir := writeFlowGeoBundle(t)
	service := NewFlowGeoService(dir)

	// Before a reload nothing is loaded.
	if service.Status().Loaded {
		t.Fatal("service must start unloaded")
	}
	if _, found := service.Lookup(netip.MustParseAddr("1.96.0.1")); found {
		t.Fatal("lookup before reload must not find anything")
	}

	if err := service.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	status := service.Status()
	if !status.Loaded || status.Version != "test-1" || status.RowsV4 != 2 || status.RowsV6 != 1 {
		t.Fatalf("status = %+v", status)
	}

	info, found := service.Lookup(netip.MustParseAddr("1.96.0.1"))
	if !found || info.Country != "CN" || info.AdminCode != "810000" {
		t.Fatalf("lookup = %+v found=%v", info, found)
	}
	label, found := service.Label("test-1", "810000")
	if !found || label.Name != "香港" || len(label.Breadcrumb) != 1 || label.Breadcrumb[0] != "香港" {
		t.Fatalf("label = %+v found=%v", label, found)
	}
	if _, found := service.Label("missing-version", "810000"); found {
		t.Fatal("label must not fall back across dictionary versions")
	}
	if _, found := service.Lookup(netip.MustParseAddr("203.0.113.1")); found {
		t.Fatal("uncovered address must not resolve")
	}
}

func TestFlowGeoServiceKeepsServingAfterFailedReload(t *testing.T) {
	dir := writeFlowGeoBundle(t)
	service := NewFlowGeoService(dir)
	if err := service.Reload(); err != nil {
		t.Fatal(err)
	}
	// Corrupt the bundle so the next reload fails.
	if err := os.Remove(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if err := service.Reload(); err == nil {
		t.Fatal("reload without manifest must fail")
	}
	// The previous index keeps serving and the error is surfaced.
	status := service.Status()
	if !status.Loaded || status.LastError == "" {
		t.Fatalf("status after failed reload = %+v", status)
	}
	if _, found := service.Lookup(netip.MustParseAddr("1.96.0.1")); !found {
		t.Fatal("failed reload must keep the previous index serving")
	}
}
